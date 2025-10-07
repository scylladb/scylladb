/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "vector_store_client.hh"
#include "dns.hh"
#include "load_balancer.hh"
#include "client_manager.hh"
#include "seastar/core/abort_source.hh"
#include "util.hh"
#include "cql3/statements/select_statement.hh"
#include "cql3/type_json.hh"
#include "db/config.hh"
#include "exceptions/exceptions.hh"
#include "dht/i_partitioner.hh"
#include "keys/keys.hh"
#include "utils/rjson.hh"
#include "schema/schema.hh"
#include <charconv>
#include <exception>
#include <fmt/ranges.h>
#include <regex>
#include <random>
#include <seastar/core/sstring.hh>
#include <seastar/core/metrics.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/http/client.hh>
#include <seastar/http/request.hh>
#include <seastar/net/dns.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/short_streams.hh>

namespace {

using namespace std::chrono_literals;

using ann_error = vector_search::vector_store_client::ann_error;
using configuration_exception = exceptions::configuration_exception;
using duration = lowres_clock::duration;
using vs_vector = vector_search::vector_store_client::vs_vector;
using limit = vector_search::vector_store_client::limit;
using host_name = vector_search::vector_store_client::host_name;
using http_path = sstring;
using inet_address = seastar::net::inet_address;
using json_content = sstring;
using milliseconds = std::chrono::milliseconds;
using operation_type = httpd::operation_type;
using port_number = vector_search::vector_store_client::port_number;
using primary_key = vector_search::vector_store_client::primary_key;
using primary_keys = vector_search::vector_store_client::primary_keys;
using service_reply_format_error = vector_search::vector_store_client::service_reply_format_error;
using tcp_keepalive_params = net::tcp_keepalive_params;
using time_point = lowres_clock::time_point;
using uri = vector_search::client_manager::uri;

/// The number of times to retry an /ann request if all nodes fail with a system error.
constexpr auto ANN_RETRIES = 3;

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
logging::logger vslogger("vector_store_client");

static thread_local auto random_engine = std::default_random_engine(std::random_device{}());

auto parse_port(std::string const& port_txt) -> std::optional<port_number> {
    auto port = port_number{};
    auto [ptr, ec] = std::from_chars(&*port_txt.begin(), &*port_txt.end(), port);
    if (*ptr != '\0' || ec != std::errc{}) {
        return std::nullopt;
    }
    return port;
}

auto parse_service_uri(std::string_view uri_) -> std::optional<uri> {
    constexpr auto URI_REGEX = R"(^http:\/\/([a-z0-9._-]+):([0-9]+)$)";
    auto const uri_regex = std::regex(URI_REGEX);
    auto uri_match = std::smatch{};
    auto uri_txt = std::string(uri_);

    if (!std::regex_match(uri_txt, uri_match, uri_regex) || uri_match.size() != 3) {
        return {};
    }
    auto host = uri_match[1].str();
    auto port = parse_port(uri_match[2].str());
    if (!port) {
        return {};
    }
    return {{host, *port}};
}

auto get_key_column_value(const rjson::value& item, std::size_t idx, const column_definition& column) -> std::expected<bytes, ann_error> {
    auto const& column_name = column.name_as_text();
    auto const* keys_obj = rjson::find(item, column_name);
    if (keys_obj == nullptr) {
        vslogger.error("Vector Store returned invalid JSON: missing key column '{}'", column_name);
        return std::unexpected{service_reply_format_error{}};
    }
    if (!keys_obj->IsArray()) {
        vslogger.error("Vector Store returned invalid JSON: key column '{}' is not an array", column_name);
        return std::unexpected{service_reply_format_error{}};
    }
    auto const& keys_arr = keys_obj->GetArray();
    if (keys_arr.Size() <= idx) {
        vslogger.error("Vector Store returned invalid JSON: key column '{}' array too small", column_name);
        return std::unexpected{service_reply_format_error{}};
    }
    auto const& key = keys_arr[idx];
    return from_json_object(*column.type, key);
}

auto pk_from_json(rjson::value const& item, std::size_t idx, schema_ptr const& schema) -> std::expected<partition_key, ann_error> {
    std::vector<bytes> raw_pk;
    for (const column_definition& cdef : schema->partition_key_columns()) {
        auto raw_value = get_key_column_value(item, idx, cdef);
        if (!raw_value) {
            return std::unexpected{raw_value.error()};
        }
        raw_pk.emplace_back(*raw_value);
    }
    return partition_key::from_exploded(raw_pk);
}

auto ck_from_json(rjson::value const& item, std::size_t idx, schema_ptr const& schema) -> std::expected<clustering_key_prefix, ann_error> {
    if (schema->clustering_key_size() == 0) {
        return clustering_key_prefix::make_empty();
    }

    std::vector<bytes> raw_ck;
    for (const column_definition& cdef : schema->clustering_key_columns()) {
        auto raw_value = get_key_column_value(item, idx, cdef);
        if (!raw_value) {
            return std::unexpected{raw_value.error()};
        }
        raw_ck.emplace_back(*raw_value);
    }

    return clustering_key_prefix::from_exploded(raw_ck);
}

auto read_ann_json(rjson::value const& json, schema_ptr const& schema) -> std::expected<primary_keys, ann_error> {
    if (!json.HasMember("primary_keys")) {
        vslogger.error("Vector Store returned invalid JSON: missing 'primary_keys'");
        return std::unexpected{service_reply_format_error{}};
    }
    auto const& keys_json = json["primary_keys"];
    if (!keys_json.IsObject()) {
        vslogger.error("Vector Store returned invalid JSON: 'primary_keys' is not an object");
        return std::unexpected{service_reply_format_error{}};
    }

    if (!json.HasMember("distances")) {
        vslogger.error("Vector Store returned invalid JSON: missing 'distances'");
        return std::unexpected{service_reply_format_error{}};
    }
    auto const& distances_json = json["distances"];
    if (!distances_json.IsArray()) {
        vslogger.error("Vector Store returned invalid JSON: 'distances' is not an array");
        return std::unexpected{service_reply_format_error{}};
    }
    auto const& distances_arr = json["distances"].GetArray();

    auto size = distances_arr.Size();
    auto keys = primary_keys{};
    for (auto idx = 0U; idx < size; ++idx) {
        auto pk = pk_from_json(keys_json, idx, schema);
        if (!pk) {
            return std::unexpected{pk.error()};
        }
        auto ck = ck_from_json(keys_json, idx, schema);
        if (!ck) {
            return std::unexpected{ck.error()};
        }
        keys.push_back(primary_key{dht::decorate_key(*schema, *pk), *ck});
    }
    return std::move(keys);
}

bool should_vector_store_service_be_disabled(std::vector<sstring> const& uris) {
    return uris.empty() || uris[0].empty();
}

auto parse_uris(std::string_view uris_csv) -> std::vector<uri> {
    std::vector<uri> ret;
    auto uris = utils::split_comma_separated_list(uris_csv);
    if (should_vector_store_service_be_disabled(uris)) {
        vslogger.info("Vector Store service URIs are empty, disabling Vector Store service");
        return ret;
    }

    for (const auto& uri : uris) {
        auto parsed = parse_service_uri(uri);
        if (!parsed) {
            throw configuration_exception(fmt::format("Invalid Vector Store service URI: {}", uri));
        }
        ret.push_back(*parsed);
    }

    vslogger.info("Vector Store service URIs set to: '{}'", uris_csv);
    return ret;
}

} // namespace

namespace vector_search {

struct vector_store_client::impl {

    using clients_type = std::vector<lw_shared_ptr<node>>;

    config const& _cfg;
    utils::observer<sstring> primary_uri_observer;
    utils::observer<sstring> secondary_uri_observer;
    uint64_t dns_refreshes = 0;
    seastar::metrics::metric_groups _metrics;
    client_manager _clients_manager;

    impl(config const& cfg)
        : _cfg(cfg)
        , primary_uri_observer(cfg.vector_store_primary_uri.observe([this](auto) {
            handle_uris_changed();
        }))
        , secondary_uri_observer(cfg.vector_store_secondary_uri.observe([this](auto) {
            handle_uris_changed();
        }))
        , _clients_manager(parse_uris(cfg.vector_store_primary_uri()), parse_uris(cfg.vector_store_secondary_uri()), vslogger, dns_refreshes) {
        _metrics.add_group("vector_store", {seastar::metrics::make_gauge("dns_refreshes", seastar::metrics::description("Number of DNS refreshes"), [this] {
            return dns_refreshes;
        }).aggregate({seastar::metrics::shard_label})});
    }

    void handle_uris_changed() {
        auto parse_and_update_uris = [](sstring uris_csv, const char* type) {
            try {
                return parse_uris(std::move(uris_csv));
            } catch (const configuration_exception& e) {
                vslogger.error("Failed to parse Vector Store {} service URI: {}", type, e.what());
                return std::vector<uri>{};
            }
        };

        _clients_manager.clear();
        _clients_manager.update_uris(
                parse_and_update_uris(_cfg.vector_store_primary_uri(), "primary"), parse_and_update_uris(_cfg.vector_store_secondary_uri(), "secondary"));
    }

    auto is_disabled() const -> bool {
        return _clients_manager.empty();
    }

    using make_request_error = std::variant<addr_unavailable, service_unavailable>;
    using clients_factory = std::function<seastar::future<clients_type>(seastar::abort_source& as)>;

    auto make_request(const clients_factory& get_clients, const sstring& keyspace, const sstring& name, const std::vector<float>& embedding, std::size_t limit,
            abort_source& as) -> future<std::expected<client::response, make_request_error>> {
        for (auto retries = 0; retries < ANN_RETRIES; ++retries) {
            auto clients = co_await get_clients(as);
            if (clients.empty()) {
                co_return std::unexpected{addr_unavailable{}};
            }

            load_balancer lb(std::move(clients), random_engine);
            while (auto client = lb.next()) {
                if (client->is_up()) {
                    auto result = co_await coroutine::as_future(client->ann(keyspace, name, embedding, limit, as));
                    if (result.failed()) {
                        auto err = result.get_exception();
                        if (try_catch<std::system_error>(err) == nullptr) {
                            co_await coroutine::return_exception_ptr(std::move(err));
                        }
                        // std::system_error means that the server is unavailable, so we retry
                    } else {
                        co_return co_await std::move(result);
                    }
                }
            }

            _clients_manager.trigger_refresh();
        }
        co_return std::unexpected{service_unavailable{}};
    }

    auto make_request(sstring keyspace, sstring name, std::vector<float> embedding, std::size_t limit, abort_source& as)
            -> future<std::expected<client::response, make_request_error>> {
        auto get_primary = [this](auto& as) -> future<clients_type> {
            return _clients_manager.get_primary_clients(as);
        };
        auto get_secondary = [this](auto& as) -> future<clients_type> {
            return _clients_manager.get_secondary_clients(as);
        };

        if (_clients_manager.has_primay()) {
            auto result = co_await make_request(get_primary, keyspace, name, embedding, limit, as);
            if (result || !_clients_manager.has_secondary()) {
                co_return result;
            }
        }

        if (_clients_manager.has_secondary()) {
            co_return co_await make_request(get_secondary, keyspace, name, embedding, limit, as);
        }

        co_return std::unexpected{service_unavailable{}};
    }

    auto ann(keyspace_name keyspace, index_name name, schema_ptr schema, vs_vector vs_vector, limit limit, abort_source& as)
            -> future<std::expected<primary_keys, ann_error>> {
        if (is_disabled()) {
            vslogger.error("Disabled Vector Store while calling ann");
            co_return std::unexpected{disabled{}};
        }

        auto f = co_await coroutine::as_future(make_request(std::move(keyspace), std::move(name), std::move(vs_vector), limit, as));
        if (f.failed()) {
            auto err = f.get_exception();
            if (try_catch<abort_requested_exception>(err)) {
                co_return std::unexpected{aborted{}};
            }
            co_await coroutine::return_exception_ptr(std::move(err));
        }

        auto resp = co_await std::move(f);
        if (!resp) {
            co_return std::unexpected{std::visit(
                    [](auto&& err) {
                        return ann_error{err};
                    },
                    resp.error())};
        }

        if (resp->status != status_type::ok) {
            vslogger.error("Vector Store returned error: HTTP status {}: {}", resp->status, seastar::value_of([&resp] {
                return response_content_to_sstring(resp->content);
            }));
            co_return std::unexpected{service_error{resp->status}};
        }

        try {
            co_return read_ann_json(rjson::parse(std::move(resp->content)), schema);
        } catch (const rjson::error& e) {
            vslogger.error("Vector Store returned invalid JSON: {}", e.what());
            co_return std::unexpected{service_reply_format_error{}};
        }
    }
};

vector_store_client::vector_store_client(config const& cfg)
    : _impl(std::make_unique<impl>(cfg)) {
}

vector_store_client::~vector_store_client() = default;

void vector_store_client::start_background_tasks() {
    _impl->_clients_manager.start_background_tasks();
}

auto vector_store_client::stop() -> future<> {
    return _impl->_clients_manager.stop();
}

auto vector_store_client::is_disabled() const -> bool {
    return _impl->is_disabled();
}

auto vector_store_client::ann(keyspace_name keyspace, index_name name, schema_ptr schema, vs_vector vs_vector, limit limit, abort_source& as)
        -> future<std::expected<primary_keys, ann_error>> {
    return _impl->ann(keyspace, name, schema, vs_vector, limit, as);
}

void vector_store_client_tester::set_dns_refresh_interval(vector_store_client& vsc, std::chrono::milliseconds interval) {
    vsc._impl->_clients_manager.get_dns().refresh_interval(interval);
}

void vector_store_client_tester::set_wait_for_client_timeout(vector_store_client& vsc, std::chrono::milliseconds timeout) {
    vsc._impl->_clients_manager.timeout(timeout);
}

void vector_store_client_tester::set_dns_resolver(vector_store_client& vsc, std::function<future<std::vector<inet_address>>(sstring const&)> resolver) {
    vsc._impl->_clients_manager.get_dns().resolver(std::move(resolver));
}

void vector_store_client_tester::trigger_dns_resolver(vector_store_client& vsc) {
    vsc._impl->_clients_manager.get_dns().trigger_refresh();
}

auto vector_store_client_tester::resolve_hostname(vector_store_client& vsc, abort_source& as) -> future<std::vector<inet_address>> {
    auto clients = co_await vsc._impl->_clients_manager.get_primary_clients(as);
    std::vector<inet_address> ret;
    for (auto const& c : clients) {
        auto ip = c->endpoint().ip;
        ret.push_back(ip);
    }
    co_return ret;
}

} // namespace vector_search
