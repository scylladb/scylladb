/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "vector_store_client.hh"
#include "dns.hh"
#include "clients.hh"
#include "uri.hh"
#include "utils.hh"
#include "truststore.hh"
#include "db/config.hh"
#include "exceptions/exceptions.hh"
#include "dht/i_partitioner.hh"
#include "keys/keys.hh"
#include "utils/rjson.hh"
#include "types/json_utils.hh"
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
#include <seastar/util/lazy.hh>
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
using primary_key = vector_search::primary_key;
using primary_keys = vector_search::vector_store_client::primary_keys;
using ann_results = vector_search::vector_store_client::ann_results;
using service_reply_format_error = vector_search::vector_store_client::service_reply_format_error;
using uri = vector_search::uri;

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
    constexpr auto URI_REGEX = R"(^(http|https):\/\/([a-z0-9._-]+):([0-9]+)$)";
    auto const uri_regex = std::regex(URI_REGEX);
    auto uri_match = std::smatch{};
    auto uri_txt = std::string(uri_);

    if (!std::regex_match(uri_txt, uri_match, uri_regex) || uri_match.size() != 4) {
        return {};
    }
    auto schema = uri_match[1].str() == "https" ? uri::schema_type::https : uri::schema_type::http;
    auto host = uri_match[2].str();
    auto port = parse_port(uri_match[3].str());
    if (!port) {
        return {};
    }
    return {{schema, host, *port}};
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

auto distance_from_json(rjson::value const& item) -> std::expected<float, ann_error> {
    if (!item.IsNumber()) {
        vslogger.error("Vector Store returned invalid JSON: 'distances' array element is not a number");
        return std::unexpected{service_reply_format_error{}};
    }
    return item.GetFloat();
}

auto write_ann_json(vs_vector vs_vector, limit limit) -> json_content {
    return seastar::format(R"({{"vector":[{}],"limit":{}}})", fmt::join(vs_vector, ","), limit);
}

auto read_ann_json(rjson::value const& json, schema_ptr const& schema) -> std::expected<ann_results, ann_error> {
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
    auto const& distances_arr = distances_json.GetArray();

    auto size = distances_arr.Size();
    ann_results results;
    results.reserve(size);
    for (auto idx = 0U; idx < size; ++idx) {
        auto pk = pk_from_json(keys_json, idx, schema);
        if (!pk) {
            return std::unexpected{pk.error()};
        }
        auto ck = ck_from_json(keys_json, idx, schema);
        if (!ck) {
            return std::unexpected{ck.error()};
        }
        auto distance = distance_from_json(distances_arr[idx]);
        if (!distance) {
            return std::unexpected{distance.error()};
        }
        results.push_back({primary_key{dht::decorate_key(*schema, *pk), *ck}, *distance});
    }
    return results;
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

auto parse_uris_no_throw(std::string_view uris_csv) -> std::vector<uri> {
    try {
        return parse_uris(uris_csv);
    } catch (const configuration_exception& e) {
        vslogger.error("Failed to parse Vector Store service URIs [{}]: {}", uris_csv, e.what());
    }
    return {};
}

std::vector<sstring> get_hosts(const std::vector<uri>& primary_uris, const std::vector<uri>& secondary_uris) {
    std::vector<sstring> ret;
    for (const auto& uri : primary_uris) {
        ret.push_back(uri.host);
    }
    for (const auto& uri : secondary_uris) {
        ret.push_back(uri.host);
    }
    return ret;
}

} // namespace

namespace vector_search {

struct vector_store_client::impl {
    using invoke_on_others_func = std::function<future<>(std::function<future<>(impl&)>)>;

    utils::observer<sstring> _primary_uri_observer;
    utils::observer<sstring> _secondary_uri_observer;
    std::vector<uri> _primary_uris;
    std::vector<uri> _secondary_uris;
    dns dns;
    uint64_t dns_refreshes = 0;
    seastar::metrics::metric_groups _metrics;
    truststore _truststore;
    clients _primary_clients;
    clients _secondary_clients;

    impl(utils::config_file::named_value<sstring> primary_uris, utils::config_file::named_value<sstring> secondary_uris,
            utils::config_file::named_value<uint32_t> read_request_timeout_in_ms,
            utils::config_file::named_value<utils::config_file::string_map> encryption_options, invoke_on_others_func invoke_on_others)
        : _primary_uri_observer(primary_uris.observe([this](seastar::sstring uris_csv) {
            handle_uris_changed(std::move(uris_csv), _primary_uris, _primary_clients);
        }))
        , _secondary_uri_observer(secondary_uris.observe([this](seastar::sstring uris_csv) {
            handle_uris_changed(std::move(uris_csv), _secondary_uris, _secondary_clients);
        }))
        , _primary_uris(parse_uris(primary_uris()))
        , _secondary_uris(parse_uris(secondary_uris()))
        , dns(
                  vslogger, get_hosts(_primary_uris, _secondary_uris),
                  [this](auto const& addrs) -> future<> {
                      co_await handle_addresses_changed(addrs);
                  },
                  dns_refreshes)
        , _truststore(vslogger, encryption_options,
                  [invoke_on_others = std::move(invoke_on_others)](auto func) {
                      return invoke_on_others([func = std::move(func)](auto& self) {
                          return func(self._truststore);
                      });
                  })
        , _primary_clients(
                  vslogger,
                  [this]() {
                      dns.trigger_refresh();
                  },
                  read_request_timeout_in_ms, _truststore)

        , _secondary_clients(
                  vslogger,
                  [this]() {
                      dns.trigger_refresh();
                  },
                  read_request_timeout_in_ms, _truststore) {
        _metrics.add_group("vector_store", {seastar::metrics::make_gauge("dns_refreshes", seastar::metrics::description("Number of DNS refreshes"), [this] {
            return dns_refreshes;
        }).aggregate({seastar::metrics::shard_label})});
    }

    void handle_uris_changed(seastar::sstring uris_csv, std::vector<uri>& uris, clients& clients) {
        clients.clear();
        uris = parse_uris_no_throw(uris_csv);
        dns.hosts(get_hosts(_primary_uris, _secondary_uris));
    }

    future<> handle_addresses_changed(const dns::host_address_map& addrs) {
        co_await _primary_clients.handle_changed(_primary_uris, addrs);
        co_await _secondary_clients.handle_changed(_secondary_uris, addrs);
    }

    auto is_disabled() const -> bool {
        return _primary_uris.empty() && _secondary_uris.empty();
    }

    auto ann(keyspace_name keyspace, index_name name, schema_ptr schema, vs_vector vs_vector, limit limit, abort_source& as)
            -> future<std::expected<ann_results, ann_error>> {
        if (is_disabled()) {
            vslogger.error("Disabled Vector Store while calling ann");
            co_return std::unexpected{disabled{}};
        }

        auto path = format("/api/v1/indexes/{}/{}/ann", keyspace, name);
        auto content = write_ann_json(std::move(vs_vector), limit);

        auto resp = co_await request(operation_type::POST, std::move(path), std::move(content), as);
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

    future<clients::request_result> request(
            seastar::httpd::operation_type method, seastar::sstring path, std::optional<seastar::sstring> content, seastar::abort_source& as) {
        auto success_or_aborted = [](const auto& result) {
            return result || std::holds_alternative<vector_store_client::aborted>(result.error());
        };
        if (!_primary_uris.empty()) {
            auto result = co_await _primary_clients.request(method, path, content, as);
            if (success_or_aborted(result) || _secondary_uris.empty()) {
                co_return result;
            }
        }

        if (!_secondary_uris.empty()) {
            co_return co_await _secondary_clients.request(method, path, content, as);
        }

        co_return std::unexpected{service_unavailable{}};
    }
};

vector_store_client::vector_store_client(config const& cfg)
    : _impl(std::make_unique<impl>(cfg.vector_store_primary_uri, cfg.vector_store_secondary_uri, cfg.read_request_timeout_in_ms,
              cfg.vector_store_encryption_options, [this](auto func) {
                  return container().invoke_on_others([func = std::move(func)](auto& self) {
                      return func(*self._impl);
                  });
              })) {
}

vector_store_client::~vector_store_client() = default;

void vector_store_client::start_background_tasks() {
    _impl->dns.start_background_tasks();
}

auto vector_store_client::stop() -> future<> {
    co_await _impl->_primary_clients.stop();
    co_await _impl->_secondary_clients.stop();
    co_await _impl->dns.stop();
    co_await _impl->_truststore.stop();
}

auto vector_store_client::is_disabled() const -> bool {
    return _impl->is_disabled();
}

auto vector_store_client::ann(keyspace_name keyspace, index_name name, schema_ptr schema, vs_vector vs_vector, limit limit, abort_source& as)
        -> future<std::expected<ann_results, ann_error>> {
    return _impl->ann(keyspace, name, schema, vs_vector, limit, as);
}

void vector_store_client_tester::set_dns_refresh_interval(vector_store_client& vsc, std::chrono::milliseconds interval) {
    vsc._impl->dns.refresh_interval(interval);
}

void vector_store_client_tester::set_wait_for_client_timeout(vector_store_client& vsc, std::chrono::milliseconds timeout) {
    vsc._impl->_primary_clients.timeout(timeout);
    vsc._impl->_secondary_clients.timeout(timeout);
}

void vector_store_client_tester::set_dns_resolver(vector_store_client& vsc, std::function<future<std::vector<inet_address>>(sstring const&)> resolver) {
    vsc._impl->dns.resolver(std::move(resolver));
}

void vector_store_client_tester::trigger_dns_resolver(vector_store_client& vsc) {
    vsc._impl->dns.trigger_refresh();
}

auto vector_store_client_tester::resolve_hostname(vector_store_client& vsc, abort_source& as) -> future<std::vector<inet_address>> {
    auto clients = co_await vsc._impl->_primary_clients.get_clients(as);
    std::vector<inet_address> ret;
    if (!clients) {
        co_return ret;
    }
    for (auto const& c : *clients) {
        ret.push_back(c->endpoint().ip);
    }
    co_return ret;
}

} // namespace vector_search
