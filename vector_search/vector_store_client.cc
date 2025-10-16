/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "vector_store_client.hh"
#include "dns.hh"
#include "load_balancer.hh"
#include "db/config.hh"
#include "exceptions/exceptions.hh"
#include "utils/sequential_producer.hh"
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
using service_reply_format_error = vector_search::vector_store_client::service_reply_format_error;
using tcp_keepalive_params = net::tcp_keepalive_params;
using time_point = lowres_clock::time_point;

/// Timeout for waiting for a new client to be available
constexpr auto WAIT_FOR_CLIENT_TIMEOUT = std::chrono::seconds(5);

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

struct uri {
    host_name host;
    port_number port;
};

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


/// Wait for a condition variable to be signaled or timeout.
auto wait_for_signal(condition_variable& cv, time_point timeout) -> future<void> {
    auto result = co_await coroutine::as_future(cv.wait(timeout));
    if (result.failed()) {
        auto err = result.get_exception();
        if (try_catch<condition_variable_timed_out>(err) != nullptr) {
            co_return;
        }
        co_await coroutine::return_exception_ptr(std::move(err));
    }
    co_return;
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

auto write_ann_json(vs_vector vs_vector, limit limit) -> json_content {
    return seastar::format(R"({{"vector":[{}],"limit":{}}})", fmt::join(vs_vector, ","), limit);
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

class client_connection_factory : public http::experimental::connection_factory {
    socket_address _addr;

public:
    explicit client_connection_factory(socket_address addr)
        : _addr(addr) {
    }

    future<connected_socket> make([[maybe_unused]] abort_source* as) override {
        auto socket = co_await seastar::connect(_addr, {}, transport::TCP);
        socket.set_nodelay(true);
        socket.set_keepalive_parameters(tcp_keepalive_params{
                .idle = 60s,
                .interval = 60s,
                .count = 10,
        });
        socket.set_keepalive(true);
        co_return socket;
    }
};

class http_client {

    uri _uri;
    inet_address _addr;

    http::experimental::client impl;

public:
    http_client(uri host_port_, inet_address addr)
        : _uri(std::move(host_port_))
        , _addr(std::move(addr))
        , impl(std::make_unique<client_connection_factory>(socket_address(addr, _uri.port))) {
    }

    bool connects_to(inet_address const& a, port_number p) const {
        return _addr == a && _uri.port == p;
    }

    seastar::future<> make_request(operation_type method, const http_path& path, const std::optional<json_content>& content,
            http::experimental::client::reply_handler&& handle, abort_source* as) {
        auto req = http::request::make(method, _uri.host, path);
        if (content) {
            req.write_body("json", *content);
        }
        return impl.make_request(std::move(req), std::move(handle), std::nullopt, as);
    }

    seastar::future<> close() {
        return impl.close();
    }

    const inet_address& addr() const {
        return _addr;
    }
};

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

sstring response_content_to_sstring(const std::vector<temporary_buffer<char>>& buffers) {
    sstring result;
    for (const auto& buf : buffers) {
        result.append(buf.get(), buf.size());
    }
    return result;
}

std::vector<sstring> get_hosts(const std::vector<uri>& uris) {
    std::vector<sstring> ret;
    for (const auto& uri : uris) {
        ret.push_back(uri.host);
    }
    return ret;
}

} // namespace

namespace vector_search {

struct vector_store_client::impl {

    using clients_type = std::vector<lw_shared_ptr<http_client>>;

    utils::observer<sstring> uri_observer;
    clients_type current_clients;
    clients_type old_clients;
    std::vector<uri> _uris;
    gate client_producer_gate;
    condition_variable refresh_client_cv;
    milliseconds wait_for_client_timeout = WAIT_FOR_CLIENT_TIMEOUT;
    sequential_producer<clients_type> clients_producer;
    dns dns;
    uint64_t dns_refreshes = 0;
    seastar::metrics::metric_groups _metrics;


    impl(utils::config_file::named_value<sstring> cfg)
        : uri_observer(cfg.observe([this](seastar::sstring uris_csv) {
            try {
                handle_uris_changed(parse_uris(uris_csv));
            } catch (const configuration_exception& e) {
                vslogger.error("Failed to parse Vector Store service URI: {}", e.what());
                handle_uris_changed({});
            }
        }))
        , _uris(parse_uris(cfg()))
        , clients_producer([&]() -> future<clients_type> {
            return try_with_gate(client_producer_gate, [this] -> future<clients_type> {
                dns.trigger_refresh();
                co_await wait_for_signal(refresh_client_cv, lowres_clock::now() + wait_for_client_timeout);
                co_return current_clients;
            });
        })
        , dns(vslogger, get_hosts(_uris), [this](auto const& addrs) -> future<> {
            co_await handle_addresses_changed(addrs);
        }, dns_refreshes) {
        _metrics.add_group("vector_store", {seastar::metrics::make_gauge("dns_refreshes", seastar::metrics::description("Number of DNS refreshes"), [this] {
            return dns_refreshes;
        }).aggregate({seastar::metrics::shard_label})});
    }

    void handle_uris_changed(std::vector<uri> uris) {
        clear_current_clients();
        _uris = std::move(uris);
        dns.hosts(get_hosts(_uris));
    }

    auto handle_addresses_changed(const dns::host_address_map& addrs) -> future<> {
        clear_current_clients();
        for (const auto& uri : _uris) {
            auto it = addrs.find(uri.host);
            if (it != addrs.end()) {
                for (const auto& addr : it->second) {
                    current_clients.push_back(make_lw_shared<http_client>(uri, addr));
                }
            }
        }

        refresh_client_cv.broadcast();
        co_await cleanup_old_clients();
    }

    auto is_disabled() const -> bool {
        return _uris.empty();
    }

    void clear_current_clients() {
        old_clients.insert(old_clients.end(), std::make_move_iterator(current_clients.begin()), std::make_move_iterator(current_clients.end()));
        current_clients.clear();
    }

    /// Cleanup current clients
    auto cleanup_current_clients() -> future<> {
        for (auto& client : current_clients) {
            co_await client->close();
        }
        current_clients.clear();
    }

    /// Cleanup old clients that are no longer used.
    auto cleanup_old_clients() -> future<> {
        // iterate over old clients and close them. There is a co_await in the loop
        // so we need to use [] accessor and copying clients to avoid dangling references of iterators.
        // NOLINTNEXTLINE(modernize-loop-convert)
        for (auto it = 0U; it < old_clients.size(); ++it) {
            auto& client = old_clients[it];
            if (client && client.owned()) {
                auto client_cloned = client;
                co_await client_cloned->close();
                client_cloned = nullptr;
            }
        }
        std::erase_if(old_clients, [](auto const& client) {
            return !client;
        });
    }

    using get_client_error = std::variant<aborted, addr_unavailable, disabled>;

    /// Get the current http client or wait for a new one to be available.
    auto get_clients(abort_source& as) -> future<std::expected<clients_type, get_client_error>> {
        if (is_disabled()) {
            co_return std::unexpected{disabled{}};
        }
        if (!current_clients.empty()) {
            co_return current_clients;
        }

        auto current_clients = co_await coroutine::as_future(clients_producer(as));

        if (current_clients.failed()) {
            auto err = current_clients.get_exception();
            if (as.abort_requested()) {
                co_return std::unexpected{aborted{}};
            }
            co_await coroutine::return_exception_ptr(std::move(err));
        }
        auto clients = co_await std::move(current_clients);
        if (clients.empty()) {
            co_return std::unexpected{addr_unavailable{}};
        }
        co_return clients;
    }

    struct make_request_response {
        http::reply::status_type status;             ///< The HTTP status of the response.
        std::vector<temporary_buffer<char>> content; ///< The content of the response.
    };

    using make_request_error = std::variant<aborted, addr_unavailable, service_unavailable, disabled>;

    auto make_request(operation_type method, http_path path, std::optional<json_content> content, abort_source& as)
            -> future<std::expected<make_request_response, make_request_error>> {
        auto resp = make_request_response{.status = http::reply::status_type::ok, .content = std::vector<temporary_buffer<char>>()};

        for (auto retries = 0; retries < ANN_RETRIES; ++retries) {
            auto clients = co_await get_clients(as);
            if (!clients) {
                co_return std::unexpected{std::visit(
                        [](auto&& err) {
                            return make_request_error{err};
                        },
                        clients.error())};
            }

            load_balancer lb(std::move(*clients), random_engine);
            while (auto client = lb.next()) {
                auto result = co_await coroutine::as_future(client->make_request(
                        method, path, content,
                        [&resp](http::reply const& reply, input_stream<char> body) -> future<> {
                            resp.status = reply._status;
                            resp.content = co_await util::read_entire_stream(body);
                        },
                        &as));
                if (result.failed()) {
                    auto err = result.get_exception();
                    if (as.abort_requested()) {
                        co_return std::unexpected{aborted{}};
                    }
                    if (try_catch<std::system_error>(err) == nullptr) {
                        co_await coroutine::return_exception_ptr(std::move(err));
                    }
                    // std::system_error means that the server is unavailable, so we retry
                } else {
                    co_return resp;
                }
            }

            dns.trigger_refresh();
        }

        co_return std::unexpected{service_unavailable{}};
    }

    auto ann(keyspace_name keyspace, index_name name, schema_ptr schema, vs_vector vs_vector, limit limit, abort_source& as)
            -> future<std::expected<primary_keys, ann_error>> {
        if (is_disabled()) {
            vslogger.error("Disabled Vector Store while calling ann");
            co_return std::unexpected{disabled{}};
        }

        auto path = format("/api/v1/indexes/{}/{}/ann", keyspace, name);
        auto content = write_ann_json(std::move(vs_vector), limit);

        auto resp = co_await make_request(operation_type::POST, std::move(path), std::move(content), as);
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
    : _impl(std::make_unique<impl>(cfg.vector_store_primary_uri)) {
}

vector_store_client::~vector_store_client() = default;

void vector_store_client::start_background_tasks() {
    _impl->dns.start_background_tasks();
}

auto vector_store_client::stop() -> future<> {
    _impl->refresh_client_cv.signal();
    co_await _impl->client_producer_gate.close();
    co_await _impl->dns.stop();
    co_await _impl->cleanup_old_clients();
    co_await _impl->cleanup_current_clients();
}

auto vector_store_client::is_disabled() const -> bool {
    return _impl->is_disabled();
}

auto vector_store_client::ann(keyspace_name keyspace, index_name name, schema_ptr schema, vs_vector vs_vector, limit limit, abort_source& as)
        -> future<std::expected<primary_keys, ann_error>> {
    return _impl->ann(keyspace, name, schema, vs_vector, limit, as);
}

void vector_store_client_tester::set_dns_refresh_interval(vector_store_client& vsc, std::chrono::milliseconds interval) {
    vsc._impl->dns.refresh_interval(interval);
}

void vector_store_client_tester::set_wait_for_client_timeout(vector_store_client& vsc, std::chrono::milliseconds timeout) {
    vsc._impl->wait_for_client_timeout = timeout;
}

void vector_store_client_tester::set_dns_resolver(vector_store_client& vsc, std::function<future<std::vector<inet_address>>(sstring const&)> resolver) {
    vsc._impl->dns.resolver(std::move(resolver));
}

void vector_store_client_tester::trigger_dns_resolver(vector_store_client& vsc) {
    vsc._impl->dns.trigger_refresh();
}

auto vector_store_client_tester::resolve_hostname(vector_store_client& vsc, abort_source& as) -> future<std::vector<inet_address>> {
    auto clients = co_await vsc._impl->get_clients(as);
    std::vector<inet_address> ret;
    if (!clients) {
        co_return ret;
    }
    for (auto const& c : *clients) {
        ret.push_back(c->addr());
    }
    co_return ret;
}

} // namespace vector_search
