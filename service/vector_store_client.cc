/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "vector_store_client.hh"
#include "cql3/statements/select_statement.hh"
#include "cql3/type_json.hh"
#include "db/config.hh"
#include "exceptions/exceptions.hh"
#include "utils/sequential_producer.hh"
#include "dht/i_partitioner.hh"
#include "keys/keys.hh"
#include "utils/rjson.hh"
#include "schema/schema.hh"
#include <charconv>
#include <exception>
#include <fmt/ranges.h>
#include <regex>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/http/client.hh>
#include <seastar/http/request.hh>
#include <seastar/net/dns.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/short_streams.hh>

namespace {

using ann_error = service::vector_store_client::ann_error;
using configuration_exception = exceptions::configuration_exception;
using duration = lowres_clock::duration;
using embedding = service::vector_store_client::embedding;
using limit = service::vector_store_client::limit;
using host_name = service::vector_store_client::host_name;
using http_path = sstring;
using inet_address = seastar::net::inet_address;
using json_content = sstring;
using milliseconds = std::chrono::milliseconds;
using operation_type = httpd::operation_type;
using port_number = service::vector_store_client::port_number;
using primary_key = service::vector_store_client::primary_key;
using primary_keys = service::vector_store_client::primary_keys;
using service_reply_format_error = service::vector_store_client::service_reply_format_error;
using time_point = lowres_clock::time_point;

// Wait time before retrying after an exception occurred
constexpr auto EXCEPTION_OCCURED_WAIT = std::chrono::seconds(5);

// Minimum interval between dns name refreshes
constexpr auto DNS_REFRESH_INTERVAL = std::chrono::seconds(5);

/// Timeout for waiting for a new client to be available
constexpr auto WAIT_FOR_CLIENT_TIMEOUT = std::chrono::seconds(5);

/// How many retries to do for HTTP requests
constexpr auto HTTP_REQUEST_RETRIES = 3;

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
logging::logger vslogger("vector_store_client");

auto parse_port(std::string const& port_txt) -> std::optional<port_number> {
    auto port = port_number{};
    auto [ptr, ec] = std::from_chars(&*port_txt.begin(), &*port_txt.end(), port);
    if (*ptr != '\0' || ec != std::errc{}) {
        return std::nullopt;
    }
    return port;
}

struct host_port {
    host_name host;
    port_number port;
};

auto parse_service_uri(std::string_view uri) -> std::optional<host_port> {
    constexpr auto URI_REGEX = R"(^http:\/\/([a-z0-9._-]+):([0-9]+)$)";
    auto const uri_regex = std::regex(URI_REGEX);
    auto uri_match = std::smatch{};
    auto uri_txt = std::string(uri);

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

/// Wait for a timeout ar abort signal.
auto wait_for_timeout(duration timeout, abort_source& as) -> future<bool> {
    auto result = co_await coroutine::as_future(sleep_abortable(timeout, as));
    if (result.failed()) {
        auto err = result.get_exception();
        if (as.abort_requested()) {
            co_return false;
        }
        co_await coroutine::return_exception_ptr(std::move(err));
    }
    co_return true;
}

/// Wait for a condition variable to be signaled or timeout.
auto wait_for_signal(condition_variable& cv, time_point timeout) -> future<bool> {
    auto result = co_await coroutine::as_future(cv.wait(timeout));
    if (result.failed()) {
        auto err = result.get_exception();
        if (try_catch<condition_variable_timed_out>(err) != nullptr) {
            co_return false;
        }
        co_await coroutine::return_exception_ptr(std::move(err));
    }
    co_return true;
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

auto write_ann_json(embedding embedding, limit limit) -> json_content {
    return seastar::format(R"({{"embedding":[{}],"limit":{}}})", fmt::join(embedding, ","), limit);
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

class http_client {

    host_port _host_port;
    inet_address _addr;

    http::experimental::client impl;

public:
    http_client(host_port host_port_, inet_address addr)
        : _host_port(std::move(host_port_))
        , _addr(std::move(addr))
        , impl(socket_address(addr, _host_port.port)) {
    }

    bool connects_to(inet_address const& a, port_number p) const {
        return _addr == a && _host_port.port == p;
    }

    seastar::future<> make_request(
            operation_type method, http_path path, std::optional<json_content> content, http::experimental::client::reply_handler&& handle, abort_source* as) {
        auto req = http::request::make(method, _host_port.host, std::move(path));
        if (content) {
            req.write_body("json", std::move(*content));
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

bool should_vector_store_service_be_disabled(std::string_view const& uri) {
    return uri.empty();
}

auto get_host_port(std::string_view uri) -> std::optional<host_port> {
    if (should_vector_store_service_be_disabled(uri)) {
        vslogger.info("Vector Store service URI is empty, disabling Vector Store service");
        return std::nullopt;
    }
    auto parsed = parse_service_uri(uri);
    if (!parsed) {
        throw configuration_exception(format("Invalid Vector Store service URI: {}", uri));
    }
    vslogger.info("Vector Store service URI is set to '{}'", uri);
    return *parsed;
}

} // namespace

namespace service {

struct vector_store_client::impl {

    utils::observer<sstring> uri_observer;
    lw_shared_ptr<http_client> current_client;
    std::vector<lw_shared_ptr<http_client>> old_clients;
    std::optional<host_port> _host_port;
    time_point last_dns_refresh;
    gate tasks_gate;
    condition_variable refresh_cv;
    condition_variable refresh_client_cv;
    abort_source abort_refresh;
    milliseconds dns_refresh_interval = DNS_REFRESH_INTERVAL;
    milliseconds wait_for_client_timeout = WAIT_FOR_CLIENT_TIMEOUT;
    unsigned http_request_retries = HTTP_REQUEST_RETRIES;
    std::function<future<std::optional<inet_address>>(sstring const&)> dns_resolver;
    sequential_producer<lw_shared_ptr<http_client>> client_producer;

    impl(utils::config_file::named_value<sstring> cfg)
        : uri_observer(cfg.observe([this](std::string_view uri) {
            try {
                _host_port = get_host_port(uri);
                trigger_dns_refresh();
            } catch (const configuration_exception& e) {
                vslogger.error("Failed to parse Vector Store service URI: {}", e.what());
                _host_port = std::nullopt;
            }
        }))
        , _host_port(get_host_port(cfg()))
        , dns_resolver([](auto const& host) -> future<std::optional<inet_address>> {
            auto addr = co_await coroutine::as_future(net::dns::resolve_name(host));
            if (addr.failed()) {
                auto err = addr.get_exception();
                if (try_catch<std::system_error>(err) != nullptr) {
                    co_return std::nullopt;
                }
                co_await coroutine::return_exception_ptr(std::move(err));
            }
            co_return co_await std::move(addr);
        })
        , client_producer([&]() -> future<lw_shared_ptr<http_client>> {
            trigger_dns_refresh();
            co_await wait_for_signal(refresh_client_cv, lowres_clock::now() + wait_for_client_timeout);
            co_return current_client;
        }) {
    }

    auto is_disabled() const -> bool {
        return !bool{_host_port};
    }

    auto host() const -> std::expected<host_name, disabled> {
        if (is_disabled()) {
            return std::unexpected{disabled{}};
        }
        return _host_port->host;
    }

    auto port() const -> std::expected<port_number, disabled> {
        if (is_disabled()) {
            return std::unexpected{disabled{}};
        }
        return _host_port->port;
    }

    /// Refresh the http client with a new address resolved from the DNS name.
    /// If the DNS resolution fails, the current client is set to nullptr.
    /// If the address is the same as the current one, do nothing.
    /// Old clients are saved for later cleanup in a specific task.
    auto refresh_addr() -> future<> {
        if (is_disabled()) {
            current_client = nullptr;
            co_return;
        }
        auto [host, port] = *_host_port;
        auto new_addr = co_await dns_resolver(host);
        if (!new_addr) {
            current_client = nullptr;
            co_return;
        }

        // Check if the new address and port is the same as the current one
        if (current_client && current_client->connects_to(*new_addr, port)) {
            co_return;
        }

        old_clients.emplace_back(current_client);
        current_client = make_lw_shared<http_client>(*_host_port, std::move(*new_addr));
    }

    /// A task for refreshing the vector store http client.
    auto refresh_addr_task() -> future<> {
        for (;;) {
            auto exception_occured = false;
            try {
                if (abort_refresh.abort_requested()) {
                    break;
                }

                // Do not refresh the service address too often
                auto now = lowres_clock::now();
                auto current_duration = now - last_dns_refresh;
                if (current_duration > dns_refresh_interval) {
                    last_dns_refresh = now;
                    co_await refresh_addr();
                } else {
                    // Wait till the end of the refreshing interval
                    if (co_await wait_for_timeout(dns_refresh_interval - current_duration, abort_refresh)) {
                        continue;
                    }
                    // If the wait was aborted, we stop refreshing
                    break;
                }

                if (abort_refresh.abort_requested()) {
                    break;
                }

                // new client is available
                refresh_client_cv.broadcast();

                co_await cleanup_old_clients();

                co_await refresh_cv.when();
            } catch (const std::exception& e) {
                vslogger.error("Vector Store Client refresh task failed: {}", e.what());
                exception_occured = true;
            } catch (...) {
                vslogger.error("Vector Store Client refresh task failed with unknown exception");
                exception_occured = true;
            }
            if (exception_occured) {
                // If an exception occurred, we wait for the next signal to refresh the address
                co_await wait_for_timeout(EXCEPTION_OCCURED_WAIT, abort_refresh);
            }
        }

        co_await cleanup_old_clients();
        co_await cleanup_current_client();
    }

    /// Request a DNS refresh in the specific task.
    void trigger_dns_refresh() {
        refresh_cv.signal();
    }

    /// Cleanup current client
    auto cleanup_current_client() -> future<> {
        if (current_client) {
            co_await current_client->close();
        }
        current_client = nullptr;
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
    auto get_client(abort_source& as) -> future<std::expected<lw_shared_ptr<http_client>, get_client_error>> {
        if (is_disabled()) {
            co_return std::unexpected{disabled{}};
        }
        if (current_client) {
            co_return current_client;
        }

        auto current_client = co_await coroutine::as_future(client_producer(as));

        if (current_client.failed()) {
            auto err = current_client.get_exception();
            if (as.abort_requested()) {
                co_return std::unexpected{aborted{}};
            }
            co_await coroutine::return_exception_ptr(std::move(err));
        }
        auto client = co_await std::move(current_client);
        if (!client) {
            co_return std::unexpected{addr_unavailable{}};
        }
        co_return client;
    }

    struct make_request_response {
        http::reply::status_type status;             ///< The HTTP status of the response.
        std::vector<temporary_buffer<char>> content; ///< The content of the response.
    };

    using make_request_error = std::variant<aborted, addr_unavailable, service_unavailable, disabled>;

    auto make_request(operation_type method, http_path path, std::optional<json_content> content, abort_source& as)
            -> future<std::expected<make_request_response, make_request_error>> {
        auto resp = make_request_response{.status = http::reply::status_type::ok, .content = std::vector<temporary_buffer<char>>()};

        for (auto retries = 0; retries < HTTP_REQUEST_RETRIES; ++retries) {
            auto client = co_await get_client(as);
            if (!client) {
                co_return std::unexpected{std::visit(
                        [](auto&& err) {
                            return make_request_error{err};
                        },
                        client.error())};
            }

            auto result = co_await coroutine::as_future(client.value()->make_request(
                    method, std::move(path), std::move(content),
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

            trigger_dns_refresh();
        }

        co_return std::unexpected{service_unavailable{}};
    }
};

vector_store_client::vector_store_client(config const& cfg)
    : _impl(std::make_unique<impl>(cfg.vector_store_uri)) {
}

vector_store_client::~vector_store_client() = default;

void vector_store_client::start_background_tasks() {
    /// start the background task to refresh the service address
    (void)try_with_gate(_impl->tasks_gate, [this] {
        return _impl->refresh_addr_task();
    }).handle_exception([](std::exception_ptr eptr) {
        on_internal_error_noexcept(vslogger, format("The Vector Store Client refresh task failed: {}", eptr));
    });
}

auto vector_store_client::stop() -> future<> {
    _impl->abort_refresh.request_abort();
    _impl->refresh_cv.signal();
    co_await _impl->tasks_gate.close();
}

auto vector_store_client::is_disabled() const -> bool {
    return _impl->is_disabled();
}

auto vector_store_client::host() const -> std::expected<host_name, disabled> {
    return _impl->host();
}

auto vector_store_client::port() const -> std::expected<port_number, disabled> {
    return _impl->port();
}

auto vector_store_client::ann(keyspace_name keyspace, index_name name, schema_ptr schema, embedding embedding, limit limit, abort_source& as)
        -> future<std::expected<primary_keys, ann_error>> {
    if (is_disabled()) {
        vslogger.error("Disabled Vector Store while calling ann");
        co_return std::unexpected{disabled{}};
    }

    auto path = format("/api/v1/indexes/{}/{}/ann", keyspace, name);
    auto content = write_ann_json(std::move(embedding), limit);

    auto resp = co_await _impl->make_request(operation_type::POST, std::move(path), std::move(content), as);
    if (!resp) {
        co_return std::unexpected{std::visit(
                [](auto&& err) {
                    return ann_error{err};
                },
                resp.error())};
    }

    if (resp->status != status_type::ok) {
        vslogger.error("Vector Store returned error: HTTP status {}: {}", resp->status, resp->content);
        co_return std::unexpected{service_error{resp->status}};
    }

    try {
        co_return read_ann_json(rjson::parse(std::move(resp->content)), schema);
    } catch (const rjson::error& e) {
        vslogger.error("Vector Store returned invalid JSON: {}", e.what());
        co_return std::unexpected{service_reply_format_error{}};
    }
}

void vector_store_client_tester::set_dns_refresh_interval(vector_store_client& vsc, std::chrono::milliseconds interval) {
    vsc._impl->dns_refresh_interval = interval;
}

void vector_store_client_tester::set_wait_for_client_timeout(vector_store_client& vsc, std::chrono::milliseconds timeout) {
    vsc._impl->wait_for_client_timeout = timeout;
}

void vector_store_client_tester::set_http_request_retries(vector_store_client& vsc, unsigned retries) {
    vsc._impl->http_request_retries = retries;
}

void vector_store_client_tester::set_dns_resolver(vector_store_client& vsc, std::function<future<std::optional<inet_address>>(sstring const&)> resolver) {
    vsc._impl->dns_resolver = std::move(resolver);
}

void vector_store_client_tester::trigger_dns_resolver(vector_store_client& vsc) {
    vsc._impl->trigger_dns_refresh();
}

auto vector_store_client_tester::resolve_hostname(vector_store_client& vsc, abort_source& as) -> future<std::optional<inet_address>> {
    auto client = co_await vsc._impl->get_client(as);
    if (!client) {
        co_return std::nullopt;
    }
    co_return client.value()->addr();
}

} // namespace service
