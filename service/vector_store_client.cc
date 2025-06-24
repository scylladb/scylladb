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
#include "dht/i_partitioner.hh"
#include "keys.hh"
#include "utils/rjson.hh"
#include "schema/schema.hh"
#include <charconv>
#include <fmt/ranges.h>
#include <regex>
#include <seastar/http/client.hh>
#include <seastar/http/request.hh>
#include <seastar/net/dns.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/short_streams.hh>

namespace {

using ann_error = service::vector_store_client::ann_error;
using configuration_exception = exceptions::configuration_exception;
using embedding = service::vector_store_client::embedding;
using limit = service::vector_store_client::limit;
using host_name = service::vector_store_client::host_name;
using http_client = http::experimental::client;
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

// Minimum interval between cleanup tasks
constexpr auto CLEANUP_INTERVAL = std::chrono::seconds(5);

// Minimum interval between dns name refreshes
constexpr auto DNS_REFRESH_INTERVAL = std::chrono::seconds(5);

/// Timeout for waiting for a new client to be available
constexpr auto WAIT_FOR_CLIENT_TIMEOUT = std::chrono::seconds(5);

/// Timeout for HTTP requests to the vector store service
constexpr auto HTTP_REQUEST_TIMEOUT = std::chrono::seconds(5);

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

auto parse_service_uri(std::string_view uri) -> std::optional<std::tuple<host_name, port_number>> {
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

/// Wait for a condition variable to be signaled or timeout.
auto wait_for_signal(condition_variable& cv, time_point timeout) -> future<bool> {
    try {
        co_await cv.wait(timeout);
        co_return true;
    } catch (condition_variable_timed_out&) { // NOLINT(bugprone-empty-catch)
    }
    co_return false;
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

} // namespace

namespace service {

struct vector_store_client::impl {
    lw_shared_ptr<http_client> current_client;
    std::vector<lw_shared_ptr<http_client>> old_clients;
    host_name host;
    port_number port{};
    inet_address addr;
    time_point last_dns_refresh;
    gate tasks_gate;
    condition_variable refresh_cv;
    condition_variable refresh_client_cv;
    condition_variable refresh_stop_cv;
    condition_variable cleanup_stop_cv;
    bool stop_refreshing{};
    milliseconds dns_refresh_interval = DNS_REFRESH_INTERVAL;
    milliseconds wait_for_client_timeout = WAIT_FOR_CLIENT_TIMEOUT;
    milliseconds http_request_timeout = HTTP_REQUEST_TIMEOUT;
    std::function<future<std::optional<inet_address>>(sstring const&)> dns_resolver;

    impl(host_name host_, port_number port_)
        : host(std::move(host_))
        , port(port_)
        , dns_resolver([](auto const& host) -> future<std::optional<inet_address>> {
            try {
                co_return co_await net::dns::resolve_name(host);
            } catch (std::system_error const&) { // NOLINT(bugprone-empty-catch)
            }
            co_return std::nullopt;
        }) {
    }

    /// Refresh the http client with a new address resolved from the DNS name.
    /// If the DNS resolution fails, the current client is set to nullptr.
    /// If the address is the same as the current one, do nothing.
    /// Old clients are saved for later cleanup in a specific task.
    auto refresh_addr() -> future<> {
        auto new_addr = co_await dns_resolver(host);
        if (!new_addr) {
            current_client = nullptr;
            co_return;
        }

        // Check if the new address is the same as the current one
        if (current_client && *new_addr == addr) {
            co_return;
        }

        addr = *new_addr;
        old_clients.emplace_back(current_client);
        current_client = make_lw_shared<http_client>(socket_address(addr, port));
    }

    /// A task for refreshing the vector store http client.
    auto refresh_addr_task() -> future<> {
        for (;;) {
            if (stop_refreshing) {
                break;
            }

            // Do not refresh the service address too often
            auto now = lowres_clock::now();
            if (now - last_dns_refresh > dns_refresh_interval) {
                last_dns_refresh = now;
                co_await refresh_addr();
            } else {
                // Wait till the end of the refreshing interval
                co_await wait_for_signal(refresh_stop_cv, last_dns_refresh + dns_refresh_interval);
                continue;
            }

            if (stop_refreshing) {
                break;
            }

            // new client is available
            refresh_client_cv.broadcast();

            co_await refresh_cv.when();
        }
    }

    /// Request a DNS refresh in the specific task.
    void trigger_dns_refresh() {
        refresh_cv.signal();
    }

    /// Cleanup old clients that are no longer used.
    auto cleanup() -> future<> {
        for (auto& client : old_clients) {
            if (client && client.owned()) {
                co_await client->close();
                client = nullptr;
            }
        }
        std::erase_if(old_clients, [](auto const& client) {
            return !client;
        });
    }

    /// A task for cleaning up old clients.
    auto cleanup_task() -> future<> {
        for (;;) {
            if (co_await wait_for_signal(cleanup_stop_cv, lowres_clock::now() + CLEANUP_INTERVAL)) {
                break;
            }
            co_await cleanup();
        }
        co_await cleanup();
        if (current_client) {
            co_await current_client->close();
        }
        current_client = nullptr;
    }

    struct get_client_response {
        lw_shared_ptr<http_client> client; ///< The http client.
        host_name host;                    ///< The host name for the vector-store service.
    };

    /// Get the current http client or wait for a new one to be available.
    auto get_client() -> future<std::expected<get_client_response, addr_unavailable>> {
        if (current_client) {
            co_return get_client_response{.client = current_client, .host = host};
        }

        trigger_dns_refresh();

        co_await wait_for_signal(refresh_client_cv, lowres_clock::now() + wait_for_client_timeout);

        if (!current_client || stop_refreshing) {
            co_return std::unexpected{addr_unavailable{}};
        }
        co_return get_client_response{.client = current_client, .host = host};
    }

    struct make_request_response {
        http::reply::status_type status;             ///< The HTTP status of the response.
        std::vector<temporary_buffer<char>> content; ///< The content of the response.
    };

    using make_request_error = std::variant<addr_unavailable, service_unavailable>;

    auto make_request(operation_type method, http_path path, std::optional<json_content> content)
            -> future<std::expected<make_request_response, make_request_error>> {
        auto start_time = lowres_clock::now();
        auto resp = make_request_response{.status = http::reply::status_type::ok, .content = std::vector<temporary_buffer<char>>()};
        for (;;) {
            auto client_host = co_await get_client();
            if (!client_host) {
                co_return std::unexpected{client_host.error()};
            }
            auto [client, host] = *std::move(client_host);

            auto req = http::request::make(method, host, path);
            if (content) {
                req.write_body("json", *content);
            }

            auto exception_txt = sstring{};
            try {
                co_await client->make_request(std::move(req), [&resp](http::reply const& reply, input_stream<char> body) -> future<> {
                    resp.status = reply._status;
                    resp.content = co_await util::read_entire_stream(body);
                });
                break;
            } catch (const std::system_error& e) {
                exception_txt = e.what();
            }

            trigger_dns_refresh();

            if (lowres_clock::now() - start_time > http_request_timeout) {
                co_return std::unexpected{service_unavailable{}};
            }
        }
        co_return resp;
    }
};

vector_store_client::vector_store_client(config const& cfg) {
    auto config_uri = cfg.vector_store_uri();
    if (config_uri.empty()) {
        vslogger.info("Vector Store service URI is not configured.");
        return;
    }

    auto parsed_uri = parse_service_uri(config_uri);
    if (!parsed_uri) {
        throw configuration_exception(format("Invalid Vector Store service URI: {}", config_uri));
    }

    auto [host, port] = *parsed_uri;
    _impl = std::make_unique<impl>(std::move(host), port);
    vslogger.info("Vector Store service uri = {}:{}.", _impl->host, _impl->port);
}

vector_store_client::~vector_store_client() = default;

auto vector_store_client::start_background_tasks() -> future<> {
    if (is_disabled()) {
        co_return;
    }

    /// start the background task to refresh the service address
    (void)try_with_gate(_impl->tasks_gate, [this] {
        return _impl->refresh_addr_task();
    }).handle_exception([](std::exception_ptr eptr) {
        vslogger.error("Failed to start a Vector Store Client refresh task: {}", eptr);
    });

    /// start the background task to cleanup
    (void)try_with_gate(_impl->tasks_gate, [this] {
        return _impl->cleanup_task();
    }).handle_exception([](std::exception_ptr eptr) {
        vslogger.error("Failed to start a Vector Store Client cleanup task: {}", eptr);
    });
}

auto vector_store_client::stop() -> future<> {
    if (is_disabled()) {
        co_return;
    }

    _impl->stop_refreshing = true;
    _impl->refresh_stop_cv.signal();
    _impl->refresh_cv.signal();
    _impl->cleanup_stop_cv.signal();
    co_await _impl->tasks_gate.close();
}

auto vector_store_client::host() const -> std::expected<host_name, disabled> {
    if (is_disabled()) {
        return std::unexpected{disabled{}};
    }
    return {_impl->host};
}

auto vector_store_client::port() const -> std::expected<port_number, disabled> {
    if (is_disabled()) {
        return std::unexpected{disabled{}};
    }
    return {_impl->port};
}

auto vector_store_client::ann(keyspace_name keyspace, index_name name, schema_ptr schema, embedding embedding, limit limit)
        -> future<std::expected<primary_keys, ann_error>> {
    if (is_disabled()) {
        vslogger.error("Disabled Vector Store while calling ann");
        co_return std::unexpected{disabled{}};
    }

    auto path = format("/api/v1/indexes/{}/{}/ann", keyspace, name);
    auto content = write_ann_json(std::move(embedding), limit);

    auto resp = co_await _impl->make_request(operation_type::POST, std::move(path), std::move(content));
    if (!resp) {
        co_return std::unexpected{std::visit(
                [](auto&& err) -> ann_error {
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
    if (vsc.is_disabled()) {
        on_internal_error(vslogger, "Cannot set dns_refresh_interval on a disabled vector store client");
    }
    vsc._impl->dns_refresh_interval = interval;
}

void vector_store_client_tester::set_wait_for_client_timeout(vector_store_client& vsc, std::chrono::milliseconds timeout) {
    if (vsc.is_disabled()) {
        on_internal_error(vslogger, "Cannot set wait_for_client_timeout on a disabled vector store client");
    }
    vsc._impl->wait_for_client_timeout = timeout;
}

void vector_store_client_tester::set_http_request_timeout(vector_store_client& vsc, std::chrono::milliseconds timeout) {
    if (vsc.is_disabled()) {
        on_internal_error(vslogger, "Cannot set http_request_timeout on a disabled vector store client");
    }
    vsc._impl->http_request_timeout = timeout;
}

void vector_store_client_tester::set_dns_resolver(vector_store_client& vsc, std::function<future<std::optional<inet_address>>(sstring const&)> resolver) {
    if (vsc.is_disabled()) {
        on_internal_error(vslogger, "Cannot set dns_resolver on a disabled vector store client");
    }
    vsc._impl->dns_resolver = std::move(resolver);
}

void vector_store_client_tester::trigger_dns_resolver(vector_store_client& vsc) {
    if (vsc.is_disabled()) {
        on_internal_error(vslogger, "Cannot trigger a dns resolver on a disabled vector store client");
    }
    vsc._impl->trigger_dns_refresh();
}

auto vector_store_client_tester::resolve_hostname(vector_store_client& vsc) -> future<std::optional<inet_address>> {
    if (vsc.is_disabled()) {
        on_internal_error(vslogger, "Cannot check hostname resolving on a disabled vector store client");
    }
    auto client_host = co_await vsc._impl->get_client();
    if (!client_host) {
        co_return std::nullopt;
    }
    co_return vsc._impl->addr;
}

} // namespace service

