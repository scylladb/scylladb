/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "vector_store_client.hh"
#include "db/config.hh"
#include "exceptions/exceptions.hh"
#include "utils/sequential_producer.hh"
#include <charconv>
#include <exception>
#include <regex>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/http/client.hh>
#include <seastar/net/dns.hh>
#include <seastar/net/inet_address.hh>

namespace {

using configuration_exception = exceptions::configuration_exception;
using duration = lowres_clock::duration;
using host_name = service::vector_store_client::host_name;
using http_client = http::experimental::client;
using inet_address = seastar::net::inet_address;
using milliseconds = std::chrono::milliseconds;
using port_number = service::vector_store_client::port_number;
using time_point = lowres_clock::time_point;

// Wait time before retrying after an exception occurred
constexpr auto EXCEPTION_OCCURED_WAIT = std::chrono::seconds(5);

// Minimum interval between dns name refreshes
constexpr auto DNS_REFRESH_INTERVAL = std::chrono::seconds(5);

/// Timeout for waiting for a new client to be available
constexpr auto WAIT_FOR_CLIENT_TIMEOUT = std::chrono::seconds(5);

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
    abort_source abort_refresh;
    milliseconds dns_refresh_interval = DNS_REFRESH_INTERVAL;
    milliseconds wait_for_client_timeout = WAIT_FOR_CLIENT_TIMEOUT;
    std::function<future<std::optional<inet_address>>(sstring const&)> dns_resolver;
    sequential_producer<lw_shared_ptr<http_client>> client_producer;

    impl(host_name host_, port_number port_)
        : host(std::move(host_))
        , port(port_)
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

    struct get_client_response {
        lw_shared_ptr<http_client> client; ///< The http client.
        host_name host;                    ///< The host name for the vector-store service.
    };

    using get_client_error = std::variant<aborted, addr_unavailable>;

    /// Get the current http client or wait for a new one to be available.
    auto get_client(abort_source& as) -> future<std::expected<get_client_response, get_client_error>> {
        if (current_client) {
            co_return get_client_response{.client = current_client, .host = host};
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
        co_return get_client_response{.client = client, .host = host};
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

void vector_store_client::start_background_tasks() {
    if (is_disabled()) {
        return;
    }

    /// start the background task to refresh the service address
    (void)try_with_gate(_impl->tasks_gate, [this] {
        return _impl->refresh_addr_task();
    }).handle_exception([](std::exception_ptr eptr) {
        on_internal_error_noexcept(vslogger, format("The Vector Store Client refresh task failed: {}", eptr));
    });
}

auto vector_store_client::stop() -> future<> {
    if (is_disabled()) {
        co_return;
    }

    _impl->abort_refresh.request_abort();
    _impl->refresh_cv.signal();
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

auto vector_store_client_tester::resolve_hostname(vector_store_client& vsc, abort_source& as) -> future<std::optional<inet_address>> {
    if (vsc.is_disabled()) {
        on_internal_error(vslogger, "Cannot check hostname resolving on a disabled vector store client");
    }
    auto client_host = co_await vsc._impl->get_client(as);
    if (!client_host) {
        co_return std::nullopt;
    }
    co_return vsc._impl->addr;
}

} // namespace service

