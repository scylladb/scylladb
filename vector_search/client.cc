/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "client.hh"
#include "utils.hh"
#include "utils/exceptions.hh"
#include "utils/exponential_backoff_retry.hh"
#include "utils/rjson.hh"
#include <seastar/http/request.hh>
#include <seastar/http/short_streams.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/net/api.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/with_timeout.hh>
#include <chrono>
#include <fmt/format.h>
#include <netinet/tcp.h>

using namespace seastar;
using namespace std::chrono_literals;

namespace vector_search {
namespace {

class client_connection_factory : public http::experimental::connection_factory {
    client::endpoint_type _endpoint;
    shared_ptr<tls::certificate_credentials> _creds;

public:
    explicit client_connection_factory(
            client::endpoint_type endpoint, shared_ptr<tls::certificate_credentials> creds, utils::updateable_value<uint32_t> connect_timeout_in_ms)
        : _endpoint(std::move(endpoint))
        , _creds(std::move(creds))
        , _connect_timeout_in_ms(std::move(connect_timeout_in_ms)) {
    }

    future<connected_socket> make([[maybe_unused]] abort_source* as) override {
        auto deadline = std::chrono::steady_clock::now() + timeout();
        auto socket = co_await with_timeout(deadline, connect());
        socket.set_nodelay(true);
        socket.set_keepalive_parameters(get_keepalive_parameters(timeout()));
        socket.set_keepalive(true);
        unsigned int timeout_ms = timeout().count();
        socket.set_sockopt(IPPROTO_TCP, TCP_USER_TIMEOUT, &timeout_ms, sizeof(timeout_ms));
        co_return socket;
    }

private:
    future<connected_socket> connect() {
        auto addr = socket_address(_endpoint.ip, _endpoint.port);
        if (_creds) {
            return tls::connect(_creds, addr, tls::tls_options{.server_name = _endpoint.host});
        }
        return seastar::connect(addr, {}, transport::TCP);
    }

    std::chrono::milliseconds timeout() const {
        constexpr std::chrono::milliseconds MIN_TIMEOUT = 5s;
        auto timeout_ms = std::chrono::milliseconds(_connect_timeout_in_ms.get());
        if (timeout_ms < MIN_TIMEOUT) {
            timeout_ms = MIN_TIMEOUT;
        }
        return timeout_ms;
    }

    utils::updateable_value<uint32_t> _connect_timeout_in_ms;
};

bool is_server_unavailable(std::exception_ptr& err) {
    return try_catch<std::system_error>(err) != nullptr;
}

bool is_server_problem(std::exception_ptr& err) {
    return is_server_unavailable(err) || try_catch<tls::verification_error>(err) != nullptr || try_catch<timed_out_error>(err) != nullptr;
}

bool is_request_aborted(std::exception_ptr& err) {
    return try_catch<abort_requested_exception>(err) != nullptr;
}

future<client::request_error> map_err(std::exception_ptr& err) {
    if (is_server_problem(err)) {
        co_return service_unavailable_error{};
    }
    if (is_request_aborted(err)) {
        co_return aborted_error{};
    }
    co_await coroutine::return_exception_ptr(err); // rethrow
    co_return client::request_error{};             // unreachable
}

auto constexpr BACKOFF_RETRY_MIN_TIME = 100ms;

} // namespace

client::client(logging::logger& logger, endpoint_type endpoint_, utils::updateable_value<uint32_t> request_timeout_in_ms,
        ::shared_ptr<seastar::tls::certificate_credentials> credentials)
    : _endpoint(std::move(endpoint_))
    , _http_client(std::make_unique<client_connection_factory>(_endpoint, std::move(credentials), request_timeout_in_ms))
    , _logger(logger)
    , _request_timeout(std::move(request_timeout_in_ms)) {
}

seastar::future<client::request_result> client::request(
        seastar::httpd::operation_type method, seastar::sstring path, std::optional<seastar::sstring> content, seastar::abort_source& as) {
    if (is_checking_status_in_progress()) {
        co_return std::unexpected(service_unavailable_error{});
    }

    auto f = co_await seastar::coroutine::as_future(request_impl(method, std::move(path), std::move(content), std::nullopt, as));
    if (f.failed()) {
        auto err = f.get_exception();
        if (as.abort_requested()) {
            co_return std::unexpected{aborted_error{}};
        }
        if (is_server_problem(err)) {
            handle_server_unavailable();
        }
        co_return std::unexpected{co_await map_err(err)};
    }
    co_return co_await std::move(f);
}

seastar::future<client::response> client::request_impl(seastar::httpd::operation_type method, seastar::sstring path, std::optional<seastar::sstring> content,
        std::optional<seastar::http::reply::status_type>&& expected_status, seastar::abort_source& as) {

    auto req = http::request::make(method, _endpoint.host, std::move(path));
    if (content) {
        req.write_body("json", std::move(*content));
    }
    auto resp = response{seastar::http::reply::status_type::ok, std::vector<seastar::temporary_buffer<char>>()};
    auto handler = [&resp](http::reply const& reply, input_stream<char> body) -> future<> {
        resp.status = reply._status;
        resp.content = co_await util::read_entire_stream(body);
    };

    co_await _http_client.make_request(std::move(req), std::move(handler), std::move(expected_status), &as);
    co_return resp;
}

seastar::future<bool> client::check_status() {
    auto f = co_await coroutine::as_future(request_impl(httpd::operation_type::GET, "/api/v1/status", std::nullopt, http::reply::status_type::ok, _as));
    if (f.failed()) {
        f.ignore_ready_future();
        co_return false;
    }
    auto resp = co_await std::move(f);
    auto json = rjson::parse(std::move(resp.content));
    co_return json.IsString() && json.GetString() == std::string_view("SERVING");
}

seastar::future<> client::close() {
    _as.request_abort();
    co_await std::exchange(_checking_status_future, make_ready_future());
    co_await _http_client.close();
}

void client::handle_server_unavailable() {
    if (!is_checking_status_in_progress()) {
        _checking_status_future = run_checking_status();
    }
}

seastar::future<> client::run_checking_status() {
    struct stop_retry {};
    auto f = co_await coroutine::as_future(
            exponential_backoff_retry::do_until_value(BACKOFF_RETRY_MIN_TIME, backoff_retry_max(), _as, [this] -> future<std::optional<stop_retry>> {
                auto success = co_await check_status();
                if (success) {
                    co_return stop_retry{};
                }
                co_return std::nullopt;
            }));
    if (f.failed()) {
        if (auto err = f.get_exception(); !is_request_aborted(err)) {
            // Report internal error for exceptions other than abort
            on_internal_error_noexcept(_logger, fmt::format("exception while checking status: {}", err));
        }
    }
    co_return;
}

bool client::is_checking_status_in_progress() const {
    return !_checking_status_future.available();
}

std::chrono::milliseconds client::backoff_retry_max() const {
    std::chrono::milliseconds ret{_request_timeout.get()};
    return ret * 2;
}

} // namespace vector_search
