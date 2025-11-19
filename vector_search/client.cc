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
#include <chrono>
#include <fmt/format.h>

using namespace seastar;
using namespace std::chrono_literals;

namespace vector_search {
namespace {

class client_connection_factory : public http::experimental::connection_factory {
    socket_address _addr;

public:
    explicit client_connection_factory(socket_address addr)
        : _addr(addr) {
    }

    future<connected_socket> make([[maybe_unused]] abort_source* as) override {
        auto socket = co_await seastar::connect(_addr, {}, transport::TCP);
        socket.set_nodelay(true);
        socket.set_keepalive_parameters(net::tcp_keepalive_params{
                .idle = 60s,
                .interval = 60s,
                .count = 10,
        });
        socket.set_keepalive(true);
        co_return socket;
    }
};

bool is_server_unavailable(std::exception_ptr& err) {
    return try_catch<std::system_error>(err) != nullptr;
}

bool is_request_aborted(std::exception_ptr& err) {
    return try_catch<abort_requested_exception>(err) != nullptr;
}

bool is_server_error(http::reply::status_type status) {
    return status >= http::reply::status_type::internal_server_error;
}

future<client::request_error> map_err(std::exception_ptr& err) {
    if (is_server_unavailable(err)) {
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

client::client(logging::logger& logger, endpoint_type endpoint_, utils::updateable_value<uint32_t> request_timeout_in_ms)
    : _endpoint(std::move(endpoint_))
    , _http_client(std::make_unique<client_connection_factory>(socket_address(endpoint_.ip, endpoint_.port)))
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
        if (is_server_unavailable(err)) {
            handle_server_unavailable();
        }
        co_return std::unexpected{co_await map_err(err)};
    }
    auto resp = co_await std::move(f);
    if (is_server_error(resp.status)) {
        _logger.warn("client ({}:{}): received HTTP status {}: {}", _endpoint.host, _endpoint.port, static_cast<int>(resp.status),
                response_content_to_sstring(resp.content));
        handle_server_unavailable();
        co_return std::unexpected{service_unavailable_error{}};
    }
    co_return resp;
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
