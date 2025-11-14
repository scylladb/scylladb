/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "client.hh"
#include "utils/exceptions.hh"
#include "utils/exponential_backoff_retry.hh"
#include <seastar/http/request.hh>
#include <seastar/http/short_streams.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/net/api.hh>
#include <seastar/coroutine/as_future.hh>
#include <chrono>

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
auto constexpr BACKOFF_RETRY_MAX_TIME = 20s;

} // namespace

client::client(endpoint_type endpoint_)
    : _endpoint(std::move(endpoint_))
    , _http_client(std::make_unique<client_connection_factory>(socket_address(endpoint_.ip, endpoint_.port))) {
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
    auto ret = !f.failed();
    f.ignore_ready_future();
    co_return ret;
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
    co_await exponential_backoff_retry::do_until_value(BACKOFF_RETRY_MIN_TIME, BACKOFF_RETRY_MAX_TIME, _as, [this] -> future<std::optional<stop_retry>> {
        auto success = co_await check_status();
        if (success) {
            co_return stop_retry{};
        }
        co_return std::nullopt;
    });
}

bool client::is_checking_status_in_progress() const {
    return !_checking_status_future.available();
}

} // namespace vector_search
