/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "client.hh"
#include <seastar/http/request.hh>
#include <seastar/http/short_streams.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/net/api.hh>

#include "seastar/core/future.hh"
#include "utils/exceptions.hh"
#include "utils/exponential_backoff_retry.hh"
// #include <optional>
#include <seastar/coroutine/as_future.hh>
#include <seastar/core/sleep.hh>
#include <chrono>
#include <system_error>


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

bool succeed(const seastar::future<>& f) {
    return f.available() && !f.failed();
}

bool is_server_unavailable(std::exception_ptr& err) {
    return try_catch<std::system_error>(err) != nullptr;
}

auto constexpr BACKOFF_RETRY_MIN_TIME = 100ms;
auto constexpr BACKOFF_RETRY_MAX_TIME = 20s;

} // namespace

client::client(endpoint_type endpoint_)
    : _endpoint(std::move(endpoint_))
    , _http_client(std::make_unique<client_connection_factory>(socket_address(_endpoint.ip, _endpoint.port))) {
}


seastar::future<client::response> client::request(
        seastar::httpd::operation_type method, seastar::sstring path, seastar::sstring content, seastar::abort_source& as) {
    if (is_checking_status_in_progress()) {
        co_await coroutine::return_exception(is_down_exception{});
    }

    auto f = co_await seastar::coroutine::as_future(request_impl(method, std::move(path), std::move(content), std::nullopt, as));
    if (f.failed()) {
        auto err = f.get_exception();
        if (is_server_unavailable(err)) {
            co_await handle_request_failed();
        }
        co_await seastar::coroutine::return_exception_ptr(err);
    }
    co_return co_await std::move(f);
}

seastar::future<client::response> client::request_impl(seastar::httpd::operation_type method, seastar::sstring path, seastar::sstring content,
        std::optional<seastar::http::reply::status_type>&& expected_status, seastar::abort_source& as) {

    auto req = http::request::make(method, _endpoint.host, std::move(path));
    if (!content.empty()) {
        req.write_body("json", std::move(content));
    }
    auto resp = response{seastar::http::reply::status_type::ok, std::vector<seastar::temporary_buffer<char>>()};
    auto handler = [&resp](http::reply const& reply, input_stream<char> body) -> future<> {
        resp.status = reply._status;
        resp.content = co_await util::read_entire_stream(body);
    };

    co_await _http_client.make_request(std::move(req), std::move(handler), std::move(expected_status), &as);
    co_return resp;
}

seastar::future<void> client::check_status() {
    co_await request_impl(httpd::operation_type::GET, "/api/v1/status", "", http::reply::status_type::ok, _as);
}


seastar::future<> client::close() {
    _as.request_abort();
    co_await std::move(_checking_status).handle_exception([](auto) {});
    _checking_status = make_ready_future<>();
    co_await _http_client.close();
}

seastar::future<> client::handle_request_failed() {
    if (!is_checking_status_in_progress()) {
        co_await start_checking_status();
    }
}

seastar::future<> client::start_checking_status() {
    struct stop_retry {};
    _checking_status =
            exponential_backoff_retry::do_until_value(BACKOFF_RETRY_MIN_TIME, BACKOFF_RETRY_MAX_TIME, _as, [this] -> future<std::optional<stop_retry>> {
                auto f = co_await coroutine::as_future(check_status());
                bool success = succeed(f);
                co_await std::move(f).handle_exception([](const auto&) {});
                if (success) {
                    co_return stop_retry{};
                }
                co_return std::nullopt;
            }).discard_result();
    return make_ready_future();
}

bool client::is_checking_status_in_progress() const {
    return !_checking_status.available();
}

} // namespace vector_search
