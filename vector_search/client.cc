/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "client.hh"
#include "utils/exceptions.hh"
#include <seastar/http/request.hh>
#include <seastar/http/short_streams.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/net/api.hh>
#include <seastar/coroutine/as_future.hh>

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

} // namespace

client::client(endpoint_type endpoint_)
    : _endpoint(std::move(endpoint_))
    , _http_client(std::make_unique<client_connection_factory>(socket_address(endpoint_.ip, endpoint_.port))) {
}

seastar::future<client::request_result> client::request(
        seastar::httpd::operation_type method, seastar::sstring path, std::optional<seastar::sstring> content, seastar::abort_source& as) {

    auto req = http::request::make(method, _endpoint.host, std::move(path));
    if (content) {
        req.write_body("json", std::move(*content));
    }
    auto resp = response{seastar::http::reply::status_type::ok, std::vector<seastar::temporary_buffer<char>>()};
    auto handler = [&resp](http::reply const& reply, input_stream<char> body) -> future<> {
        resp.status = reply._status;
        resp.content = co_await util::read_entire_stream(body);
    };

    auto f = co_await seastar::coroutine::as_future(_http_client.make_request(std::move(req), std::move(handler), std::nullopt, &as));
    if (f.failed()) {
        auto err = f.get_exception();
        co_return std::unexpected(co_await map_err(err));
    }
    co_return resp;
}

seastar::future<> client::close() {
    return _http_client.close();
}

} // namespace vector_search
