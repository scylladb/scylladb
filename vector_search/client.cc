/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "client.hh"
#include <seastar/core/format.hh>
#include <seastar/http/request.hh>
#include <seastar/http/common.hh>
#include <seastar/http/short_streams.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/net/api.hh>

using namespace seastar;
using namespace std::chrono_literals;

namespace vector_search {
namespace {

auto write_ann_json(std::vector<float> embedding, std::size_t limit) -> seastar::sstring {
    return seastar::format(R"({{"vector":[{}],"limit":{}}})", fmt::join(embedding, ","), limit);
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
        socket.set_keepalive_parameters(net::tcp_keepalive_params{
                .idle = 60s,
                .interval = 60s,
                .count = 10,
        });
        socket.set_keepalive(true);
        co_return socket;
    }
};

} // namespace

client::client(endpoint_type endpoint_)
    : _endpoint(std::move(endpoint_))
    , _http_client(std::make_unique<client_connection_factory>(socket_address(_endpoint.ip, _endpoint.port))) {
}

seastar::future<void> client::status(seastar::abort_source& as) {
    co_await request(http::request::make(httpd::operation_type::GET, _endpoint.host, "/api/v1/status"), http::reply::status_type::ok, as);
}

seastar::future<client::response> client::ann(
        seastar::sstring keyspace, seastar::sstring name, std::vector<float> embedding, std::size_t limit, seastar::abort_source& as) {
    auto path = format("/api/v1/indexes/{}/{}/ann", keyspace, name);
    auto content = write_ann_json(std::move(embedding), limit);
    auto req = http::request::make(httpd::operation_type::POST, _endpoint.host, std::move(path));
    req.write_body("json", std::move(content));
    co_return co_await request(std::move(req), std::nullopt, as);
}

seastar::future<client::response> client::request(
        http::request req, std::optional<seastar::http::reply::status_type> expected_status, seastar::abort_source& as) {
    auto resp = response{seastar::http::reply::status_type::ok, std::vector<seastar::temporary_buffer<char>>()};
    auto handler = [&resp](http::reply const& reply, input_stream<char> body) -> future<> {
        resp.status = reply._status;
        resp.content = co_await util::read_entire_stream(body);
    };

    co_await _http_client.make_request(std::move(req), std::move(handler), std::move(expected_status), &as);
    co_return resp;
}

seastar::future<> client::close() {
    return _http_client.close();
}

} // namespace vector_search
