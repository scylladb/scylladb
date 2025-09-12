/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "client.hh"
#include "exception.hh"
#include <seastar/core/format.hh>
#include <seastar/http/request.hh>
#include <seastar/http/common.hh>
#include <seastar/http/short_streams.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/net/api.hh>
#include "utils/rjson.hh"
#include <chrono>

using namespace seastar;
using namespace std::chrono_literals;

namespace service::vector_search {
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

auto write_ann_json(std::vector<float> embedding, std::size_t limit) -> seastar::sstring {
    return seastar::format(R"({{"embedding":[{}],"limit":{}}})", fmt::join(embedding, ","), limit);
}

sstring to_string(const std::vector<temporary_buffer<char>>& buffers) {
    sstring result;
    for (const auto& buf : buffers) {
        result.append(buf.get(), buf.size());
    }
    return result;
}

} // namespace

client::client(::service::vector_search::endpoint endpoint_)
    : _endpoint(std::move(endpoint_))
    , _http_client(std::make_unique<client_connection_factory>(socket_address(_endpoint.ip, _endpoint.port))) {
}

seastar::future<client::ann_result> client::ann(
        seastar::sstring keyspace, seastar::sstring name, std::vector<float> embedding, std::size_t limit, seastar::abort_source* as) {
    auto path = format("/api/v1/indexes/{}/{}/ann", keyspace, name);
    auto content = write_ann_json(std::move(embedding), limit);
    auto req = http::request::make(httpd::operation_type::POST, _endpoint.host, std::move(path));
    req.write_body("json", std::move(content));

    co_return co_await request(std::move(req), as);
}

seastar::future<std::vector<seastar::temporary_buffer<char>>> client::request(http::request req, seastar::abort_source* as) {
    auto resp = std::vector<seastar::temporary_buffer<char>>{};
    auto status = seastar::http::reply::status_type::ok;
    auto handler = [&resp, &status](http::reply const& reply, input_stream<char> body) -> future<> {
        status = reply._status;
        resp = co_await util::read_entire_stream(body);
    };

    co_await _http_client.make_request(std::move(req), std::move(handler), std::nullopt, as);
    if (status != seastar::http::reply::status_type::ok) {
        throw service_status_exception(status, to_string(resp));
    }
    co_return resp;
}

} // namespace service::vector_search
