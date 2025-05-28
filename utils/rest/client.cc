/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/net/tls.hh>
#include <seastar/net/dns.hh>
#include <seastar/util/short_streams.hh>

#include "client.hh"

using namespace seastar;

static std::string default_server_name(const std::string& host) {
    // don't verify host cert name if "host" is just an ip address.
    // typically testing.
    bool is_numeric_host = seastar::net::inet_address::parse_numerical(host).has_value();
    return is_numeric_host ? std::string{} : host;
}

rest::httpclient::httpclient(std::string host, uint16_t port, shared_ptr<tls::certificate_credentials> creds, std::optional<tls::tls_options> options)
    : _host(std::move(host))
    , _port(port)
    , _creds(std::move(creds))
    , _tls_options(options.value_or(tls::tls_options{ .server_name = default_server_name(_host) }))
    , _req(http::request::make(httpd::operation_type::GET, _host, ""))
{
    _req._version = "1.1";
}

rest::httpclient& rest::httpclient::add_header(std::string_view key, std::string_view value) {
    _req._headers[sstring(key)] = sstring(value);
    return *this;
}

void rest::httpclient::clear_headers() {
    _req._headers.clear();
}

seastar::future<rest::httpclient::result_type> rest::httpclient::send() {
    result_type res;
    co_await send([&](const http::reply& r, std::string_view body) {
        res.reply._status = r._status;
        res.reply._content = sstring(body);
        res.reply._headers = r._headers;
        res.reply._version = r._version;
    });
    co_return res;
}

seastar::future<> rest::httpclient::send(const handler_func& f) {
    auto addr = co_await net::dns::resolve_name(_host, net::inet_address::family::INET /* TODO: our client does not handle ipv6 well?*/);

    // NOTE: similar to utils::http::dns_connection_factory, but that type does
    // not properly handle numeric hosts (don't validate certs for those)
    class my_connection_factory : public http::experimental::connection_factory {
        socket_address _addr;
        shared_ptr<tls::certificate_credentials> _creds;
        tls::tls_options _tls_options;
        sstring _host;
    public:
        my_connection_factory(socket_address addr, shared_ptr<tls::certificate_credentials> creds, tls::tls_options options, sstring host)
            : _addr(std::move(addr))
            , _creds(std::move(creds))
            , _tls_options(std::move(options))
            , _host(std::move(host))
        {}
        future<connected_socket> make(abort_source* as) override {
            connected_socket s = co_await (_creds 
                ? tls::connect(_creds, _addr, _tls_options)
                : seastar::connect(_addr, {}, transport::TCP)
            );
            s.set_nodelay(true);
            co_return s;
        }
    };

    if (_req._url.empty()) {
        _req._url = "/";
    }
    if (!_req._headers.count(CONTENT_TYPE_HEADER)) {
        _req._headers[CONTENT_TYPE_HEADER] = "application/x-www-form-urlencoded";
    }

    http::experimental::client client(std::make_unique<my_connection_factory>(socket_address(addr, _port), _creds, _tls_options, _host));

    std::exception_ptr p;
    try {
        co_await client.make_request(std::move(_req), [&](const http::reply& rep, input_stream<char>&& in) -> future<> {
            // ensure these are on our coroutine frame.
            auto& resp_handler = f;
            auto in_stream = std::move(in);
            auto result = co_await util::read_entire_stream_contiguous(in_stream);
            resp_handler(rep, result);
        });
    } catch (...) {
        p = std::current_exception();
    }

    co_await client.close();

    if (p) {
        std::rethrow_exception(p);
    }
}

void rest::httpclient::method(method_type type) {
    _req._method = httpd::type2str(type);
}

void rest::httpclient::content(std::string_view content) {
    _req.content_length = content.size();
    _req.content = sstring(content);
}

void rest::httpclient::target(std::string_view s) {
    _req._url = sstring(s);
}

constexpr auto linesep = '\n';

auto 
fmt::formatter<rest::httpclient::request_type>::format(const rest::httpclient::request_type& r, fmt::format_context& ctx) const -> decltype(ctx.out()) {
    auto os = fmt::format_to(ctx.out(), "{} {} HTTP/{}{}", r._method, r._url, r._version, linesep);
    for (auto& [k, v] : r._headers) {
        os = fmt::format_to(os, "{}: {}{}", k, v, linesep);
    }
    os = fmt::format_to(os, "{}{}", linesep, r.content);
    return os;
}

auto 
fmt::formatter<rest::httpclient::result_type>::format(const rest::httpclient::result_type& r, fmt::format_context& ctx) const -> decltype(ctx.out()) {
    auto s = r.reply.response_line();
    // remove the trailing \r\n from response_line string. we want our own linebreak, hence substr.
    auto os = fmt::format_to(ctx.out(), "{}{}", std::string_view(s).substr(0, s.size()-2), linesep);
    for (auto& [k, v] : r.reply._headers) {
        os = fmt::format_to(os, "{}: {}{}", k, v, linesep);
    }
    os = fmt::format_to(os, "{}{}", linesep, r.body());
    return os;
}


