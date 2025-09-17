/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <boost/regex.hpp>

#include <seastar/net/tls.hh>
#include <seastar/net/dns.hh>
#include <seastar/util/short_streams.hh>

#include "client.hh"
#include "utils/http.hh"

using namespace seastar;

rest::request_wrapper::request_wrapper(std::string_view host)
    : _req(http::request::make(httpd::operation_type::GET, sstring(host), ""))
{
    _req._version = "1.1";
}

rest::request_wrapper::request_wrapper(request_wrapper&&) = default;

rest::request_wrapper& rest::request_wrapper::add_header(std::string_view key, std::string_view value) {
    _req._headers[sstring(key)] = sstring(value);
    return *this;
}

void rest::request_wrapper::clear_headers() {
    _req._headers.clear();
}

void rest::request_wrapper::method(method_type type) {
    _req._method = httpd::type2str(type);
}

void rest::request_wrapper::content(std::string_view content) {
    _req.content_length = content.size();
    _req.content = sstring(content);
}

void rest::request_wrapper::content(body_writer w, size_t len) {
    _req.content_length = len;
    _req.body_writer = std::move(w);
}

void rest::request_wrapper::target(std::string_view s) {
    _req._url = sstring(s);
}

static std::string default_server_name(const std::string& host) {
    // don't verify host cert name if "host" is just an ip address.
    // typically testing.
    bool is_numeric_host = seastar::net::inet_address::parse_numerical(host).has_value();
    return is_numeric_host ? std::string{} : host;
}

rest::httpclient::httpclient(std::string host, uint16_t port, shared_ptr<tls::certificate_credentials> creds, std::optional<tls::tls_options> options)
    : request_wrapper(host)
    , _host(std::move(host))
    , _port(port)
    , _creds(std::move(creds))
    , _tls_options(options.value_or(tls::tls_options{ .server_name = default_server_name(_host) }))
{}

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

    http::experimental::client client(std::make_unique<my_connection_factory>(socket_address(addr, _port), _creds, _tls_options, _host));

    std::exception_ptr p;
    try {
        co_await simple_send(client, _req, [&](const seastar::http::reply& rep, seastar::input_stream<char>& in_stream) -> future<> {
            // ensure these are on our coroutine frame.
            auto& resp_handler = f;
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

seastar::future<> rest::simple_send(seastar::http::experimental::client& client, seastar::http::request& req, const handler_func_ex& f) {
    if (req._url.empty()) {
        req._url = "/";
    }
    if (req._version.empty()) {
        req._version = "1.1";
    }
    if (!req._headers.count(httpclient::CONTENT_TYPE_HEADER)) {
        req._headers[httpclient::CONTENT_TYPE_HEADER] = "application/x-www-form-urlencoded";
    }

    co_await client.make_request(std::move(req), [&](const http::reply& rep, input_stream<char>&& in) -> future<> {
        // ensure these are on our coroutine frame.
        auto& resp_handler = f;
        auto in_stream = std::move(in);
        co_await resp_handler(rep, in_stream);
    });
}


rest::unexpected_status_error::unexpected_status_error(seastar::http::reply::status_type status, key_values headers)
    : httpd::unexpected_status_error(status)
    , _headers(headers.begin(), headers.end())
{}

future<rjson::value> rest::send_request(std::string_view uri
    , seastar::shared_ptr<seastar::tls::certificate_credentials> creds
    , const rjson::value& body
    , httpclient::method_type op
    , key_values headers)
{
    return send_request(uri, std::move(creds), rjson::print(body), "application/json", op, std::move(headers));
}

future<rjson::value> rest::send_request(std::string_view uri
    , seastar::shared_ptr<seastar::tls::certificate_credentials> creds
    , std::string body
    , std::string_view content_type
    , httpclient::method_type op
    , key_values headers)
{
    rjson::value v;
    co_await send_request(uri, std::move(creds), std::move(body), content_type, [&](const http::reply& rep, std::string_view s) {
        if (rep._status != http::reply::status_type::ok) {
            std::vector<key_value> tmp(rep._headers.begin(), rep._headers.end());
            throw unexpected_status_error(rep._status, tmp);
        }
        v = rjson::parse(s); 
    }, op, std::move(headers));
    co_return v;
}

future<> rest::send_request(std::string_view uri
    , seastar::shared_ptr<seastar::tls::certificate_credentials> creds
    , std::string body
    , std::string_view content_type
    , const std::function<void(const http::reply&, std::string_view)>& handler
    , httpd::operation_type op
    , key_values headers)
{
    // Extremely simplified URI parsing. Does not handle any params etc. But we do not expect such here.
    static boost::regex simple_url(R"foo((https?):\/\/([^\/:]+)(:\d+)?(\/.*)?)foo");

    boost::smatch m;
    std::string tmp(uri);
    if (!boost::regex_match(tmp, m, simple_url)) {
        throw std::invalid_argument(fmt::format("Could not parse URI {}", uri));
    }

    auto scheme = m[1].str();
    auto host = m[2].str();
    auto port = m[3].str();
    auto path = m[4].str();

    if (scheme != "https") {
        creds = nullptr;
    } else if (!creds) {
        creds = co_await utils::http::system_trust_credentials();
    }

    uint16_t pi = port.empty() ? (creds ? 443 : 80) : uint16_t(std::stoi(port.substr(1)));

    httpclient client(host, pi, std::move(creds));

    client.target(path);
    client.method(op);

    for (auto& [k, v] : headers) {
        client.add_header(k, v);
    }

    if (!body.empty()) {
        if (content_type.empty()) {
            content_type = "application/x-www-form-urlencoded";
        }
        client.content(std::move(body));
        client.add_header(httpclient::CONTENT_TYPE_HEADER, content_type);
    }

    co_await client.send([&] (const http::reply& rep, std::string_view result) {
        handler(rep, result);
    });
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
    return fmt::format_to(ctx.out(), "{}", r.reply);
}

auto 
fmt::formatter<seastar::http::reply>::format(const seastar::http::reply& r, fmt::format_context& ctx) const -> decltype(ctx.out()) {
    auto s = r.response_line();
    // remove the trailing \r\n from response_line string. we want our own linebreak, hence substr.
    auto os = fmt::format_to(ctx.out(), "{}{}", std::string_view(s).substr(0, s.size()-2), linesep);
    for (auto& [k, v] : r._headers) {
        os = fmt::format_to(os, "{}: {}{}", k, v, linesep);
    }
    os = fmt::format_to(os, "{}{}", linesep, r._content);
    return os;
}

auto
fmt::formatter<rest::redacted_request_type>::format(const rest::redacted_request_type& rr, fmt::format_context& ctx) const -> decltype(ctx.out()) {
    const auto& r = rr.original;
    auto os = fmt::format_to(ctx.out(), "{} {} HTTP/{}{}", r._method, r._url, r._version, linesep);
    for (auto& [k, v] : r._headers) {
        os = fmt::format_to(os, "{}: {}{}", k, rr.filter_header(k, v).value_or(v), linesep);
    }
    os = fmt::format_to(os, "{}{}", linesep, rr.filter_body(r.content).value_or(r.content));
    return os;
}

auto
fmt::formatter<rest::redacted_result_type>::format(const rest::redacted_result_type& rr, fmt::format_context& ctx) const -> decltype(ctx.out()) {
    const auto& r = rr.original;
    auto s = r.reply.response_line();
    // remove the trailing \r\n from response_line string. we want our own linebreak, hence substr.
    auto os = fmt::format_to(ctx.out(), "{}{}", std::string_view(s).substr(0, s.size()-2), linesep);
    for (auto& [k, v] : r.reply._headers) {
        os = fmt::format_to(os, "{}: {}{}", k, rr.filter_header(k, v).value_or(v), linesep);
    }
    auto redacted_body_opt = rr.filter_body(r.body());
    auto redacted_body_view = redacted_body_opt.has_value() ? *redacted_body_opt : r.body();
    os = fmt::format_to(os, "{}{}", linesep, redacted_body_view);
    return os;
}
