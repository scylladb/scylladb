/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include <seastar/core/future.hh>
#include <seastar/http/url.hh>
#include <seastar/http/client.hh>
#include <seastar/http/request.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/exception.hh>

#include "utils/rjson.hh"

namespace rest {

/**
 * Wrapper for http::request, making setting headers, body, etc
 * more convinient for our purposes. Separated from the below client
 * so the logic can be shared with non-single usage, i.e. the free-form
 * simple_send method, possibly caching a http::client across calls.
 */
class request_wrapper {
public:
    request_wrapper(std::string_view host);
    request_wrapper(request_wrapper&&);

    request_wrapper& add_header(std::string_view key, std::string_view value);
    void clear_headers();

    using reply_status = seastar::http::reply::status_type;
    using request_type = seastar::http::request;
    using reply_type = seastar::http::reply;

    using method_type = seastar::httpd::operation_type;
    using body_writer = decltype(std::declval<seastar::http::request>().body_writer);

    void method(method_type);
    void content(std::string_view content_type, std::string_view);
    void content(std::string_view content_type, body_writer, size_t);

    void target(std::string_view);

    request_type& request() {
        return _req;
    }
    const request_type& request() const {
        return _req;
    }
    operator request_type&() {
        return request();
    }
    operator const request_type&() const {
        return request();
    }
protected:
    request_type _req;
};

/**
 * HTTP client wrapper for making short, stateless REST calls, such as
 * OAUTH, GCP/AWS/Azure queries, etc.
 * No statefulness, no reuse, no sessions.
 * Just a GET/POST and a result. 
 */
class httpclient : public request_wrapper {
public:
    httpclient();
    httpclient(std::string host, uint16_t port, seastar::shared_ptr<seastar::tls::certificate_credentials> = {}, std::optional<seastar::tls::tls_options> = {});

    struct result_type {
        seastar::http::reply reply;
        reply_status result() const {
            return reply._status;
        }
        int result_int() const {
            return int(result());
        }
        std::string_view body() const {
            return reply._content;
        }
    };

    using handler_func = std::function<void(const seastar::http::reply&, std::string_view)>;

    seastar::future<result_type> send();
    seastar::future<> send(const handler_func&);

    const std::string& host() const {
        return _host;
    }
    uint16_t port() const {
        return _port;
    }

    static inline constexpr const char* CONTENT_TYPE_HEADER = "content-type";

private:
    std::string _host;
    uint16_t _port;
    seastar::shared_ptr<seastar::tls::certificate_credentials> _creds;
    seastar::tls::tls_options _tls_options;
};

using handler_func_ex = std::function<future<>(const seastar::http::reply&, seastar::input_stream<char>&)>;

seastar::future<> simple_send(seastar::http::experimental::client&, seastar::http::request&, const handler_func_ex&);

// Interface for redacting sensitive data from HTTP requests and responses before logging.
class http_log_filter {
public:
    static constexpr char REDACTED_VALUE[] = "[REDACTED]";
    enum class body_type {
        request,
        response,
    };
    using string_opt = std::optional<std::string>;
    // Filter a request/response header.
    // Returns an optional containing the filtered value. If no filtering is required, the optional is not engaged.
    virtual string_opt filter_header(std::string_view name, std::string_view value) const = 0;
    // Filter the request/response body.
    // Returns an optional containing the filtered value. If no filtering is required, the optional is not engaged.
    virtual string_opt filter_body(body_type type, const std::string_view body) const = 0;
};

class nop_log_filter: public http_log_filter {
public:
    string_opt filter_header(std::string_view name, std::string_view value) const override { return std::nullopt; }
    string_opt filter_body(body_type type, std::string_view body) const override { return std::nullopt; }
};

template<typename T, http_log_filter::body_type type>
struct redacted {
    const T& original;
    const http_log_filter& filter;

    std::optional<std::string> filter_header(std::string_view name, std::string_view value) const {
        return filter.filter_header(name, value);
    }
    std::optional<std::string> filter_body(std::string_view value) const {
        return filter.filter_body(type, value);
    }
};

using redacted_request_type = redacted<httpclient::request_type, http_log_filter::body_type::request>;
using redacted_result_type  = redacted<httpclient::result_type, http_log_filter::body_type::response>;

using key_value = std::pair<std::string_view, std::string_view>;
using key_values = std::span<const key_value>;

class unexpected_status_error : public seastar::httpd::unexpected_status_error {
    std::vector<std::pair<std::string, std::string>> _headers;
public:
    unexpected_status_error(seastar::http::reply::status_type, key_values);

    const auto& headers() const {
        return _headers;
    }
};

future<rjson::value> send_request(std::string_view uri
    , seastar::shared_ptr<seastar::tls::certificate_credentials>
    , const rjson::value& body
    , httpclient::method_type op
    , key_values headers = {}
);

future<rjson::value> send_request(std::string_view uri
    , seastar::shared_ptr<seastar::tls::certificate_credentials>
    , std::string body
    , std::string_view content_type
    , httpclient::method_type op
    , key_values headers = {}
);

future<> send_request(std::string_view uri
    , seastar::shared_ptr<seastar::tls::certificate_credentials>
    , std::string body
    , std::string_view content_type
    , const std::function<void(const httpclient::reply_type&, std::string_view)>& handler
    , httpclient::method_type op
    , key_values headers = {}
);

}

template <>
struct fmt::formatter<rest::httpclient::request_type> : fmt::formatter<std::string_view> {
    auto format(const rest::httpclient::request_type&, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <>
struct fmt::formatter<rest::httpclient::result_type> : fmt::formatter<std::string_view> {
    auto format(const rest::httpclient::result_type&, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <>
struct fmt::formatter<rest::redacted_request_type> : fmt::formatter<std::string_view> {
    auto format(const rest::redacted_request_type&, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <>
struct fmt::formatter<rest::redacted_result_type> : fmt::formatter<std::string_view> {
    auto format(const rest::redacted_result_type&, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <>
struct fmt::formatter<seastar::http::reply> : fmt::formatter<std::string_view> {
    auto format(const seastar::http::reply&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
