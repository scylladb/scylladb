/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/future.hh>
#include <seastar/http/url.hh>
#include <seastar/http/client.hh>
#include <seastar/http/request.hh>
#include <seastar/http/reply.hh>

namespace rest {

/**
 * HTTP client wrapper for making short, stateless REST calls, such as
 * OAUTH, GCP/AWS/Azure queries, etc.
 * No statefulness, no reuse, no sessions.
 * Just a GET/POST and a result. 
 */
class httpclient {
public:
    httpclient(std::string host, uint16_t port, seastar::shared_ptr<seastar::tls::certificate_credentials> = {}, std::optional<seastar::tls::tls_options> = {});

    httpclient& add_header(std::string_view key, std::string_view value);
    void clear_headers();

    using reply_status = seastar::http::reply::status_type;
    using request_type = seastar::http::request;

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

    using method_type = seastar::httpd::operation_type;

    void method(method_type);
    void content(std::string_view);
    void target(std::string_view);

    const request_type& request() const {
        return _req;
    }
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
    request_type _req;
};

}

template <>
struct fmt::formatter<rest::httpclient::request_type> : fmt::formatter<std::string_view> {
    auto format(const rest::httpclient::request_type&, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <>
struct fmt::formatter<rest::httpclient::result_type> : fmt::formatter<std::string_view> {
    auto format(const rest::httpclient::result_type&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
