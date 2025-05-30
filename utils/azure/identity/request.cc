/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "request.hh"

namespace azure {

constexpr auto linesep = '\n';

static void format_headers_and_body(fmt::memory_buffer& buf, const auto& headers, std::string_view body, const body_filter& filter) {
    for (auto& [k, v] : headers) {
        fmt::format_to(fmt::appender(buf), "{}: {}{}", k, v, linesep);
    }
    auto redacted_body_opt = filter(body);
    auto redacted_body_view = redacted_body_opt.has_value() ? *redacted_body_opt : body;
    fmt::format_to(fmt::appender(buf), "{}{}", redacted_body_view, linesep);
}

std::string format_request(const seastar::http::request& req, const body_filter& filter) {
    fmt::memory_buffer buf;
    fmt::format_to(fmt::appender(buf), "{} {} HTTP/{}{}", req._method, req._url, req._version, linesep);
    format_headers_and_body(buf, req._headers, req.content, filter);
    return to_string(buf);
}

std::string format_reply(const seastar::http::reply& rep, std::string_view body, const body_filter& filter) {
    fmt::memory_buffer buf;
    auto s = rep.response_line();
    fmt::format_to(fmt::appender(buf), "{}{}", std::string_view(s).substr(0, s.size()-2), linesep);
    format_headers_and_body(buf, rep._headers, body, filter);
    return to_string(buf);
}

}