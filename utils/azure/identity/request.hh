/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/http/request.hh>
#include <seastar/http/reply.hh>

namespace azure {

using body_filter = std::function<std::optional<std::string>(std::string_view)>;

inline const body_filter nop_filter = [](std::string_view body) { return std::nullopt; };

// Logging utilities to format HTTP requests and responses into human-readable strings.
//
// An optional filter can be provided to redact sensitive content from the message body.
// The formatting is consistent with `encryption::httpclient` formatters.
std::string format_request(const seastar::http::request& req, const body_filter& filter = nop_filter);
std::string format_reply(const seastar::http::reply& rep, std::string_view body, const body_filter& filter = nop_filter);

}