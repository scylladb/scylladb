/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once
#include <seastar/http/reply.hh>
#include <seastar/util/bool_class.hh>
#include <string_view>

namespace utils::http {

using retryable = seastar::bool_class<struct is_retryable>;

retryable from_http_code(seastar::http::reply::status_type http_code);

retryable from_system_error(const std::system_error& system_error);

// True when the reply declared a body length that was not fully delivered.
// Only valid once the body has been read to end of stream: stopping early
// leaves the same trace.
bool body_ended_early(const seastar::http::reply& rep);

[[noreturn]] void throw_body_ended_early(std::string_view what);
} // namespace utils::http
