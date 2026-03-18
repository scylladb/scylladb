/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include <seastar/http/reply.hh>
#include <seastar/util/bool_class.hh>

namespace utils::http {

using retryable = seastar::bool_class<struct is_retryable>;

retryable from_http_code(seastar::http::reply::status_type http_code);

retryable from_system_error(const std::system_error& system_error);
} // namespace utils::http
