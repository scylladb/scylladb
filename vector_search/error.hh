/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include <seastar/http/reply.hh>
#include <seastar/core/sstring.hh>
#include <fmt/format.h>

namespace vector_search {

/// The service is disabled.
struct disabled_error {};

/// The operation was aborted.
struct aborted_error {};

/// The vector-store addr is unavailable (not possible to get an addr from the dns service).
struct addr_unavailable_error {};

/// The vector-store service is unavailable.
struct service_unavailable_error {};

/// The error from the vector-store service.
struct service_error {
    seastar::http::reply::status_type status; ///< The HTTP status code from the vector-store service.
};

/// An unsupported reply format from the vector-store service.
struct service_reply_format_error {};

struct error_visitor {
    seastar::sstring operator()(service_error e) const {
        return fmt::format("Vector Store error: HTTP status {}", e.status);
    }
    seastar::sstring operator()(disabled_error) const {
        return fmt::format("Vector Store is disabled");
    }
    seastar::sstring operator()(aborted_error) const {
        return fmt::format("Vector Store request was aborted");
    }
    seastar::sstring operator()(addr_unavailable_error) const {
        return fmt::format("Vector Store service address could not be fetched from DNS");
    }
    seastar::sstring operator()(service_unavailable_error) const {
        return fmt::format("Vector Store service is unavailable");
    }
    seastar::sstring operator()(service_reply_format_error) const {
        return fmt::format("Vector Store returned an invalid JSON");
    }
};

} // namespace vector_search
