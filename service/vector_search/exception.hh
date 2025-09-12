/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/http/reply.hh>
#include <stdexcept>
#include <fmt/format.h>

namespace service::vector_search {

class service_status_exception : public std::runtime_error {
public:
    explicit service_status_exception(seastar::http::reply::status_type status, seastar::sstring content)
        : std::runtime_error(fmt::format("Vector Store error: HTTP status {}", status))
        , _status{status}
        , _content{std::move(content)} {
    }

    const seastar::http::reply::status_type& status() const noexcept {
        return _status;
    }

    const seastar::sstring& content() const noexcept {
        return _content;
    }

private:
    seastar::http::reply::status_type _status;
    seastar::sstring _content;
};

class service_reply_format_exception : public std::runtime_error {
public:
    explicit service_reply_format_exception()
        : std::runtime_error("Vector Store returned an invalid JSON") {
    }

    const seastar::http::reply::status_type& status() const noexcept {
        return _status;
    }

private:
    seastar::http::reply::status_type _status;
};

class service_unavailable_exception : public std::runtime_error {
public:
    explicit service_unavailable_exception()
        : std::runtime_error("Vector Store service is unavailable") {
    }
};

class service_disabled_exception : public std::runtime_error {
public:
    explicit service_disabled_exception()
        : std::runtime_error("Vector Store is disabled") {
    }
};

class service_address_unavailable_exception : public std::runtime_error {
public:
    explicit service_address_unavailable_exception()
        : std::runtime_error("Vector Store service address could not be fetched from DNS") {
    }
};

} // namespace service::vector_search
