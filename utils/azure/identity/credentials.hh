/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <chrono>
#include <utility>

#include <seastar/core/future.hh>

#include "utils/log.hh"

using namespace seastar;

extern logger az_creds_logger;

namespace azure {

using timeout_clock = std::chrono::system_clock;
using timestamp_type = typename timeout_clock::time_point;
using resource_type = sstring;

struct access_token {
    sstring token;
    timestamp_type expiry;
    resource_type resource_uri;

    access_token() = default;
    access_token(const sstring& token, const timestamp_type& expiry, const resource_type& resource_uri);

    bool empty() const;
    bool expired() const;
};

class credentials {
protected:
    access_token token;
    // A context string that is prepended to all log messages.
    // Helps to identify the owner of the credentials.
    const sstring logctx;
private:
    virtual const char* get_name() const = 0;
    virtual future<> refresh(const resource_type& resource_uri) = 0;
public:
    credentials(const sstring& logctx = "") : logctx(logctx) {}
    virtual ~credentials() = default;
    // Throws azure::auth_error on failure.
    future<access_token> get_access_token(const resource_type& resource_uri);
protected:
    template <typename... Args>
    void log(log_level level, std::string_view fmt, const Args&... args) const {
        if (az_creds_logger.is_enabled(level)) {
            auto ctx_msg = !logctx.empty() ? fmt::format("{}/", logctx) : "";
            auto msg = fmt::format(fmt::runtime(fmt), args...);
            az_creds_logger.log(level, "[{}{}] {}", ctx_msg, get_name(), msg);
        }
    }
    template <typename... Args>
    void log_error(std::string_view fmt, Args&&... args) const {
        log(log_level::error, std::move(fmt), std::forward<Args>(args)...);
    }
    template <typename... Args>
    void log_warning(std::string_view fmt, Args&&... args) const {
        log(log_level::warn, std::move(fmt), std::forward<Args>(args)...);
    }
    template <typename... Args>
    void log_info(std::string_view fmt, Args&&... args) const {
        log(log_level::info, std::move(fmt), std::forward<Args>(args)...);
    }
    template <typename... Args>
    void log_debug(std::string_view fmt, Args&&... args) const {
        log(log_level::debug, std::move(fmt), std::forward<Args>(args)...);
    }
    template <typename... Args>
    void log_trace(std::string_view fmt, Args&&... args) const {
        log(log_level::trace, std::move(fmt), std::forward<Args>(args)...);
    }
};

}