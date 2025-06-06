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

class credentials;

/**
 * @brief A specialized logger for Azure credentials operations.
 *
 * This logger automatically enriches log messages with credential context information,
 * making it easier to trace credential-related operations across different Azure
 * authentication flows.
 *
 * The logger prepends a context string (if provided) and the credential type name
 * to all log messages in the format: "[context/credential_name] message"
 *
 * @example
 * credential_logger logger("AzureVault:host1");
 * logger.info(creds, "Token refreshed successfully");
 * // Output: [AzureVault:host1/ServicePrincipalCredentials] Token refreshed successfully
 */
class credential_logger {
private:
    // A context string that is prepended to all log messages.
    // Helps to identify the owner of the credentials.
    const std::string _context;
public:
    credential_logger(const std::string& context = "") : _context(context) {}

    template <typename... Args>
    void log(const azure::credentials& creds, log_level level, std::string_view fmt, const Args&... args) const {
        if (az_creds_logger.is_enabled(level)) {
            auto prefix = _context.empty() ? "" : fmt::format("{}/", _context);
            auto msg = fmt::format(fmt::runtime(fmt), args...);
            az_creds_logger.log(level, "[{}{}] {}", prefix, creds, msg);
        }
    }
    template <typename... Args>
    void error(const azure::credentials& creds, std::string_view fmt, Args&&... args) const {
        log(creds, log_level::error, std::move(fmt), std::forward<Args>(args)...);
    }
    template <typename... Args>
    void warning(const azure::credentials& creds, std::string_view fmt, Args&&... args) const {
        log(creds, log_level::warn, std::move(fmt), std::forward<Args>(args)...);
    }
    template <typename... Args>
    void info(const azure::credentials& creds, std::string_view fmt, Args&&... args) const {
        log(creds, log_level::info, std::move(fmt), std::forward<Args>(args)...);
    }
    template <typename... Args>
    void debug(const azure::credentials& creds, std::string_view fmt, Args&&... args) const {
        log(creds, log_level::debug, std::move(fmt), std::forward<Args>(args)...);
    }
    template <typename... Args>
    void trace(const azure::credentials& creds, std::string_view fmt, Args&&... args) const {
        log(creds, log_level::trace, std::move(fmt), std::forward<Args>(args)...);
    }
};

class credentials {
protected:
    access_token token;
    credential_logger logger;
private:
    friend struct fmt::formatter<credentials>;

    virtual std::string_view get_name() const = 0;
    virtual future<> refresh(const resource_type& resource_uri) = 0;
public:
    explicit credentials(credential_logger logger) : logger(std::move(logger)) {}
    virtual ~credentials() = default;

    // Throws azure::auth_error on failure.
    future<access_token> get_access_token(const resource_type& resource_uri);
};

}

template <>
struct fmt::formatter<azure::credentials> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const azure::credentials& creds, fmt::format_context& ctxt) const {
        return fmt::format_to(ctxt.out(), "{}", creds.get_name());
    }
};