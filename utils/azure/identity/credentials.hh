/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <chrono>

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
    access_token _token;
    // An externally provided context string for logging.
    // Allows disambiguating logs from multiple instances of the same credential type.
    const sstring _logctx;
private:
    friend struct fmt::formatter<credentials>;

    virtual std::string_view get_name() const = 0;
    virtual future<> refresh(const resource_type& resource_uri) = 0;
public:
    explicit credentials(const sstring& logctx = "") : _logctx(logctx) {}
    virtual ~credentials() = default;

    // Throws azure::auth_error on failure.
    future<access_token> get_access_token(const resource_type& resource_uri);
};

}

template <>
struct fmt::formatter<azure::credentials> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const azure::credentials& creds, fmt::format_context& ctxt) const {
        if (creds._logctx.empty()) {
            return fmt::format_to(ctxt.out(), "{}", creds.get_name());
        } else {
            return fmt::format_to(ctxt.out(), "{}/{}", creds._logctx, creds.get_name());
        }
    }
};