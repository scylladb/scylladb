/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <string_view>
#include <unordered_map>

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>

namespace s3 {

struct aws_credentials {
    // the access key of the credentials
    std::string access_key_id;
    // the secret key of the credentials
    std::string secret_access_key;
    // the security token, only for session credentials
    std::string session_token;
    // session token expiration
    seastar::lowres_clock::time_point expires_at{seastar::lowres_clock::time_point::min()};
    explicit operator bool() const { return !access_key_id.empty() && !secret_access_key.empty(); }
    std::strong_ordering operator<=>(const aws_credentials& o) const = default;
};

// Connection cap per scheduling group, keyed by group name. All service-level
// groups share one pool under the "service_levels" key.
using group_connections = std::unordered_map<std::string, unsigned>;

// Pool shared by every group with no entry of its own. The client falls back to it and
// the storage manager's built-in table sizes it, so both have to spell it the same.
inline constexpr std::string_view general_pool_name = "general";

struct endpoint_config {
    static constexpr unsigned default_max_connections = 128;

    unsigned port;
    bool use_https;
    std::string region;
    // Amazon Resource Names (ARNs) to access AWS resources
    std::string role_arn;
    // Gives every scheduling group this cap, overriding the client's group_connections.
    std::optional<unsigned> max_connections;

    std::strong_ordering operator<=>(const endpoint_config& o) const = default;
};

using endpoint_config_ptr = seastar::lw_shared_ptr<endpoint_config>;

} // namespace s3
