/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <optional>
#include <seastar/core/shared_ptr.hh>

namespace s3 {

struct endpoint_config {
    unsigned port;
    bool use_https;

    struct aws_config {
        // the access key of the credentials
        std::string key;
        // the secret key of the credentials
        std::string secret;
        // the security token, only for session credentials
        std::string token;
        std::string region;
    };

    std::optional<aws_config> aws;
};

using endpoint_config_ptr = seastar::lw_shared_ptr<endpoint_config>;

} // s3 namespace
