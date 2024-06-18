/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <fmt/core.h>

namespace gms {

enum class application_state {
    STATUS = 0,
    LOAD,
    SCHEMA,
    DC,
    RACK,
    RELEASE_VERSION,
    REMOVAL_COORDINATOR,
    INTERNAL_IP,
    RPC_ADDRESS,
    X_11_PADDING, // padding specifically for 1.1
    SEVERITY,
    NET_VERSION,
    HOST_ID,
    TOKENS,
    SUPPORTED_FEATURES,
    CACHE_HITRATES,
    SCHEMA_TABLES_VERSION,
    RPC_READY,
    VIEW_BACKLOG,
    SHARD_COUNT,
    IGNORE_MSB_BITS,
    CDC_GENERATION_ID,
    SNITCH_NAME,
};

}

template <>
struct fmt::formatter<gms::application_state> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(gms::application_state, fmt::format_context& ctx) const -> decltype(ctx.out());
};
