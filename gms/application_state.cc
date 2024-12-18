/*
 * Modified by ScyllaDB
 * Copyright 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include "gms/application_state.hh"
#include <seastar/core/sstring.hh>
#include <fmt/ostream.h>

namespace gms {

static constexpr std::string_view application_state_name(application_state state) {
    switch (state) {
    case application_state::STATUS:
        return "STATUS";
    case application_state::LOAD:
        return "LOAD";
    case application_state::SCHEMA:
        return "SCHEMA";
    case application_state::DC:
        return "DC";
    case application_state::RACK:
        return "RACK";
    case application_state::RELEASE_VERSION:
        return "RELEASE_VERSION";
    case application_state::REMOVAL_COORDINATOR:
        return "REMOVAL_COORDINATOR";
    case application_state::INTERNAL_IP:
        return "INTERNAL_IP";
    case application_state::RPC_ADDRESS:
        return "RPC_ADDRESS";
    case application_state::SEVERITY:
        return "SEVERITY";
    case application_state::NET_VERSION:
        return "NET_VERSION";
    case application_state::HOST_ID:
        return "HOST_ID";
    case application_state::TOKENS:
        return "TOKENS";
    case application_state::SUPPORTED_FEATURES:
        return "SUPPORTED_FEATURES";
    case application_state::CACHE_HITRATES:
        return "CACHE_HITRATES";
    case application_state::SCHEMA_TABLES_VERSION:
        return "SCHEMA_TABLES_VERSION";
    case application_state::RPC_READY:
        return "RPC_READY";
    case application_state::VIEW_BACKLOG:
        return "VIEW_BACKLOG";
    case application_state::SHARD_COUNT:
        return "SHARD_COUNT";
    case application_state::IGNORE_MSB_BITS:
        return "IGNOR_MSB_BITS"; /* keeping the typo in "IGNOR_MSB_BITS" (missing "E") for backward compatibility */
    case application_state::CDC_GENERATION_ID:
        return "CDC_STREAMS_TIMESTAMP"; /* not named "CDC_GENERATION_ID" for backward compatibility */
    case application_state::SNITCH_NAME:
        return "SNITCH_NAME";
    case application_state::GROUP0_STATE_ID:
        return "GROUP0_STATE_ID";

    // cases that we don'd want to handle
    case application_state::X_11_PADDING:
        break;

        // no default branch - trigger compiler error for missing enum values
    }

    return "UNKNOWN";
}

} // namespace gms

auto fmt::formatter<gms::application_state>::format(gms::application_state m, fmt::format_context& ctx) const -> decltype(ctx.out()) {
    const std::string_view name = application_state_name(m);
    return fmt::format_to(ctx.out(), "{}", name);
}
