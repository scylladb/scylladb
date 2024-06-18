/*
 * Modified by ScyllaDB
 * Copyright 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "gms/application_state.hh"
#include <seastar/core/sstring.hh>
#include <ostream>
#include <map>
#include <fmt/ostream.h>
#include "seastarx.hh"

namespace gms {

static const std::map<application_state, sstring> application_state_names = {
    {application_state::STATUS,                 "STATUS"},
    {application_state::LOAD,                   "LOAD"},
    {application_state::SCHEMA,                 "SCHEMA"},
    {application_state::DC,                     "DC"},
    {application_state::RACK,                   "RACK"},
    {application_state::RELEASE_VERSION,        "RELEASE_VERSION"},
    {application_state::REMOVAL_COORDINATOR,    "REMOVAL_COORDINATOR"},
    {application_state::INTERNAL_IP,            "INTERNAL_IP"},
    {application_state::RPC_ADDRESS,            "RPC_ADDRESS"},
    {application_state::SEVERITY,               "SEVERITY"},
    {application_state::NET_VERSION,            "NET_VERSION"},
    {application_state::HOST_ID,                "HOST_ID"},
    {application_state::TOKENS,                 "TOKENS"},
    {application_state::SUPPORTED_FEATURES,     "SUPPORTED_FEATURES"},
    {application_state::CACHE_HITRATES,         "CACHE_HITRATES"},
    {application_state::SCHEMA_TABLES_VERSION,  "SCHEMA_TABLES_VERSION"},
    {application_state::RPC_READY,              "RPC_READY"},
    {application_state::VIEW_BACKLOG,           "VIEW_BACKLOG"},
    {application_state::SHARD_COUNT,            "SHARD_COUNT"},
    {application_state::IGNORE_MSB_BITS,        "IGNOR_MSB_BITS"},
    {application_state::CDC_GENERATION_ID,      "CDC_STREAMS_TIMESTAMP"}, /* not named "CDC_GENERATION_ID" for backward compatibility */
    {application_state::SNITCH_NAME,            "SNITCH_NAME"},
};

}

auto fmt::formatter<gms::application_state>::format(gms::application_state m,
                                                    fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    std::string_view name = "UNKNOWN";
    auto it = gms::application_state_names.find(m);
    if (it != gms::application_state_names.end()) {
        name = it->second;
    }
    return fmt::format_to(ctx.out(), "{}", name);
}
