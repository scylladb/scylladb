/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <ostream>

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
    // RAFT ID is a server identifier which is maintained
    // and gossiped in addition to HOST_ID because it's truly
    // unique: any new node gets a new RAFT ID, while may keep
    // its existing HOST ID, e.g. if it's replacing an existing node.
    RAFT_SERVER_ID,
};

std::ostream& operator<<(std::ostream& os, const application_state& m);

}
