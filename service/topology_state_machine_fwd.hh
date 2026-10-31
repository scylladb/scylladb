/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Small types from service/topology_state_machine.hh, for headers that need
// them without the full topology state machine (which includes raft/raft.hh,
// gms/feature_service.hh and more).

#include <cstdint>

namespace service {

// The version of the cluster topology (topology::version_t).
using topology_version_t = int64_t;

// The order of the requests is a priority
// order in which requests are executes in case
// there are multiple requests in the queue.
// The order tries to minimize the amount of cleanups.
enum class topology_request: uint16_t {
    replace,
    join,
    remove,
    leave,
    rebuild
};

enum class global_topology_request: uint16_t {
    new_cdc_generation,
    cleanup,
    keyspace_rf_change,
    truncate_table,

    // High priority no-operation request.
    // Used to synchronize API calls with topology coordinator.
    // Ensures that all later requests and tablet scheduler will see prior updates to group0.
    noop_request,
    snapshot_tables,
    finalize_migration,
    quiesce,
    restore_tablets,

    // Creates tablet maps for all tables of a keyspace, the first phase of a
    // vnodes-to-tablets migration. Gated by the
    // PREPARE_MIGRATION_AS_TOPOLOGY_OPERATION cluster feature.
    prepare_migration,
};

// The storage mode of a node during vnodes-to-tablets migration.
//
// When migrating a table from vnodes to tablets, each node needs to reshard
// its local SSTables on vnode boundaries. Conversely, SSTables need to be
// resharded in the opposite direction (i.e., with the static sharder) when the
// operation is rolled back. A node reshards on restart, in the direction given
// by replica_state::intended_storage_mode.
enum class storage_mode : uint16_t {
    vnodes,
    tablets,
};

} // namespace service
