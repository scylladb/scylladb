// Copyright (C) 2024-present ScyllaDB
// SPDX-License-Identifier: AGPL-3.0-or-later

#pragma once

#include <seastar/core/future.hh>
#include "locator/tablets.hh"
#include "service/tablet_operation.hh"
#include "service/session.hh"
#include "raft/raft.hh"

namespace service {

/// Interface for handling tablet migration RPC operations.
/// This abstraction decouples the topology coordinator from actual RPC sending,
/// allowing unit tests to provide a local implementation that simulates RPC
/// behavior without network communication.
///
/// Each method corresponds to an RPC verb that the topology coordinator sends
/// to tablet replicas during migration stages.
class tablet_migration_rpc_handler {
public:
    virtual ~tablet_migration_rpc_handler() = default;

    /// Initiates tablet repair on a replica.
    /// Called during the rebuild_repair or repair stage.
    ///
    /// @param dst destination host
    /// @param dst_id destination raft server ID
    /// @param tablet tablet to repair
    /// @param sid session ID for the repair operation
    /// @return repair result containing the repair timestamp
    virtual seastar::future<tablet_operation_repair_result> tablet_repair(
        locator::host_id dst,
        raft::server_id dst_id,
        locator::global_tablet_id tablet,
        session_id sid) = 0;

    /// Streams tablet data to a replica.
    /// Called during the streaming stage.
    ///
    /// @param dst destination host
    /// @param dst_id destination raft server ID
    /// @param tablet tablet to stream
    virtual seastar::future<> tablet_stream_data(
        locator::host_id dst,
        raft::server_id dst_id,
        locator::global_tablet_id tablet) = 0;

    /// Cleans up tablet data on a replica.
    /// Called during the cleanup or cleanup_target stage.
    ///
    /// @param dst destination host
    /// @param dst_id destination raft server ID
    /// @param tablet tablet to clean up
    virtual seastar::future<> tablet_cleanup(
        locator::host_id dst,
        raft::server_id dst_id,
        locator::global_tablet_id tablet) = 0;

    /// Updates compaction controller after repair.
    /// Called during the end_repair stage for each tablet replica.
    ///
    /// @param dst destination host
    /// @param tablet tablet to update
    /// @param sid session ID from the repair operation
    virtual seastar::future<> repair_update_compaction_ctrl(
        locator::host_id dst,
        locator::global_tablet_id tablet,
        session_id sid) = 0;
};

}
