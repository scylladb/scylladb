// Copyright (C) 2024-present ScyllaDB
// SPDX-License-Identifier: AGPL-3.0-or-later

#pragma once

#include "service/tablet_migration_rpc_handler.hh"
#include "service/storage_service.hh"

namespace service {

/// Test implementation of tablet_migration_rpc_handler that calls local RPC handlers
/// instead of sending network RPCs. This allows testing the topology coordinator logic
/// in unit tests without network communication.
///
/// Usage in tests:
///   local_tablet_rpc_simulator sim(storage_service);
///   // Use sim as the RPC handler when testing topology_coordinator logic
class local_tablet_rpc_simulator : public tablet_migration_rpc_handler {
    storage_service& _storage_service;

public:
    explicit local_tablet_rpc_simulator(storage_service& ss)
        : _storage_service(ss) {}

    seastar::future<tablet_operation_repair_result> tablet_repair(
            locator::host_id dst,
            raft::server_id dst_id,
            locator::global_tablet_id tablet,
            session_id sid) override {
        // Call the local RPC handler directly
        co_return co_await _storage_service.repair_tablet(tablet, sid);
    }

    seastar::future<> tablet_stream_data(
            locator::host_id dst,
            raft::server_id dst_id,
            locator::global_tablet_id tablet) override {
        // Call the local RPC handler directly
        co_return co_await _storage_service.stream_tablet(tablet);
    }

    seastar::future<> tablet_cleanup(
            locator::host_id dst,
            raft::server_id dst_id,
            locator::global_tablet_id tablet) override {
        // Call the local RPC handler directly
        co_return co_await _storage_service.cleanup_tablet(tablet);
    }

    seastar::future<> repair_update_compaction_ctrl(
            locator::host_id dst,
            locator::global_tablet_id tablet,
            session_id sid) override {
        // For the test simulator, we don't need to actually update compaction controller
        // as there's no real compaction happening in the tests.
        // Just return success.
        co_return;
    }
};

}
