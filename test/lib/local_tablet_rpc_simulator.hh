// Copyright (C) 2026-present ScyllaDB
// SPDX-License-Identifier: AGPL-3.0-or-later

#pragma once

#include "service/tablet_migration_rpc_handler.hh"
#include "service/storage_service.hh"
#include "utils/log.hh"

namespace service {

extern logging::logger rtlogger;

/// Test implementation of tablet_migration_rpc_handler that calls local RPC handlers
/// instead of sending network RPCs. This allows testing the topology coordinator logic
/// in unit tests without network communication.
///
/// Usage in tests:
///   auto handler = std::make_unique<local_tablet_rpc_simulator>(storage_service);
///   // Pass handler to run_topology_coordinator
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
        rtlogger.debug("local_tablet_rpc_simulator: repair tablet {} on {} ({})", tablet, dst, dst_id);
        // Call the local RPC handler directly
        co_return co_await _storage_service.repair_tablet(tablet, sid);
    }

    seastar::future<> tablet_stream_data(
            locator::host_id dst,
            raft::server_id dst_id,
            locator::global_tablet_id tablet) override {
        rtlogger.debug("local_tablet_rpc_simulator: stream tablet {} to {} ({})", tablet, dst, dst_id);
        // Call the local RPC handler directly
        co_return co_await _storage_service.stream_tablet(tablet);
    }

    seastar::future<> tablet_cleanup(
            locator::host_id dst,
            raft::server_id dst_id,
            locator::global_tablet_id tablet) override {
        rtlogger.debug("local_tablet_rpc_simulator: cleanup tablet {} on {} ({})", tablet, dst, dst_id);
        // Call the local RPC handler directly
        co_return co_await _storage_service.cleanup_tablet(tablet);
    }

    seastar::future<> repair_update_compaction_ctrl(
            locator::host_id dst,
            locator::global_tablet_id tablet,
            session_id sid) override {
        rtlogger.debug("local_tablet_rpc_simulator: repair_update_compaction_ctrl for tablet {} on {}", tablet, dst);
        // For the test simulator, we don't need to actually update compaction controller
        // as there's no real compaction happening in the tests.
        // Just log and return success.
        co_return;
    }
};

}
