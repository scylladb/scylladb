# Tablet Migration RPC Decoupling - Implementation Summary

## What Was Changed

### Problem Statement
The topology_coordinator's `handle_tablet_migration()` function was tightly coupled with RPC sending, making it difficult to test in unit tests without setting up a full multi-node cluster.

### Solution
Introduced an abstraction layer (`tablet_migration_rpc_handler`) that decouples the coordinator logic from actual RPC operations.

## Files Added

### 1. `service/tablet_migration_rpc_handler.hh`
Interface defining the RPC operations needed for tablet migration:
- `tablet_repair()` - Repair a tablet on a replica
- `tablet_stream_data()` - Stream tablet data to a replica
- `tablet_cleanup()` - Clean up tablet data on a replica  
- `repair_update_compaction_ctrl()` - Update compaction controller after repair

### 2. `test/lib/local_tablet_rpc_simulator.hh`
Test implementation that calls local RPC handlers instead of sending network RPCs.

### 3. `docs/dev/tablet-migration-rpc-decoupling.md`
Documentation explaining the architecture and how to use it.

## Files Modified

### 1. `service/topology_coordinator.cc`
- Added `messaging_tablet_rpc_handler` class - production implementation that wraps `messaging_service` RPCs
- Updated `handle_tablet_migration()` to use `_tablet_rpc_handler` interface instead of direct RPC calls
- Updated constructor to accept optional RPC handler and initialize it
- Modified 5 locations where tablet RPCs were called:
  - rebuild_repair stage → `tablet_repair()`
  - streaming stage → `tablet_stream_data()`
  - cleanup stage → `tablet_cleanup()` (2 locations)
  - repair stage → `tablet_repair()`
  - end_repair stage → `repair_update_compaction_ctrl()`

### 2. `service/topology_coordinator.hh`
- Added forward declaration for `tablet_migration_rpc_handler`
- Updated `run_topology_coordinator()` signature to accept optional `tablet_rpc_handler` parameter

## How It Works

### Production Mode (Default)
```cpp
run_topology_coordinator(...);
// Automatically uses messaging_tablet_rpc_handler
```

The topology coordinator creates a `messaging_tablet_rpc_handler` that sends actual network RPCs.

### Test Mode
```cpp
run_topology_coordinator(..., std::make_unique<local_tablet_rpc_simulator>(storage_service));
```

Tests can provide a `local_tablet_rpc_simulator` that calls local RPC handlers directly, bypassing the network layer.

## Benefits

1. **Testability**: Can now test `handle_tablet_migration()` logic in unit tests
2. **Separation of Concerns**: Coordinator logic (group0 transitions, barriers) is separated from replica logic (RPCs)
3. **Minimal Changes**: The refactoring is minimal and surgical - only abstracts the RPC layer
4. **Backward Compatible**: Existing code continues to work without changes

## What Can Now Be Tested

With this refactoring, tests can now:
1. Verify that tablet migrations progress through the correct stages
2. Test barrier coordination between stages
3. Test rollback logic when migrations fail
4. Test the interaction between migrations and topology changes
5. Simulate various failure scenarios at the RPC level

All without needing multiple nodes or network communication.

## Future Work

This refactoring enables future enhancements:
1. More comprehensive topology coordinator unit tests
2. Fault injection at the RPC level in tests
3. Better simulation of network conditions
4. Potential extraction of more coordinator logic into testable components
