# Tablet Migration RPC Decoupling

## Overview

The topology coordinator's tablet migration logic has been refactored to decouple it from RPC operations. This allows the topology coordinator logic to be tested in unit tests without network communication.

## Architecture

### Components

1. **`tablet_migration_rpc_handler`** (interface): Defines the RPC operations needed for tablet migration
   - `tablet_repair()`: Initiates tablet repair on a replica
   - `tablet_stream_data()`: Streams tablet data to a replica  
   - `tablet_cleanup()`: Cleans up tablet data on a replica
   - `repair_update_compaction_ctrl()`: Updates compaction controller after repair

2. **`messaging_tablet_rpc_handler`**: Production implementation that sends real network RPCs via `messaging_service`

3. **`local_tablet_rpc_simulator`**: Test implementation that calls RPC handlers locally without network communication

### How It Works

#### Production (Real RPCs)
```cpp
// In storage_service.cc
run_topology_coordinator(
    sys_dist_ks, gossiper, messaging, shared_tm,
    sys_ks, db, group0, topo_sm, vb_sm, as, raft,
    raft_topology_cmd_handler,
    tablet_allocator, cdc_gens, ring_delay,
    lifecycle_notifier, feature_service,
    sl_controller, topology_cmd_rpc_tracker
    // tablet_rpc_handler defaults to nullptr, so messaging_tablet_rpc_handler is used
);
```

#### Testing (Local Simulation)
```cpp
// In test code
run_topology_coordinator(
    sys_dist_ks, gossiper, messaging, shared_tm,
    sys_ks, db, group0, topo_sm, vb_sm, as, raft,
    raft_topology_cmd_handler,
    tablet_allocator, cdc_gens, ring_delay,
    lifecycle_notifier, feature_service,
    sl_controller, topology_cmd_rpc_tracker,
    std::make_unique<local_tablet_rpc_simulator>(storage_service)  // Create unique_ptr directly
);
```

## Benefits

1. **Unit Testability**: The topology coordinator's `handle_tablet_migration()` logic can now be tested in unit tests without setting up a multi-node cluster
2. **Separation of Concerns**: Coordinator-side logic (group0 state transitions, barriers) is clearly separated from replica-side logic (RPC operations)
3. **Flexibility**: Different RPC implementations can be provided for different testing scenarios

## Migration Stages

The tablet migration process goes through multiple stages. Each stage has:
- **Coordinator-side logic**: What group0 changes to make, whether barriers are needed, whether rollback is needed
- **Replica-side logic**: RPC operations to perform on tablet replicas

Example stages:
- `allow_write_both_read_old` → coordinator initiates transition
- `write_both_read_old` → coordinator waits for barrier
- `streaming` → coordinator calls `tablet_stream_data()` RPC
- `write_both_read_new` → coordinator waits for barrier
- `use_new` → coordinator transitions to final state
- `cleanup` → coordinator calls `tablet_cleanup()` RPC
- `end_migration` → coordinator removes transition state

## Future Enhancements

The current refactoring enables future enhancements such as:
1. More comprehensive testing of topology coordinator logic
2. Fault injection at the RPC level in tests
3. Better simulation of network conditions in tests
4. Cleaner separation of concerns in the codebase
