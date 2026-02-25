/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include <seastar/core/gate.hh>
#include <seastar/core/abort_source.hh>

#include "data_dictionary/data_dictionary.hh"
#include "keys/keys.hh"
#include "service/broadcast_tables/experimental/lang.hh"
#include "raft/raft.hh"
#include "service/raft/group0_state_id_handler.hh"
#include "mutation/canonical_mutation.hh"
#include "service/raft/raft_state_machine.hh"
#include "gms/feature.hh"
#include "gms/inet_address.hh"

namespace gms {
class feature_service;
}

namespace service {
class raft_group0_client;
class migration_manager;
class storage_proxy;
class storage_service;
struct group0_state_machine_merger;

struct schema_change {
    // Mutations of schema tables (such as `system_schema.keyspaces`, `system_schema.tables` etc.)
    // e.g. computed from a DDL statement (keyspace/table/type create/drop/alter etc.)
    utils::chunked_vector<canonical_mutation> mutations;
};

struct broadcast_table_query {
    service::broadcast_tables::query query;
};

struct topology_change {
    utils::chunked_vector<canonical_mutation> mutations;
};

// Allows executing combined topology & schema mutations under a single RAFT command.
// The order of the mutations doesn't matter.
struct mixed_change {
    utils::chunked_vector<canonical_mutation> mutations;
};

// This command is used to write data to tables other than topology or
// schema tables. and it updates any in-memory data structures based on
// mutations' table_id.
struct write_mutations {
    utils::chunked_vector<canonical_mutation> mutations;
};

struct group0_command {
    std::variant<schema_change, broadcast_table_query, topology_change, write_mutations, mixed_change> change;

    // Mutation of group0 history table, appending a new state ID and optionally a description.
    canonical_mutation history_append;

    // Each state of the group0 state machine has a unique ID (which is a timeuuid).
    //
    // There is only one state of the group0 state machine to which this change can be correctly applied:
    // the state which was used to validate and compute the change.
    //
    // When the change is computed, we read the state ID from the state machine and save it in the command
    // (`prev_state_id`).
    //
    // When we apply the change (in `state_machine::apply`), we verify that `prev_state_id` is still equal to the machine's state ID.
    //
    // If not, it means there was a concurrent group0 update which invalidated our change;
    // in that case we won't apply our change, effectively making the command a no-op.
    // The creator of the change must recompute it using the new state and retry (or find that the group0 update
    // they are trying to perform is no longer valid in the context of this new state).
    //
    // Otherwise we update the state ID (`new_state_id`).
    //
    // Exception: if `prev_state_id` is `nullopt`, we skip the verification step.
    // This can be used to apply group0 changes unconditionally if the caller is sure they don't conflict with each other.
    std::optional<utils::UUID> prev_state_id;
    utils::UUID new_state_id;

    // Address and Raft ID of the creator of this command. For debugging.
    gms::inet_address creator_addr;
    raft::server_id creator_id;
};

// Raft state machine implementation for managing group 0 changes (e.g. schema changes).
// NOTE: group 0 raft server is always instantiated on shard 0.
class group0_state_machine : public raft_state_machine {
    struct modules_to_reload {
        struct entry {
            partition_key pk;
            table_id table;
        };
        std::vector<entry> entries;
    };

    raft_group0_client& _client;
    migration_manager& _mm;
    storage_proxy& _sp;
    storage_service& _ss;
    seastar::named_gate _gate;
    abort_source _abort_source;
    bool _topology_change_enabled;
    group0_state_id_handler _state_id_handler;
    gms::feature_service& _feature_service;
    gms::feature::listener_registration _topology_on_raft_support_listener;

    // This boolean controls whether the in-memory data structures should be updated
    // after snapshot transfer / command application.
    //
    // The reason for the flag is to protect from reading a partially applied state.
    // A group0 command may consist of multiple mutations that are not applied
    // in a single, atomic operation, but rather separately. A node can crash
    // in the middle of applying such a command, leaving the group0 in an inconsistent
    // state. Thanks to the idempotency of mutations, applying the group0 command
    // again, fully, will make the state consistent again. Therefore, we use this
    // flag to control when the in memory state machine should be updated from the
    // on-disk state - we can only do that if we know that the group0 table state
    // is consistent.
    //
    // The only exception to the above rule is the schema - the schema state is
    // loaded into memory before group0 is initialized, and the in-memory state
    // is reloaded even if _in_memory_state_machine_enabled is set to false.
    // Resolving this exception should be possible, but would require considerable
    // effort in refactoring the migration manager code. In the meantime, we are
    // fine with this exception because the migration manager applies all schema
    // mutations of a single command atomically, in a single commitlog entry -
    // therefore, we should not observe broken invariants in the schema module.
    bool _in_memory_state_machine_enabled;

    modules_to_reload get_modules_to_reload(const utils::chunked_vector<canonical_mutation>& mutations);
    future<> reload_modules(modules_to_reload modules);
    future<> merge_and_apply(group0_state_machine_merger& merger);
    future<> reload_state();
public:
    group0_state_machine(raft_group0_client& client, migration_manager& mm, storage_proxy& sp, storage_service& ss,
            gms::gossiper& gossiper, gms::feature_service& feat, bool topology_change_enabled);
    future<> apply(std::vector<raft::command_cref> command) override;
    future<raft::snapshot_id> take_snapshot() override;
    void drop_snapshot(raft::snapshot_id id) override;
    future<> load_snapshot(raft::snapshot_id id) override;
    future<> transfer_snapshot(raft::server_id from_id, raft::snapshot_descriptor snp) override;
    future<> abort() override;
    future<> enable_in_memory_state_machine();
};

bool should_flush_system_topology_after_applying(const mutation& mut, const data_dictionary::database db);

// Used to write data to topology and other tables except schema tables.
future<> write_mutations_to_database(storage_service& storage_service, storage_proxy& proxy, gms::inet_address from, utils::chunked_vector<canonical_mutation> cms);

} // end of namespace service
