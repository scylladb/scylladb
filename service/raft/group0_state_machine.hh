/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <seastar/core/gate.hh>
#include <seastar/core/abort_source.hh>
#include <unordered_map>

#include "data_dictionary/data_dictionary.hh"
#include "keys/keys.hh"
#include "service/broadcast_tables/experimental/lang.hh"
#include "raft/raft.hh"
#include "service/raft/group0_state_id_handler.hh"
#include "mutation/canonical_mutation.hh"
#include "mutation/mutation.hh"
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


/// Collects updates to the group0 state machine
/// Merges mutations of the same partition in order to minimize output mutation.
///
/// Abstracts away details like what is the optimal split size of the output mutation,
/// how to merge mutations of the same partition, how to store them without causing reactor stalls,
/// and what is the serialized format of the output mutations (canonical_mutation).
class group0_update_collector {
    // Upper bound on the size of a single output mutation. add() keeps the
    // accumulated per-partition mutations within this bound, and collect()
    // splits any mutation that still exceeds it.
    const size_t _max_mutation_size;

    // Currently accumulating mutation per partition, together with a running
    // upper-bound estimate of its size. Kept within _max_mutation_size by add().
    std::unordered_map<mutation, size_t, mutation_hash_by_key, mutation_equals_by_key> _mutations;

    // Mutations that reached _max_mutation_size and are no longer merged into,
    // together with their running upper-bound size estimate.
    utils::chunked_vector<std::pair<mutation, size_t>> _closed_mutations;
    utils::chunked_vector<canonical_mutation> _frozen_mutations;
    uint64_t _change_counter = 0;

    // Locates the accumulating mutation that `m` should be merged into,
    // accounting its size against it. Returns nullptr if `m` was instead stored
    // as a new accumulation bucket - either because there was none for its
    // partition, or the existing one was sealed to keep it within
    // _max_mutation_size. In the nullptr cases `m` is moved-from.
    mutation* locate_for_merge(mutation& m);

public:
    /// Default upper bound on the size of a single output canonical_mutation.
    /// Should be large enough to amortize the per canonical_mutation memory overhead,
    /// but small enough to avoid reactor stalls when (de)serializing and applying the mutation to memtables.
    /// There is a lot of code which calls non-yielding canonical_mutation::to_mutation() and canonical_mutation(cons mutation&)
    /// on group0 command fragments. Until that's fixed, the splitting before group0 command is built must be there.
    static constexpr size_t default_max_mutation_size = 1 * 1024 * 1024;

    explicit group0_update_collector(size_t max_mutation_size = default_max_mutation_size)
        : _max_mutation_size(max_mutation_size) {}

    /// Adds a mutation to the collector.
    /// The mutation is merged with already collected mutations of the same
    /// partition, without letting the accumulated mutation grow beyond
    /// max_mutation_size.
    future<> add(mutation);

    /// Like add() but assumes the mutation is small-enough to not need yielding.
    /// For places which don't want to defer due to future<>.
    void add_small(mutation);

    /// Like add() but assumes the mutation is large-enough to not need merging.
    /// It may be split later in collect().
    void add_large(mutation);

    /// Adds a canonical mutation to the collector.
    /// Not merged with other mutations and not split.
    /// This is available for interoperability with old code, prefer add(mutation).
    void add(canonical_mutation);

    /// Adds a canonical mutation to the collector, without merging or splitting.
    /// Vector-like alias for add(canonical_mutation), so that call sites which
    /// used to emplace_back() into a canonical_mutation vector stay unchanged.
    void emplace_back(canonical_mutation&& cm) {
        add(std::move(cm));
    }

    /// Adds a vector of canonical mutations to the collector.
    /// Not merged with other mutations and not split.
    /// This is available for interoperability with old code, prefer add(mutation).
    void add(utils::chunked_vector<canonical_mutation>);

    /// Returns a reference to the vector of canonical_mutations that have been added to the collector.
    /// This is available for interoperability with old code, prefer add(mutation).
    utils::chunked_vector<canonical_mutation>& frozen_mutations() {
        return _frozen_mutations;
    }

    /// Converts accumulated mutations into a vector of canonical_mutations.
    /// Any collected mutation whose (in-memory) size exceeds max_mutation_size
    /// is split into several smaller mutations of the same partition, so that no
    /// single output mutation is much larger than max_mutation_size.
    /// It's a destructive conversion, the collected mutations are cleared.
    future<utils::chunked_vector<canonical_mutation>> collect();

    /// Clears the collected mutations.
    void clear();

    /// The change counter is incremented each time observable state of the collector changes.
    uint64_t change_counter() const {
        return _change_counter;
    }

    bool empty() const {
        return _mutations.empty() && _closed_mutations.empty() && _frozen_mutations.empty();
    }
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
    group0_state_id_handler _state_id_handler;
    gms::feature_service& _feature_service;

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
    future<> sync_audit_known_entities();
public:
    group0_state_machine(raft_group0_client& client, migration_manager& mm, storage_proxy& sp, storage_service& ss,
            gms::gossiper& gossiper, gms::feature_service& feat, bool enable_immediately);
    future<> apply(raft::log_entry_ptr_list commands) override;
    future<raft::snapshot_id> take_snapshot() override;
    void drop_snapshot(raft::snapshot_id id) override;
    future<> load_snapshot(raft::snapshot_id id) override;
    future<> transfer_snapshot(raft::server_id from_id, raft::snapshot_descriptor snp) override;
    future<> abort() override;
    future<> enable_in_memory_state_machine();
};

bool should_flush_system_topology_after_applying(const mutation& mut, const data_dictionary::database db);

// Used to write mutations directly to the database, bypassing in-memory
// state machine processing. Used for topology tables during normal operation
// and also for schema tables during early raft log replay (before
// non-system keyspaces are loaded).
future<> write_mutations_to_database(storage_service& storage_service, storage_proxy& proxy, gms::inet_address from, utils::chunked_vector<canonical_mutation> cms);

} // end of namespace service
