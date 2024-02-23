/*
 * Copyright (C) 2022-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <memory>
#include <seastar/core/semaphore.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/condition-variable.hh>

#include "service/broadcast_tables/experimental/query_result.hh"
#include "service/raft/raft_group_registry.hh"
#include "service/raft/group0_fwd.hh"
#include "utils/UUID.hh"
#include "timestamp.hh"
#include "gc_clock.hh"
#include "service/raft/group0_state_machine.hh"
#include "db/system_keyspace.hh"
#include "service/maintenance_mode.hh"

namespace service {
// Obtaining this object means that all previously finished operations on group 0 are visible on this node.

// It is also required in order to perform group 0 changes
// See `group0_guard::impl` for more detailed explanations.
class group0_guard {
    friend class raft_group0_client;
    struct impl;
    std::unique_ptr<impl> _impl;

    group0_guard(std::unique_ptr<impl>);

public:
    ~group0_guard();
    group0_guard(group0_guard&&) noexcept;
    group0_guard& operator=(group0_guard&&) noexcept;

    utils::UUID observed_group0_state_id() const;
    utils::UUID new_group0_state_id() const;

    // Use this timestamp when creating group 0 mutations.
    api::timestamp_type write_timestamp() const;

    // Are we *actually* using group 0 yet?
    // Until the upgrade procedure finishes, we will perform operations such as schema changes using the old way,
    // but still pass the guard around to synchronize operations with the upgrade procedure.
    bool with_raft() const;

    explicit operator bool() const { return bool(_impl); }
};

void release_guard(group0_guard guard);

class group0_concurrent_modification : public std::runtime_error {
public:
    group0_concurrent_modification()
        : std::runtime_error("Failed to apply group 0 change due to concurrent modification")
    {}
};

// Singleton that exists only on shard zero. Used to post commands to group zero
class raft_group0_client {
    service::raft_group_registry& _raft_gr;
    db::system_keyspace& _sys_ks;

    // See `group0_guard::impl` for explanation of the purpose of these locks.
    semaphore _read_apply_mutex = semaphore(1);
    semaphore _operation_mutex = semaphore(1);

    gc_clock::duration _history_gc_duration = gc_clock::duration{std::chrono::duration_cast<gc_clock::duration>(std::chrono::weeks{1})};

    // `_upgrade_state` is a cached (perhaps outdated) version of the upgrade state stored on disk.
    group0_upgrade_state _upgrade_state{group0_upgrade_state::recovery}; // loaded from disk in `init()`
    seastar::rwlock _upgrade_lock;
    seastar::condition_variable _upgraded;

    std::unordered_map<utils::UUID, std::optional<service::broadcast_tables::query_result>> _results;

    maintenance_mode_enabled _maintenance_mode;

    // Guard manages the result of a single query. If it is created for a particular query,
    // then `group0_state_machine` will save the result of that query and it can be returned by the guard.
    // Guard manages the lifetime of the _results entry. It creates and destroys the entry, which state machine puts the result in.
    class query_result_guard {
        utils::UUID _query_id;
        raft_group0_client* _client;
    public:
        query_result_guard(utils::UUID query_id, raft_group0_client& client);
        query_result_guard(query_result_guard&&);
        ~query_result_guard();

        // Preconditon: set_query_result was called with query_id=this->_query_id.
        service::broadcast_tables::query_result get();
    };

public:
    raft_group0_client(service::raft_group_registry&, db::system_keyspace&, maintenance_mode_enabled);

    // Call after `system_keyspace` is initialized.
    future<> init();

    future<> add_entry(group0_command group0_cmd, group0_guard guard, seastar::abort_source* as, std::optional<raft_timeout> timeout = std::nullopt);

    future<> add_entry_unguarded(group0_command group0_cmd, seastar::abort_source* as);

    // Ensures that all previously finished operations on group 0 are visible on this node;
    // in particular, performs a Raft read barrier on group 0.
    //
    // Keep the guard for the entire duration of your operation:
    // - if the operation requires reading group 0 state (such as schema state), take the guard before doing any read.
    // - if the operation finishes with appending an entry to the group 0 log, move the guard to `add_entry`.
    // - if the operation only consists of reads (it does not append any log entry), release the guard
    //   only after the last read.
    //
    // The guard will ensure that given two operations, either:
    // 1. they won't overlap (where start and end of an operation is defined by the taking and releasing of the guard),
    // 2. or if they do, one of them will fail (throw `group0_concurrent_modification`) - the application
    //    of the entry to the group 0 state machine becomes a no-op.
    //
    // All successful operations are therefore strictly serialized.
    //
    // Call only on shard 0.
    // FIXME?: this is kind of annoying for the user.
    // we could forward the call to shard 0, have group0_guard keep a foreign_ptr to the internal data structures on shard 0,
    // and add_entry would again forward to shard 0.
    future<group0_guard> start_operation(seastar::abort_source* as, std::optional<raft_timeout> timeout = std::nullopt);

    template<typename Command>
    requires std::same_as<Command, broadcast_table_query> || std::same_as<Command, write_mutations>
    group0_command prepare_command(Command change, std::string_view description);
    template<typename Command>
    requires std::same_as<Command, schema_change> || std::same_as<Command, topology_change> || std::same_as<Command, write_mutations>
    group0_command prepare_command(Command change, group0_guard& guard, std::string_view description);
    // Checks maximum allowed serialized command size, server rejects bigger commands with command_is_too_big_error exception
    size_t max_command_size() const;

    // Returns the current group 0 upgrade state.
    //
    // The possible transitions are: `use_pre_raft_procedures` -> `synchronize` -> `use_post_raft_procedures`.
    // Once entering a state, we cannot rollback (even after a restart - the state is persisted).
    //
    // An exception to these rules is manual recovery, represented by `recovery` state.
    // It can be entered by the user manually modifying the system.local table through CQL
    // and restarting the node. In this state the node will not join group 0 or start the group 0 Raft server,
    // it will perform all operations as in `use_pre_raft_procedures`, and not attempt to perform the upgrade.
    //
    // If the returned state is `use_pre_raft_procedures`, the returned `rwlock::holder`
    // prevents the state from being changed (`set_group0_upgrade_state` will wait).
    //
    // When performing an operation that assumes the state to be `use_pre_raft_procedures`
    // (e.g. a schema change using the old method), keep the holder until your operation finishes.
    //
    // Note that we don't need to hold the lock during `synchronize` or `use_post_raft_procedures`:
    // in `synchronize` group 0 operations are disabled, and `use_post_raft_procedures` is the final
    // state so it won't change (unless through manual recovery - which should not be required
    // in the final state anyway). Thus, when `synchronize` or `use_post_raft_procedures` is returned,
    // the holder does not actually hold any lock.
    future<std::pair<rwlock::holder, group0_upgrade_state>> get_group0_upgrade_state();

    // Ensures that nobody holds any `rwlock::holder`s returned from `get_group0_upgrade_state()`
    // then changes the state to `s`.
    //
    // Should only be called either by the upgrade algorithm or the bootstrap procedure
    // and follow the correct sequence of states.
    future<> set_group0_upgrade_state(group0_upgrade_state s);

    // Wait until group 0 upgrade enters the `use_post_raft_procedures` state.
    future<> wait_until_group0_upgraded(abort_source&);

    future<semaphore_units<>> hold_read_apply_mutex();
    future<semaphore_units<>> hold_read_apply_mutex(abort_source&);

    db::system_keyspace& sys_ks();

    bool in_recovery() const;

    // for test only
    void set_history_gc_duration(gc_clock::duration d);
    semaphore& operation_mutex();

    query_result_guard create_result_guard(utils::UUID query_id);
    void set_query_result(utils::UUID query_id, service::broadcast_tables::query_result qr);
};

}
