/*
 * Copyright (C) 2022-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <optional>
#include <seastar/core/coroutine.hh>
#include "raft_group0_client.hh"

#include "frozen_schema.hh"
#include "schema_mutations.hh"
#include "service/broadcast_tables/experimental/lang.hh"
#include "idl/experimental/broadcast_tables_lang.dist.hh"
#include "idl/experimental/broadcast_tables_lang.dist.impl.hh"
#include "idl/group0_state_machine.dist.hh"
#include "idl/group0_state_machine.dist.impl.hh"
#include "service/raft/group0_state_machine.hh"
#include "replica/database.hh"


namespace service {

static logging::logger logger("group0_client");

/* *** Linearizing group 0 operations ***
 *
 * Group 0 changes (e.g. schema changes) are performed through Raft commands, which are executing in the same order
 * on every node, according to the order they appear in the Raft log
 * (executing a command happens in `group0_state_machine::apply`).
 * The commands contain mutations which modify tables that store group 0 state.
 *
 * However, constructing these mutations often requires reading the current state and validating the change against it.
 * This happens outside the code which applies the commands in order and may race with it. At the moment of applying
 * a command, the mutations stored within may be 'invalid' because a different command managed to be concurrently applied,
 * changing the state.
 *
 * For example, consider the sequence of commands:
 *
 * C1, C2, C3.
 *
 * Suppose that mutations inside C2 were constructed on a node which already applied C1. Thus, when applying C2,
 * the state of group 0 is the same as when the change was validated and its mutations were constructed.
 *
 * On the other hand, suppose that mutations inside C3 were also constructed on a node which applied C1, but didn't
 * apply C2 yet. This could easily happen e.g. when C2 and C3 were constructed concurrently on two different nodes.
 * Thus, when applying C3, the state of group 0 is different than it was when validating the change and constructing
 * its mutations: the state consists of the changes from C1 and C2, but when C3 was created, it used the state consisting
 * of changes from C1 (but not C2). Thus the mutations in C3 are not valid and we must not apply them.
 *
 * To protect ourselves from applying such 'obsolete' changes, we detect such commands during `group0_state_machine:apply`
 * and skip their mutations.
 *
 * For this, group 0 state was extended with a 'history table' (system.group0_history), which stores a sequence of
 * 'group 0 state IDs' (which are timeuuids). Each group 0 command also holds a unique state ID; if the command is successful,
 * the ID is appended to the history table. Each command also stores a 'previous state ID'; the change described by the command
 * is only applied when this 'previous state ID' is equal to the last state ID in the history table. If it's different,
 * we skip the change.
 *
 * To perform a group 0 change the user must first read the last state ID from the history table. This happens by obtaining
 * a `group0_guard` through `migration_manager::start_group0_operation`; the observed last state ID is stored in
 * `_observed_group0_state_id`. `start_group0_operation` also generates a new state ID for this change and stores it in
 * `_new_group0_state_id`. We ensure that the new state ID is greater than the observed state ID (in timeuuid order).
 *
 * The user then reads group 0 state, validates the change against the observed state, and constructs the mutations
 * which modify group 0 state. Finally, the user calls `announce`, passing the mutations and the guard.
 *
 * `announce` constructs a command for the group 0 state machine. The command stores the mutations and the state IDs.
 *
 * When the command is applied, we compare the stored observed state ID against the last state ID in the history table.
 * If it's the same, that means no change happened in between - no other command managed to 'sneak in' between the moment
 * the user started the operation and the moment the command was applied.
 *
 * The user must use `group0_guard::write_timestamp()` when constructing the mutations. The timestamp is extracted
 * from the new state ID. This ensures that mutations applied by successful commands have monotonic timestamps.
 * Indeed: the state IDs of successful commands are increasing (the previous state ID of a command that is successful
 * is equal to the new state ID of the previous successful command, and we ensure that the new state ID of a command
 * is greater than the previous state ID of this command).
 *
 * To perform a linearized group 0 read the user must also obtain a `group0_guard`. This ensures that all previously
 * completed changes are visible on this node, as obtaining the guard requires performing a Raft read barrier.
 *
 * Furthermore, obtaining the guard ensures that we don't read partial state, since it holds a lock that is also taken
 * during command application (`_read_apply_mutex_holder`). The lock is released just before sending the command to Raft.
 * TODO: we may still read partial state if we crash in the middle of command application.
 * See `group0_state_machine::apply` for a proposed fix.
 *
 * Obtaining the guard also ensures that there is no concurrent group 0 operation running on this node using another lock
 * (`_operation_mutex_holder`); if we allowed multiple concurrent operations to run, some of them could fail
 * due to the state ID protection. Concurrent operations may still run on different nodes. This lock is thus used
 * for improving liveness of operations running on the same node by serializing them.
 */
struct group0_guard::impl {
    semaphore_units<> _operation_mutex_holder;
    semaphore_units<> _read_apply_mutex_holder;

    utils::UUID _observed_group0_state_id;
    utils::UUID _new_group0_state_id;

    rwlock::holder _upgrade_lock_holder;
    bool _raft_enabled;

    impl(const impl&) = delete;
    impl& operator=(const impl&) = delete;

    impl(semaphore_units<> operation_mutex_holder, semaphore_units<> read_apply_mutex_holder, utils::UUID observed_group0_state_id, utils::UUID new_group0_state_id, rwlock::holder upgrade_lock_holder, bool raft_enabled)
        : _operation_mutex_holder(std::move(operation_mutex_holder)), _read_apply_mutex_holder(std::move(read_apply_mutex_holder))
        , _observed_group0_state_id(observed_group0_state_id), _new_group0_state_id(new_group0_state_id)
        , _upgrade_lock_holder(std::move(upgrade_lock_holder)), _raft_enabled(raft_enabled)
    {}

    void release_read_apply_mutex() {
        assert(_read_apply_mutex_holder.count() == 1);
        _read_apply_mutex_holder.return_units(1);
    }
};

group0_guard::group0_guard(std::unique_ptr<impl> p) : _impl(std::move(p)) {}

group0_guard::~group0_guard() = default;

group0_guard::group0_guard(group0_guard&&) noexcept = default;

group0_guard& group0_guard::operator=(group0_guard&&) noexcept = default;

utils::UUID group0_guard::observed_group0_state_id() const {
    return _impl->_observed_group0_state_id;
}

utils::UUID group0_guard::new_group0_state_id() const {
    return _impl->_new_group0_state_id;
}

api::timestamp_type group0_guard::write_timestamp() const {
    return utils::UUID_gen::micros_timestamp(_impl->_new_group0_state_id);
}

bool group0_guard::with_raft() const {
    return _impl->_raft_enabled;
}

void release_guard(group0_guard guard) {}

void raft_group0_client::set_history_gc_duration(gc_clock::duration d) {
    _history_gc_duration = d;
}

semaphore& raft_group0_client::operation_mutex() {
    return _operation_mutex;
}

future<> raft_group0_client::add_entry(group0_command group0_cmd, group0_guard guard, seastar::abort_source* as,
        std::optional<raft_timeout> timeout)
{
    if (this_shard_id() != 0) {
        // This should not happen since all places which construct `group0_guard` also check that they are on shard 0.
        // Note: `group0_guard::impl` is private to this module, making this easy to verify.
        on_internal_error(logger, "add_entry: must run on shard 0");
    }

    auto new_group0_state_id = guard.new_group0_state_id();

    co_await [&, guard = std::move(guard)] () -> future<> { // lambda is needed to limit guard's lifetime
        raft::command cmd;
        ser::serialize(cmd, group0_cmd);

        // Release the read_apply mutex so `group0_state_machine::apply` can take it.
        guard._impl->release_read_apply_mutex();

        bool retry;
        do {
            retry = false;
            try {
                co_await _raft_gr.group0_with_timeouts().add_entry(cmd, raft::wait_type::applied, as, timeout);
            } catch (const raft::dropped_entry& e) {
                logger.warn("add_entry: returned \"{}\". Retrying the command (prev_state_id: {}, new_state_id: {})",
                        e, group0_cmd.prev_state_id, group0_cmd.new_state_id);
                retry = true;
            } catch (const raft::commit_status_unknown& e) {
                logger.warn("add_entry: returned \"{}\". Retrying the command (prev_state_id: {}, new_state_id: {})",
                        e, group0_cmd.prev_state_id, group0_cmd.new_state_id);
                retry = true;
            } catch (const raft::not_a_leader& e) {
                // This should not happen since follower-to-leader entry forwarding is enabled in group 0.
                // Just fail the operation by propagating the error.
                logger.error("add_entry: unexpected `not_a_leader` error: \"{}\". Please file an issue.", e);
                throw;
            }

            // Thanks to the `prev_state_id` check in `group0_state_machine::apply`, the command is idempotent.
            // It's safe to retry it, even if it means it will be applied multiple times; only the first time
            // can have an effect.
        } while (retry);

        // dropping the guard releases `_group0_operation_mutex`, allowing other operations
        // on this node to proceed
    } ();

    if (!(co_await _sys_ks.group0_history_contains(new_group0_state_id))) {
        // The command was applied but the history table does not contain the new group 0 state ID.
        // This means `apply` skipped the change due to previous state ID mismatch.
        throw group0_concurrent_modification{};
    }
}

future<> raft_group0_client::add_entry_unguarded(group0_command group0_cmd, seastar::abort_source* as) {
    if (this_shard_id() != 0) {
        on_internal_error(logger, "add_entry_unguarded: must run on shard 0");
    }

    raft::command cmd;
    ser::serialize(cmd, group0_cmd);

    // Command is not retried, because for now it's not idempotent.
    try {
        co_await _raft_gr.group0().add_entry(cmd, raft::wait_type::applied, as);
    } catch (const raft::not_a_leader& e) {
        // This should not happen since follower-to-leader entry forwarding is enabled in group 0.
        // Just fail the operation by propagating the error.
        logger.error("add_entry_unguarded: unexpected `not_a_leader` error: \"{}\". Please file an issue.", e);
        throw;
    }
}

static utils::UUID generate_group0_state_id(utils::UUID prev_state_id) {
    auto ts = api::new_timestamp();
    if (prev_state_id != utils::UUID{}) {
        auto lower_bound = utils::UUID_gen::micros_timestamp(prev_state_id);
        if (ts <= lower_bound) {
            ts = lower_bound + 1;
        }
    }
    return utils::UUID_gen::get_random_time_UUID_from_micros(std::chrono::microseconds{ts});
}

future<group0_guard> raft_group0_client::start_operation(seastar::abort_source* as, std::optional<raft_timeout> timeout) {
    if (this_shard_id() != 0) {
        on_internal_error(logger, "start_group0_operation: must run on shard 0");
    }

    if (_maintenance_mode) {
        throw exceptions::configuration_exception{"cannot start group0 operation in the maintenance mode"};
    }

    auto [upgrade_lock_holder, upgrade_state] = co_await get_group0_upgrade_state();
    switch (upgrade_state) {
        case group0_upgrade_state::use_post_raft_procedures: {
            auto operation_holder = co_await get_units(_operation_mutex, 1);
            co_await _raft_gr.group0_with_timeouts().read_barrier(as, timeout);

            // Take `_group0_read_apply_mutex` *after* read barrier.
            // Read barrier may wait for `group0_state_machine::apply` which also takes this mutex.
            auto read_apply_holder = co_await hold_read_apply_mutex();

            auto observed_group0_state_id = co_await _sys_ks.get_last_group0_state_id();
            auto new_group0_state_id = generate_group0_state_id(observed_group0_state_id);

            co_return group0_guard {
                std::make_unique<group0_guard::impl>(
                    std::move(operation_holder),
                    std::move(read_apply_holder),
                    observed_group0_state_id,
                    new_group0_state_id,
                    // Not holding any lock in this case, but move the upgrade lock holder for consistent code
                    std::move(upgrade_lock_holder),
                    true
                )
            };
       }

        case group0_upgrade_state::synchronize:
            throw std::runtime_error{
                "Cannot perform schema or topology changes during this time; the cluster is currently upgrading to use Raft for schema operations."
                " If this error keeps happening, check the logs of your nodes to learn the state of upgrade. The upgrade procedure may get stuck"
                " if there was a node failure."};

        case group0_upgrade_state::recovery:
            logger.warn("starting operation in RECOVERY mode (using old procedures)");
            [[fallthrough]];
        case group0_upgrade_state::use_pre_raft_procedures:
            co_return group0_guard {
                std::make_unique<group0_guard::impl>(
                    semaphore_units<>{},
                    semaphore_units<>{},
                    utils::UUID{},
                    generate_group0_state_id(utils::UUID{}),
                    std::move(upgrade_lock_holder),
                    false
                )
            };
    }
}

template<typename Command>
requires std::same_as<Command, schema_change> || std::same_as<Command, topology_change> || std::same_as<Command, write_mutations>
group0_command raft_group0_client::prepare_command(Command change, group0_guard& guard, std::string_view description) {
    group0_command group0_cmd {
        .change{std::move(change)},
        .history_append{db::system_keyspace::make_group0_history_state_id_mutation(
            guard.new_group0_state_id(), _history_gc_duration, description)},

        // IMPORTANT: the retry mechanism below assumes that `prev_state_id` is engaged (not nullopt).
        // Here it is: the return type of `guard.observerd_group0_state_id()` is `utils::UUID`.
        .prev_state_id{guard.observed_group0_state_id()},
        .new_state_id{guard.new_group0_state_id()},

        .creator_addr{_sys_ks.local_db().get_token_metadata().get_topology().my_address()},
        .creator_id{_raft_gr.group0().id()}
    };

    return group0_cmd;
}

template<typename Command>
requires std::same_as<Command, broadcast_table_query> || std::same_as<Command, write_mutations>
group0_command raft_group0_client::prepare_command(Command change, std::string_view description) {
    const auto new_group0_state_id = generate_group0_state_id(utils::UUID{});

    group0_command group0_cmd {
        .change{std::move(change)},
        .history_append{db::system_keyspace::make_group0_history_state_id_mutation(
            new_group0_state_id, _history_gc_duration, description)},

        .prev_state_id{std::nullopt},
        .new_state_id{new_group0_state_id},

        .creator_addr{_sys_ks.local_db().get_token_metadata().get_topology().my_address()},
        .creator_id{_raft_gr.group0().id()}
    };

    return group0_cmd;
}

raft_group0_client::raft_group0_client(service::raft_group_registry& raft_gr, db::system_keyspace& sys_ks, maintenance_mode_enabled maintenance_mode)
        : _raft_gr(raft_gr), _sys_ks(sys_ks), _maintenance_mode(maintenance_mode) {
}

size_t raft_group0_client::max_command_size() const {
    return _raft_gr.group0().max_command_size();
}

future<> raft_group0_client::init() {
    auto value = [] (std::optional<sstring> s) {
        if (!s || *s == "use_pre_raft_procedures") {
            return service::group0_upgrade_state::use_pre_raft_procedures;
        } else if (*s == "synchronize") {
            return service::group0_upgrade_state::synchronize;
        } else if (*s == "use_post_raft_procedures") {
            return service::group0_upgrade_state::use_post_raft_procedures;
        } else if (*s == "recovery") {
            return service::group0_upgrade_state::recovery;
        }

        logger.error(
                "load_group0_upgrade_state(): unknown value '{}' for key 'group0_upgrade_state' in Scylla local table."
                " Did you change the value manually?"
                " Correct values are: 'use_pre_raft_procedures', 'synchronize', 'use_post_raft_procedures', 'recovery'."
                " Assuming 'recovery'.", *s);
        // We don't call `on_internal_error` which would probably prevent the node from starting, but enter `recovery`
        // allowing the user to fix their cluster.
        return service::group0_upgrade_state::recovery;
    };

    _upgrade_state = _maintenance_mode
        ? group0_upgrade_state::recovery
        : value(co_await _sys_ks.load_group0_upgrade_state());
    if (_upgrade_state == group0_upgrade_state::recovery) {
        logger.warn("RECOVERY mode.");
    }
}

future<std::pair<rwlock::holder, group0_upgrade_state>> raft_group0_client::get_group0_upgrade_state() {
    auto holder = co_await _upgrade_lock.hold_read_lock();

    if (_upgrade_state == group0_upgrade_state::use_pre_raft_procedures) {
        co_return std::pair{std::move(holder), _upgrade_state};
    }

    co_return std::pair{rwlock::holder{}, _upgrade_state};
}

future<> raft_group0_client::set_group0_upgrade_state(group0_upgrade_state state) {
    // We could explicitly handle abort here but we assume that if someone holds the lock,
    // they will eventually finish (say, due to abort) and release it.
    auto holder = co_await _upgrade_lock.hold_write_lock();

    auto value = [] (group0_upgrade_state s) constexpr {
        switch (s) {
            case service::group0_upgrade_state::use_post_raft_procedures:
                return "use_post_raft_procedures";
            case service::group0_upgrade_state::synchronize:
                return "synchronize";
            case service::group0_upgrade_state::recovery:
                // It should not be necessary to ever save this state internally - the user sets it manually
                // (e.g. from cqlsh) if recovery is needed - but handle the case anyway.
                return "recovery";
            case service::group0_upgrade_state::use_pre_raft_procedures:
                // It should not be necessary to ever save this state, but handle the case anyway.
                return "use_pre_raft_procedures";
        }

        on_internal_error(logger, format(
                "save_group0_upgrade_state: given value is outside the set of possible values (integer value: {})."
                " This may have been caused by undefined behavior; best restart your system.",
                static_cast<uint8_t>(s)));
    };

    co_await _sys_ks.save_group0_upgrade_state(value(state));
    _upgrade_state = state;
    if (_upgrade_state == group0_upgrade_state::use_post_raft_procedures) {
        _upgraded.broadcast();
    }
}

future<> raft_group0_client::wait_until_group0_upgraded(abort_source& as) {
    auto sub = as.subscribe([this] () noexcept { _upgraded.broadcast(); });
    if (!sub) {
        throw abort_requested_exception{};
    }

    co_await _upgraded.wait([this, &as, sub = std::move(sub)] {
        return _upgrade_state == group0_upgrade_state::use_post_raft_procedures || as.abort_requested();
    });

    if (as.abort_requested()) {
        throw abort_requested_exception{};
    }
}

future<semaphore_units<>> raft_group0_client::hold_read_apply_mutex() {
    if (this_shard_id() != 0) {
        on_internal_error(logger, "hold_read_apply_mutex: must run on shard 0");
    }

    return get_units(_read_apply_mutex, 1);
}

future<semaphore_units<>> raft_group0_client::hold_read_apply_mutex(abort_source& as) {
    if (this_shard_id() != 0) {
        on_internal_error(logger, "hold_read_apply_mutex: must run on shard 0");
    }

    return get_units(_read_apply_mutex, 1, as);
}

db::system_keyspace& raft_group0_client::sys_ks() {
    return _sys_ks;
}

bool raft_group0_client::in_recovery() const {
    return _upgrade_state == group0_upgrade_state::recovery;
}

raft_group0_client::query_result_guard::query_result_guard(utils::UUID query_id, raft_group0_client& client)
    : _query_id{query_id}, _client{&client} {
    auto [_, emplaced] = _client->_results.emplace(_query_id, std::nullopt);
    if (!emplaced) {
        on_internal_error(logger, "query_result_guard::query_result_guard: there is another query_result_guard alive with the same query_id");
    }
}

raft_group0_client::query_result_guard::query_result_guard(raft_group0_client::query_result_guard&& other)
    : _query_id{other._query_id}, _client{other._client} {
    other._client = nullptr;
}

raft_group0_client::query_result_guard::~query_result_guard() {
    if (_client != nullptr) {
        _client->_results.erase(_query_id);
    }
}

service::broadcast_tables::query_result raft_group0_client::query_result_guard::get() {
    auto it = _client->_results.find(_query_id);

    if (it == _client->_results.end() || !it->second.has_value()) {
        on_internal_error(logger, "query_result_guard::get: no result");
    }

    return std::move(*it->second);
}

raft_group0_client::query_result_guard raft_group0_client::create_result_guard(utils::UUID query_id) {
    return query_result_guard{query_id, *this};
}

void raft_group0_client::set_query_result(utils::UUID query_id, service::broadcast_tables::query_result qr) {
    auto it = _results.find(query_id);
    if (it != _results.end()) {
        it->second = std::move(qr);
    }
}

template group0_command raft_group0_client::prepare_command(schema_change change, group0_guard& guard, std::string_view description);
template group0_command raft_group0_client::prepare_command(topology_change change, group0_guard& guard, std::string_view description);
template group0_command raft_group0_client::prepare_command(write_mutations change, group0_guard& guard, std::string_view description);
template group0_command raft_group0_client::prepare_command(broadcast_table_query change, std::string_view description);
template group0_command raft_group0_client::prepare_command(write_mutations change, std::string_view description);

}
