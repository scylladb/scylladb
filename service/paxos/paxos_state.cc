/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include <exception>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/all.hh>
#include "seastar/coroutine/exception.hh"
#include "service/storage_proxy.hh"
#include "service/paxos/proposal.hh"
#include "service/paxos/paxos_state.hh"
#include "db/system_keyspace.hh"
#include "replica/database.hh"

#include "utils/error_injection.hh"

#include "db/schema_tables.hh"
#include "service/migration_manager.hh"

namespace service::paxos {

logging::logger paxos_state::logger("paxos");
thread_local paxos_state::key_lock_map paxos_state::_paxos_table_lock;
thread_local paxos_state::key_lock_map paxos_state::_coordinator_lock;

paxos_state::key_lock_map::semaphore& paxos_state::key_lock_map::get_semaphore_for_key(const dht::token& key) {
    return _locks.try_emplace(key, 1).first->second;
}

void paxos_state::key_lock_map::release_semaphore_for_key(const dht::token& key) {
    auto it = _locks.find(key);
    if (it != _locks.end() && (*it).second.current() == 1) {
        _locks.erase(it);
    }
}

future<paxos_state::guard> paxos_state::get_replica_lock(const dht::token& key, clock_type::time_point timeout) {
    guard m(_paxos_table_lock, key, timeout);
    co_await m.lock();
    co_return m;
}

future<paxos_state::guard> paxos_state::get_cas_lock(const dht::token& key, clock_type::time_point timeout) {
    guard m(_coordinator_lock, key, timeout);
    co_await m.lock();
    co_return m;
}

future<prepare_response> paxos_state::prepare(storage_proxy& sp, db::system_keyspace& sys_ks, tracing::trace_state_ptr tr_state, schema_ptr schema,
        const query::read_command& cmd, const partition_key& key, utils::UUID ballot,
        bool only_digest, query::digest_algorithm da, clock_type::time_point timeout) {
    co_await utils::get_local_injector().inject("paxos_prepare_timeout", timeout);
    dht::token token = dht::get_token(*schema, key);
    utils::latency_counter lc;
    lc.start();

    auto stats_updater = defer([&sp, schema, lc] () mutable {
        if (auto table = sp.get_db().local().get_tables_metadata().get_table_if_exists(schema->id())) {
            auto& stats = table->get_stats();
            stats.cas_prepare.mark(lc.stop().latency());
        }
    });

    auto guard = co_await get_replica_lock(token, timeout);
    // FIXME: Handle tablet intra-node migration: #16594.
    // The shard can change concurrently, so we cannot rely on locking on this shard.

    // When preparing, we need to use the same time as "now" (that's the time we use to decide if something
    // is expired or not) across nodes, otherwise we may have a window where a Most Recent Decision shows up
    // on some replica and not others during a new proposal (in storage_proxy::begin_and_repair_paxos()), and no
    // amount of re-submit will fix this (because the node on which the commit has expired will have a
    // tombstone that hides any re-submit). See CASSANDRA-12043 for details.
    auto now_in_sec = utils::UUID_gen::unix_timestamp_in_sec(ballot);

    paxos_state state = co_await sys_ks.load_paxos_state(key, schema, gc_clock::time_point(now_in_sec), timeout);
    // If received ballot is newer that the one we already accepted it has to be accepted as well,
    // but we will return the previously accepted proposal so that the new coordinator will use it instead of
    // its own.
    if (ballot.timestamp() > state._promised_ballot.timestamp()) {
        logger.debug("Promising ballot {}", ballot);
        tracing::trace(tr_state, "Promising ballot {}", ballot);
        if (utils::get_local_injector().enter("paxos_error_before_save_promise")) {
            co_await coroutine::return_exception(utils::injected_error("injected_error_before_save_promise"));
        }

        // The all() below throws only if save_paxos_promise fails.
        // If querying the result fails we continue without read round optimization
        auto [data_or_digest] = co_await coroutine::all(
            [&] {
                return sys_ks.save_paxos_promise(*schema, std::ref(key), ballot, timeout);
            },
            [&] () -> future<std::optional<std::variant<foreign_ptr<lw_shared_ptr<query::result>>, query::result_digest>>> {
                try {
                    auto&& [result, hit_rate] = co_await sp.get_db().local().query(schema, cmd,
                            {only_digest ? query::result_request::only_digest : query::result_request::result_and_digest, da},
                            dht::partition_range_vector({dht::partition_range::make_singular({token, key})}), tr_state, timeout);
                    if (only_digest) {
                        co_return *result->digest();
                    } else {
                        co_return make_foreign(std::move(result));
                    }
                } catch(...) {
                    logger.debug("Failed to get data or digest: {}. Ignored.", std::current_exception());
                    co_return std::nullopt;
                }
            }
        );

        if (utils::get_local_injector().enter("paxos_error_after_save_promise")) {
            co_await coroutine::return_exception(utils::injected_error("injected_error_after_save_promise"));
        }

        auto upgrade_if_needed = [schema = std::move(schema), &sys_ks] (std::optional<proposal> p) -> future<std::optional<proposal>> {
            if (!p || p->update.schema_version() == schema->version()) {
                co_return std::move(p);
            }
            // In case current schema is not the same as the schema in the proposal
            // try to look it up first in the local schema_registry cache and upgrade
            // the mutation using schema from the cache.
            //
            // If there's no schema in the cache, then retrieve persisted column mapping
            // for that version and upgrade the mutation with it.
            logger.debug("Stored mutation references outdated schema version. "
                "Trying to upgrade the accepted proposal mutation to the most recent schema version.");
            const column_mapping& cm = co_await service::get_column_mapping(sys_ks, p->update.column_family_id(), p->update.schema_version());

            co_return std::make_optional(proposal(p->ballot, freeze(p->update.unfreeze_upgrading(schema, cm))));
        };

        auto [u1, u2] = co_await coroutine::all(std::bind(upgrade_if_needed, std::move(state._accepted_proposal)), std::bind(upgrade_if_needed, std::move(state._most_recent_commit)));

        co_return prepare_response(promise(std::move(u1), std::move(u2), std::move(data_or_digest)));
    } else {
        logger.debug("Promise rejected; {} is not sufficiently newer than {}", ballot, state._promised_ballot);
        tracing::trace(tr_state, "Promise rejected; {} is not sufficiently newer than {}", ballot, state._promised_ballot);
        // Return the currently promised ballot (rather than, e.g., the ballot of the last
        // accepted proposal) so the coordinator can make sure it uses a newer ballot next
        // time (#5667).
        co_return std::move(state._promised_ballot);
    }
}

future<bool> paxos_state::accept(storage_proxy& sp, db::system_keyspace& sys_ks, tracing::trace_state_ptr tr_state, schema_ptr schema, dht::token token, const proposal& proposal,
        clock_type::time_point timeout) {
    co_await utils::get_local_injector().inject("paxos_accept_proposal_timeout", timeout);
    utils::latency_counter lc;
    lc.start();

    auto stats_updater = defer([&sp, schema, lc] () mutable {
        if (auto table = sp.get_db().local().get_tables_metadata().get_table_if_exists(schema->id())) {
            auto& stats = table->get_stats();
            stats.cas_accept.mark(lc.stop().latency());
        }
    });

    // FIXME: Handle tablet intra-node migration: #16594.
    // The shard can change concurrently, so we cannot rely on locking on this shard.
    auto guard = co_await get_replica_lock(token, timeout);

    auto now_in_sec = utils::UUID_gen::unix_timestamp_in_sec(proposal.ballot);
    paxos_state state = co_await sys_ks.load_paxos_state(proposal.update.key(), schema, gc_clock::time_point(now_in_sec), timeout);

    // Accept the proposal if we promised to accept it or the proposal is newer than the one we promised.
    // Otherwise the proposal was cutoff by another Paxos proposer and has to be rejected.
    if (proposal.ballot == state._promised_ballot || proposal.ballot.timestamp() > state._promised_ballot.timestamp()) {
        logger.debug("Accepting proposal {}", proposal);
        tracing::trace(tr_state, "Accepting proposal {}", proposal);

        if (utils::get_local_injector().enter("paxos_error_before_save_proposal")) {
            co_await coroutine::return_exception(utils::injected_error("injected_error_before_save_proposal"));
        }

        co_await sys_ks.save_paxos_proposal(*schema, proposal, timeout);

        if (utils::get_local_injector().enter("paxos_error_after_save_proposal")) {
            co_await coroutine::return_exception(utils::injected_error("injected_error_after_save_proposal"));
        }
        co_return true;
    } else {
        logger.debug("Rejecting proposal for {} because in_progress is now {}", proposal, state._promised_ballot);
        tracing::trace(tr_state, "Rejecting proposal for {} because in_progress is now {}", proposal, state._promised_ballot);
        co_return false;
    }
}

future<> paxos_state::learn(storage_proxy& sp, db::system_keyspace& sys_ks, schema_ptr schema, proposal decision, clock_type::time_point timeout,
        tracing::trace_state_ptr tr_state) {
    if (utils::get_local_injector().enter("paxos_error_before_learn")) {
        co_await coroutine::return_exception(utils::injected_error("injected_error_before_learn"));
    }

    utils::latency_counter lc;
    lc.start();

    auto stats_updater = defer([&sp, schema, lc] () mutable {
        if (auto table = sp.get_db().local().get_tables_metadata().get_table_if_exists(schema->id())) {
            auto& stats = table->get_stats();
            stats.cas_learn.mark(lc.stop().latency());
        }
    });

    co_await utils::get_local_injector().inject("paxos_state_learn_timeout", timeout);

    replica::table& cf = sp.get_db().local().find_column_family(schema);
    db_clock::time_point t = cf.get_truncation_time();
    auto truncated_at = std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch());
    // When saving a decision, also delete the last accepted proposal. This is just an
    // optimization to save space.
    // Even though there is no guarantee we will see decisions in the right order,
    // because messages can get delayed, so this decision can be older than our current most
    // recent accepted proposal/committed decision, saving it is always safe due to column timestamps.
    // Since the mutation uses the decision ballot timestamp, if cell timestamp of any current cell
    // is strictly greater than the decision one, saving the decision will not erase it.
    //
    // The table may have been truncated since the proposal was initiated. In that case, we
    // don't want to perform the mutation and potentially resurrect truncated data.
    if (utils::UUID_gen::unix_timestamp(decision.ballot) >= truncated_at) {
        logger.debug("Committing decision {}", decision);
        tracing::trace(tr_state, "Committing decision {}", decision);

        // In case current schema is not the same as the schema in the decision
        // try to look it up first in the local schema_registry cache and upgrade
        // the mutation using schema from the cache.
        //
        // If there's no schema in the cache, then retrieve persisted column mapping
        // for that version and upgrade the mutation with it.
        if (decision.update.schema_version() != schema->version()) {
            on_internal_error(logger, format("schema version in learn does not match current schema"));
        }

        co_await sp.mutate_locally(schema, decision.update, tr_state, db::commitlog::force_sync::yes, timeout);
    } else {
        logger.debug("Not committing decision {} as ballot timestamp predates last truncation time", decision);
        tracing::trace(tr_state, "Not committing decision {} as ballot timestamp predates last truncation time", decision);
    }

    // We don't need to lock the partition key if there is no gap between loading paxos
    // state and saving it, and here we're just blindly updating.
    co_await utils::get_local_injector().inject("paxos_timeout_after_save_decision", timeout);
    co_return co_await sys_ks.save_paxos_decision(*schema, decision, timeout);
}

future<> paxos_state::prune(db::system_keyspace& sys_ks, schema_ptr schema, const partition_key& key, utils::UUID ballot, clock_type::time_point timeout,
        tracing::trace_state_ptr tr_state) {
    logger.debug("Delete paxos state for ballot {}", ballot);
    tracing::trace(tr_state, "Delete paxos state for ballot {}", ballot);
    return sys_ks.delete_paxos_decision(*schema, key, ballot, timeout);
}

} // end of namespace "service::paxos"
