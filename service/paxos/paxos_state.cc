/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include <seastar/core/coroutine.hh>
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

future<paxos_state::guard> paxos_state::get_cas_lock(const dht::token& key, clock_type::time_point timeout) {
    guard m(_coordinator_lock, key, timeout);
    co_await m.lock();
    co_return m;
}

future<prepare_response> paxos_state::prepare(storage_proxy& sp, db::system_keyspace& sys_ks, tracing::trace_state_ptr tr_state, schema_ptr schema,
        const query::read_command& cmd, const partition_key& key, utils::UUID ballot,
        bool only_digest, query::digest_algorithm da, clock_type::time_point timeout) {
    return utils::get_local_injector().inject("paxos_prepare_timeout", timeout, [&sp, &sys_ks, &cmd, &key, ballot, tr_state, schema, only_digest, da, timeout] {
        dht::token token = dht::get_token(*schema, key);
        utils::latency_counter lc;
        lc.start();
        return with_locked_key(token, timeout, [&sp, &sys_ks, &cmd, token, &key, ballot, tr_state, schema, only_digest, da, timeout] () mutable {
            // When preparing, we need to use the same time as "now" (that's the time we use to decide if something
            // is expired or not) across nodes, otherwise we may have a window where a Most Recent Decision shows up
            // on some replica and not others during a new proposal (in storage_proxy::begin_and_repair_paxos()), and no
            // amount of re-submit will fix this (because the node on which the commit has expired will have a
            // tombstone that hides any re-submit). See CASSANDRA-12043 for details.
            auto now_in_sec = utils::UUID_gen::unix_timestamp_in_sec(ballot);

            auto f = sys_ks.load_paxos_state(key, schema, gc_clock::time_point(now_in_sec), timeout);
            return f.then([&sp, &sys_ks, &cmd, token = std::move(token), &key, ballot, tr_state, schema, only_digest, da, timeout] (paxos_state state) {
                // If received ballot is newer that the one we already accepted it has to be accepted as well,
                // but we will return the previously accepted proposal so that the new coordinator will use it instead of
                // its own.
                if (ballot.timestamp() > state._promised_ballot.timestamp()) {
                    logger.debug("Promising ballot {}", ballot);
                    tracing::trace(tr_state, "Promising ballot {}", ballot);
                    if (utils::get_local_injector().enter("paxos_error_before_save_promise")) {
                        return make_exception_future<prepare_response>(utils::injected_error("injected_error_before_save_promise"));
                    }
                    auto f1 = futurize_invoke([&] {
                        return sys_ks.save_paxos_promise(*schema, std::ref(key), ballot, timeout);
                    });
                    auto f2 = futurize_invoke([&] {
                        return do_with(dht::partition_range_vector({dht::partition_range::make_singular({token, key})}),
                                [&sp, tr_state, schema, &cmd, only_digest, da, timeout] (const dht::partition_range_vector& prv) {
                            return sp.get_db().local().query(schema, cmd,
                                    {only_digest ? query::result_request::only_digest : query::result_request::result_and_digest, da},
                                    prv, tr_state, timeout);
                        });
                    });
                    return when_all(std::move(f1), std::move(f2)).then([state = std::move(state), only_digest, schema, &sys_ks] (auto t) mutable {
                        if (utils::get_local_injector().enter("paxos_error_after_save_promise")) {
                            return make_exception_future<prepare_response>(utils::injected_error("injected_error_after_save_promise"));
                        }
                        auto&& f1 = std::get<0>(t);
                        auto&& f2 = std::get<1>(t);
                        if (f1.failed()) {
                            f2.ignore_ready_future();
                            // Failed to save promise. Nothing we can do but throw.
                            return make_exception_future<prepare_response>(f1.get_exception());
                        }
                        std::optional<std::variant<foreign_ptr<lw_shared_ptr<query::result>>, query::result_digest>> data_or_digest;
                        if (!f2.failed()) {
                            auto&& [result, hit_rate] = f2.get();
                            if (only_digest) {
                                data_or_digest = *result->digest();
                            } else {
                                data_or_digest = std::move(make_foreign(std::move(result)));
                            }
                        } else {
                            // Don't return errors querying the current value, just debug-log them, as the caller is prepared to fall back
                            // on querying it by itself in case it's missing in the response.
                            auto ex = f2.get_exception();
                            logger.debug("Failed to get data or digest: {}. Ignored.", std::move(ex));
                        }
                        auto upgrade_if_needed = [schema = std::move(schema), &sys_ks] (std::optional<proposal> p) {
                            if (!p || p->update.schema_version() == schema->version()) {
                                return make_ready_future<std::optional<proposal>>(std::move(p));
                            }
                            // In case current schema is not the same as the schema in the proposal
                            // try to look it up first in the local schema_registry cache and upgrade
                            // the mutation using schema from the cache.
                            //
                            // If there's no schema in the cache, then retrieve persisted column mapping
                            // for that version and upgrade the mutation with it.
                            logger.debug("Stored mutation references outdated schema version. "
                                "Trying to upgrade the accepted proposal mutation to the most recent schema version.");
                            return service::get_column_mapping(sys_ks, p->update.column_family_id(), p->update.schema_version()).then([schema, p = std::move(p)] (const column_mapping& cm) {
                                return make_ready_future<std::optional<proposal>>(proposal(p->ballot, freeze(p->update.unfreeze_upgrading(schema, cm))));
                            });
                        };
                        return when_all_succeed(upgrade_if_needed(std::move(state._accepted_proposal)), upgrade_if_needed(std::move(state._most_recent_commit))).then([data_or_digest = std::move(data_or_digest)] (auto&& u) mutable {
                            return prepare_response(promise(std::move(std::get<0>(u)), std::move(std::get<1>(u)), std::move(data_or_digest)));
                        });
                    });
                } else {
                    logger.debug("Promise rejected; {} is not sufficiently newer than {}", ballot, state._promised_ballot);
                    tracing::trace(tr_state, "Promise rejected; {} is not sufficiently newer than {}", ballot, state._promised_ballot);
                    // Return the currently promised ballot (rather than, e.g., the ballot of the last
                    // accepted proposal) so the coordinator can make sure it uses a newer ballot next
                    // time (#5667).
                    return make_ready_future<prepare_response>(prepare_response(std::move(state._promised_ballot)));
                }
            });
        }).finally([&sp, schema, lc] () mutable {
            auto& stats = sp.get_db().local().find_column_family(schema).get_stats();
            stats.cas_prepare.mark(lc.stop().latency());
        });
    });
}

future<bool> paxos_state::accept(storage_proxy& sp, db::system_keyspace& sys_ks, tracing::trace_state_ptr tr_state, schema_ptr schema, dht::token token, const proposal& proposal,
        clock_type::time_point timeout) {
    return utils::get_local_injector().inject("paxos_accept_proposal_timeout", timeout,
            [&sp, &sys_ks, token = std::move(token), &proposal, schema, tr_state, timeout] {
        utils::latency_counter lc;
        lc.start();
        return with_locked_key(token, timeout, [&sys_ks, &proposal, schema, tr_state, timeout] () mutable {
            auto now_in_sec = utils::UUID_gen::unix_timestamp_in_sec(proposal.ballot);
            auto f = sys_ks.load_paxos_state(proposal.update.key(), schema, gc_clock::time_point(now_in_sec), timeout);
            return f.then([&sys_ks, &proposal, tr_state, schema, timeout] (paxos_state state) {
                // Accept the proposal if we promised to accept it or the proposal is newer than the one we promised.
                // Otherwise the proposal was cutoff by another Paxos proposer and has to be rejected.
                if (proposal.ballot == state._promised_ballot || proposal.ballot.timestamp() > state._promised_ballot.timestamp()) {
                    logger.debug("Accepting proposal {}", proposal);
                    tracing::trace(tr_state, "Accepting proposal {}", proposal);

                    if (utils::get_local_injector().enter("paxos_error_before_save_proposal")) {
                        return make_exception_future<bool>(utils::injected_error("injected_error_before_save_proposal"));
                    }

                    return sys_ks.save_paxos_proposal(*schema, proposal, timeout).then([] {
                        if (utils::get_local_injector().enter("paxos_error_after_save_proposal")) {
                            return make_exception_future<bool>(utils::injected_error("injected_error_after_save_proposal"));
                        }
                        return make_ready_future<bool>(true);
                    });
                } else {
                    logger.debug("Rejecting proposal for {} because in_progress is now {}", proposal, state._promised_ballot);
                    tracing::trace(tr_state, "Rejecting proposal for {} because in_progress is now {}", proposal, state._promised_ballot);
                    return make_ready_future<bool>(false);
                }
            });
        }).finally([&sp, schema, lc] () mutable {
            auto& stats = sp.get_db().local().find_column_family(schema).get_stats();
            stats.cas_accept.mark(lc.stop().latency());
        });
    });
}

future<> paxos_state::learn(storage_proxy& sp, db::system_keyspace& sys_ks, schema_ptr schema, proposal decision, clock_type::time_point timeout,
        tracing::trace_state_ptr tr_state) {
    if (utils::get_local_injector().enter("paxos_error_before_learn")) {
        return make_exception_future<>(utils::injected_error("injected_error_before_learn"));
    }

    utils::latency_counter lc;
    lc.start();

    return do_with(std::move(decision), [&sp, &sys_ks, tr_state = std::move(tr_state), schema, timeout] (proposal& decision) {
        auto f = utils::get_local_injector().inject("paxos_state_learn_timeout", timeout);

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
            f = f.then([&sp, schema, &decision, timeout, tr_state] {
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

                return sp.mutate_locally(schema, decision.update, tr_state, db::commitlog::force_sync::yes, timeout);
            });
        } else {
            logger.debug("Not committing decision {} as ballot timestamp predates last truncation time", decision);
            tracing::trace(tr_state, "Not committing decision {} as ballot timestamp predates last truncation time", decision);
        }
        return f.then([&sys_ks, &decision, schema, timeout] {
            // We don't need to lock the partition key if there is no gap between loading paxos
            // state and saving it, and here we're just blindly updating.
            return utils::get_local_injector().inject("paxos_timeout_after_save_decision", timeout, [&sys_ks, &decision, schema, timeout] {
                return sys_ks.save_paxos_decision(*schema, decision, timeout);
            });
        });
    }).finally([&sp, schema, lc] () mutable {
        auto& stats = sp.get_db().local().find_column_family(schema).get_stats();
        stats.cas_learn.mark(lc.stop().latency());
    });
}

future<> paxos_state::prune(db::system_keyspace& sys_ks, schema_ptr schema, const partition_key& key, utils::UUID ballot, clock_type::time_point timeout,
        tracing::trace_state_ptr tr_state) {
    logger.debug("Delete paxos state for ballot {}", ballot);
    tracing::trace(tr_state, "Delete paxos state for ballot {}", ballot);
    return sys_ks.delete_paxos_decision(*schema, key, ballot, timeout);
}

} // end of namespace "service::paxos"
