/*
 * Copyright (C) 2019 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "service/storage_proxy.hh"
#include "service/paxos/proposal.hh"
#include "service/paxos/paxos_state.hh"
#include "db/system_keyspace.hh"
#include "schema_registry.hh"
#include "database.hh"

#include "service/storage_service.hh"

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

future<prepare_response> paxos_state::prepare(tracing::trace_state_ptr tr_state, schema_ptr schema,
        const query::read_command& cmd, const partition_key& key, utils::UUID ballot,
        bool only_digest, query::digest_algorithm da, clock_type::time_point timeout) {
    dht::token token = dht::get_token(*schema, key);
    utils::latency_counter lc;
    lc.start();
    return with_locked_key(token, timeout, [&cmd, token, &key, ballot, tr_state, schema, only_digest, da, timeout] () mutable {
        // When preparing, we need to use the same time as "now" (that's the time we use to decide if something
        // is expired or not) across nodes, otherwise we may have a window where a Most Recent Decision shows up
        // on some replica and not others during a new proposal (in storage_proxy::begin_and_repair_paxos()), and no
        // amount of re-submit will fix this (because the node on which the commit has expired will have a
        // tombstone that hides any re-submit). See CASSANDRA-12043 for details.
        auto now_in_sec = utils::UUID_gen::unix_timestamp_in_sec(ballot);
        auto f = db::system_keyspace::load_paxos_state(key, schema, gc_clock::time_point(now_in_sec), timeout);
        return f.then([&cmd, token = std::move(token), &key, ballot, tr_state, schema, only_digest, da, timeout] (paxos_state state) {
            // If received ballot is newer that the one we already accepted it has to be accepted as well,
            // but we will return the previously accepted proposal so that the new coordinator will use it instead of
            // its own.
            if (ballot.timestamp() > state._promised_ballot.timestamp()) {
                logger.debug("Promising ballot {}", ballot);
                tracing::trace(tr_state, "Promising ballot {}", ballot);
                auto f1 = futurize_apply(db::system_keyspace::save_paxos_promise, *schema, std::ref(key), ballot, timeout);
                auto f2 = futurize_apply([&] {
                    return do_with(dht::partition_range_vector({dht::partition_range::make_singular({token, key})}),
                            [tr_state, schema, &cmd, only_digest, da, timeout] (const dht::partition_range_vector& prv) {
                        return get_local_storage_proxy().get_db().local().query(schema, cmd,
                                {only_digest ? query::result_request::only_digest : query::result_request::result_and_digest, da},
                                prv, tr_state, query::result_memory_limiter::maximum_result_size, timeout);
                    });
                });
                return when_all(std::move(f1), std::move(f2)).then([state = std::move(state), only_digest] (auto t) {
                    auto&& f1 = std::get<0>(t);
                    auto&& f2 = std::get<1>(t);
                    if (f1.failed()) {
                        // Failed to save promise. Nothing we can do but throw.
                        return make_exception_future<prepare_response>(f1.get_exception());
                    }
                    std::optional<std::variant<foreign_ptr<lw_shared_ptr<query::result>>, query::result_digest>> data_or_digest;
                    // Silently ignore any errors querying the current value as the caller is prepared to fall back
                    // on querying it by itself in case it's missing in the response.
                    if (!f2.failed()) {
                        auto&& [result, hit_rate] = f2.get();
                        if (only_digest) {
                            data_or_digest = *result->digest();
                        } else {
                            data_or_digest = std::move(make_foreign(std::move(result)));
                        }
                    }
                    return make_ready_future<prepare_response>(prepare_response(promise(std::move(state._accepted_proposal),
                                    std::move(state._most_recent_commit), std::move(data_or_digest))));
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
    }).finally([schema, lc] () mutable {
        auto& stats = get_local_storage_proxy().get_db().local().find_column_family(schema).get_stats();
        stats.cas_prepare.mark(lc.stop().latency());
        if (lc.is_start()) {
            stats.estimated_cas_prepare.add(lc.latency(), stats.cas_prepare.hist.count);
        }
    });
}

future<bool> paxos_state::accept(tracing::trace_state_ptr tr_state, schema_ptr schema, dht::token token, const proposal& proposal,
        clock_type::time_point timeout) {
    utils::latency_counter lc;
    lc.start();
    return with_locked_key(token, timeout, [proposal = std::move(proposal), schema, tr_state, timeout] () mutable {
        auto now_in_sec = utils::UUID_gen::unix_timestamp_in_sec(proposal.ballot);
        auto f = db::system_keyspace::load_paxos_state(proposal.update.decorated_key(*schema).key(), schema,
                gc_clock::time_point(now_in_sec), timeout);
        return f.then([proposal = std::move(proposal), tr_state, schema, timeout] (paxos_state state) {
            // Accept the proposal if we promised to accept it or the proposal is newer than the one we promised.
            // Otherwise the proposal was cutoff by another Paxos proposer and has to be rejected.
            if (proposal.ballot == state._promised_ballot || proposal.ballot.timestamp() > state._promised_ballot.timestamp()) {
                logger.debug("Accepting proposal {}", proposal);
                tracing::trace(tr_state, "Accepting proposal {}", proposal);
                return db::system_keyspace::save_paxos_proposal(*schema, proposal, timeout).then([] {
                        return true;
                });
            } else {
                logger.debug("Rejecting proposal for {} because in_progress is now {}", proposal, state._promised_ballot);
                tracing::trace(tr_state, "Rejecting proposal for {} because in_progress is now {}", proposal, state._promised_ballot);
                return make_ready_future<bool>(false);
            }
        });
    }).finally([schema, lc] () mutable {
        auto& stats = get_local_storage_proxy().get_db().local().find_column_family(schema).get_stats();
        stats.cas_propose.mark(lc.stop().latency());
        if (lc.is_start()) {
            stats.estimated_cas_propose.add(lc.latency(), stats.cas_propose.hist.count);
        }
    });
}

future<> paxos_state::learn(schema_ptr schema, proposal decision, clock_type::time_point timeout,
        tracing::trace_state_ptr tr_state) {
    utils::latency_counter lc;
    lc.start();

    return do_with(std::move(decision), [tr_state = std::move(tr_state), schema, timeout] (proposal& decision) {
        auto f = make_ready_future();
        table& cf = get_local_storage_proxy().get_db().local().find_column_family(schema);
        db_clock::time_point t = cf.get_truncation_record();
        auto truncated_at = std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch()).count();
        // When saving a decision, also delete the last accepted proposal. This is just an
        // optimization to save space.
        // Even though there is no guarantee we will see decisions in the right order,
        // because messages can get delayed, so this decision can be older than our current most
        // recent accepted proposal/committed decision, saving it is always safe due to column timestamps.
        // Since the mutation uses the decision ballot timestamp, if cell timestmap of any current cell
        // is strictly greater than the decision one, saving the decision will not erase it.
        //
        // The table may have been truncated since the proposal was initiated. In that case, we
        // don't want to perform the mutation and potentially resurrect truncated data.
        if (utils::UUID_gen::unix_timestamp(decision.ballot) >= truncated_at) {
            logger.debug("Committing decision {}", decision);
            tracing::trace(tr_state, "Committing decision {}", decision);
            f = get_local_storage_proxy().mutate_locally(schema, decision.update, db::commitlog::force_sync::yes, timeout);
        } else {
            logger.debug("Not committing decision {} as ballot timestamp predates last truncation time", decision);
            tracing::trace(tr_state, "Not committing decision {} as ballot timestamp predates last truncation time", decision);
        }
        return f.then([&decision, schema, timeout] {
            // We don't need to lock the partition key if there is no gap between loading paxos
            // state and saving it, and here we're just blindly updating.
            return db::system_keyspace::save_paxos_decision(*schema, decision, timeout);
        });
    }).finally([schema, lc] () mutable {
        auto& stats = get_local_storage_proxy().get_db().local().find_column_family(schema).get_stats();
        stats.cas_commit.mark(lc.stop().latency());
        if (lc.is_start()) {
            stats.estimated_cas_commit.add(lc.latency(), stats.cas_commit.hist.count);
        }
    });
}

future<> paxos_state::prune(schema_ptr schema, const partition_key& key, utils::UUID ballot, clock_type::time_point timeout,
        tracing::trace_state_ptr tr_state) {
    logger.debug("Delete paxos state for ballot {}", ballot);
    tracing::trace(tr_state, "Delete paxos state for ballot {}", ballot);
    return db::system_keyspace::delete_paxos_decision(*schema, key, ballot, timeout);
}

} // end of namespace "service::paxos"
