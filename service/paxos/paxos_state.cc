/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#include <exception>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/all.hh>
#include <seastar/coroutine/exception.hh>
#include "service/storage_proxy.hh"
#include "service/paxos/proposal.hh"
#include "service/paxos/paxos_state.hh"
#include "service/query_state.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "db/system_keyspace.hh"
#include "replica/database.hh"

#include "utils/error_injection.hh"

#include "db/schema_tables.hh"
#include "service/migration_manager.hh"

#include "idl/frozen_mutation.dist.hh"
#include "idl/frozen_mutation.dist.impl.hh"

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

future<prepare_response> paxos_state::prepare(storage_proxy& sp, paxos_store& paxos_store, tracing::trace_state_ptr tr_state, schema_ptr schema,
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

    paxos_state state = co_await paxos_store.load_paxos_state(key, schema, gc_clock::time_point(now_in_sec), timeout);
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
                return paxos_store.save_paxos_promise(*schema, std::ref(key), ballot, timeout);
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

        auto upgrade_if_needed = [schema = std::move(schema), &paxos_store] (std::optional<proposal> p) -> future<std::optional<proposal>> {
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
            const column_mapping& cm = co_await paxos_store.get_column_mapping(p->update.column_family_id(), p->update.schema_version());

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

future<bool> paxos_state::accept(storage_proxy& sp, paxos_store& paxos_store, tracing::trace_state_ptr tr_state, schema_ptr schema, dht::token token, const proposal& proposal,
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
    paxos_state state = co_await paxos_store.load_paxos_state(proposal.update.key(), schema, gc_clock::time_point(now_in_sec), timeout);

    // Accept the proposal if we promised to accept it or the proposal is newer than the one we promised.
    // Otherwise the proposal was cutoff by another Paxos proposer and has to be rejected.
    if (proposal.ballot == state._promised_ballot || proposal.ballot.timestamp() > state._promised_ballot.timestamp()) {
        logger.debug("Accepting proposal {}", proposal);
        tracing::trace(tr_state, "Accepting proposal {}", proposal);

        if (utils::get_local_injector().enter("paxos_error_before_save_proposal")) {
            co_await coroutine::return_exception(utils::injected_error("injected_error_before_save_proposal"));
        }

        co_await paxos_store.save_paxos_proposal(*schema, proposal, timeout);

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

future<> paxos_state::learn(storage_proxy& sp, paxos_store& paxos_store, schema_ptr schema, proposal decision, clock_type::time_point timeout,
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
    co_return co_await paxos_store.save_paxos_decision(*schema, decision, timeout);
}

future<> paxos_state::prune(paxos_store& paxos_store, schema_ptr schema, const partition_key& key, utils::UUID ballot, clock_type::time_point timeout,
        tracing::trace_state_ptr tr_state) {
    logger.debug("Delete paxos state for ballot {}", ballot);
    tracing::trace(tr_state, "Delete paxos state for ballot {}", ballot);
    return paxos_store.delete_paxos_decision(*schema, key, ballot, timeout);
}

static int32_t paxos_ttl_sec(const schema& s) {
    // Keep paxos state around for paxos_grace_seconds. If one of the Paxos participants
    // is down for longer than paxos_grace_seconds it is considered to be dead and must rebootstrap.
    // Otherwise its Paxos table state will be repaired by nodetool repair or Paxos repair.
    return std::chrono::duration_cast<std::chrono::seconds>(s.paxos_grace_seconds()).count();
}

paxos_store::paxos_store(db::system_keyspace& sys_ks)
: _sys_ks(sys_ks)
{
}

template <typename... Args>
future<::shared_ptr<cql3::untyped_result_set>> paxos_store::execute_cql_with_timeout(sstring req,
        db::timeout_clock::time_point timeout,
        Args&&... args) {
    const db::timeout_clock::time_point now = db::timeout_clock::now();
    const db::timeout_clock::duration d =
        now < timeout ?
            timeout - now :
            // let the `storage_proxy` time out the query down the call chain
            db::timeout_clock::duration::zero();

    struct timeout_context {
        std::unique_ptr<service::client_state> client_state;
        service::query_state query_state;
        timeout_context(db::timeout_clock::duration d)
                : client_state(std::make_unique<service::client_state>(service::client_state::internal_tag{}, timeout_config{d, d, d, d, d, d, d}))
                , query_state(*client_state, empty_service_permit())
        {}
    };
    return do_with(timeout_context(d), [this, req = std::move(req), &args...] (auto& tctx) {
        return _sys_ks.query_processor().execute_internal(req,
            ::cql3::query_options::DEFAULT.get_consistency(),
            tctx.query_state,
            { data_value(std::forward<Args>(args))... },
            ::cql3::query_processor::cache_internal::yes);
    });
}

future<column_mapping> paxos_store::get_column_mapping(table_id table_id, table_schema_version version) {
    return service::get_column_mapping(_sys_ks, table_id, version);
}

future<paxos_state> paxos_store::load_paxos_state(partition_key_view key, schema_ptr s, gc_clock::time_point now,
    db::timeout_clock::time_point timeout)
{
    static auto cql = format("SELECT * FROM system.{} WHERE row_key = ? AND cf_id = ?", db::system_keyspace::PAXOS);
    // FIXME: we need execute_cql_with_now()
    (void)now;
    auto f = execute_cql_with_timeout(cql, timeout, to_legacy(*key.get_compound_type(*s), key.representation()), s->id().uuid());
    return f.then([s, key = std::move(key)] (shared_ptr<cql3::untyped_result_set> results) mutable {
        if (results->empty()) {
            return service::paxos::paxos_state();
        }
        auto& row = results->one();
        auto promised = row.has("promise")
                        ? row.get_as<utils::UUID>("promise") : utils::UUID_gen::min_time_UUID();

        std::optional<service::paxos::proposal> accepted;
        if (row.has("proposal")) {
            accepted = service::paxos::proposal(row.get_as<utils::UUID>("proposal_ballot"),
                    ser::deserialize_from_buffer<>(row.get_blob_unfragmented("proposal"),  std::type_identity<frozen_mutation>(), 0));
        }

        std::optional<service::paxos::proposal> most_recent;
        if (row.has("most_recent_commit_at")) {
            // the value can be missing if it was pruned, supply empty one since
            // it will not going to be used anyway
            auto fm = row.has("most_recent_commit") ?
                     ser::deserialize_from_buffer<>(row.get_blob_unfragmented("most_recent_commit"), std::type_identity<frozen_mutation>(), 0) :
                     freeze(mutation(s, key));
            most_recent = service::paxos::proposal(row.get_as<utils::UUID>("most_recent_commit_at"),
                    std::move(fm));
        }

        return service::paxos::paxos_state(promised, std::move(accepted), std::move(most_recent));
    });
}

future<> paxos_store::save_paxos_promise(const schema& s, const partition_key& key, const utils::UUID& ballot, db::timeout_clock::time_point timeout) {
    static auto cql = format("UPDATE system.{} USING TIMESTAMP ? AND TTL ? SET promise = ? WHERE row_key = ? AND cf_id = ?", db::system_keyspace::PAXOS);
    return execute_cql_with_timeout(cql,
            timeout,
            utils::UUID_gen::micros_timestamp(ballot),
            paxos_ttl_sec(s),
            ballot,
            to_legacy(*key.get_compound_type(s), key.representation()),
            s.id().uuid()
        ).discard_result();
}

future<> paxos_store::save_paxos_proposal(const schema& s, const proposal& proposal, db::timeout_clock::time_point timeout) {
    static auto cql = format("UPDATE system.{} USING TIMESTAMP ? AND TTL ? SET promise = ?, proposal_ballot = ?, proposal = ? WHERE row_key = ? AND cf_id = ?", db::system_keyspace::PAXOS);
    partition_key_view key = proposal.update.key();
    return execute_cql_with_timeout(cql,
            timeout,
            utils::UUID_gen::micros_timestamp(proposal.ballot),
            paxos_ttl_sec(s),
            proposal.ballot,
            proposal.ballot,
            ser::serialize_to_buffer<bytes>(proposal.update),
            to_legacy(*key.get_compound_type(s), key.representation()),
            s.id().uuid()
        ).discard_result();
}

future<> paxos_store::save_paxos_decision(const schema& s, const proposal& decision, db::timeout_clock::time_point timeout) {
    // We always erase the last proposal when we learn about a new Paxos decision. The ballot
    // timestamp of the decision is used for entire mutation, so if the "erased" proposal is more
    // recent it will naturally stay on top.
    // Erasing the last proposal is just an optimization and does not affect correctness:
    // sp::begin_and_repair_paxos will exclude an accepted proposal if it is older than the most
    // recent commit.
    static auto cql = format("UPDATE system.{} USING TIMESTAMP ? AND TTL ? SET proposal_ballot = null, proposal = null,"
            " most_recent_commit_at = ?, most_recent_commit = ? WHERE row_key = ? AND cf_id = ?", db::system_keyspace::PAXOS);
    partition_key_view key = decision.update.key();
    return execute_cql_with_timeout(cql,
            timeout,
            utils::UUID_gen::micros_timestamp(decision.ballot),
            paxos_ttl_sec(s),
            decision.ballot,
            ser::serialize_to_buffer<bytes>(decision.update),
            to_legacy(*key.get_compound_type(s), key.representation()),
            s.id().uuid()
        ).discard_result();
}

future<> paxos_store::delete_paxos_decision(const schema& s, const partition_key& key, const utils::UUID& ballot, db::timeout_clock::time_point timeout) {
    // This should be called only if a learn stage succeeded on all replicas.
    // In this case we can remove learned paxos value using ballot's timestamp which
    // guarantees that if there is more recent round it will not be affected.
    static auto cql = format("DELETE most_recent_commit FROM system.{} USING TIMESTAMP ?  WHERE row_key = ? AND cf_id = ?", db::system_keyspace::PAXOS);

    return execute_cql_with_timeout(cql,
            timeout,
            utils::UUID_gen::micros_timestamp(ballot),
            to_legacy(*key.get_compound_type(s), key.representation()),
            s.id().uuid()
        ).discard_result();
}

} // end of namespace "service::paxos"
