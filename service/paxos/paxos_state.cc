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
#include "schema/schema_builder.hh"

#include "utils/error_injection.hh"

#include "db/schema_tables.hh"
#include "service/migration_manager.hh"
#include "gms/feature_service.hh"

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

future<paxos_state::replica_guard> paxos_state::get_replica_lock(const dht::token& key, 
        clock_type::time_point timeout, const dht::shard_replica_set& shards)
{
    if (shards.empty()) {
        on_internal_error(logger, "empty shards");
    }
    replica_guard replica_guard;
    replica_guard.resize(shards.size());

    auto acquire = [&shards, &replica_guard, &key, timeout](unsigned index) {
        return smp::submit_to(shards[index], [&key, timeout] {
            auto g = make_lw_shared<guard>(_paxos_table_lock, key, timeout);
            return g->lock().then([g]{ return make_foreign(std::move(g)); });
        }).then([&replica_guard, index](guard_foreign_ptr g) {
            replica_guard[index] = std::move(g);
        });
    };

    co_await acquire(0);
    if (shards.size() > 1) {
        co_await acquire(1);
    }

    co_return replica_guard;
}

future<paxos_state::guard> paxos_state::get_cas_lock(const dht::token& key, clock_type::time_point timeout) {
    guard m(_coordinator_lock, key, timeout);
    co_await m.lock();
    co_return m;
}

static dht::shard_replica_set shards_for_writes(const schema& s, dht::token token) {
    auto shards = s.table().shard_for_writes(token);
    if (const auto it = std::ranges::find(shards, this_shard_id()); it == shards.end()) {
        on_internal_error(paxos_state::logger,
            format("invalid shard, this_shard_id {}, shard_for_writes {}", this_shard_id(), shards));
    }
    std::ranges::sort(shards);
    return shards;
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

    const auto shards = shards_for_writes(*schema, token);
    auto guard = co_await get_replica_lock(token, timeout, shards);

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

    const auto shards = shards_for_writes(*schema, token);
    auto guard = co_await get_replica_lock(token, timeout, shards);

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

static sstring paxos_state_cf_filter(const schema& s, const schema& state_schema) {
    return state_schema.cf_name() == db::system_keyspace::PAXOS
        ? format(" AND cf_id = {}", s.id().uuid())
        : sstring();
}

static const sstring paxos_state_table_suffix = "$paxos";

static sstring paxos_state_table_name(std::string_view table_name) {
    sstring result(sstring::initialized_later(), table_name.size() + paxos_state_table_suffix.size());
    auto it = result.begin();
    it = std::copy(table_name.begin(), table_name.end(), it);
    std::copy(paxos_state_table_suffix.begin(), paxos_state_table_suffix.end(), it);
    return result;
}

paxos_store::paxos_store(db::system_keyspace& sys_ks, gms::feature_service& features, replica::database& db, migration_manager& mm)
    : _sys_ks(sys_ks)
    , _features(features)
    , _db(db)
    , _mm(mm)
{
    if (this_shard_id() == 0) {
        _mm.get_notifier().register_listener(this);
    }
}

paxos_store::~paxos_store() {
    SCYLLA_ASSERT(_stopped);
}

future<> paxos_store::stop() {
    auto stopped = this_shard_id() == 0 
        ? _mm.get_notifier().unregister_listener(this) 
        : make_ready_future<>();
    return stopped.then([this] { _stopped = true; });
}

schema_ptr paxos_store::create_paxos_state_schema(const schema& s) {
    schema_builder builder(std::nullopt, s.ks_name(), paxos_state_table_name(s.cf_name()),
        // partition key
        {{"row_key", bytes_type}}, // byte representation of a row key that hashes to the same token as original
        // clustering key
        {},
        // regular columns
        {
            {"promise", timeuuid_type},
            {"most_recent_commit", bytes_type}, // serialization format is defined by frozen_mutation idl
            {"most_recent_commit_at", timeuuid_type},
            {"proposal", bytes_type}, // serialization format is defined by frozen_mutation idl
            {"proposal_ballot", timeuuid_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        fmt::format("Paxos state for {}.{}", s.ks_name(), s.cf_name())
    );
    builder.set_gc_grace_seconds(0);
    builder.with_hash_version();
    return builder.build(schema_builder::compact_storage::no);
}

schema_ptr paxos_store::try_get_paxos_state_schema(const schema& s) const {
    // Throw if the base table has been dropped or recreated.
    // We can't rely on s.table() since a table is stored in tables_metadata as an lw_shared_ptr,
    // and can live for a while without being referenced from it.
    _db.find_column_family(s.id());

    const auto& tables = _db.get_tables_metadata();
    const auto state_table_id = tables.get_table_id_if_exists({s.ks_name(), paxos_state_table_name(s.cf_name())});
    return state_table_id ? tables.get_table(state_table_id).schema() : nullptr;
}

void paxos_store::check_raft_is_enabled(const schema& s) const {
    if (!_mm.use_raft()) {
        throw std::runtime_error(format("Cannot create paxos state table for {}.{} "
            "because raft-based schema management is not enabled.",
            s.ks_name(), s.cf_name()));
    }
}

future<> paxos_store::create_paxos_state_table(const schema& s, db::timeout_clock::time_point timeout) {
    auto retries = _mm.get_concurrent_ddl_retries();
    while (true) {
        try {
            auto guard = co_await _mm.start_group0_operation(raft_timeout{.value = timeout});
            if (try_get_paxos_state_schema(s)) {
                // read_apply_mutex is acquired in start_group0_operation and in
                // group0_state_machine::apply. This guarantees that merge_schema has
                // already completed, the new schema is published on all shards,
                // and it's safe to use it on the original shard once we return from invoke_on.

                break;
            }
            const auto state_schema = create_paxos_state_schema(s);
            auto muts = co_await prepare_new_column_family_announcement(_mm.get_storage_proxy(), 
                std::move(state_schema),
                guard.write_timestamp());
            co_await _mm.announce(std::move(muts),
                std::move(guard),
                fmt::format("Create paxos state table for \"{}.{}\"", s.ks_name(), s.cf_name()),
                raft_timeout{.value = timeout});
            break;
        } catch (const service::group0_concurrent_modification& ex) {
            paxos_state::logger.warn("Failed to create paxos state table for \"{}.{}\" due to guard conflict.{}.",
                s.ks_name(), s.cf_name(),
                retries ? " Retrying" : " Number of retries exceeded, giving up");
            if (retries--) {
                continue;
            }
            throw;
        }
    }
}

future<> paxos_store::ensure_initialized(const schema& s, db::timeout_clock::time_point timeout) {
    if (try_get_paxos_state_schema(s)) {
        return make_ready_future<>();
    }
    check_raft_is_enabled(s);
    paxos_state::logger.info("Creating paxos state table for \"{}.{}\", timeout {} millis", 
        s.ks_name(), s.cf_name(), duration_cast<std::chrono::milliseconds>(timeout - lowres_clock::now()).count());
    return container().invoke_on(0, &paxos_store::create_paxos_state_table, std::ref(s), timeout);
}

void paxos_store::on_before_drop_column_family(const schema& schema, utils::chunked_vector<mutation>& mutations, api::timestamp_type timestamp) {
    if (const auto state_schema = try_get_paxos_state_schema(schema); state_schema) {
        auto muts = prepare_column_family_drop_announcement(_mm.get_storage_proxy(), 
            state_schema->ks_name(),
            state_schema->cf_name(),
            timestamp,
            drop_views::yes).get();
        mutations.insert(mutations.end(), std::make_move_iterator(muts.begin()), std::make_move_iterator(muts.end()));
    }
}

std::optional<std::string_view> paxos_store::try_get_base_table(std::string_view cf_name) {
    if (!cf_name.ends_with(paxos_state_table_suffix)) {
        return std::nullopt;
    }
    return cf_name.substr(0, cf_name.size() - paxos_state_table_suffix.size());
}

static future<cql3::untyped_result_set> do_execute_cql_with_timeout(sstring req,
        db::timeout_clock::time_point timeout,
        std::vector<data_value_or_unset> values,
        cql3::query_processor& qp)
{
    const auto now = db::timeout_clock::now();
    const auto d = now < timeout
        ? timeout - now
        : db::timeout_clock::duration::zero(); // let the `storage_proxy` time out the query down the call chain
    service::client_state client_state(service::client_state::internal_tag{},
        timeout_config{d, d, d, d, d, d, d});
    service::query_state qs(client_state, empty_service_permit());

    const auto cache_key = qp.compute_id(req, "", cql3::internal_dialect());
    auto ps_ptr = qp.get_prepared(cache_key);
    if (!ps_ptr) {
        const auto msg_ptr = co_await qp.prepare(req, qs, cql3::internal_dialect());
        ps_ptr = std::move(msg_ptr->get_prepared());
        if (!ps_ptr) {
            on_internal_error(paxos_state::logger, "prepared statement is null");
        }
    }
    const auto qo = qp.make_internal_options(ps_ptr, values, db::consistency_level::ONE,
        -1, service::node_local_only::yes);
    const auto st = ps_ptr->statement;

    const auto msg_ptr = co_await st->execute(qp, qs, qo, std::nullopt);
    co_return cql3::untyped_result_set(msg_ptr);
}

template <typename... Args>
future<cql3::untyped_result_set> paxos_store::execute_cql_with_timeout(sstring req,
        db::timeout_clock::time_point timeout,
        Args&&... args) {
    return do_execute_cql_with_timeout(std::move(req),
        timeout,
        { data_value(std::forward<Args>(args))... },
        _sys_ks.query_processor()
    );
}

future<column_mapping> paxos_store::get_column_mapping(table_id table_id, table_schema_version version) {
    return service::get_column_mapping(_sys_ks, table_id, version);
}

future<schema_ptr> paxos_store::get_paxos_state_schema(const schema& s, db::timeout_clock::time_point timeout) const {
    if (!s.table().uses_tablets()) {
        co_return db::system_keyspace::paxos();
    }

    // The paxos state schema only includes the partition key from the base table.
    // Since partition keys can't be altered via CQL, the paxos schema doesn't need to change
    // when the base schema changes.
    // When the LWT coordinator sends RPCs to replicas, some may not yet have the paxos schema.
    // Wait for the schema to appear here via read_barrier, or throw 'no_such_column_family'
    // if the base table was dropped (see try_get_paxos_state_schema).

    if (!_features.lwt_with_tablets) {
        on_internal_error(paxos_state::logger, "lwt_with_tablets is not supported");
    }
    if (const auto state_schema = try_get_paxos_state_schema(s); state_schema) {
        co_return state_schema;
    }

    check_raft_is_enabled(s);
    paxos_state::logger.debug("get_paxos_state_schema for {}.{}({}), paxos state table doesn't exist, "
        "running group0.read_barrier", s.ks_name(), s.cf_name(), s.id());
    abort_on_expiry aoe(timeout);
    co_await _mm.get_group0_barrier().trigger(aoe.abort_source());

    if (const auto state_schema = try_get_paxos_state_schema(s); state_schema) {
        co_return state_schema;
    }

    // 1. It is guaranteed that the `s` parameter refers to the exact same schema version
    // that was present at the beginning of sp::cas(). This holds because the schema
    // version is passed explicitly as an RPC argument to replicas.
    //
    // 2. The initial call to ensure_initialized() in sp::cas() ensures that the Paxos state
    // table is created for that specific schema version of the user table.
    //
    // 3. The call to group0.read_barrier() (get_group0_barrier().trigger above) ensures that
    // the current node and shard observe the latest version of the group0 state.
    // While this version may include unrelated schema changes, it cannot miss the
    // Paxos state table creation (from step 2), as that happens before sending RPCs to replicas.
    //
    // 4. The "unrelated schema changes" may include a drop of the base user table
    // (and with it, the Paxos state table). However, we guard against this case via the
    // db.find_column_family(s.id()) check in try_get_paxos_state_schema().
    //
    // 5. If we still encounter a case where the base table exists but the Paxos state table
    // does not, it likely indicates a broken invariant — possibly due to a bug, so we
    // call on_internal_error() here.

    on_internal_error(paxos_state::logger, fmt::format("failed to obtain paxos state schema for '{}.{}'",
        s.ks_name(), s.cf_name()));
}

future<paxos_state> paxos_store::load_paxos_state(partition_key_view key, schema_ptr s, gc_clock::time_point now,
    db::timeout_clock::time_point timeout)
{
    const auto state_schema = co_await get_paxos_state_schema(*s, timeout);
    // FIXME: we need execute_cql_with_now()
    (void)now;
    const auto results = co_await execute_cql_with_timeout(
        format("SELECT * FROM \"{}\".\"{}\" WHERE row_key = ?{}", 
            state_schema->ks_name(), state_schema->cf_name(), 
            paxos_state_cf_filter(*s, *state_schema)
        ),
        timeout,
        to_legacy(*key.get_compound_type(*s), key.representation())
    );
    if (results.empty()) {
        co_return service::paxos::paxos_state();
    }
    auto& row = results.one();
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

    co_return service::paxos::paxos_state(promised, std::move(accepted), std::move(most_recent));
}

future<> paxos_store::save_paxos_promise(const schema& s, const partition_key& key, const utils::UUID& ballot, db::timeout_clock::time_point timeout) {
    const auto state_schema = co_await get_paxos_state_schema(s, timeout);
    co_await execute_cql_with_timeout(
            format("UPDATE \"{}\".\"{}\" USING TIMESTAMP ? AND TTL ? SET promise = ? WHERE row_key = ?{}",
                state_schema->ks_name(), state_schema->cf_name(), 
                paxos_state_cf_filter(s, *state_schema)
            ),
            timeout,
            utils::UUID_gen::micros_timestamp(ballot),
            paxos_ttl_sec(s),
            ballot,
            to_legacy(*key.get_compound_type(s), key.representation())
        );
}

future<> paxos_store::save_paxos_proposal(const schema& s, const proposal& proposal, db::timeout_clock::time_point timeout) {
    const auto state_schema = co_await get_paxos_state_schema(s, timeout);
    partition_key_view key = proposal.update.key();
    co_await execute_cql_with_timeout(
            format("UPDATE \"{}\".\"{}\" USING TIMESTAMP ? AND TTL ? SET promise = ?, proposal_ballot = ?, proposal = ? WHERE row_key = ?{}", 
                state_schema->ks_name(), state_schema->cf_name(), 
                paxos_state_cf_filter(s, *state_schema)
            ),
            timeout,
            utils::UUID_gen::micros_timestamp(proposal.ballot),
            paxos_ttl_sec(s),
            proposal.ballot,
            proposal.ballot,
            ser::serialize_to_buffer<bytes>(proposal.update),
            to_legacy(*key.get_compound_type(s), key.representation())
        );
}

future<> paxos_store::save_paxos_decision(const schema& s, const proposal& decision, db::timeout_clock::time_point timeout) {
    const auto state_schema = co_await get_paxos_state_schema(s, timeout);

    // We always erase the last proposal when we learn about a new Paxos decision. The ballot
    // timestamp of the decision is used for entire mutation, so if the "erased" proposal is more
    // recent it will naturally stay on top.
    // Erasing the last proposal is just an optimization and does not affect correctness:
    // sp::begin_and_repair_paxos will exclude an accepted proposal if it is older than the most
    // recent commit.
    partition_key_view key = decision.update.key();
    co_await execute_cql_with_timeout(
            format("UPDATE \"{}\".\"{}\" USING TIMESTAMP ? AND TTL ? SET proposal_ballot = null, proposal = null, "
                   "most_recent_commit_at = ?, most_recent_commit = ? WHERE row_key = ?{}",
                state_schema->ks_name(), state_schema->cf_name(), 
                paxos_state_cf_filter(s, *state_schema)
            ),
            timeout,
            utils::UUID_gen::micros_timestamp(decision.ballot),
            paxos_ttl_sec(s),
            decision.ballot,
            ser::serialize_to_buffer<bytes>(decision.update),
            to_legacy(*key.get_compound_type(s), key.representation())
        );
}

future<> paxos_store::delete_paxos_decision(const schema& s, const partition_key& key, utils::UUID ballot, db::timeout_clock::time_point timeout) {
    const auto state_schema = co_await get_paxos_state_schema(s, timeout);

    // This should be called only if a learn stage succeeded on all replicas.
    // In this case we can remove learned paxos value using ballot's timestamp which
    // guarantees that if there is more recent round it will not be affected.

    co_await execute_cql_with_timeout(
            format("DELETE most_recent_commit FROM \"{}\".\"{}\" USING TIMESTAMP ? WHERE row_key = ?{}",
                state_schema->ks_name(), state_schema->cf_name(), 
                paxos_state_cf_filter(s, *state_schema)
            ),
            timeout,
            utils::UUID_gen::micros_timestamp(ballot),
            to_legacy(*key.get_compound_type(s), key.representation())
        );
}

} // end of namespace "service::paxos"
