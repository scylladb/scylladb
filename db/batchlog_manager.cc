/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include <chrono>
#include <exception>
#include <ranges>
#include <seastar/core/future-util.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

#include "batchlog_manager.hh"
#include "batchlog.hh"
#include "data_dictionary/data_dictionary.hh"
#include "mutation/canonical_mutation.hh"
#include "service/storage_proxy.hh"
#include "system_keyspace.hh"
#include "utils/rate_limiter.hh"
#include "utils/log.hh"
#include "utils/murmur_hash.hh"
#include "db_clock.hh"
#include "unimplemented.hh"
#include "idl/frozen_schema.dist.hh"
#include "idl/frozen_schema.dist.impl.hh"
#include "db/schema_tables.hh"
#include "message/messaging_service.hh"
#include "cql3/untyped_result_set.hh"
#include "service_permit.hh"
#include "cql3/query_processor.hh"

static logging::logger blogger("batchlog_manager");

namespace db {

// Yields 256 batchlog shards. Even on the largest nodes we currently run on,
// this should be enough to give every core a batchlog partition.
static constexpr unsigned batchlog_shard_bits = 8;

int32_t batchlog_shard_of(db_clock::time_point written_at) {
    const int64_t count = written_at.time_since_epoch().count();
    std::array<uint64_t, 2> result;
    utils::murmur_hash::hash3_x64_128(bytes_view(reinterpret_cast<const signed char*>(&count), sizeof(count)), 0, result);
    uint64_t hash = result[0] ^ result[1];
    return hash & ((1ULL << batchlog_shard_bits) - 1);
}

std::pair<partition_key, clustering_key>
get_batchlog_key(const schema& schema, int32_t version, db::batchlog_stage stage, int32_t batchlog_shard, db_clock::time_point written_at, std::optional<utils::UUID> id) {
    auto pkey = partition_key::from_exploded(schema, {serialized(version), serialized(int8_t(stage)), serialized(batchlog_shard)});

    std::vector<bytes> ckey_components;
    ckey_components.reserve(2);
    ckey_components.push_back(serialized(written_at));
    if (id) {
        ckey_components.push_back(serialized(*id));
    }
    auto ckey = clustering_key::from_exploded(schema, ckey_components);

    return {std::move(pkey), std::move(ckey)};
}

std::pair<partition_key, clustering_key>
get_batchlog_key(const schema& schema, int32_t version, db::batchlog_stage stage, db_clock::time_point written_at, std::optional<utils::UUID> id) {
    return get_batchlog_key(schema, version, stage, batchlog_shard_of(written_at), written_at, id);
}

mutation get_batchlog_mutation_for(schema_ptr schema, managed_bytes data, int32_t version, db::batchlog_stage stage, db_clock::time_point now, const utils::UUID& id) {
    auto [key, ckey] = get_batchlog_key(*schema, version, stage, now, id);

    auto timestamp = api::new_timestamp();

    mutation m(schema, key);
    // Avoid going through data_value and therefore `bytes`, as it can be large (#24809).
    auto cdef_data = schema->get_column_definition(to_bytes("data"));
    m.set_cell(ckey, *cdef_data, atomic_cell::make_live(*cdef_data->type, timestamp, std::move(data)));

    return m;
}

mutation get_batchlog_mutation_for(schema_ptr schema, const utils::chunked_vector<mutation>& mutations, int32_t version, db::batchlog_stage stage, db_clock::time_point now, const utils::UUID& id) {
    auto data = [&mutations] {
        utils::chunked_vector<canonical_mutation> fm(mutations.begin(), mutations.end());
        bytes_ostream out;
        for (auto& m : fm) {
            ser::serialize(out, m);
        }
        return std::move(out).to_managed_bytes();
    }();

    return get_batchlog_mutation_for(std::move(schema), std::move(data), version, stage, now, id);
}

mutation get_batchlog_mutation_for(schema_ptr schema, const utils::chunked_vector<mutation>& mutations, int32_t version, db_clock::time_point now, const utils::UUID& id) {
    return get_batchlog_mutation_for(std::move(schema), mutations, version, batchlog_stage::initial, now, id);
}

mutation get_batchlog_delete_mutation(schema_ptr schema, int32_t version, db::batchlog_stage stage, db_clock::time_point now, const utils::UUID& id) {
    auto [key, ckey] = get_batchlog_key(*schema, version, stage, now, id);
    mutation m(schema, key);
    auto timestamp = api::new_timestamp();
    m.partition().apply_delete(*schema, ckey, tombstone(timestamp, gc_clock::now()));
    return m;
}

mutation get_batchlog_delete_mutation(schema_ptr schema, int32_t version, db_clock::time_point now, const utils::UUID& id) {
    return get_batchlog_delete_mutation(std::move(schema), version, batchlog_stage::initial, now, id);
}

} // namespace db

const std::chrono::seconds db::batchlog_manager::replay_interval;
const uint32_t db::batchlog_manager::page_size;

db::batchlog_manager::batchlog_manager(cql3::query_processor& qp, db::system_keyspace& sys_ks, batchlog_manager_config config)
        : _qp(qp)
        , _sys_ks(sys_ks)
        , _replay_timeout(config.replay_timeout)
        , _replay_rate(config.replay_rate)
        , _delay(config.delay)
        , _replay_cleanup_after_replays(config.replay_cleanup_after_replays)
        , _gate("batchlog_manager")
        , _loop_done(batchlog_replay_loop())
{
    namespace sm = seastar::metrics;

    _metrics.add_group("batchlog_manager", {
        sm::make_counter("total_write_replay_attempts", _stats.write_attempts,
                        sm::description("Counts write operations issued in a batchlog replay flow. "
                                        "The high value of this metric indicates that we have a long batch replay list.")),
    });
}

future<db::all_batches_replayed> db::batchlog_manager::do_batch_log_replay(post_replay_cleanup cleanup) {
    return container().invoke_on(0, [cleanup] (auto& bm) -> future<db::all_batches_replayed> {
        auto gate_holder = bm._gate.hold();
        auto sem_units = co_await get_units(bm._sem, 1);

        auto dest = bm._cpu++ % smp::count;
        blogger.debug("Batchlog replay on shard {}: starts", dest);
        auto last_replay = gc_clock::now();
        all_batches_replayed all_replayed = all_batches_replayed::yes;
        if (dest == 0) {
            all_replayed = co_await bm.replay_all_failed_batches(cleanup);
        } else {
            all_replayed = co_await bm.container().invoke_on(dest, [cleanup] (auto& bm) {
                return with_gate(bm._gate, [&bm, cleanup] {
                    return bm.replay_all_failed_batches(cleanup);
                });
            });
        }
        if (all_replayed == all_batches_replayed::yes) {
            co_await bm.container().invoke_on_all([last_replay] (auto& bm) {
                bm._last_replay = last_replay;
            });
        }
        blogger.debug("Batchlog replay on shard {}: done", dest);
        co_return all_replayed;
    });
}

future<> db::batchlog_manager::batchlog_replay_loop() {
    if (this_shard_id() != 0) {
        // Since replay is a "node global" operation, we should not attempt to do
        // it in parallel on each shard. It will just overlap/interfere.  To
        // simplify syncing between batchlog_replay_loop and user initiated replay operations,
        // we use the _sem on shard zero only. Replaying batchlog can
        // generate a lot of work, so we distrute the real work on all cpus with
        // round-robin scheduling.
        co_return;
    }

    unsigned replay_counter = 0;
    auto delay = _delay;
    while (!_stop.abort_requested()) {
        try {
            co_await sleep_abortable(delay, _stop);
        } catch (sleep_aborted&) {
            co_return;
        }
        try {
            auto cleanup = post_replay_cleanup::no;
            if (++replay_counter >= _replay_cleanup_after_replays) {
                replay_counter = 0;
                cleanup = post_replay_cleanup::yes;
            }
            co_await do_batch_log_replay(cleanup);
        } catch (seastar::broken_semaphore&) {
            if (_stop.abort_requested()) {
                co_return;
            }
            on_internal_error_noexcept(blogger, fmt::format("Unexcepted exception in batchlog reply: {}", std::current_exception()));
        } catch (...) {
            blogger.error("Exception in batch replay: {}", std::current_exception());
        }
        delay = utils::get_local_injector().is_enabled("short_batchlog_manager_replay_interval") ?
                std::chrono::seconds(1) : replay_interval;
    }
}

future<> db::batchlog_manager::drain() {
    if (_stop.abort_requested()) {
        co_return;
    }

    blogger.info("Asked to drain");
    _stop.request_abort();
    if (this_shard_id() == 0) {
        // Abort do_batch_log_replay if waiting on the semaphore.
        _sem.broken();
    }

    co_await _qp.proxy().abort_batch_writes();

    co_await std::move(_loop_done);
    blogger.info("Drained");
}

future<> db::batchlog_manager::stop() {
    blogger.info("Asked to stop");
    co_await drain();
    co_await _gate.close();
    blogger.info("Stopped");
}

future<size_t> db::batchlog_manager::count_all_batches() const {
    sstring query = format("SELECT count(*) FROM {}.{} BYPASS CACHE", system_keyspace::NAME, system_keyspace::BATCHLOG_V2);
    return _qp.execute_internal(query, cql3::query_processor::cache_internal::yes).then([](::shared_ptr<cql3::untyped_result_set> rs) {
       return size_t(rs->one().get_as<int64_t>("count"));
    });
}

future<> db::batchlog_manager::maybe_migrate_v1_to_v2() {
    if (_migration_done) {
        return make_ready_future<>();
    }
    return with_gate(_gate, [this] () mutable -> future<> {
        blogger.info("Migrating batchlog entries from v1 -> v2");

        auto schema_v1 = _qp.db().find_schema(system_keyspace::NAME, system_keyspace::BATCHLOG);
        auto schema_v2 = _qp.db().find_schema(system_keyspace::NAME, system_keyspace::BATCHLOG_V2);

        auto batch = [this, schema_v1, schema_v2] (const cql3::untyped_result_set::row& row) -> future<stop_iteration> {
            // check version of serialization format
            if (!row.has("version")) {
                blogger.warn("Not migrating logged batch because of unknown version");
                co_return stop_iteration::no;
            }

            auto version = row.get_as<int32_t>("version");
            if (version != netw::messaging_service::current_version) {
                blogger.warn("Not migrating logged batch because of incorrect version");
                co_return stop_iteration::no;
            }

            auto id = row.get_as<utils::UUID>("id");
            auto written_at = row.get_as<db_clock::time_point>("written_at");
            auto data = row.get_blob_fragmented("data");

            auto& sp = _qp.proxy();

            utils::get_local_injector().inject("batchlog_manager_fail_migration", [] { throw std::runtime_error("Error injection: failing batchlog migration"); });

            auto migrate_mut = get_batchlog_mutation_for(schema_v2, std::move(data), version, batchlog_stage::failed_replay, written_at, id);
            co_await sp.mutate_locally(migrate_mut, tracing::trace_state_ptr(), db::commitlog::force_sync::no);

            mutation delete_mut(schema_v1, partition_key::from_single_value(*schema_v1, serialized(id)));
            delete_mut.partition().apply_delete(*schema_v1, clustering_key_prefix::make_empty(), tombstone(api::new_timestamp(), gc_clock::now()));
            co_await sp.mutate_locally(delete_mut, tracing::trace_state_ptr(), db::commitlog::force_sync::no);

            co_return stop_iteration::no;
        };
        try {
            co_await _qp.query_internal(
                    format("SELECT * FROM {}.{} BYPASS CACHE", system_keyspace::NAME, system_keyspace::BATCHLOG),
                    db::consistency_level::ONE,
                    {},
                    page_size,
                    std::move(batch));
        } catch (...) {
            blogger.warn("Batchlog v1 to v2 migration failed: {}; will retry", std::current_exception());
            co_return;
        }

        co_await container().invoke_on_all([] (auto& bm) {
            bm._migration_done = true;
        });

        blogger.info("Done migrating batchlog entries from v1 -> v2");
    });
}

future<db::all_batches_replayed> db::batchlog_manager::replay_all_failed_batches(post_replay_cleanup cleanup) {
    co_await maybe_migrate_v1_to_v2();

    typedef db_clock::rep clock_type;

    db::all_batches_replayed all_replayed = all_batches_replayed::yes;
    // rate limit is in bytes per second. Uses Double.MAX_VALUE if disabled (set to 0 in cassandra.yaml).
    // max rate is scaled by the number of nodes in the cluster (same as for HHOM - see CASSANDRA-5272).
    auto throttle = _replay_rate / _qp.proxy().get_token_metadata_ptr()->count_normal_token_owners();
    auto limiter = make_lw_shared<utils::rate_limiter>(throttle);

    auto schema = _qp.db().find_schema(system_keyspace::NAME, system_keyspace::BATCHLOG_V2);

    struct replay_stats {
        std::optional<db_clock::time_point> min_too_fresh;
        bool need_cleanup = false;
    };

    std::unordered_map<int32_t, replay_stats> replay_stats_per_shard;

    // Use a stable `now` across all batches, so skip/replay decisions are the
    // same across a while prefix of written_at (across all ids).
    const auto now = db_clock::now();

    auto batch = [this, cleanup, limiter, schema, &all_replayed, &replay_stats_per_shard, now] (const cql3::untyped_result_set::row& row) -> future<stop_iteration> {
        const auto stage = static_cast<batchlog_stage>(row.get_as<int8_t>("stage"));
        const auto batch_shard = row.get_as<int32_t>("shard");
        auto written_at = row.get_as<db_clock::time_point>("written_at");
        auto id = row.get_as<utils::UUID>("id");
        // enough time for the actual write + batchlog entry mutation delivery (two separate requests).
        auto timeout = _replay_timeout;

        if (utils::get_local_injector().is_enabled("skip_batch_replay")) {
            blogger.debug("Skipping batch replay due to skip_batch_replay injection");
            all_replayed = all_batches_replayed::no;
            co_return stop_iteration::no;
        }

        auto data = row.get_blob_unfragmented("data");

        blogger.debug("Replaying batch {} from stage {} and batch shard {}", id, int32_t(stage), batch_shard);

        utils::chunked_vector<mutation> mutations;
        bool send_failed = false;

        auto& shard_written_at = replay_stats_per_shard.try_emplace(batch_shard, replay_stats{}).first->second;

        try {
            utils::chunked_vector<std::pair<canonical_mutation, schema_ptr>> fms;
            auto in = ser::as_input_stream(data);
            while (in.size()) {
                auto fm = ser::deserialize(in, std::type_identity<canonical_mutation>());
                const auto tbl = _qp.db().try_find_table(fm.column_family_id());
                if (!tbl) {
                    continue;
                }
                if (written_at <= tbl->get_truncation_time()) {
                    continue;
                }
                schema_ptr s = tbl->schema();
                if (s->tombstone_gc_options().mode() == tombstone_gc_mode::repair) {
                    timeout = std::min(timeout, std::chrono::duration_cast<db_clock::duration>(s->tombstone_gc_options().propagation_delay_in_seconds()));
                }
                fms.emplace_back(std::move(fm), std::move(s));
            }

            if (now < written_at + timeout) {
                blogger.debug("Skipping replay of {}, too fresh", id);

                shard_written_at.min_too_fresh = std::min(shard_written_at.min_too_fresh.value_or(written_at), written_at);

                co_return stop_iteration::no;
            }

            auto size = data.size();

            for (const auto& [fm, s] : fms) {
                mutations.emplace_back(fm.to_mutation(s));
                co_await maybe_yield();
            }

            if (!mutations.empty()) {
                const auto ttl = [written_at]() -> clock_type {
                    /*
                    * Calculate ttl for the mutations' hints (and reduce ttl by the time the mutations spent in the batchlog).
                    * This ensures that deletes aren't "undone" by an old batch replay.
                    */
                    auto unadjusted_ttl = std::numeric_limits<gc_clock::rep>::max();
                    warn(unimplemented::cause::HINT);
#if 0
                    for (auto& m : *mutations) {
                        unadjustedTTL = Math.min(unadjustedTTL, HintedHandOffManager.calculateHintTTL(mutation));
                    }
#endif
                    return unadjusted_ttl - std::chrono::duration_cast<gc_clock::duration>(db_clock::now() - written_at).count();
                }();

                if (ttl > 0) {
                    // Origin does the send manually, however I can't see a super great reason to do so.
                    // Our normal write path does not add much redundancy to the dispatch, and rate is handled after send
                    // in both cases.
                    // FIXME: verify that the above is reasonably true.
                    co_await limiter->reserve(size);
                    _stats.write_attempts += mutations.size();
                    auto timeout = db::timeout_clock::now() + write_timeout;
                    if (cleanup) {
                        co_await _qp.proxy().send_batchlog_replay_to_all_replicas(mutations, timeout);
                    } else {
                        co_await _qp.proxy().send_batchlog_replay_to_all_replicas(std::move(mutations), timeout);
                    }
                }
            }
        } catch (data_dictionary::no_such_keyspace& ex) {
            // should probably ignore and drop the batch
        } catch (const data_dictionary::no_such_column_family&) {
            // As above -- we should drop the batch if the table doesn't exist anymore.
        } catch (...) {
            blogger.warn("Replay failed (will retry): {}", std::current_exception());
            all_replayed = all_batches_replayed::no;
            // timeout, overload etc.
            // Do _not_ remove the batch, assuning we got a node write error.
            // Since we don't have hints (which origin is satisfied with),
            // we have to resort to keeping this batch to next lap.
            if (!cleanup || stage == batchlog_stage::failed_replay) {
                co_return stop_iteration::no;
            }
            send_failed = true;
        }

        auto& sp = _qp.proxy();

        if (send_failed) {
            blogger.debug("Moving batch {} to stage failed_replay", id);
            auto m = get_batchlog_mutation_for(schema, mutations, netw::messaging_service::current_version, batchlog_stage::failed_replay, written_at, id);
            co_await sp.mutate_locally(m, tracing::trace_state_ptr(), db::commitlog::force_sync::no);
        }

        // delete batch
        auto m = get_batchlog_delete_mutation(schema, netw::messaging_service::current_version, stage, written_at, id);
        co_await _qp.proxy().mutate_locally(m, tracing::trace_state_ptr(), db::commitlog::force_sync::no);

        shard_written_at.need_cleanup = true;

        co_return stop_iteration::no;
    };

    co_await with_gate(_gate, [this, cleanup, &all_replayed, batch = std::move(batch), now, &replay_stats_per_shard] () mutable -> future<> {
        blogger.debug("Started replayAllFailedBatches with cleanup: {}", cleanup);
        co_await utils::get_local_injector().inject("add_delay_to_batch_replay", std::chrono::milliseconds(1000));

        auto schema = _qp.db().find_schema(system_keyspace::NAME, system_keyspace::BATCHLOG_V2);

        co_await coroutine::parallel_for_each(std::views::iota(0, 16), [&] (int32_t chunk) -> future<> {
            const int32_t batchlog_chunk_base = chunk * 16;
            for (int32_t i = 0; i < 16; ++i) {
                int32_t batchlog_shard = batchlog_chunk_base + i;

                co_await _qp.query_internal(
                        format("SELECT * FROM {}.{} WHERE version = ? AND stage = ? AND shard = ? BYPASS CACHE", system_keyspace::NAME, system_keyspace::BATCHLOG_V2),
                        db::consistency_level::ONE,
                        {data_value(netw::messaging_service::current_version), data_value(int8_t(batchlog_stage::failed_replay)), data_value(batchlog_shard)},
                        page_size,
                        batch);

                co_await _qp.query_internal(
                        format("SELECT * FROM {}.{} WHERE version = ? AND stage = ? AND shard = ? BYPASS CACHE", system_keyspace::NAME, system_keyspace::BATCHLOG_V2),
                        db::consistency_level::ONE,
                        {data_value(netw::messaging_service::current_version), data_value(int8_t(batchlog_stage::initial)), data_value(batchlog_shard)},
                        page_size,
                        batch);

                if (cleanup != post_replay_cleanup::yes) {
                    continue;
                }

                auto it = replay_stats_per_shard.find(batchlog_shard);
                if (it == replay_stats_per_shard.end() || !it->second.need_cleanup) {
                    // Nothing was replayed on this batchlog shard, nothing to cleanup.
                    continue;
                }

                const auto write_time = it->second.min_too_fresh.value_or(now - _replay_timeout);
                const auto end_weight  = it->second.min_too_fresh ? bound_weight::before_all_prefixed : bound_weight::after_all_prefixed;
                auto [key, ckey] = get_batchlog_key(*schema, netw::messaging_service::current_version, batchlog_stage::initial, batchlog_shard, write_time, {});
                auto end_pos = position_in_partition(partition_region::clustered, end_weight, std::move(ckey));

                range_tombstone rt(position_in_partition::before_all_clustered_rows(), std::move(end_pos), tombstone(api::new_timestamp(), gc_clock::now()));

                blogger.trace("Clean up batchlog shard {} with range tombstone {}", batchlog_shard, rt);

                mutation m(schema, key);
                m.partition().apply_row_tombstone(*schema, std::move(rt));
                co_await _qp.proxy().mutate_locally(m, tracing::trace_state_ptr(), db::commitlog::force_sync::no);
            }
        });

        blogger.debug("Finished replayAllFailedBatches with all_replayed: {}", all_replayed);
    });

    co_return all_replayed;
}
