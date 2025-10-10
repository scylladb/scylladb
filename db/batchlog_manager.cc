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
#include "data_dictionary/data_dictionary.hh"
#include "mutation/canonical_mutation.hh"
#include "service/storage_proxy.hh"
#include "system_keyspace.hh"
#include "utils/rate_limiter.hh"
#include "utils/log.hh"
#include "db_clock.hh"
#include "unimplemented.hh"
#include "idl/frozen_schema.dist.hh"
#include "idl/frozen_schema.dist.impl.hh"
#include "db/schema_tables.hh"
#include "message/messaging_service.hh"
#include "cql3/untyped_result_set.hh"
#include "service_permit.hh"
#include "cql3/query_processor.hh"
#include "replica/database.hh"

static logging::logger blogger("batchlog_manager");

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

future<std::optional<db::batchlog_manager::batchlog_replay_stats>>
db::batchlog_manager::do_batch_log_replay(post_replay_cleanup cleanup, bool trace) {
    return container().invoke_on(0, [cleanup, trace] (auto& bm) -> future<std::optional<db::batchlog_manager::batchlog_replay_stats>> {
        auto gate_holder = bm._gate.hold();
        auto sem_units = co_await get_units(bm._sem, 1);

        auto dest = bm._cpu++ % smp::count;
        blogger.debug("Batchlog replay on shard {}: starts", dest);
        auto last_replay = gc_clock::now();
        std::optional<batchlog_replay_stats> stats;
        if (dest == 0) {
            stats = co_await bm.replay_all_failed_batches(cleanup, trace);
        } else {
            stats = co_await bm.container().invoke_on(dest, [cleanup, trace] (auto& bm) {
                return with_gate(bm._gate, [&bm, cleanup, trace] {
                    return bm.replay_all_failed_batches(cleanup, trace);
                });
            });
        }
        co_await bm.container().invoke_on_all([last_replay] (auto& bm) {
            bm._last_replay = last_replay;
        });
        blogger.debug("Batchlog replay on shard {}: done", dest);
        co_return stats;
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
            blogger.debug("trigger do_batch_log_replay({}) replay_counter={}, _replay_cleanup_after_replays={}", cleanup, replay_counter, _replay_cleanup_after_replays);
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
    sstring query = format("SELECT count(*) FROM {}.{} BYPASS CACHE", system_keyspace::NAME, system_keyspace::BATCHLOG);
    return _qp.execute_internal(query, cql3::query_processor::cache_internal::yes).then([](::shared_ptr<cql3::untyped_result_set> rs) {
       return size_t(rs->one().get_as<int64_t>("count"));
    });
}

future<std::optional<db::batchlog_manager::batchlog_replay_stats>>
db::batchlog_manager::replay_all_failed_batches(post_replay_cleanup cleanup, bool trace) {
    typedef db_clock::rep clock_type;

    // rate limit is in bytes per second. Uses Double.MAX_VALUE if disabled (set to 0 in cassandra.yaml).
    // max rate is scaled by the number of nodes in the cluster (same as for HHOM - see CASSANDRA-5272).
    auto throttle = _replay_rate / _qp.proxy().get_token_metadata_ptr()->count_normal_token_owners();
    auto limiter = make_lw_shared<utils::rate_limiter>(throttle);

    batchlog_replay_stats stats;

    auto batch = [this, cleanup, limiter, &stats](const cql3::untyped_result_set::row& row) -> future<stop_iteration> {
        const auto stage = static_cast<batchlog_stage>(row.get_as<int32_t>("stage"));
        auto written_at = row.get_as<db_clock::time_point>("written_at");
        auto id = row.get_as<utils::UUID>("id");

        if (utils::get_local_injector().is_enabled("skip_batch_replay")) {
            blogger.debug("Skipping batch replay due to skip_batch_replay injection");
            ++stats.skipped_batches;
            co_return stop_iteration::no;
        }

        ++stats.replayed_batches;

        auto data = row.get_blob_unfragmented("data");

        blogger.debug("Replaying batch {}", id);

        utils::chunked_vector<mutation> mutations;
        auto in = ser::as_input_stream(data);
        while (in.size()) {
            auto fm = ser::deserialize(in, std::type_identity<canonical_mutation>());
            const auto& tbl = _qp.proxy().local_db().find_column_family(fm.column_family_id());
            if (written_at <= tbl.get_truncation_time()) {
                continue;
            }
            mutations.emplace_back(fm.to_mutation(tbl.schema()));
        }

        auto size = data.size();
        bool send_failed = false;

        try {
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
            auto rl_fut = limiter->reserve(size);
            const bool rl_engaged = !rl_fut.available();
            const gc_clock::time_point rl_start = gc_clock::now();
            if (rl_engaged) {
                ++stats.rate_limiter_engaged;
            }
            co_await std::move(rl_fut);
            if (rl_engaged) {
                const auto rl_delay = std::chrono::duration_cast<std::chrono::milliseconds>(gc_clock::now() - rl_start);
                stats.rate_limiter_total_delay += rl_delay;
            }
                _stats.write_attempts += mutations.size();
                // #1222 - change cl level to ALL, emulating origins behaviour of sending/hinting
                // to all natural end points.
                // Note however that origin uses hints here, and actually allows for this
                // send to partially or wholly fail in actually sending stuff. Since we don't
                // have hints (yet), send with CL=ALL, and hope we can re-do this soon.
                // See below, we use retry on write failure.
                auto timeout = db::timeout_clock::now() + write_timeout;
                co_await _qp.proxy().send_batchlog_replay_to_all_replicas(std::move(mutations), timeout);
           }
          } else {
            ++stats.dropped_batches;
          }
            } catch (data_dictionary::no_such_keyspace& ex) {
                // should probably ignore and drop the batch
            } catch (const data_dictionary::no_such_column_family&) {
                // As above -- we should drop the batch if the table doesn't exist anymore.
            } catch (...) {
                blogger.warn("Replay failed (will retry): {}", std::current_exception());
                if (!cleanup || stage == batchlog_stage::failed_replay) {
                    co_return stop_iteration::no;
                }
                send_failed = true;
            }

            auto& sp = _qp.proxy();

            if (send_failed) {
                auto m = sp.get_batchlog_mutation_for(mutations, id, netw::messaging_service::current_version, written_at, batchlog_stage::failed_replay);
                co_await sp.mutate_locally(m, tracing::trace_state_ptr(), db::commitlog::force_sync::no);
            } else {
                ++stats.deleted_batches;
            }

            // delete batch
            auto schema = _qp.db().find_schema(system_keyspace::NAME, system_keyspace::BATCHLOG);
            auto [key, ckey] = get_batchlog_key(*schema, netw::messaging_service::current_version, stage, written_at, id);
            mutation m(schema, key);
            auto now = service::client_state(service::client_state::internal_tag()).get_timestamp();
            m.partition().apply_delete(*schema, ckey, tombstone(now, gc_clock::now()));
            co_await sp.mutate_locally(m, tracing::trace_state_ptr(), db::commitlog::force_sync::no);
            co_return stop_iteration::no;
    };

    co_return co_await with_gate(_gate, [this, cleanup, trace, batch = std::move(batch), &stats] () mutable -> future<std::optional<batchlog_replay_stats>> {
        blogger.debug("Started replayAllFailedBatches with cleanup: {}, trace: {}", cleanup, trace);

        co_await utils::get_local_injector().inject("add_delay_to_batch_replay", std::chrono::milliseconds(1000));

        // Exclude batches too fresh to be replayed.
        const auto written_at_limit = db_clock::now() - _replay_timeout;

        tracing::trace_state_ptr trace_state;
        if (trace) {
            tracing::trace_state_props_set trace_props;
            trace_props.set<tracing::trace_state_props::full_tracing>();

            trace_state = tracing::tracing::get_local_tracing_instance().create_session(tracing::trace_type::QUERY, trace_props);
            tracing::begin(trace_state, "Batchlog replay with tracing", service::client_state::for_internal_calls().get_client_address());

            stats.tracing_session_id = trace_state->session_id();
        }

        auto schema = _qp.db().find_schema(system_keyspace::NAME, system_keyspace::BATCHLOG);

        co_await coroutine::parallel_for_each(std::views::iota(0, 16), [&] (int i) -> future<> {
            const auto chunk_base = std::numeric_limits<int8_t>::min() + i * 16;
            for (int j = 0; j < 16; ++j) {
                int8_t shard = static_cast<int8_t>(chunk_base + j);
                const gc_clock::time_point replay_start = gc_clock::now();

                co_await _qp.query_internal_with_tracing(
                        format("SELECT * FROM {}.{} WHERE version = ? AND stage = ? AND shard = ? BYPASS CACHE", system_keyspace::NAME, system_keyspace::BATCHLOG),
                        db::consistency_level::ONE,
                        {data_value(netw::messaging_service::current_version), data_value(int32_t(batchlog_stage::failed_replay)), data_value(shard)},
                        page_size,
                        {},
                        batch);

                 co_await _qp.query_internal_with_tracing(
                        format("SELECT * FROM {}.{} WHERE version = ? AND stage = ? AND shard = ? AND written_at < ? BYPASS CACHE", system_keyspace::NAME, system_keyspace::BATCHLOG),
                        db::consistency_level::ONE,
                        {data_value(netw::messaging_service::current_version), data_value(int32_t(batchlog_stage::initial)), data_value(shard), data_value(written_at_limit)},
                        page_size,
                        trace_state,
                        batch);

                const gc_clock::time_point replay_end = gc_clock::now();
                stats.replay_duration += std::chrono::duration_cast<std::chrono::milliseconds>(replay_end - replay_start);

                if (cleanup == post_replay_cleanup::yes) {
                    auto [key, ckey] = get_batchlog_key(*schema, netw::messaging_service::current_version, batchlog_stage::initial, shard, written_at_limit, {});
                    auto now = service::client_state(service::client_state::internal_tag()).get_timestamp();
                    range_tombstone rt(position_in_partition_view::before_all_clustered_rows(), position_in_partition::before_key(ckey), tombstone(now, gc_clock::now()));

                    mutation m(schema, key);
                    m.partition().apply_row_tombstone(*schema, std::move(rt));
                    co_await _qp.proxy().mutate_locally(m, tracing::trace_state_ptr(), db::commitlog::force_sync::no);

                    stats.cleanup_duration += std::chrono::duration_cast<std::chrono::milliseconds>(gc_clock::now() - replay_end);
                }
            }
        });

        blogger.debug("Finished replayAllFailedBatches with trace session id: {}", stats.tracing_session_id);

        co_return stats;
    });
}

future<> db::batchlog_manager::log_batch_traces(utils::UUID tracing_session_id) {
    if (!tracing_session_id) {
        co_return;
    }
    unsigned tries = 0;
    bool has_traces = false;
    do {
        has_traces = false;
        co_await _qp.query_internal(fmt::format("SELECT * FROM system_traces.events WHERE session_id = {}", tracing_session_id),
                [tracing_session_id, &has_traces] (const cql3::untyped_result_set::row& row) -> future<stop_iteration> {
            has_traces = true;
            const auto activity = row.get_as<sstring>("activity");
            if (activity.contains("Page stats")) {
                blogger.info("Batchlog replay page stats (session_id={}): {}", tracing_session_id, activity);
            }
            return make_ready_future<stop_iteration>(stop_iteration::no);
        });
        if (has_traces) {
            break;
        }
        blogger.trace("No batchlog traces yet for session_id={}, try #{}", tracing_session_id, tries);
        co_await sleep(std::chrono::seconds(1));
    } while (tries++ < 10);
}

std::pair<partition_key, clustering_key>
db::batchlog_manager::get_batchlog_key(const schema& schema, int32_t version, batchlog_stage stage, int8_t shard, db_clock::time_point written_at, std::optional<utils::UUID> id) {
    auto pkey = partition_key::from_exploded(schema, {serialized(version), serialized(int32_t(stage)), serialized(shard)});

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
db::batchlog_manager::get_batchlog_key(const schema& schema, int32_t version, batchlog_stage stage, db_clock::time_point written_at, std::optional<utils::UUID> id) {
    const int64_t count = written_at.time_since_epoch().count();
    const int8_t shard = count & 0x00000000000000FF;

    return get_batchlog_key(schema, version, stage, shard, written_at, id);
}
