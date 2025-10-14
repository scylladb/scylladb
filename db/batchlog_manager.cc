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

namespace db {

mutation get_batchlog_mutation_for(schema_ptr schema, const utils::chunked_vector<mutation>& mutations, const utils::UUID& id, int32_t version, db_clock::time_point now) {
    auto key = partition_key::from_singular(*schema, id);
    auto timestamp = api::new_timestamp();
    auto data = [&mutations] {
        utils::chunked_vector<canonical_mutation> fm(mutations.begin(), mutations.end());
        bytes_ostream out;
        for (auto& m : fm) {
            ser::serialize(out, m);
        }
        return std::move(out).to_managed_bytes();
    }();

    mutation m(schema, key);
    m.set_cell(clustering_key_prefix::make_empty(), to_bytes("version"), version, timestamp);
    m.set_cell(clustering_key_prefix::make_empty(), to_bytes("written_at"), now, timestamp);
    // Avoid going through data_value and therefore `bytes`, as it can be large (#24809).
    auto cdef_data = schema->get_column_definition(to_bytes("data"));
    m.set_cell(clustering_key_prefix::make_empty(), *cdef_data, atomic_cell::make_live(*cdef_data->type, timestamp, std::move(data)));

    return m;
}

mutation get_batchlog_delete_mutation(schema_ptr schema, const utils::UUID& id) {
    auto key = partition_key::from_exploded(*schema, {uuid_type->decompose(id)});
    auto now = service::client_state(service::client_state::internal_tag()).get_timestamp();
    mutation m(schema, key);
    m.partition().apply_delete(*schema, clustering_key_prefix::make_empty(), tombstone(now, gc_clock::now()));
    return m;
}

} // namespace db

const std::chrono::seconds db::batchlog_manager::replay_interval;
const uint32_t db::batchlog_manager::page_size;

db::batchlog_manager::batchlog_manager(cql3::query_processor& qp, db::system_keyspace& sys_ks, batchlog_manager_config config)
        : _qp(qp)
        , _sys_ks(sys_ks)
        , _write_request_timeout(std::chrono::duration_cast<db_clock::duration>(config.write_request_timeout))
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

future<> db::batchlog_manager::do_batch_log_replay(post_replay_cleanup cleanup) {
    return container().invoke_on(0, [cleanup] (auto& bm) -> future<> {
        auto gate_holder = bm._gate.hold();
        auto sem_units = co_await get_units(bm._sem, 1);

        auto dest = bm._cpu++ % smp::count;
        blogger.debug("Batchlog replay on shard {}: starts", dest);
        auto last_replay = gc_clock::now();
        if (dest == 0) {
            co_await bm.replay_all_failed_batches(cleanup);
        } else {
            co_await bm.container().invoke_on(dest, [cleanup] (auto& bm) {
                return with_gate(bm._gate, [&bm, cleanup] {
                    return bm.replay_all_failed_batches(cleanup);
                });
            });
        }
        co_await bm.container().invoke_on_all([last_replay] (auto& bm) {
            bm._last_replay = last_replay;
        });
        blogger.debug("Batchlog replay on shard {}: done", dest);
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
    sstring query = format("SELECT count(*) FROM {}.{} BYPASS CACHE", system_keyspace::NAME, system_keyspace::BATCHLOG);
    return _qp.execute_internal(query, cql3::query_processor::cache_internal::yes).then([](::shared_ptr<cql3::untyped_result_set> rs) {
       return size_t(rs->one().get_as<int64_t>("count"));
    });
}

db_clock::duration db::batchlog_manager::get_batch_log_timeout() const {
    // enough time for the actual write + BM removal mutation
    return _write_request_timeout * 2;
}

future<> db::batchlog_manager::replay_all_failed_batches(post_replay_cleanup cleanup) {
    typedef db_clock::rep clock_type;

    // rate limit is in bytes per second. Uses Double.MAX_VALUE if disabled (set to 0 in cassandra.yaml).
    // max rate is scaled by the number of nodes in the cluster (same as for HHOM - see CASSANDRA-5272).
    auto throttle = _replay_rate / _qp.proxy().get_token_metadata_ptr()->count_normal_token_owners();
    auto limiter = make_lw_shared<utils::rate_limiter>(throttle);

<<<<<<< HEAD
    auto batch = [this, limiter](const cql3::untyped_result_set::row& row) -> future<stop_iteration> {
||||||| parent of 9434ec2fd1 (service,db: extract generation of batchlog delete mutation)
    auto schema = _qp.db().find_schema(system_keyspace::NAME, system_keyspace::BATCHLOG);
    auto delete_batch = [this, schema = std::move(schema)] (utils::UUID id) {
        auto key = partition_key::from_singular(*schema, id);
        mutation m(schema, key);
        auto now = service::client_state(service::client_state::internal_tag()).get_timestamp();
        m.partition().apply_delete(*schema, clustering_key_prefix::make_empty(), tombstone(now, gc_clock::now()));
        return _qp.proxy().mutate_locally(m, tracing::trace_state_ptr(), db::commitlog::force_sync::no);
    };

    auto batch = [this, limiter, delete_batch = std::move(delete_batch), &all_replayed](const cql3::untyped_result_set::row& row) -> future<stop_iteration> {
=======
    auto schema = _qp.db().find_schema(system_keyspace::NAME, system_keyspace::BATCHLOG);
    auto delete_batch = [this, schema = std::move(schema)] (utils::UUID id) {
        auto m = get_batchlog_delete_mutation(schema, id);
        return _qp.proxy().mutate_locally(m, tracing::trace_state_ptr(), db::commitlog::force_sync::no);
    };

    auto batch = [this, limiter, delete_batch = std::move(delete_batch), &all_replayed](const cql3::untyped_result_set::row& row) -> future<stop_iteration> {
>>>>>>> 9434ec2fd1 (service,db: extract generation of batchlog delete mutation)
        auto written_at = row.get_as<db_clock::time_point>("written_at");
        auto id = row.get_as<utils::UUID>("id");
        // enough time for the actual write + batchlog entry mutation delivery (two separate requests).
        auto timeout = get_batch_log_timeout();
        if (db_clock::now() < written_at + timeout) {
            blogger.debug("Skipping replay of {}, too fresh", id);
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }

        if (utils::get_local_injector().is_enabled("skip_batch_replay")) {
            blogger.debug("Skipping batch replay due to skip_batch_replay injection");
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }

        // check version of serialization format
        if (!row.has("version")) {
            blogger.warn("Skipping logged batch because of unknown version");
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }

        auto version = row.get_as<int32_t>("version");
        if (version != netw::messaging_service::current_version) {
            blogger.warn("Skipping logged batch because of incorrect version");
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }

        auto data = row.get_blob_unfragmented("data");

        blogger.debug("Replaying batch {}", id);

<<<<<<< HEAD
        auto fms = make_lw_shared<std::deque<canonical_mutation>>();
        auto in = ser::as_input_stream(data);
        while (in.size()) {
            fms->emplace_back(ser::deserialize(in, std::type_identity<canonical_mutation>()));
||||||| parent of 337f417b13 (db/batchlog_manager: batch(): replace map_reduce() with simple loop)
        try {
            auto fms = make_lw_shared<std::deque<canonical_mutation>>();
            auto in = ser::as_input_stream(data);
            while (in.size()) {
                fms->emplace_back(ser::deserialize(in, std::type_identity<canonical_mutation>()));
                schema_ptr s = _qp.db().find_schema(fms->back().column_family_id());
                timeout = std::min(timeout, std::chrono::duration_cast<db_clock::duration>(s->tombstone_gc_options().propagation_delay_in_seconds()));
            }

            if (now < written_at + timeout) {
                blogger.debug("Skipping replay of {}, too fresh", id);
                co_return stop_iteration::no;
            }

            auto size = data.size();

            auto mutations = co_await map_reduce(*fms, [this, written_at] (canonical_mutation& fm) {
                const auto& cf = _qp.proxy().local_db().find_column_family(fm.column_family_id());
                return make_ready_future<canonical_mutation*>(written_at > cf.get_truncation_time() ? &fm : nullptr);
            },
            utils::chunked_vector<mutation>(),
            [this] (utils::chunked_vector<mutation> mutations, canonical_mutation* fm) {
                if (fm) {
                    schema_ptr s = _qp.db().find_schema(fm->column_family_id());
                    mutations.emplace_back(fm->to_mutation(s));
                }
                return mutations;
            });

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
                    co_await _qp.proxy().send_batchlog_replay_to_all_replicas(std::move(mutations), timeout);
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
            co_return stop_iteration::no;
=======
        try {
            utils::chunked_vector<std::pair<canonical_mutation, schema_ptr>> fms;
            utils::chunked_vector<mutation> mutations;
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
                    co_await _qp.proxy().send_batchlog_replay_to_all_replicas(std::move(mutations), timeout);
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
            co_return stop_iteration::no;
>>>>>>> 337f417b13 (db/batchlog_manager: batch(): replace map_reduce() with simple loop)
        }

        auto size = data.size();

        return map_reduce(*fms, [this, written_at] (canonical_mutation& fm) {
            const auto& cf = _qp.proxy().local_db().find_column_family(fm.column_family_id());
            return make_ready_future<canonical_mutation*>(written_at > cf.get_truncation_time() ? &fm : nullptr);
        },
        utils::chunked_vector<mutation>(),
        [this] (utils::chunked_vector<mutation> mutations, canonical_mutation* fm) {
            if (fm) {
                schema_ptr s = _qp.db().find_schema(fm->column_family_id());
                mutations.emplace_back(fm->to_mutation(s));
            }
            return mutations;
        }).then([this, limiter, written_at, size, fms] (utils::chunked_vector<mutation> mutations) {
            if (mutations.empty()) {
                return make_ready_future<>();
            }
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

            if (ttl <= 0) {
                return make_ready_future<>();
            }
            // Origin does the send manually, however I can't see a super great reason to do so.
            // Our normal write path does not add much redundancy to the dispatch, and rate is handled after send
            // in both cases.
            // FIXME: verify that the above is reasonably true.
            return limiter->reserve(size).then([this, mutations = std::move(mutations)] {
                _stats.write_attempts += mutations.size();
                auto timeout = db::timeout_clock::now() + write_timeout;
                return _qp.proxy().send_batchlog_replay_to_all_replicas(std::move(mutations), timeout);
            });
        }).then_wrapped([this, id](future<> batch_result) {
            try {
                batch_result.get();
            } catch (data_dictionary::no_such_keyspace& ex) {
                // should probably ignore and drop the batch
            } catch (const data_dictionary::no_such_column_family&) {
                // As above -- we should drop the batch if the table doesn't exist anymore.
            } catch (...) {
                blogger.warn("Replay failed (will retry): {}", std::current_exception());
                // timeout, overload etc.
                // Do _not_ remove the batch, assuning we got a node write error.
                // Since we don't have hints (which origin is satisfied with),
                // we have to resort to keeping this batch to next lap.
                return make_ready_future<>();
            }
            // delete batch
            auto schema = _qp.db().find_schema(system_keyspace::NAME, system_keyspace::BATCHLOG);
            auto key = partition_key::from_singular(*schema, id);
            mutation m(schema, key);
            auto now = service::client_state(service::client_state::internal_tag()).get_timestamp();
            m.partition().apply_delete(*schema, clustering_key_prefix::make_empty(), tombstone(now, gc_clock::now()));
            return _qp.proxy().mutate_locally(m, tracing::trace_state_ptr(), db::commitlog::force_sync::no);
        }).then([] { return make_ready_future<stop_iteration>(stop_iteration::no); });
    };

    co_await with_gate(_gate, [this, cleanup, &all_replayed, batch = std::move(batch)] () mutable -> future<> {
        blogger.debug("Started replayAllFailedBatches with cleanup: {}", cleanup);
        co_await utils::get_local_injector().inject("add_delay_to_batch_replay", std::chrono::milliseconds(1000));
        co_await _qp.query_internal(
                format("SELECT id, data, written_at, version FROM {}.{} BYPASS CACHE", system_keyspace::NAME, system_keyspace::BATCHLOG),
                db::consistency_level::ONE,
                {},
                page_size,
                std::move(batch));
        if (cleanup == post_replay_cleanup::yes) {
            // Replaying batches could have generated tombstones, flush to disk,
            // where they can be compacted away.
            co_await replica::database::flush_table_on_all_shards(_qp.proxy().get_db(), system_keyspace::NAME, system_keyspace::BATCHLOG);
        }
        blogger.debug("Finished replayAllFailedBatches with all_replayed: {}", all_replayed);
    });
}
