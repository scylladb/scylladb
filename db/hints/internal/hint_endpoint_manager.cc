/*
 * Modified by ScyllaDB
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include "db/hints/internal/hint_endpoint_manager.hh"

// Seastar features.
#include <seastar/core/do_with.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>

// Scylla includes.
#include "db/hints/internal/common.hh"
#include "db/hints/internal/hint_logger.hh"
#include "db/hints/internal/hint_storage.hh"
#include "db/hints/manager.hh"
#include "gms/gossiper.hh"
#include "replica/database.hh"
#include "service/storage_proxy.hh"
#include "utils/disk-error-handler.hh"
#include "utils/div_ceil.hh"
#include "utils/error_injection.hh"
#include "utils/runtime.hh"
#include "converting_mutation_partition_applier.hh"
#include "gc_clock.hh"

// STD.
#include <algorithm>
#include <utility>
#include <vector>

namespace db::hints {
namespace internal {

bool end_point_hints_manager::store_hint(schema_ptr s, lw_shared_ptr<const frozen_mutation> fm, tracing::trace_state_ptr tr_state) noexcept {
    try {
        // Future is waited on indirectly in `stop()` (via `_store_gate`).
        (void)with_gate(_store_gate, [this, s = std::move(s), fm = std::move(fm), tr_state] () mutable {
            ++_hints_in_progress;
            size_t mut_size = fm->representation().size();
            shard_stats().size_of_hints_in_progress += mut_size;

            return with_shared(file_update_mutex(), [this, fm, s, tr_state] () mutable -> future<> {
                return get_or_load().then([this, fm = std::move(fm), s = std::move(s), tr_state] (hints_store_ptr log_ptr) mutable {
                    commitlog_entry_writer cew(s, *fm, db::commitlog::force_sync::no);
                    return log_ptr->add_entry(s->id(), cew, db::timeout_clock::now() + _shard_manager.hint_file_write_timeout);
                }).then([this, tr_state] (db::rp_handle rh) {
                    auto rp = rh.release();
                    if (_last_written_rp < rp) {
                        _last_written_rp = rp;
                        manager_logger.debug("[{}] Updated last written replay position to {}", end_point_key(), rp);
                    }
                    ++shard_stats().written;

                    manager_logger.trace("Hint to {} was stored", end_point_key());
                    tracing::trace(tr_state, "Hint to {} was stored", end_point_key());
                }).handle_exception([this, tr_state] (std::exception_ptr eptr) {
                    ++shard_stats().errors;

                    manager_logger.debug("store_hint(): got the exception when storing a hint to {}: {}", end_point_key(), eptr);
                    tracing::trace(tr_state, "Failed to store a hint to {}: {}", end_point_key(), eptr);
                });
            }).finally([this, mut_size, fm, s] {
                --_hints_in_progress;
                shard_stats().size_of_hints_in_progress -= mut_size;
            });;
        });
    } catch (...) {
        manager_logger.trace("Failed to store a hint to {}: {}", end_point_key(), std::current_exception());
        tracing::trace(tr_state, "Failed to store a hint to {}: {}", end_point_key(), std::current_exception());

        ++shard_stats().dropped;
        return false;
    }
    return true;
}

future<> end_point_hints_manager::populate_segments_to_replay() {
    return with_lock(file_update_mutex(), [this] {
        return get_or_load().discard_result();
    });
}

void end_point_hints_manager::start() {
    clear_stopped();
    allow_hints();
    _sender.start();
}

future<> end_point_hints_manager::stop(drain should_drain) noexcept {
    if(stopped()) {
        return make_exception_future<>(std::logic_error(format("ep_manager[{}]: stop() is called twice", _key).c_str()));
    }

    return seastar::async([this, should_drain] {
        std::exception_ptr eptr;

        // This is going to prevent further storing of new hints and will break all sending in progress.
        set_stopping();

        _store_gate.close().handle_exception([&eptr] (auto e) { eptr = std::move(e); }).get();
        _sender.stop(should_drain).handle_exception([&eptr] (auto e) { eptr = std::move(e); }).get();

        with_lock(file_update_mutex(), [this] {
            if (_hints_store_anchor) {
                hints_store_ptr tmp = std::exchange(_hints_store_anchor, nullptr);
                return tmp->shutdown().finally([tmp] {
                    return tmp->release();
                }).finally([tmp] {});
            }
            return make_ready_future<>();
        }).handle_exception([&eptr] (auto e) { eptr = std::move(e); }).get();

        if (eptr) {
            manager_logger.error("ep_manager[{}]: exception: {}", _key, eptr);
        }

        set_stopped();
    });
}

end_point_hints_manager::end_point_hints_manager(const endpoint_id& key, manager& shard_manager)
    : _key(key)
    , _shard_manager(shard_manager)
    , _file_update_mutex_ptr(make_lw_shared<seastar::shared_mutex>())
    , _file_update_mutex(*_file_update_mutex_ptr)
    , _state(state_set::of<state::stopped>())
    , _hints_dir(_shard_manager.hints_dir() / format("{}", _key).c_str())
    // Approximate the position of the last written hint by using the same formula as for segment id calculation in commitlog
    // TODO: Should this logic be deduplicated with what is in the commitlog?
    , _last_written_rp(this_shard_id(), std::chrono::duration_cast<std::chrono::milliseconds>(runtime::get_boot_time().time_since_epoch()).count())
    , _sender(*this, _shard_manager.local_storage_proxy(), _shard_manager.local_db(), _shard_manager.local_gossiper())
{}

end_point_hints_manager::end_point_hints_manager(end_point_hints_manager&& other)
    : _key(other._key)
    , _shard_manager(other._shard_manager)
    , _file_update_mutex_ptr(std::move(other._file_update_mutex_ptr))
    , _file_update_mutex(*_file_update_mutex_ptr)
    , _state(other._state)
    , _hints_dir(std::move(other._hints_dir))
    , _last_written_rp(other._last_written_rp)
    , _sender(other._sender, *this)
{}

end_point_hints_manager::~end_point_hints_manager() {
    assert(stopped());
}

future<hints_store_ptr> end_point_hints_manager::get_or_load() {
    if (!_hints_store_anchor) {
        return _shard_manager.store_factory().get_or_load(_key, [this] (const endpoint_id&) noexcept {
            return add_store();
        }).then([this] (hints_store_ptr log_ptr) {
            _hints_store_anchor = log_ptr;
            return make_ready_future<hints_store_ptr>(std::move(log_ptr));
        });
    }

    return make_ready_future<hints_store_ptr>(_hints_store_anchor);
}

future<db::commitlog> end_point_hints_manager::add_store() noexcept {
    manager_logger.trace("Going to add a store to {}", _hints_dir.c_str());

    return futurize_invoke([this] {
        return io_check([name = _hints_dir.c_str()] { return recursive_touch_directory(name); }).then([this] () {
            commitlog::config cfg;

            cfg.commit_log_location = _hints_dir.c_str();
            cfg.commitlog_segment_size_in_mb = resource_manager::hint_segment_size_in_mb;
            cfg.commitlog_total_space_in_mb = resource_manager::max_hints_per_ep_size_mb;
            cfg.fname_prefix = manager::FILENAME_PREFIX;
            cfg.extensions = &_shard_manager.local_db().extensions();

            // HH leaves segments on disk after commitlog shutdown, and later reads
            // them when commitlog is re-created. This is expected to happen regularly
            // during standard HH workload, so no need to print a warning about it.
            cfg.warn_about_segments_left_on_disk_after_shutdown = false;
            // Allow going over the configured size limit of the commitlog
            // (resource_manager::max_hints_per_ep_size_mb). The commitlog will
            // be more conservative with its disk usage when going over the limit.
            // On the other hand, HH counts used space using the space_watchdog
            // in resource_manager, so its redundant for the commitlog to apply
            // a hard limit.
            cfg.allow_going_over_size_limit = true;
            // The API for waiting for hint replay relies on replay positions
            // monotonically increasing. When there are no segments on disk,
            // by default the commitlog will calculate the first segment ID
            // based on the boot time. This may cause the following sequence
            // of events to occur:
            //
            // 1. Node starts with empty hints queue
            // 2. Some hints are written and some segments are created
            // 3. All hints are replayed
            // 4. Hint sync point is created
            // 5. Commitlog instance gets re-created and resets it segment ID counter
            // 6. New hint segment has the first ID as the first (deleted by now) segment
            // 7. Waiting for the sync point commences but resolves immediately
            //    before new hints are replayed - since point 5., `_last_written_rp`
            //    and `_sent_upper_bound_rp` are not updated because RPs of new
            //    hints are much lower than both of those marks.
            //
            // In order to prevent this situation, we override the base segment ID
            // of the newly created commitlog instance - it should start with an ID
            // which is larger than the segment ID of the RP of the last written hint.
            cfg.base_segment_id = _last_written_rp.base_id();

            return commitlog::create_commitlog(std::move(cfg)).then([this] (commitlog l) -> future<commitlog> {
                // add_store() is triggered every time hint files are forcefully flushed to I/O (every hints_flush_period).
                // When this happens we want to refill _sender's segments only if it has finished with the segments he had before.
                if (_sender.have_segments()) {
                    co_return l;
                }

                std::vector<sstring> segs_vec = co_await l.get_segments_to_replay();

                if (segs_vec.empty()) {
                    // If the segs_vec is empty, this means that there are no more
                    // hints to be replayed. We can safely skip to the position of the
                    // last written hint.
                    //
                    // This is necessary: remember that we artificially set
                    // the last replayed position based on the creation time
                    // of the endpoint manager. If we replay all segments from
                    // previous runtimes but won't write any new hints during
                    // this runtime, then without the logic below the hint replay
                    // tracker won't reach the hint written tracker.
                    auto rp = _last_written_rp;
                    rp.pos++;
                    _sender.rewind_sent_replay_position_to(rp);
                    co_return l;
                }

                std::vector<std::pair<db::segment_id_type, sstring>> local_segs_vec;
                local_segs_vec.reserve(segs_vec.size());

                // Divide segments into those that were created on this shard
                // and those which were moved to it during rebalancing.
                for (auto& seg : segs_vec) {
                    db::commitlog::descriptor desc(seg, manager::FILENAME_PREFIX);
                    unsigned shard_id = db::replay_position(desc).shard_id();
                    if (shard_id == this_shard_id()) {
                        local_segs_vec.emplace_back(desc.id, std::move(seg));
                    } else {
                        _sender.add_foreign_segment(std::move(seg));
                    }
                }

                // Sort local segments by their segment ids, which should
                // correspond to the chronological order.
                std::sort(local_segs_vec.begin(), local_segs_vec.end());

                for (auto& [segment_id, seg] : local_segs_vec) {
                    _sender.add_segment(std::move(seg));
                }

                co_return l;
            });
        });
    });
}

future<> end_point_hints_manager::flush_current_hints() noexcept {
    // flush the currently created hints to disk
    if (_hints_store_anchor) {
        return futurize_invoke([this] {
            return with_lock(file_update_mutex(), [this]() -> future<> {
                return get_or_load().then([] (hints_store_ptr cptr) {
                    return cptr->shutdown().finally([cptr] {
                        return cptr->release();
                    }).finally([cptr] {});
                }).then([this] {
                    // Un-hold the commitlog object. Since we are under the exclusive _file_update_mutex lock there are no
                    // other hints_store_ptr copies and this would destroy the commitlog shared value.
                    _hints_store_anchor = nullptr;

                    // Re-create the commitlog instance - this will re-populate the _segments_to_replay if needed.
                    return get_or_load().discard_result();
                });
            });
        });
    }

    return make_ready_future<>();
}

class no_column_mapping : public std::out_of_range {
public:
    no_column_mapping(const table_schema_version& id) : std::out_of_range(format("column mapping for CF schema_version {} is missing", id)) {}
};

future<> end_point_hints_manager::sender::flush_maybe() noexcept {
    auto current_time = clock::now();
    if (current_time >= _next_flush_tp) {
        return _ep_manager.flush_current_hints().then([this, current_time] {
            _next_flush_tp = current_time + manager::hints_flush_period;
        }).handle_exception([] (auto eptr) {
            manager_logger.trace("flush_maybe() failed: {}", eptr);
            return make_ready_future<>();
        });
    }
    return make_ready_future<>();
}

future<timespec> end_point_hints_manager::sender::get_last_file_modification(const sstring& fname) {
    return open_file_dma(fname, open_flags::ro).then([] (file f) {
        return do_with(std::move(f), [] (file& f) {
            return f.stat();
        });
    }).then([] (struct stat st) {
        return make_ready_future<timespec>(st.st_mtim);
    });
}

future<> end_point_hints_manager::sender::do_send_one_mutation(frozen_mutation_and_schema m, locator::effective_replication_map_ptr ermp, const inet_address_vector_replica_set& natural_endpoints) noexcept {
    return futurize_invoke([this, m = std::move(m), ermp = std::move(ermp), &natural_endpoints] () mutable -> future<> {
        // The fact that we send with CL::ALL in both cases below ensures that new hints are not going
        // to be generated as a result of hints sending.
        if (boost::range::find(natural_endpoints, end_point_key()) != natural_endpoints.end()) {
            manager_logger.trace("Sending directly to {}", end_point_key());
            return _proxy.send_hint_to_endpoint(std::move(m), std::move(ermp), end_point_key());
        } else {
            manager_logger.trace("Endpoints set has changed and {} is no longer a replica. Mutating from scratch...", end_point_key());
            return _proxy.send_hint_to_all_replicas(std::move(m));
        }
    });
}

bool end_point_hints_manager::sender::can_send() noexcept {
    if (stopping() && !draining()) {
        return false;
    }

    try {
        if (_gossiper.is_alive(end_point_key())) {
            _state.remove(state::ep_state_left_the_ring);
            return true;
        } else {
            if (!_state.contains(state::ep_state_left_the_ring)) {
                _state.set_if<state::ep_state_left_the_ring>(!_shard_manager.local_db().get_token_metadata().is_normal_token_owner(end_point_key()));
            }
            // send the hints out if the destination Node is part of the ring - we will send to all new replicas in this case
            return _state.contains(state::ep_state_left_the_ring);
        }
    } catch (...) {
        return false;
    }
}

frozen_mutation_and_schema end_point_hints_manager::sender::get_mutation(lw_shared_ptr<send_one_file_ctx> ctx_ptr, fragmented_temporary_buffer& buf) {
    hint_entry_reader hr(buf);
    auto& fm = hr.mutation();
    auto& cm = get_column_mapping(std::move(ctx_ptr), fm, hr);
    auto schema = _db.find_schema(fm.column_family_id());

    if (schema->version() != fm.schema_version()) {
        mutation m(schema, fm.decorated_key(*schema));
        converting_mutation_partition_applier v(cm, *schema, m.partition());
        fm.partition().accept(cm, v);
        return {freeze(m), std::move(schema)};
    }
    return {std::move(hr).mutation(), std::move(schema)};
}

const column_mapping& end_point_hints_manager::sender::get_column_mapping(lw_shared_ptr<send_one_file_ctx> ctx_ptr, const frozen_mutation& fm, const hint_entry_reader& hr) {
    auto cm_it = ctx_ptr->schema_ver_to_column_mapping.find(fm.schema_version());
    if (cm_it == ctx_ptr->schema_ver_to_column_mapping.end()) {
        if (!hr.get_column_mapping()) {
            throw no_column_mapping(fm.schema_version());
        }

        manager_logger.debug("new schema version {}", fm.schema_version());
        cm_it = ctx_ptr->schema_ver_to_column_mapping.emplace(fm.schema_version(), *hr.get_column_mapping()).first;
    }

    return cm_it->second;
}

end_point_hints_manager::sender::sender(end_point_hints_manager& parent, service::storage_proxy& local_storage_proxy,replica::database& local_db, gms::gossiper& local_gossiper) noexcept
    : _stopped(make_ready_future<>())
    , _ep_key(parent.end_point_key())
    , _ep_manager(parent)
    , _shard_manager(_ep_manager._shard_manager)
    , _resource_manager(_shard_manager._resource_manager)
    , _proxy(local_storage_proxy)
    , _db(local_db)
    , _hints_cpu_sched_group(_db.get_streaming_scheduling_group())
    , _gossiper(local_gossiper)
    , _file_update_mutex(_ep_manager.file_update_mutex())
{}

end_point_hints_manager::sender::sender(const sender& other, end_point_hints_manager& parent) noexcept
    : _stopped(make_ready_future<>())
    , _ep_key(parent.end_point_key())
    , _ep_manager(parent)
    , _shard_manager(_ep_manager._shard_manager)
    , _resource_manager(_shard_manager._resource_manager)
    , _proxy(other._proxy)
    , _db(other._db)
    , _hints_cpu_sched_group(other._hints_cpu_sched_group)
    , _gossiper(other._gossiper)
    , _file_update_mutex(_ep_manager.file_update_mutex())
{}

end_point_hints_manager::sender::~sender() {
    dismiss_replay_waiters();
}


future<> end_point_hints_manager::sender::stop(drain should_drain) noexcept {
    return seastar::async([this, should_drain] {
        set_stopping();
        _stop_as.request_abort();
        _stopped.get();

        if (should_drain == drain::yes) {
            // "Draining" is performed by a sequence of following calls:
            // set_draining() -> send_hints_maybe() -> flush_current_hints() -> send_hints_maybe()
            //
            // Before sender::stop() is called the storing path for this end point is blocked and no new hints
            // will be generated when this method is running.
            //
            // send_hints_maybe() in a "draining" mode is going to send all hints from segments in the
            // _segments_to_replay.
            //
            // Therefore after the first call for send_hints_maybe() the _segments_to_replay is going to become empty
            // and the following flush_current_hints() is going to store all in-memory hints to the disk and re-populate
            // the _segments_to_replay.
            //
            // The next call for send_hints_maybe() will send the last hints to the current end point and when it is
            // done there is going to be no more pending hints and the corresponding hints directory may be removed.
            manager_logger.trace("Draining for {}: start", end_point_key());
            set_draining();
            send_hints_maybe();
            _ep_manager.flush_current_hints().handle_exception([] (auto e) {
                manager_logger.error("Failed to flush pending hints: {}. Ignoring...", e);
            }).get();
            send_hints_maybe();
            manager_logger.trace("Draining for {}: end", end_point_key());
        }
        manager_logger.trace("ep_manager({})::sender: exiting", end_point_key());
    });
}

void end_point_hints_manager::sender::add_segment(sstring seg_name) {
    _segments_to_replay.emplace_back(std::move(seg_name));
}

void end_point_hints_manager::sender::add_foreign_segment(sstring seg_name) {
    _foreign_segments_to_replay.emplace_back(std::move(seg_name));
}

end_point_hints_manager::sender::clock::duration end_point_hints_manager::sender::next_sleep_duration() const {
    clock::time_point current_time = clock::now();
    clock::time_point next_flush_tp = std::max(_next_flush_tp, current_time);
    clock::time_point next_retry_tp = std::max(_next_send_retry_tp, current_time);

    clock::duration d = std::min(next_flush_tp, next_retry_tp) - current_time;

    // Don't sleep for less than 10 ticks of the "clock" if we are planning to sleep at all - the sleep() function is not perfect.
    return clock::duration(10 * div_ceil(d.count(), 10));
}

void end_point_hints_manager::sender::start() {
    seastar::thread_attributes attr;

    attr.sched_group = _hints_cpu_sched_group;
    _stopped = seastar::async(std::move(attr), [this] {
        manager_logger.trace("ep_manager({})::sender: started", end_point_key());
        while (!stopping()) {
            try {
                flush_maybe().get();
                send_hints_maybe();

                // If we got here means that either there are no more hints to send or we failed to send hints we have.
                // In both cases it makes sense to wait a little before continuing.
                sleep_abortable(next_sleep_duration(), _stop_as).get();
            } catch (seastar::sleep_aborted&) {
                break;
            } catch (...) {
                // log and keep on spinning
                manager_logger.trace("sender: got the exception: {}", std::current_exception());
            }
        }
    });
}

future<> end_point_hints_manager::sender::send_one_mutation(frozen_mutation_and_schema m) {
    auto erm = _db.find_column_family(m.s).get_effective_replication_map();
    auto token = dht::get_token(*m.s, m.fm.key());
    inet_address_vector_replica_set natural_endpoints = erm->get_natural_endpoints(std::move(token));

    return do_send_one_mutation(std::move(m), std::move(erm), std::move(natural_endpoints));
}

future<> end_point_hints_manager::sender::send_one_hint(lw_shared_ptr<send_one_file_ctx> ctx_ptr, fragmented_temporary_buffer buf, db::replay_position rp, gc_clock::duration secs_since_file_mod, const sstring& fname) {
    return _resource_manager.get_send_units_for(buf.size_bytes()).then([this, secs_since_file_mod, &fname, buf = std::move(buf), rp, ctx_ptr] (auto units) mutable {
        ctx_ptr->mark_hint_as_in_progress(rp);

        // Future is waited on indirectly in `send_one_file()` (via `ctx_ptr->file_send_gate`).
        auto h = ctx_ptr->file_send_gate.hold();
        (void)std::invoke([this, secs_since_file_mod, &fname, buf = std::move(buf), rp, ctx_ptr] () mutable {
            try {
                auto m = this->get_mutation(ctx_ptr, buf);
                gc_clock::duration gc_grace_sec = m.s->gc_grace_seconds();

                // The hint is too old - drop it.
                //
                // Files are aggregated for at most manager::hints_timer_period therefore the oldest hint there is
                // (last_modification - manager::hints_timer_period) old.
                if (const auto now = gc_clock::now().time_since_epoch(); now - secs_since_file_mod > gc_grace_sec - manager::hints_flush_period) {
                    manager_logger.debug("send_hints(): the hint is too old, skipping it, "
                        "secs since file last modification {}, gc_grace_sec {}, hints_flush_period {}",
                        now - secs_since_file_mod, gc_grace_sec, manager::hints_flush_period);
                    return make_ready_future<>();
                }

                return this->send_one_mutation(std::move(m)).then([this, ctx_ptr] {
                    ++this->shard_stats().sent;
                }).handle_exception([this, ctx_ptr] (auto eptr) {
                    manager_logger.trace("send_one_hint(): failed to send to {}: {}", end_point_key(), eptr);
                    ++this->shard_stats().send_errors;
                    return make_exception_future<>(std::move(eptr));
                });

            // ignore these errors and move on - probably this hint is too old and the KS/CF has been deleted...
            } catch (replica::no_such_column_family& e) {
                manager_logger.debug("send_hints(): no_such_column_family: {}", e.what());
                ++this->shard_stats().discarded;
            } catch (replica::no_such_keyspace& e) {
                manager_logger.debug("send_hints(): no_such_keyspace: {}", e.what());
                ++this->shard_stats().discarded;
            } catch (no_column_mapping& e) {
                manager_logger.debug("send_hints(): {} at {}: {}", fname, rp, e.what());
                ++this->shard_stats().discarded;
            } catch (...) {
                auto eptr = std::current_exception();
                manager_logger.debug("send_hints(): unexpected error in file {} at {}: {}", fname, rp, eptr);
                ++this->shard_stats().send_errors;
                return make_exception_future<>(std::move(eptr));
            }
            return make_ready_future<>();
        }).then_wrapped([this, units = std::move(units), rp, ctx_ptr, h = std::move(h)] (future<>&& f) {
            // Information about the error was already printed somewhere higher.
            // We just need to account in the ctx that sending of this hint has failed.
            if (!f.failed()) {
                ctx_ptr->on_hint_send_success(rp);
                auto new_bound = ctx_ptr->get_replayed_bound();
                // Segments from other shards are replayed first and are considered to be "before" replay position 0.
                // Update the sent upper bound only if it is a local segment.
                if (new_bound.shard_id() == this_shard_id() && _sent_upper_bound_rp < new_bound) {
                    _sent_upper_bound_rp = new_bound;
                    notify_replay_waiters();
                }
            } else {
                ctx_ptr->on_hint_send_failure(rp);
            }
            f.ignore_ready_future();
        });
    }).handle_exception([ctx_ptr, rp] (auto eptr) {
        manager_logger.trace("send_one_file(): Hmmm. Something bad had happend: {}", eptr);
        ctx_ptr->on_hint_send_failure(rp);
    });
}

void end_point_hints_manager::sender::notify_replay_waiters() noexcept {
    if (!_foreign_segments_to_replay.empty()) {
        manager_logger.trace("[{}] notify_replay_waiters(): not notifying because there are still {} foreign segments to replay", end_point_key(), _foreign_segments_to_replay.size());
        return;
    }

    manager_logger.trace("[{}] notify_replay_waiters(): replay position upper bound was updated to {}", end_point_key(), _sent_upper_bound_rp);
    while (!_replay_waiters.empty() && _replay_waiters.begin()->first < _sent_upper_bound_rp) {
        manager_logger.trace("[{}] notify_replay_waiters(): notifying one ({} < {})", end_point_key(), _replay_waiters.begin()->first, _sent_upper_bound_rp);
        auto ptr = _replay_waiters.begin()->second;
        (**ptr).set_value();
        (*ptr) = std::nullopt; // Prevent it from being resolved by abort source subscription
        _replay_waiters.erase(_replay_waiters.begin());
    }
}

void end_point_hints_manager::sender::dismiss_replay_waiters() noexcept {
    for (auto& p : _replay_waiters) {
        manager_logger.debug("[{}] dismiss_replay_waiters(): dismissing one", end_point_key());
        auto ptr = p.second;
        (**ptr).set_exception(std::runtime_error(format("Hints manager for {} is stopping", end_point_key())));
        (*ptr) = std::nullopt; // Prevent it from being resolved by abort source subscription
    }
    _replay_waiters.clear();
}

future<> end_point_hints_manager::sender::wait_until_hints_are_replayed_up_to(abort_source& as, db::replay_position up_to_rp) {
    manager_logger.debug("[{}] wait_until_hints_are_replayed_up_to(): entering with target {}", end_point_key(), up_to_rp);
    if (_foreign_segments_to_replay.empty() && up_to_rp < _sent_upper_bound_rp) {
        manager_logger.debug("[{}] wait_until_hints_are_replayed_up_to(): hints were already replayed above the point ({} < {})", end_point_key(), up_to_rp, _sent_upper_bound_rp);
        return make_ready_future<>();
    }

    if (as.abort_requested()) {
        manager_logger.debug("[{}] wait_until_hints_are_replayed_up_to(): already aborted - stopping", end_point_key());
        return make_exception_future<>(abort_requested_exception());
    }

    auto ptr = make_lw_shared<std::optional<promise<>>>(promise<>());
    auto it = _replay_waiters.emplace(up_to_rp, ptr);
    auto sub = as.subscribe([this, ptr, it] () noexcept {
        if (!ptr->has_value()) {
            // The promise already was resolved by `notify_replay_waiters` and removed from the map
            return;
        }
        manager_logger.debug("[{}] wait_until_hints_are_replayed_up_to(): abort requested - stopping", end_point_key());
        _replay_waiters.erase(it);
        (**ptr).set_exception(abort_requested_exception());
    });

    // When the future resolves, the endpoint manager is not guaranteed to exist anymore
    // therefore we cannot capture `this`
    auto ep = end_point_key();
    return (**ptr).get_future().finally([sub = std::move(sub), ep] {
        manager_logger.debug("[{}] wait_until_hints_are_replayed_up_to(): returning afther the future was satisfied", ep);
    });
}

void end_point_hints_manager::sender::send_one_file_ctx::mark_hint_as_in_progress(db::replay_position rp) {
    in_progress_rps.insert(rp);
}

void end_point_hints_manager::sender::send_one_file_ctx::on_hint_send_success(db::replay_position rp) noexcept {
    in_progress_rps.erase(rp);
    if (!last_succeeded_rp || *last_succeeded_rp < rp) {
        last_succeeded_rp = rp;
    }
}

void end_point_hints_manager::sender::send_one_file_ctx::on_hint_send_failure(db::replay_position rp) noexcept {
    in_progress_rps.erase(rp);
    segment_replay_failed = true;
    if (!first_failed_rp || rp < *first_failed_rp) {
        first_failed_rp = rp;
    }
}

db::replay_position end_point_hints_manager::sender::send_one_file_ctx::get_replayed_bound() const noexcept {
    // We are sure that all hints were sent _below_ the position which is the minimum of the following:
    // - Position of the first hint that failed to be sent in this replay (first_failed_rp),
    // - Position of the last hint which was successfully sent (last_succeeded_rp, inclusive bound),
    // - Position of the lowest hint which is being currently sent (in_progress_rps.begin()).

    db::replay_position rp;
    if (first_failed_rp) {
        rp = *first_failed_rp;
    } else if (last_succeeded_rp) {
        // It is always true that `first_failed_rp` <= `last_succeeded_rp`, so no need to compare
        rp = *last_succeeded_rp;
        // We replayed _up to_ `last_attempted_rp`, so the bound is not strict; we can increase `pos` by one
        rp.pos++;
    }

    if (!in_progress_rps.empty() && *in_progress_rps.begin() < rp) {
        rp = *in_progress_rps.begin();
    }

    return rp;
}

void end_point_hints_manager::sender::rewind_sent_replay_position_to(db::replay_position rp) {
    _sent_upper_bound_rp = rp;
    notify_replay_waiters();
}

// runs in a seastar::async context
bool end_point_hints_manager::sender::send_one_file(const sstring& fname) {
    timespec last_mod = get_last_file_modification(fname).get0();
    gc_clock::duration secs_since_file_mod = std::chrono::seconds(last_mod.tv_sec);
    lw_shared_ptr<send_one_file_ctx> ctx_ptr = make_lw_shared<send_one_file_ctx>(_last_schema_ver_to_column_mapping);

    try {
        commitlog::read_log_file(fname, manager::FILENAME_PREFIX, [this, secs_since_file_mod, &fname, ctx_ptr] (commitlog::buffer_and_replay_position buf_rp) -> future<> {
            auto& buf = buf_rp.buffer;
            auto& rp = buf_rp.position;

            while (true) {
                // Check that we can still send the next hint. Don't try to send it if the destination host
                // is DOWN or if we have already failed to send some of the previous hints.
                if (!draining() && ctx_ptr->segment_replay_failed) {
                    co_return;
                }

                // Break early if stop() was called or the destination node went down.
                if (!can_send()) {
                    ctx_ptr->segment_replay_failed = true;
                    co_return;
                }

                co_await flush_maybe();

                if (utils::get_local_injector().enter("hinted_handoff_pause_hint_replay")) {
                    // We cannot send the hint because hint replay is paused.
                    // Sleep 100ms and do the whole loop again.
                    //
                    // Jumping to the beginning of the loop makes sure that
                    // - We regularly check if we should stop - so that we won't
                    //   get stuck in shutdown.
                    // - flush_maybe() is called regularly - so that new segments
                    //   are created and we help enforce the "at most 10s worth of
                    //   hints in a segment".
                    co_await sleep(std::chrono::milliseconds(100));
                    continue;
                } else {
                    co_await send_one_hint(ctx_ptr, std::move(buf), rp, secs_since_file_mod, fname);
                    break;
                }
            };
        }, _last_not_complete_rp.pos, &_db.extensions()).get();
    } catch (db::commitlog::segment_error& ex) {
        manager_logger.error("{}: {}. Dropping...", fname, ex.what());
        ctx_ptr->segment_replay_failed = false;
        ++this->shard_stats().corrupted_files;
    } catch (...) {
        manager_logger.trace("sending of {} failed: {}", fname, std::current_exception());
        ctx_ptr->segment_replay_failed = true;
    }

    // wait till all background hints sending is complete
    ctx_ptr->file_send_gate.close().get();

    // If we are draining ignore failures and drop the segment even if we failed to send it.
    if (draining() && ctx_ptr->segment_replay_failed) {
        manager_logger.trace("send_one_file(): we are draining so we are going to delete the segment anyway");
        ctx_ptr->segment_replay_failed = false;
    }

    // update the next iteration replay position if needed
    if (ctx_ptr->segment_replay_failed) {
        // If some hints failed to be sent, first_failed_rp will tell the position of first such hint.
        // If there was an error thrown by read_log_file function itself, we will retry sending from
        // the last hint that was successfully sent (last_succeeded_rp).
        _last_not_complete_rp = ctx_ptr->first_failed_rp.value_or(ctx_ptr->last_succeeded_rp.value_or(_last_not_complete_rp));
        manager_logger.trace("send_one_file(): error while sending hints from {}, last RP is {}", fname, _last_not_complete_rp);
        return false;
    }

    // If we got here we are done with the current segment and we can remove it.
    with_shared(_file_update_mutex, [&fname, this] {
        auto p = _ep_manager.get_or_load().get0();
        return p->delete_segments({ fname });
    }).get();

    // clear the replay position - we are going to send the next segment...
    _last_not_complete_rp = replay_position();
    _last_schema_ver_to_column_mapping.clear();
    manager_logger.trace("send_one_file(): segment {} was sent in full and deleted", fname);
    return true;
}

const sstring* end_point_hints_manager::sender::name_of_current_segment() const {
    // Foreign segments are replayed first
    if (!_foreign_segments_to_replay.empty()) {
        return &_foreign_segments_to_replay.front();
    }
    if (!_segments_to_replay.empty()) {
        return &_segments_to_replay.front();
    }
    return nullptr;
}

void end_point_hints_manager::sender::pop_current_segment() {
    if (!_foreign_segments_to_replay.empty()) {
        _foreign_segments_to_replay.pop_front();
    } else if (!_segments_to_replay.empty()) {
        _segments_to_replay.pop_front();
    }
}

// Runs in the seastar::async context
void end_point_hints_manager::sender::send_hints_maybe() noexcept {
    using namespace std::literals::chrono_literals;
    manager_logger.trace("send_hints(): going to send hints to {}, we have {} segment to replay", end_point_key(), _segments_to_replay.size() + _foreign_segments_to_replay.size());

    int replayed_segments_count = 0;

    try {
        while (true) {
            const sstring* seg_name = name_of_current_segment();
            if (!seg_name || !replay_allowed() || !can_send()) {
                break;
            }
            if (!send_one_file(*seg_name)) {
                break;
            }
            pop_current_segment();
            ++replayed_segments_count;

            notify_replay_waiters();
        }

    // Ignore exceptions, we will retry sending this file from where we left off the next time.
    // Exceptions are not expected here during the regular operation, so just log them.
    } catch (...) {
        manager_logger.trace("send_hints(): got the exception: {}", std::current_exception());
    }

    if (have_segments()) {
        // TODO: come up with something more sophisticated here
        _next_send_retry_tp = clock::now() + 1s;
    } else {
        // if there are no segments to send we want to retry when we maybe have some (after flushing)
        _next_send_retry_tp = _next_flush_tp;
    }

    manager_logger.trace("send_hints(): we handled {} segments", replayed_segments_count);
}

hint_stats& end_point_hints_manager::sender::shard_stats() {
    return _shard_manager._stats;
}

bool end_point_hints_manager::replay_allowed() const noexcept {
    return _shard_manager.replay_allowed();
}

hint_stats& end_point_hints_manager::shard_stats() {
    return _shard_manager._stats;
}

resource_manager& end_point_hints_manager::shard_resource_manager() {
    return _shard_manager._resource_manager;
}

} // namespace internal
} // namespace db::hints
