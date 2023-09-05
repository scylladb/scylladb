/*
 * Modified by ScyllaDB
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "db/hints/internal/hint_sender.hh"

// Seastar features.
#include <exception>
#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/print.hh>
#include <seastar/core/seastar.hh>

// Scylla includes.
#include "db/hints/internal/common.hh"
#include "db/hints/internal/hint_logger.hh"
#include "db/hints/internal/host_manager.hh"
#include "db/hints/manager.hh"
#include "db/hints/resource_manager.hh"
#include "gms/gossiper.hh"
#include "gms/inet_address.hh"
#include "replica/database.hh"
#include "schema/schema_fwd.hh"
#include "service/storage_proxy.hh"
#include "utils/div_ceil.hh"
#include "utils/error_injection.hh"
#include "converting_mutation_partition_applier.hh"
#include "gc_clock.hh"

// STD.
#include <ranges>
#include <span>
#include <stdexcept>
#include <string_view>

namespace db::hints {
namespace internal {

namespace {

class no_column_mapping : public std::out_of_range {
public:
    no_column_mapping(const table_schema_version& id)
        : std::out_of_range(format("column mapping for CF schema_version {} is missing", id))
    {}
};

/// \brief Get the last modification time stamp for a given file.
/// \param fname File name
/// \return The last modification time stamp for \param fname.
future<::timespec> get_last_file_modification(const std::string_view fname) {
    auto f = co_await open_file_dma(fname, open_flags::ro);
    const auto st = co_await f.stat();
    co_return st.st_mtim;
}

} // anonymous namespace

struct send_one_file_ctx {
public:
    std::unordered_map<table_schema_version, column_mapping>& schema_ver_to_column_mapping;
    seastar::gate file_send_gate;
    std::optional<replay_position> first_failed_rp;
    std::optional<replay_position> last_succeeded_rp;
    std::set<replay_position> in_progress_rps;
    bool segment_replay_failed = false;

public:
    send_one_file_ctx(std::unordered_map<table_schema_version, column_mapping>& last_schema_ver_to_column_mapping)
        : schema_ver_to_column_mapping(last_schema_ver_to_column_mapping)
    {}

public:
    void mark_hint_as_in_progress(replay_position rp) {
        in_progress_rps.insert(rp);
    }
    // Noexcept because `std::set::erase` is noexcept when
    // the operator< of the value type is noexcept.
    void on_hint_send_success(replay_position rp) noexcept {
        in_progress_rps.erase(rp);
        if (!last_succeeded_rp || *last_succeeded_rp < rp) {
            last_succeeded_rp = rp;
        }
    }
    // Ditto.
    void on_hint_send_failure(replay_position rp) noexcept {
        in_progress_rps.erase(rp);
        segment_replay_failed = true;
        if (!first_failed_rp || rp < *first_failed_rp) {
            first_failed_rp = rp;
        }
    }

    // Returns a position below which hints were successfully replayed.
    replay_position get_replayed_bound() const noexcept {
        // We are sure that all hints were sent _below_ the position which is the minimum of the following:
        // - Position of the first hint that failed to be sent in this replay (first_failed_rp),
        // - Position of the last hint which was successfully sent (last_succeeded_rp, inclusive bound),
        // - Position of the lowest hint which is being currently sent (in_progress_rps.begin()).

        replay_position rp;
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
};


/////////////////////////////////////////////////
/////////////////////////////////////////////////


hint_sender::hint_sender(host_manager& parent, service::storage_proxy& local_storage_proxy,
        replica::database& local_db, gms::gossiper& local_gossiper)
    : _ep_key{parent.end_point_key()}
    , _host_manager{parent}
    , _shard_manager{_host_manager._shard_manager}
    , _resource_manager{_shard_manager._resource_manager}
    , _proxy{local_storage_proxy}
    , _db{local_db}
    , _gossiper{local_gossiper}
    , _hints_cpu_sched_group{_db.get_streaming_scheduling_group()}
    , _file_update_mutex{_host_manager.file_update_mutex()}
{}

hint_sender::hint_sender(hint_sender&& other, host_manager& new_parent) noexcept
    : _segments_to_replay{std::move(other._segments_to_replay)}
    , _foreign_segments_to_replay{std::move(other._foreign_segments_to_replay)}
    , _last_not_complete_rp{std::move(other._last_not_complete_rp)}
    , _sent_upper_bound_rp{std::move(other._sent_upper_bound_rp)}
    , _last_schema_ver_to_column_mapping{std::move(other._last_schema_ver_to_column_mapping)}
    , _state{other._state}
    , _stopped{std::move(other._stopped)}
    , _stop_as{std::move(other._stop_as)}
    , _next_flush_tp{std::move(other._next_flush_tp)}
    , _next_send_retry_tp{std::move(other._next_send_retry_tp)}
    , _ep_key{new_parent.end_point_key()}
    , _host_manager{new_parent}
    , _shard_manager{other._shard_manager}
    , _resource_manager{other._resource_manager}
    , _proxy{other._proxy}
    , _db{other._db}
    , _gossiper{other._gossiper}
    , _hints_cpu_sched_group{other._hints_cpu_sched_group}
    , _file_update_mutex{new_parent.file_update_mutex()}
    , _replay_waiters{std::move(other._replay_waiters)}
{}

void hint_sender::start() {
    seastar::thread_attributes attr;

    attr.sched_group = _hints_cpu_sched_group;
    _stopped = seastar::async(std::move(attr), [this] {
        manager_logger.trace("ep_manager({})::sender: started", _ep_key);
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

future<> hint_sender::stop(drain should_drain) noexcept {
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
            manager_logger.trace("Draining for {}: start", _ep_key);
            set_draining();
            send_hints_maybe();
            _host_manager.flush_current_hints().handle_exception([] (auto e) {
                manager_logger.error("Failed to flush pending hints: {}. Ignoring...", e);
            }).get();
            send_hints_maybe();
            manager_logger.trace("Draining for {}: end", _ep_key);
        }
        manager_logger.trace("ep_manager({})::sender: exiting", _ep_key);
    });
}

void hint_sender::rewind_sent_replay_position_to(replay_position rp) {
    _sent_upper_bound_rp = rp;
    notify_replay_waiters();
}

future<> hint_sender::wait_until_hints_are_replayed_up_to(abort_source& as, replay_position up_to_rp) {
    manager_logger.debug("[{}] wait_until_hints_are_replayed_up_to(): entering with target {}", _ep_key, up_to_rp);
    if (_foreign_segments_to_replay.empty() && up_to_rp < _sent_upper_bound_rp) {
        manager_logger.debug("[{}] wait_until_hints_are_replayed_up_to(): hints were already replayed above the point ({} < {})", _ep_key, up_to_rp, _sent_upper_bound_rp);
        return make_ready_future<>();
    }

    if (as.abort_requested()) {
        manager_logger.debug("[{}] wait_until_hints_are_replayed_up_to(): already aborted - stopping", _ep_key);
        return make_exception_future<>(abort_requested_exception());
    }

    auto ptr = make_lw_shared<std::optional<promise<>>>(promise<>());
    auto it = _replay_waiters.emplace(up_to_rp, ptr);
    auto sub = as.subscribe([this, ptr, it] () noexcept {
        if (!ptr->has_value()) {
            // The promise already was resolved by `notify_replay_waiters` and removed from the map
            return;
        }
        manager_logger.debug("[{}] wait_until_hints_are_replayed_up_to(): abort requested - stopping", _ep_key);
        _replay_waiters.erase(it);
        (**ptr).set_exception(abort_requested_exception());
    });

    // When the future resolves, the endpoint manager is not guaranteed to exist anymore
    // therefore we cannot capture `this`
    auto ep = _ep_key;
    return (**ptr).get_future().finally([sub = std::move(sub), ep] {
        manager_logger.debug("[{}] wait_until_hints_are_replayed_up_to(): returning afther the future was satisfied", ep);
    });
}

const sstring* hint_sender::name_of_current_segment() const {
    // Foreign segments are replayed first
    if (!_foreign_segments_to_replay.empty()) {
        return &_foreign_segments_to_replay.front();
    }
    if (!_segments_to_replay.empty()) {
        return &_segments_to_replay.front();
    }
    return nullptr;
}

void hint_sender::pop_current_segment() {
    if (!_foreign_segments_to_replay.empty()) {
        _foreign_segments_to_replay.pop_front();
    } else if (!_segments_to_replay.empty()) {
        _segments_to_replay.pop_front();
    }
}

future<> hint_sender::flush_maybe() noexcept {
    const auto current_time = clock_type::now();

    if (current_time >= _next_flush_tp) {
        try {
            co_await _host_manager.flush_current_hints();
            _next_flush_tp = current_time + shard_hint_manager::hints_flush_period;
        } catch (...) {
            manager_logger.trace("flush_maybe() failed: {}", std::current_exception());
            co_return;
        }
    }
}

bool hint_sender::can_send() noexcept {
    if (stopping() && !draining()) {
        return false;
    }

    try {
        if (_gossiper.is_alive(_ep_key)) {
            _state.remove(state::host_left_ring);
            return true;
        } else {
            if (!_state.contains(state::host_left_ring)) {
                const auto& token_md = _shard_manager.local_db().get_token_metadata();
                _state.set_if<state::host_left_ring>(!token_md.is_normal_token_owner(_ep_key));
            }
            // send the hints out if the destination Node is part of the ring - we will send to all new replicas in this case
            return _state.contains(state::host_left_ring);
        }
    } catch (...) {
        return false;
    }
}

const column_mapping& hint_sender::get_column_mapping(lw_shared_ptr<send_one_file_ctx> ctx_ptr,
        const frozen_mutation& fm, const hint_entry_reader& hr)
{
    auto cm_it = ctx_ptr->schema_ver_to_column_mapping.find(fm.schema_version());
    if (cm_it == ctx_ptr->schema_ver_to_column_mapping.end()) {
        if (!hr.get_column_mapping()) {
            throw no_column_mapping{fm.schema_version()};
        }

        manager_logger.debug("new schema version {}", fm.schema_version());
        cm_it = ctx_ptr->schema_ver_to_column_mapping.emplace(fm.schema_version(), *hr.get_column_mapping()).first;
    }

    return cm_it->second;
}

frozen_mutation_and_schema hint_sender::get_mutation(lw_shared_ptr<send_one_file_ctx> ctx_ptr,
        fragmented_temporary_buffer& buf)
{
    hint_entry_reader hr{buf};
    auto& fm = hr.mutation();
    auto& cm = get_column_mapping(std::move(ctx_ptr), fm, hr);
    auto schema = _db.find_schema(fm.column_family_id());

    if (schema->version() != fm.schema_version()) {
        mutation m{schema, fm.decorated_key(*schema)};
        converting_mutation_partition_applier v{cm, *schema, m.partition()};
        fm.partition().accept(cm, v);

        return {freeze(m), std::move(schema)};
    }

    return {std::move(hr).mutation(), std::move(schema)};
}

future<> hint_sender::do_send_one_mutation(frozen_mutation_and_schema m,
        locator::effective_replication_map_ptr ermp,
        const inet_address_vector_replica_set& natural_endpoints) noexcept
{
    // The fact that we send with CL::ALL in both cases below ensures that new hints are not going
    // to be generated as a result of hints sending.
    if (boost::range::find(natural_endpoints, _ep_key) != natural_endpoints.end()) {
        manager_logger.trace("Sending directly to {}", _ep_key);
        return _proxy.send_hint_to_endpoint(std::move(m), std::move(ermp), _ep_key);
    } else {
        manager_logger.trace("Endpoints set has changed and {} is no longer a replica. Mutating from scratch...", _ep_key);
        return _proxy.send_hint_to_all_replicas(std::move(m));
    }
}

future<> hint_sender::send_one_mutation(frozen_mutation_and_schema m) {
    auto erm = _db.find_column_family(m.s).get_effective_replication_map();
    auto token = dht::get_token(*m.s, m.fm.key());
    inet_address_vector_replica_set natural_endpoints = erm->get_natural_endpoints(std::move(token));

    return do_send_one_mutation(std::move(m), std::move(erm), std::move(natural_endpoints));
}

future<> hint_sender::send_one_hint(lw_shared_ptr<send_one_file_ctx> ctx_ptr,
        fragmented_temporary_buffer buf, replay_position rp, gc_clock::duration secs_since_file_mod,
        const sstring& fname)
{
    return _resource_manager.get_send_units_for(buf.size_bytes()).then(
            [this, secs_since_file_mod, &fname, buf = std::move(buf), rp, ctx_ptr] (auto units) mutable {
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
                const auto current_time = gc_clock::now().time_since_epoch();
                if (current_time - secs_since_file_mod > gc_grace_sec - shard_hint_manager::hints_flush_period) {
                    manager_logger.debug("send_hints(): the hint is too old, skipping it, "
                        "secs since file last modification {}, gc_grace_sec {}, hints_flush_period {}",
                        current_time - secs_since_file_mod, gc_grace_sec, shard_hint_manager::hints_flush_period);
                    return make_ready_future<>();
                }

                return this->send_one_mutation(std::move(m)).then([this, ctx_ptr] {
                    ++this->shard_stats().sent;
                }).handle_exception([this, ctx_ptr] (auto eptr) {
                    manager_logger.trace("send_one_hint(): failed to send to {}: {}", _ep_key, eptr);
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

// runs in a seastar::async context
bool hint_sender::send_one_file(const sstring& fname) {
    timespec last_mod = get_last_file_modification(fname).get0();
    gc_clock::duration secs_since_file_mod = std::chrono::seconds(last_mod.tv_sec);
    lw_shared_ptr<send_one_file_ctx> ctx_ptr = make_lw_shared<send_one_file_ctx>(_last_schema_ver_to_column_mapping);

    try {
        commitlog::read_log_file(fname, HINT_FILENAME_PREFIX,
                seastar::coroutine::lambda([this, secs_since_file_mod, &fname, ctx_ptr]
                    (commitlog::buffer_and_replay_position buf_rp) -> future<> {
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
        }), _last_not_complete_rp.pos, &_db.extensions()).get();
    } catch (commitlog::segment_error& ex) {
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
        auto p = _host_manager.get_or_load().get0();
        return p->delete_segments({ fname });
    }).get();

    // clear the replay position - we are going to send the next segment...
    _last_not_complete_rp = replay_position();
    _last_schema_ver_to_column_mapping.clear();
    manager_logger.trace("send_one_file(): segment {} was sent in full and deleted", fname);
    return true;
}

// Runs in the seastar::async context
void hint_sender::send_hints_maybe() noexcept {
    manager_logger.trace("send_hints(): going to send hints to {}, we have {} segment to replay",
            _ep_key, _segments_to_replay.size() + _foreign_segments_to_replay.size());

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

    // Ignore exceptions. We will retry sending this file from where we left off the next time.
    // Exceptions are not expected here during the regular operation, so just log them.
    } catch (...) {
        manager_logger.trace("send_hints(): got the exception: {}", std::current_exception());
    }

    if (have_segments()) {
        // TODO: come up with something more sophisticated here
        _next_send_retry_tp = clock_type::now() + std::chrono::seconds(1);
    } else {
        // If there are no segments to send, we want to retry when we might have some (after flushing).
        _next_send_retry_tp = _next_flush_tp;
    }

    manager_logger.trace("send_hints(): we handled {} segments", replayed_segments_count);
}

bool hint_sender::replay_allowed() const noexcept {
    return _host_manager.replay_allowed();
}

void hint_sender::notify_replay_waiters() noexcept {
    if (!_foreign_segments_to_replay.empty()) {
        manager_logger.trace("[{}] notify_replay_waiters(): not notifying because there are "
                             "still {} foreign segments to replay", _ep_key, _foreign_segments_to_replay.size());
        return;
    }

    manager_logger.trace("[{}] notify_replay_waiters(): replay position upper bound was updated to {}",
            _ep_key, _sent_upper_bound_rp);
    
    while (!_replay_waiters.empty() && _replay_waiters.begin()->first < _sent_upper_bound_rp) {
        manager_logger.trace("[{}] notify_replay_waiters(): notifying one ({} < {})",
                _ep_key, _replay_waiters.begin()->first, _sent_upper_bound_rp);
        
        auto ptr = _replay_waiters.begin()->second;
        (**ptr).set_value();
        (*ptr) = std::nullopt; // Prevent it from being resolved by abort source subscription
        _replay_waiters.erase(_replay_waiters.begin());
    }
}

void hint_sender::dismiss_replay_waiters() noexcept {
    for (auto& p : _replay_waiters) {
        manager_logger.debug("[{}] dismiss_replay_waiters(): dismissing one", _ep_key);
        auto ptr = p.second;
        (**ptr).set_exception(std::runtime_error{format("Hints manager for {} is stopping", _ep_key)});
        (*ptr) = std::nullopt; // Prevent it from being resolved by abort source subscription
    }
    _replay_waiters.clear();
}

typename hint_sender::duration_type hint_sender::next_sleep_duration() const {
    time_point_type current_time = clock_type::now();
    time_point_type next_flush_tp = std::max(_next_flush_tp, current_time);
    time_point_type next_retry_tp = std::max(_next_send_retry_tp, current_time);

    duration_type d = std::min(next_flush_tp, next_retry_tp) - current_time;

    // Don't sleep for less than 10 ticks of the "clock" if we are planning to sleep at all
    // -- the sleep() function is not perfect.
    return duration_type{10 * div_ceil(d.count(), 10)};
}

hint_stats& hint_sender::shard_stats() noexcept {
    return _shard_manager._stats;
}

} // namespace internal
} // namespace db::hints
