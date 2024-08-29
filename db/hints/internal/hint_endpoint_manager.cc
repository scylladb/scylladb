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
#include <seastar/core/shared_mutex.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/coroutine/exception.hh>

// Scylla includes.
#include "db/hints/internal/common.hh"
#include "db/hints/internal/hint_logger.hh"
#include "db/hints/internal/hint_storage.hh"
#include "db/hints/manager.hh"
#include "db/timeout_clock.hh"
#include "replica/database.hh"
#include "utils/assert.hh"
#include "utils/disk-error-handler.hh"
#include "utils/error_injection.hh"
#include "utils/runtime.hh"

// STD.
#include <algorithm>
#include <chrono>
#include <exception>
#include <utility>
#include <vector>

namespace db::hints {
namespace internal {

namespace {

constexpr std::chrono::seconds HINT_FILE_WRITE_TIMEOUT = std::chrono::seconds(2);

} // anonymous namespace

future<> hint_endpoint_manager::do_store_hint(schema_ptr s, lw_shared_ptr<const frozen_mutation> fm, tracing::trace_state_ptr tr_state) {
    ++_hints_in_progress;
    size_t mut_size = fm->representation().size();
    shard_stats().size_of_hints_in_progress += mut_size;

    if (utils::get_local_injector().enter("slow_down_writing_hints")) {
        co_await seastar::sleep(std::chrono::seconds(10));
    }

    try {
        const auto shared_lock = co_await get_shared_lock(file_update_mutex());

        hints_store_ptr log_ptr = co_await get_or_load();
        commitlog_entry_writer cew(s, *fm, commitlog::force_sync::no);

        rp_handle rh = co_await log_ptr->add_entry(s->id(), cew, db::timeout_clock::now() + HINT_FILE_WRITE_TIMEOUT);

        const replay_position rp = rh.release();
        if (_last_written_rp < rp) {
            _last_written_rp = rp;
            manager_logger.debug("[{}] Updated last written replay position to {}", end_point_key(), rp);
        }

        ++shard_stats().written;

        manager_logger.trace("Hint to {} was stored", end_point_key());
        tracing::trace(tr_state, "Hint to {} was stored", end_point_key());
    } catch (...) {
        ++shard_stats().errors;
        const auto eptr = std::current_exception();

        manager_logger.debug("store_hint(): got the exception when storing a hint to {}: {}", end_point_key(), eptr);
        tracing::trace(tr_state, "Failed to store a hint to {}: {}", end_point_key(), eptr);
    }

    --_hints_in_progress;
    shard_stats().size_of_hints_in_progress -= mut_size;
}

bool hint_endpoint_manager::store_hint(schema_ptr s, lw_shared_ptr<const frozen_mutation> fm, tracing::trace_state_ptr tr_state) noexcept {
    try {
        // Future is waited on indirectly in `stop()` (via `_store_gate`).
        (void) with_gate(_store_gate,
                [this, s = std::move(s), fm = std::move(fm), tr_state = tr_state] () mutable -> future<> {
            return do_store_hint(std::move(s), std::move(fm), tr_state);
        });
    } catch (...) {
        manager_logger.trace("Failed to store a hint to {}: {}", end_point_key(), std::current_exception());
        tracing::trace(tr_state, "Failed to store a hint to {}: {}", end_point_key(), std::current_exception());

        ++shard_stats().dropped;
        return false;
    }

    return true;
}

future<> hint_endpoint_manager::populate_segments_to_replay() {
    return with_lock(file_update_mutex(), [this] {
        return get_or_load().discard_result();
    });
}

void hint_endpoint_manager::start() {
    clear_stopped();
    allow_hints();
    _sender.start();
}

future<> hint_endpoint_manager::stop(drain should_drain) noexcept {
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

hint_endpoint_manager::hint_endpoint_manager(const endpoint_id& key, fs::path hint_directory, manager& shard_manager)
    : _key(key)
    , _shard_manager(shard_manager)
    , _file_update_mutex_ptr(make_lw_shared<seastar::shared_mutex>())
    , _file_update_mutex(*_file_update_mutex_ptr)
    , _state(state_set::of<state::stopped>())
    , _hints_dir(std::move(hint_directory))
    // Approximate the position of the last written hint by using the same formula as for segment id calculation in commitlog
    // TODO: Should this logic be deduplicated with what is in the commitlog?
    , _last_written_rp(this_shard_id(), std::chrono::duration_cast<std::chrono::milliseconds>(runtime::get_boot_time().time_since_epoch()).count())
    , _sender(*this, _shard_manager.local_storage_proxy(), _shard_manager.local_db(), _shard_manager.local_gossiper())
{}

hint_endpoint_manager::hint_endpoint_manager(hint_endpoint_manager&& other)
    : _key(other._key)
    , _shard_manager(other._shard_manager)
    , _file_update_mutex_ptr(std::move(other._file_update_mutex_ptr))
    , _file_update_mutex(*_file_update_mutex_ptr)
    , _state(other._state)
    , _hints_dir(std::move(other._hints_dir))
    , _last_written_rp(other._last_written_rp)
    , _sender(other._sender, *this)
{}

hint_endpoint_manager::~hint_endpoint_manager() {
    SCYLLA_ASSERT(stopped());
}

future<hints_store_ptr> hint_endpoint_manager::get_or_load() {
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

future<db::commitlog> hint_endpoint_manager::add_store() noexcept {
    manager_logger.trace("Going to add a store to {}", _hints_dir.c_str());

    return futurize_invoke([this] {
        return io_check([name = _hints_dir.c_str()] { return recursive_touch_directory(name); }).then([this] () {
            commitlog::config cfg;

            cfg.sched_group = _shard_manager.local_db().commitlog()->active_config().sched_group;
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

future<> hint_endpoint_manager::flush_current_hints() noexcept {
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

future<> hint_endpoint_manager::with_file_update_mutex(noncopyable_function<future<> ()> func) {
    return with_lock(*_file_update_mutex_ptr, std::move(func)).finally(
            [lock_ptr = _file_update_mutex_ptr] {/* extend lifetime of the lock */});
}

bool hint_endpoint_manager::replay_allowed() const noexcept {
    return _shard_manager.replay_allowed();
}

hint_stats& hint_endpoint_manager::shard_stats() {
    return _shard_manager._stats;
}

resource_manager& hint_endpoint_manager::shard_resource_manager() {
    return _shard_manager._resource_manager;
}

} // namespace internal
} // namespace db::hints
