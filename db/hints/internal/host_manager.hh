/*
 * Modified by ScyllaDB
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

// Seastar features.
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_mutex.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/smp.hh>

// Scylla includes.
#include "db/commitlog/replay_position.hh"
#include "db/hints/internal/common.hh"
#include "db/hints/internal/hint_sender.hh"
#include "db/hints/internal/hint_storage.hh"
#include "db/hints/resource_manager.hh"
#include "utils/runtime.hh"
#include "enum_set.hh"

// STD.
#include <cassert>
#include <chrono>
#include <filesystem>
#include <memory>
#include <unordered_map>

namespace service {

// This needs to be forward-declared because "service/storage_proxy.hh"
// is the owner of hinted-handoff data structures: THAT file will include
// files from this module, not the other way round.
//
// It being an incomplete type is not an issue because we only store
// a reference to it.
class storage_proxy;

} // namespace service

namespace replica {
class database;
} // namespace replica

namespace gms {
class gossiper;
} // namespace gms

namespace db::hints {

class shard_hint_manager;
class resource_manager;

namespace internal {

/// This class is responsible for managing hints for a specified host.
/// It is managed by @ref shard_hint_manager.
///
/// Each instance of host_manager is also responsible for creating a @ref commitlog instance
/// hints are written to and managing it. This class is the owner of that instance.
///
/// Due to the nature of commitlog, we have to recreate it to be able to read hints from it
/// -- shutting down a commitlog triggers flushing all of the active segments to disk,
/// and thanks to that, we can read from them. It is utilized by the @ref hint_sender instance
/// this class owns.
///
/// Note that recreating the commitlog instance is triggered by the hint_sender.
class host_manager {
private:
    enum class state {
        can_hint,   // hinting is currently allowed (used by the space_watchdog)
        stopping,   // stopping is in progress (stop() method has been called)
        stopped     // stop() has completed
    };

    using state_set = enum_set<super_enum<state,
        state::can_hint,
        state::stopping,
        state::stopped>>;

    friend class hint_sender;

private:
    endpoint_id _key;
    state_set _state;

    seastar::gate _store_gate;
    hint_store_ptr _hint_store_anchor;
    lw_shared_ptr<seastar::shared_mutex> _file_update_mutex_ptr;
    replay_position _last_written_rp;

    shard_hint_manager& _shard_manager;
    hint_sender _sender;
    
    const std::filesystem::path _hints_dir;
    uint64_t _hints_in_progress = 0;

public:
    host_manager(const endpoint_id& key, shard_hint_manager& shard_manager);
    host_manager(host_manager&&);
    
    ~host_manager() noexcept {
        assert(stopped());
    }

public:
    /// \brief Start the timer.
    void start();
    
    /// \brief Waits till all writers complete and shuts down the hints store. Drains hints if needed.
    ///
    /// If "draining" is requested - sends all pending hints out.
    ///
    /// When hints are being drained we will not stop sending after a single hint sending has failed and will continue sending hints
    /// till the end of the current segment. After that we will remove the current segment and move to the next one till
    /// there isn't any segment left.
    ///
    /// \param should_drain is drain::yes - drain all pending hints
    /// \return Ready future when all operations are complete
    future<> stop(drain should_drain = drain::no) noexcept;

    /// \brief Get the corresponding hints_store object. Create it if needed.
    /// \note Must be called under the \ref _file_update_mutex.
    /// \return The corresponding hints_store object.
    future<hint_store_ptr> get_or_load();

    /// \brief Store a single mutation hint.
    /// \param s column family descriptor
    /// \param fm frozen mutation object
    /// \param tr_state trace_state handle
    /// \return FALSE if hint is definitely not going to be stored
    bool store_hint(schema_ptr s, lw_shared_ptr<const frozen_mutation> fm,
            tracing::trace_state_ptr tr_state) noexcept;

    /// \brief Populates the _segments_to_replay list.
    ///  Populates the _segments_to_replay list with the names of the files in the <manager hints files directory> directory
    ///  in the order they should be sent out.
    ///
    /// \return Ready future when end point hints manager is initialized.
    future<> populate_segments_to_replay();

    /// \return Number of in-flight (towards the file) hints.
    uint64_t hints_in_progress() const noexcept {
        return _hints_in_progress;
    }

    bool replay_allowed() const noexcept;

    bool can_hint() const noexcept {
        return _state.contains(state::can_hint);
    }

    void allow_hints() noexcept {
        _state.set(state::can_hint);
    }

    void forbid_hints() noexcept {
        _state.remove(state::can_hint);
    }

    void set_stopping() noexcept {
        _state.set(state::stopping);
    }

    bool stopping() const noexcept {
        return _state.contains(state::stopping);
    }

    void set_stopped() noexcept {
        _state.set(state::stopped);
    }

    void clear_stopped() noexcept {
        _state.remove(state::stopped);
    }

    bool stopped() const noexcept {
        return _state.contains(state::stopped);
    }

    /// \brief Returns replay position of the most recently written hint.
    ///
    /// If there weren't any hints written during this endpoint manager's lifetime, a zero replay_position is returned.
    db::replay_position last_written_replay_position() const {
        return _last_written_rp;
    }

    /// \brief Waits until hints are replayed up to a given replay position, or given abort source is triggered.
    future<> wait_until_hints_are_replayed_up_to(abort_source& as, db::replay_position up_to_rp) {
        return _sender.wait_until_hints_are_replayed_up_to(as, up_to_rp);
    }

    /// \brief Safely runs a given functor under the file_update_mutex.
    ///
    /// This function is safe even if \ref ep_man gets destroyed before the future
    /// this function returns resolves (as long as the \ref func call itself is safe).
    ///
    /// \tparam Func Functor type.
    /// \param func Functor to run under the lock.
    /// \return Whatever \ref func returns.
    template <typename Func>
    decltype(auto) with_file_update_mutex(Func&& func) {
        auto lock_ptr = _file_update_mutex_ptr;
        return with_lock(*lock_ptr, std::forward<Func>(func)).finally([lock_ptr] {});
    }

    const std::filesystem::path& hints_dir() const noexcept {
        return _hints_dir;
    }

    const endpoint_id& end_point_key() const noexcept {
        return _key;
    }
    
    lw_shared_ptr<shared_mutex> file_update_mutex_ptr() const noexcept {
        return _file_update_mutex_ptr;
    }

private:
    seastar::shared_mutex& file_update_mutex() noexcept {
        return *_file_update_mutex_ptr;
    }

    /// \brief Creates a new hints store object.
    ///
    /// - Creates a hints store directory if doesn't exist: <shard_hints_dir>/<ep_key>
    /// - Creates a store object.
    /// - Populate _segments_to_replay if it's empty.
    ///
    /// \return A new hints store object.
    future<commitlog> add_store() noexcept;

    /// \brief Flushes all hints written so far to the disk.
    ///  - Repopulates the _segments_to_replay list if needed.
    ///
    /// \return Ready future when the procedure above completes.
    future<> flush_current_hints() noexcept;

    hint_stats& shard_stats() noexcept;

    resource_manager& shard_resource_manager() noexcept;
};

} // namespace internal
} // namespace db::hints
