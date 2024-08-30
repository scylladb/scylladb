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
#include <seastar/util/noncopyable_function.hh>

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

namespace db::hints {

class manager;

namespace internal {

class hint_endpoint_manager {
private:
    friend class hint_sender;

private:
    endpoint_id _key;
    manager& _shard_manager;
    hints_store_ptr _hints_store_anchor;
    seastar::gate _store_gate;
    lw_shared_ptr<seastar::shared_mutex> _file_update_mutex_ptr;
    seastar::shared_mutex& _file_update_mutex;

    enum class state {
        can_hint,               // hinting is currently allowed (used by the space_watchdog)
        stopping,               // stopping is in progress (stop() method has been called)
        stopped                 // stop() has completed
    };

    using state_set = enum_set<super_enum<state,
        state::can_hint,
        state::stopping,
        state::stopped>>;

    state_set _state;
    const fs::path _hints_dir;
    uint64_t _hints_in_progress = 0;
    db::replay_position _last_written_rp;
    hint_sender _sender;

public:
    hint_endpoint_manager(const endpoint_id& key, std::filesystem::path hint_directory, manager& shard_manager);
    hint_endpoint_manager(hint_endpoint_manager&&);
    ~hint_endpoint_manager();

    const endpoint_id& end_point_key() const noexcept {
        return _key;
    }

    /// \brief Get the corresponding hints_store object. Create it if needed.
    /// \note Must be called under the \ref _file_update_mutex.
    /// \return The corresponding hints_store object.
    future<hints_store_ptr> get_or_load();

    /// \brief Store a single mutation hint.
    /// \param s column family descriptor
    /// \param fm frozen mutation object
    /// \param tr_state trace_state handle
    /// \return FALSE if hint is definitely not going to be stored
    bool store_hint(schema_ptr s, lw_shared_ptr<const frozen_mutation> fm, tracing::trace_state_ptr tr_state) noexcept;

    /// \brief Populates the _segments_to_replay list.
    ///  Populates the _segments_to_replay list with the names of the files in the <manager hints files directory> directory
    ///  in the order they should be sent out.
    ///
    /// \return Ready future when end point hints manager is initialized.
    future<> populate_segments_to_replay();

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

    /// \brief Start the timer.
    void start();

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

    /// \brief Safely runs a given functor under the file_update_mutex of \ref this object.
    ///
    /// Runs a given functor under the file_update_mutex of this hint_endpoint_manager instance.
    /// This function is safe even if \ref ep_man gets destroyed before the future this function returns resolves
    /// (as long as the \ref func call itself is safe).
    ///
    /// \param func Functor to run under the lock.
    future<> with_file_update_mutex(noncopyable_function<future<> ()> func);

    const fs::path& hints_dir() const noexcept {
        return _hints_dir;
    }

private:
    future<> do_store_hint(schema_ptr s, lw_shared_ptr<const frozen_mutation> fm, tracing::trace_state_ptr tr_state);

    seastar::shared_mutex& file_update_mutex() noexcept {
        return _file_update_mutex;
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

    hint_stats& shard_stats();

    resource_manager& shard_resource_manager();
};

} // namespace internal
} // namespace db::hints
