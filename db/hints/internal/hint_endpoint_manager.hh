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
#include "db/hints/internal/hint_storage.hh"
#include "db/hints/resource_manager.hh"
#include "locator/abstract_replication_strategy.hh"
#include "mutation/frozen_mutation.hh"
#include "schema/schema.hh"
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

class end_point_hints_manager {
public:
    class sender {
        // Important: clock::now() must be noexcept.
        // TODO: add the corresponding static_assert() when seastar::lowres_clock::now() is marked as "noexcept".
        using clock = seastar::lowres_clock;

        enum class state {
            stopping,               // stop() was called
            ep_state_left_the_ring, // destination Node is not a part of the ring anymore - usually means that it has been decommissioned
            draining,               // try to send everything out and ignore errors
        };

        using state_set = enum_set<super_enum<state,
            state::stopping,
            state::ep_state_left_the_ring,
            state::draining>>;

        struct send_one_file_ctx {
            send_one_file_ctx(std::unordered_map<table_schema_version, column_mapping>& last_schema_ver_to_column_mapping)
                : schema_ver_to_column_mapping(last_schema_ver_to_column_mapping)
            {}
            std::unordered_map<table_schema_version, column_mapping>& schema_ver_to_column_mapping;
            seastar::gate file_send_gate;
            std::optional<db::replay_position> first_failed_rp;
            std::optional<db::replay_position> last_succeeded_rp;
            std::set<db::replay_position> in_progress_rps;
            bool segment_replay_failed = false;

            void mark_hint_as_in_progress(db::replay_position rp);
            void on_hint_send_success(db::replay_position rp) noexcept;
            void on_hint_send_failure(db::replay_position rp) noexcept;

            // Returns a position below which hints were successfully replayed.
            db::replay_position get_replayed_bound() const noexcept;
        };

    private:
        std::list<sstring> _segments_to_replay;
        // Segments to replay which were not created on this shard but were moved during rebalancing
        std::list<sstring> _foreign_segments_to_replay;
        replay_position _last_not_complete_rp;
        replay_position _sent_upper_bound_rp;
        std::unordered_map<table_schema_version, column_mapping> _last_schema_ver_to_column_mapping;
        state_set _state;
        future<> _stopped;
        abort_source _stop_as;
        clock::time_point _next_flush_tp;
        clock::time_point _next_send_retry_tp;
        endpoint_id _ep_key;
        end_point_hints_manager& _ep_manager;
        manager& _shard_manager;
        resource_manager& _resource_manager;
        service::storage_proxy& _proxy;
        replica::database& _db;
        seastar::scheduling_group _hints_cpu_sched_group;
        gms::gossiper& _gossiper;
        seastar::shared_mutex& _file_update_mutex;

        std::multimap<db::replay_position, lw_shared_ptr<std::optional<promise<>>>> _replay_waiters;

    public:
        sender(end_point_hints_manager& parent, service::storage_proxy& local_storage_proxy, replica::database& local_db, gms::gossiper& local_gossiper) noexcept;
        ~sender();

        /// \brief A constructor that should be called from the copy/move-constructor of end_point_hints_manager.
        ///
        /// Make sure to properly reassign the references - especially to the \param parent and its internals.
        ///
        /// \param other the "sender" instance to copy from
        /// \param parent the parent object for this "sender" instance
        sender(const sender& other, end_point_hints_manager& parent) noexcept;

        /// \brief Start sending hints.
        ///
        /// Flush hints aggregated to far to the storage every hints_flush_period.
        /// If the _segments_to_replay is not empty sending send all hints we have.
        ///
        /// Sending is stopped when stop() is called.
        void start();

        /// \brief Stop the sender - make sure all background sending is complete.
        /// \param should_drain if is drain::yes - drain all pending hints
        future<> stop(drain should_drain) noexcept;

        /// \brief Add a new segment ready for sending.
        void add_segment(sstring seg_name);

        /// \brief Add a new segment originating from another shard, ready for sending.
        void add_foreign_segment(sstring seg_name);

        /// \brief Check if there are still unsent segments.
        /// \return TRUE if there are still unsent segments.
        bool have_segments() const noexcept { return !_segments_to_replay.empty() || !_foreign_segments_to_replay.empty(); };

        /// \brief Sets the sent_upper_bound_rp marker to indicate that the hints were replayed _up to_ given position.
        void rewind_sent_replay_position_to(db::replay_position rp);

        /// \brief Waits until hints are replayed up to a given replay position, or given abort source is triggered.
        future<> wait_until_hints_are_replayed_up_to(abort_source& as, db::replay_position up_to_rp);

    private:
        /// \brief Gets the name of the current segment that should be sent.
        ///
        /// If there are no segments to be sent, nullptr will be returned.
        const sstring* name_of_current_segment() const;

        /// \brief Removes the current segment from the queue.
        void pop_current_segment();

        /// \brief Send hints collected so far.
        ///
        /// Send hints aggregated so far. This function is going to try to deplete
        /// the _segments_to_replay list. Once it's empty it's going to be repopulated during the next send_hints() call
        /// with the new hints files if any.
        ///
        /// send_hints() is going to stop sending if it sends for too long (longer than the timer period). In this case it's
        /// going to return and next send_hints() is going to continue from the point the previous call left.
        void send_hints_maybe() noexcept;

        void set_draining() noexcept {
            _state.set(state::draining);
        }

        bool draining() const noexcept {
            return _state.contains(state::draining);
        }

        void set_stopping() noexcept {
            _state.set(state::stopping);
        }

        bool stopping() const noexcept {
            return _state.contains(state::stopping);
        }

        bool replay_allowed() const noexcept {
            return _ep_manager.replay_allowed();
        }

        /// \brief Try to send one hint read from the file.
        ///  - Limit the maximum memory size of hints "in the air" and the maximum total number of hints "in the air".
        ///  - Discard the hints that are older than the grace seconds value of the corresponding table.
        ///
        /// If sending fails we are going to set the state::segment_replay_failed in the _state and _first_failed_rp will be updated to min(_first_failed_rp, \ref rp).
        ///
        /// \param ctx_ptr shared pointer to the file sending context
        /// \param buf buffer representing the hint
        /// \param rp replay position of this hint in the file (see commitlog for more details on "replay position")
        /// \param secs_since_file_mod last modification time stamp (in seconds since Epoch) of the current hints file
        /// \param fname name of the hints file this hint was read from
        /// \return future that resolves when next hint may be sent
        future<> send_one_hint(lw_shared_ptr<send_one_file_ctx> ctx_ptr, fragmented_temporary_buffer buf, db::replay_position rp, gc_clock::duration secs_since_file_mod, const sstring& fname);

        /// \brief Send all hint from a single file and delete it after it has been successfully sent.
        /// Send all hints from the given file. If we failed to send the current segment we will pick up in the next
        /// iteration from where we left in this one.
        ///
        /// \param fname file to send
        /// \return TRUE if file has been successfully sent
        bool send_one_file(const sstring& fname);

        /// \brief Checks if we can still send hints.
        /// \return TRUE if the destination Node is either ALIVE or has left the ring (e.g. after decommission or removenode).
        bool can_send() noexcept;

        /// \brief Restore a mutation object from the hints file entry.
        /// \param ctx_ptr pointer to the send context
        /// \param buf hints file entry
        /// \return The mutation object representing the original mutation stored in the hints file.
        frozen_mutation_and_schema get_mutation(lw_shared_ptr<send_one_file_ctx> ctx_ptr, fragmented_temporary_buffer& buf);

        /// \brief Get a reference to the column_mapping object for a given frozen mutation.
        /// \param ctx_ptr pointer to the send context
        /// \param fm Frozen mutation object
        /// \param hr hint entry reader object
        /// \return
        const column_mapping& get_column_mapping(lw_shared_ptr<send_one_file_ctx> ctx_ptr, const frozen_mutation& fm, const hint_entry_reader& hr);

        /// \brief Perform a single mutation send atempt.
        ///
        /// If the original destination end point is still a replica for the given mutation - send the mutation directly
        /// to it, otherwise execute the mutation "from scratch" with CL=ALL.
        ///
        /// \param m mutation to send
        /// \param ermp points to the effective_replication_map used to obtain \c natural_endpoints
        /// \param natural_endpoints current replicas for the given mutation
        /// \return future that resolves when the operation is complete
        future<> do_send_one_mutation(frozen_mutation_and_schema m, locator::effective_replication_map_ptr ermp, const inet_address_vector_replica_set& natural_endpoints) noexcept;

        /// \brief Send one mutation out.
        ///
        /// \param m mutation to send
        /// \return future that resolves when the mutation sending processing is complete.
        future<> send_one_mutation(frozen_mutation_and_schema m);

        /// \brief Notifies replay waiters for which the target replay position was reached.
        void notify_replay_waiters() noexcept;

        /// \brief Dismisses ALL current replay waiters with an exception.
        void dismiss_replay_waiters() noexcept;

        /// \brief Get the last modification time stamp for a given file.
        /// \param fname File name
        /// \return The last modification time stamp for \param fname.
        static future<timespec> get_last_file_modification(const sstring& fname);

        hint_stats& shard_stats();

        /// \brief Flush all pending hints to storage if hints_flush_period passed since the last flush event.
        /// \return Ready, never exceptional, future when operation is complete.
        future<> flush_maybe() noexcept;

        const endpoint_id& end_point_key() const noexcept {
            return _ep_key;
        }

        /// \brief Return the amount of time we want to sleep after the current iteration.
        /// \return The time till the soonest event: flushing or re-sending.
        clock::duration next_sleep_duration() const;
    };

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
    sender _sender;

public:
    end_point_hints_manager(const endpoint_id& key, manager& shard_manager);
    end_point_hints_manager(end_point_hints_manager&&);
    ~end_point_hints_manager();

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

    /// \brief Safely runs a given functor under the file_update_mutex of \ref ep_man
    ///
    /// Runs a given functor under the file_update_mutex of the given end_point_hints_manager instance.
    /// This function is safe even if \ref ep_man gets destroyed before the future this function returns resolves
    /// (as long as the \ref func call itself is safe).
    ///
    /// \tparam Func Functor type.
    /// \param ep_man end_point_hints_manager instance which file_update_mutex we want to lock.
    /// \param func Functor to run under the lock.
    /// \return Whatever \ref func returns.
    template <typename Func>
    friend inline auto with_file_update_mutex(end_point_hints_manager& ep_man, Func&& func) {
        return with_lock(*ep_man._file_update_mutex_ptr, std::forward<Func>(func)).finally([lock_ptr = ep_man._file_update_mutex_ptr] {});
    }

    const fs::path& hints_dir() const noexcept {
        return _hints_dir;
    }

private:
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
} // namesapce db::hints
