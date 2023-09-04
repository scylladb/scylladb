/*
 * Modified by ScyllaDB
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

// Seastar features.
#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/shared_mutex.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>

// Scylla includes.
#include "db/commitlog/replay_position.hh"
#include "db/hints/internal/common.hh"
#include "gms/inet_address.hh"
#include "locator/abstract_replication_strategy.hh"
#include "mutation/frozen_mutation.hh"
#include "schema/schema.hh"
#include "utils/fragmented_temporary_buffer.hh"
#include "enum_set.hh"
#include "gc_clock.hh"

// STD.
#include <list>
#include <map>
#include <optional>
#include <set>
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

class manager;
class resource_manager;

namespace internal {

// Class specifying context for sending one file with hints.
struct send_one_file_ctx;

class hint_sender {
private:
    using clock_type = seastar::lowres_clock;
    static_assert(noexcept(clock_type::now()), "clock_type::now() must be noexcept");

    using time_point_type = typename clock_type::time_point;
    using duration_type = typename clock_type::duration;

    enum class state {
        stopping,       // stop() has been called.
        host_left_ring, // Destination node is not a part of the ring anymore.
                        // That usually means that it has been decommissioned.
        draining,       // Try to send all hints and ignore errors.
    };

    using state_set = enum_set<super_enum<state,
        state::stopping,
        state::host_left_ring,
        state::draining>>;
    
    // There shouldn't be many segments stored at the same time.
    // `std::list` provides good semantics in that case
    // -- it doesn't allocate more memory than it needs.
    using segment_list = std::list<seastar::sstring>;

    // TODO: Add a comment here explaining what this type "means".
    using replay_waiter = seastar::lw_shared_ptr<std::optional<seastar::promise<>>>;

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
    time_point_type _next_flush_tp;
    time_point_type _next_send_retry_tp;
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
    hint_sender(end_point_hints_manager& parent, service::storage_proxy& local_storage_proxy, replica::database& local_db, gms::gossiper& local_gossiper) noexcept;
    ~hint_sender();

    /// \brief A constructor that should be called from the copy/move-constructor of end_point_hints_manager.
    ///
    /// Make sure to properly reassign the references - especially to the \param parent and its internals.
    ///
    /// \param other the "hint_sender" instance to copy from
    /// \param parent the parent object for this "hint_sender" instance
    hint_sender(const hint_sender& other, end_point_hints_manager& parent) noexcept;

    /// \brief Start sending hints.
    ///
    /// Flush hints aggregated to far to the storage every hints_flush_period.
    /// If the _segments_to_replay is not empty sending send all hints we have.
    ///
    /// Sending is stopped when stop() is called.
    void start();

    /// \brief Stop the hint_sender - make sure all background sending is complete.
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

    hint_stats& shard_stats() {
        return _shard_manager._stats;
    }

    /// \brief Flush all pending hints to storage if hints_flush_period passed since the last flush event.
    /// \return Ready, never exceptional, future when operation is complete.
    future<> flush_maybe() noexcept;

    const endpoint_id& end_point_key() const noexcept {
        return _ep_key;
    }

    /// \brief Return the amount of time we want to sleep after the current iteration.
    /// \return The time till the soonest event: flushing or re-sending.
    duration_type next_sleep_duration() const;
};

} // namespace internal
} // namespace db::hints
