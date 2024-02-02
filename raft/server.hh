/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once
#include <seastar/core/abort_source.hh>
#include "raft.hh"

namespace raft {

enum class wait_type {
    committed,
    applied
};

// A single uniquely identified participant of a Raft group.
class server {
public:
    struct configuration {
        // automatically snapshot state machine after applying
        // this number of entries
        size_t snapshot_threshold = 1024;
        // Automatically snapshot state machine if the log memory usage exceeds this value.
        // The value is in bytes.
        // Must be smaller than max_log_size.
        // It is recommended to set this value to no more than half of the max_log_size,
        // so that snapshots are taken in advance and there is no backpressure due to max_log_size.
        size_t snapshot_threshold_log_size = 2 * 1024 * 1024;
        // how many entries to leave in the log after taking a snapshot
        size_t snapshot_trailing = 200;
        // Limit on the total number of bytes, consumed by snapshot trailing entries.
        // Must be smaller than snapshot_threshold_log_size.
        // It is recommended to set this value to no more than half of snapshot_threshold_log_size
        // so that not all memory is held for trailing when taking a snapshot.
        size_t snapshot_trailing_size = 1 * 1024 * 1024;
        // max size of appended entries in bytes
        size_t append_request_threshold = 100000;
        // Limit in bytes on the size of in-memory part of the log after
        // which requests are stopped to be admitted until the log
        // is shrunk back by a snapshot.
        // The following condition must be satisfied:
        // max_command_size <= max_log_size - snapshot_trailing_size
        // this ensures that trailing log entries won't block incoming commands and at least
        // one command can fit in the log
        size_t max_log_size = 4 * 1024 * 1024;
        // If set to true will enable prevoting stage during election
        bool enable_prevoting = true;
        // If set to true, forward configuration and entries from
        // follower to the leader automatically. This guarantees
        // add_entry()/modify_config() never throws not_a_leader,
        // but makes timed_out_error more likely.
        bool enable_forwarding = true;

        // Max size of a single command, add_entry with a bigger command will throw command_is_too_big_error.
        // The following condition must be satisfied:
        // max_command_size <= max_log_size - snapshot_trailing_size
        // this ensures that trailing log entries won't block incoming commands and at least
        // one command can fit in the log
        size_t max_command_size = 100 * 1024;
        // A callback to invoke if one of internal server
        // background activities has stopped because of an error.
        std::function<void(std::exception_ptr e)> on_background_error;
    };

    virtual ~server() {}
    // Add command to replicated log
    // Returned future is resolved depending on wait_type parameter:
    //  'committed' - when the entry is committed
    //  'applied'   - when the entry is applied (happens after it is committed)
    //
    // If forwarding is enabled and this is a follower, and the returned future resolves without exception,
    // this means that the entry is committed/applied locally (depending on the wait type).
    // Applied locally means the local state machine replica applied this command;
    // committed locally means simply that the commit index is beyond this entry's index.
    //
    // The caller may pass a pointer to an abort_source to make the operation abortable.
    // It it passes nullptr, the operation is unabortable.
    //
    // Successful `add_entry` with `wait_type::committed` does not guarantee that `state_machine::apply` will be called
    // locally for this entry. Between the commit and the application we may receive a snapshot containing this entry,
    // so the state machine's state 'jumps' forward in time, skipping the entry application.
    // However, for `wait_type::applied`, we guarantee that the entry will be applied locally with `state_machine::apply`.
    // If a snapshot causes the state machine to jump over the entry, `add_entry` will return `commit_status_unknown`
    // (even if the snapshot included that entry).
    //
    // Exceptions:
    // raft::commit_status_unknown
    //     Thrown if the leader has changed and the log entry has either
    //     been replaced by the new leader or the server has lost track of it.
    //     It may also be thrown in case of a transport error while forwarding add_entry to the leader.L
    // raft::dropped_entry
    //     Thrown if the entry was replaced because of a leader change.
    // raft::request_aborted
    //     Thrown if abort is requested before the operation finishes.
    // raft::stopped_error
    //     Thrown if abort() was called on the server instance.
    // raft::not_a_leader
    //     Thrown if the node is not a leader and forwarding is not enabled through enable_forwarding config option.
    virtual future<> add_entry(command command, wait_type type, seastar::abort_source* as) = 0;

    // Set a new cluster configuration. If the configuration is
    // identical to the previous one does nothing.
    //
    // Does not preempt before adding entry to this server's
    // in-memory log.
    //
    // Provided node_info is passed to
    // rpc::on_configuration_change() for each added or removed
    // server.
    // struct node_info is expected to contain connection
    // information/credentials which is then used by RPC.
    // Can be called on a leader only, otherwise throws not_a_leader.
    // Cannot be called until previous set_configuration() completes
    // otherwise throws conf_change_in_progress exception.
    //
    // Waits until configuration completes, i.e. the server left the joint
    // configuration. The server will apply a dummy entry to
    // make sure this happens.
    //
    // Note: committing a dummy entry extends the opportunity for
    // uncertainty, thus commit_status_unknown exception may be
    // returned even in case of a successful config change.
    //
    // The caller may pass a pointer to an abort_source to make the operation abortable.
    // It it passes nullptr, the operation is unabortable.
    //
    // Exceptions:
    // raft::conf_change_in_progress
    //     Thrown if the previous set_configuration/modify_config is not completed.
    // raft::commit_status_unknown
    //     Thrown if the leader has changed and the config mutation has either
    //     been replaced by the new leader or the server has lost track of it.
    //     It may also be thrown in case of a transport error while
    //     forwarding the corresponding add_entry to the leader.
    // raft::request_aborted
    //     Thrown if abort is requested before the operation finishes.
    virtual future<> set_configuration(config_member_set c_new, seastar::abort_source* as) = 0;

    // A simplified wrapper around set_configuration() which adds
    // and deletes servers. Unlike set_configuration(),
    // works on a follower (if forwarding is enabled) as well as on a leader
    // (forwards the request to the current leader). If the added servers are
    // already part of the configuration, or deleted are not
    // present, does nothing. The implementation forwards the
    // list of added or removed servers to the leader, where they
    // are transformed to a new configuration, which is then
    // applied to the leader's log without preemption, bypassing
    // the log limiter semaphore.
    // This makes it possible to retry this command without
    // adverse effects to the configuration.
    //
    // If forwarding is enabled and this is a follower, and the returned future resolves without exception,
    // this means that a dummy entry appended after non-joint configuration entry was committed by the leader.
    // The local commit index is not necessarily up-to-date yet and the state of the local state machine
    // replica may still come from before the configuration entry.
    // (exception: if no server was actually added or removed, then nothing gets committed and the leader responds immediately).
    //
    // The caller may pass a pointer to an abort_source to make the operation abortable.
    // If abort is requested before the operation finishes, the future will contain `raft::request_aborted` exception.
    // It the caller passes nullptr, the operation is unabortable.
    //
    // Exceptions:
    // raft::commit_status_unknown
    //     Thrown if the leader has changed and the config mutation has either
    //     been replaced by the new leader or the server has lost track of it.
    //     It may also be thrown in case of a transport error while forwarding modify_config to the leader.
    // raft::request_aborted
    //     Thrown if abort is requested before the operation finishes.
    // raft::stopped_error
    //     Thrown if abort() was called on the server instance.
    // raft::not_a_leader
    //     Thrown if the node is not a leader and forwarding is not enabled through enable_forwarding config option.
    // raft::conf_change_in_progress
    //     Thrown if the previous set_configuration/modify_config is not completed.
    virtual future<> modify_config(std::vector<config_member> add,
        std::vector<server_id> del, seastar::abort_source* as) = 0;

    // Return the currently known configuration
    virtual raft::configuration get_configuration() const = 0;

    // Load persisted state and start background work that needs
    // to run for this Raft server to function; The object cannot
    // be used until the returned future is resolved.
    virtual future<> start() = 0;

    // Stop this Raft server, all submitted but not completed
    // operations will get an error and callers will not be able
    // to know if they succeeded or not. If this server was
    // a leader it will relinquish its leadership and cease
    // replication.
    virtual future<> abort(sstring reason = "") = 0;

    // Returns whether the server is running.
    // A server becomes alive after start() and becomes dead after abort()
    // is called or an error happens in one of the internal fibers.
    virtual bool is_alive() const = 0;

    // Return Raft protocol current term.
    virtual term_t get_current_term() const = 0;

    // May be called before attempting a read from the local state
    // machine. The read should proceed only after the returned
    // future has resolved successfully.
    //
    // The caller may pass a pointer to an abort_source to make the operation abortable.
    // It it passes nullptr, the operation is unabortable.
    //
    // Exceptions:
    // raft::request_aborted
    //     Thrown if abort is requested before the operation finishes.
    // raft::stopped_error
    //     Thrown if abort() was called on the server instance.
    virtual future<> read_barrier(seastar::abort_source* as) = 0;

    // Initiate leader stepdown process.
    //
    // Exceptions:
    // raft::timeout_error
    //     Thrown in case of timeout.
    // raft::not_a_leader
    //     Thrown if the node is not a leader and forwarding is not enabled through enable_forwarding config option.
    // raft::no_other_voting_member
    //     Thrown if there is no other voting member.
    // std::logic_error
    //     Thrown if the stepdown process is already in progress.
    virtual future<> stepdown(logical_clock::duration timeout) = 0;

    // Register metrics for this server. Metric are global but their names
    // depend on the server's ID, so it is possible to register metrics
    // of two servers iff their IDs are different.
    virtual void register_metrics() = 0;

    // Returns true if this servers thinks that it is the leader.
    // The information is only relevant for the current_term() only
    virtual bool is_leader() = 0;

    // Returns ID of the server that is thought to be the current leader.
    // Returns an empty ID if there is an ongoing election.
    // The information is only relevant for the current_term() only.
    virtual raft::server_id current_leader() const = 0;

    // The function should be called periodically to advance logical clock.
    virtual void tick() = 0;

    // Returned future is resolved when state changes
    // State changes can be coalesced, so it is not guaranteed that the caller will
    // get notification about each one of them. The state can even be the same after
    // the call as before, but term should be different.
    //
    // The caller may pass a pointer to an abort_source to make the function abortable.
    // It it passes nullptr, the function is unabortable.
    virtual future<> wait_for_state_change(seastar::abort_source* as) = 0;

    // Manually trigger snapshot creation and log truncation.
    //
    // Does nothing if the current apply index is less or equal to the last persisted snapshot descriptor index
    // and returns `false`.
    //
    // Otherwise returns `true`; when the future resolves, it is guaranteed that the snapshot descriptor
    // is persisted, but not that the snapshot is loaded to the state machine yet (it will be eventually).
    //
    // The request may be resolved by the regular snapshotting mechanisms (e.g. a snapshot
    // is created because the Raft log grows too large). In this case there is no guarantee
    // how many trailing entries will be left trailing behind the snapshot. However,
    // if there are no operations running on the server concurrently with the request and all
    // committed entries are already applied, the created snapshot is guaranteed to leave
    // zero trailing entries.
    virtual future<bool> trigger_snapshot(seastar::abort_source* as) = 0;

    // Ad hoc functions for testing
    virtual void wait_until_candidate() = 0;
    virtual future<> wait_election_done() = 0;
    virtual future<> wait_log_idx_term(std::pair<index_t, term_t> idx_log) = 0;
    virtual std::pair<index_t, term_t> log_last_idx_term() = 0;
    virtual void elapse_election() = 0;
    // Server id of this server
    virtual raft::server_id id() const = 0;
    virtual void set_applier_queue_max_size(size_t queue_max_size) = 0;

    virtual size_t max_command_size() const = 0;
};

std::unique_ptr<server> create_server(server_id uuid, std::unique_ptr<rpc> rpc,
        std::unique_ptr<state_machine> state_machine, std::unique_ptr<persistence> persistence,
        seastar::shared_ptr<failure_detector> failure_detector, server::configuration config);

} // namespace raft

