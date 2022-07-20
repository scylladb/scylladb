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
        // how many entries to leave in the log after tacking a snapshot
        size_t snapshot_trailing = 200;
        // max size of appended entries in bytes
        size_t append_request_threshold = 100000;
        // Max number of entries of in-memory part of the log after
        // which requests are stopped to be admitted until the log
        // is shrunk back by a snapshot. Should be greater than
        // whatever the default number of trailing log entries
        // is configured by the snapshot, otherwise the state
        // machine will deadlock on attempt to submit a new entry.
        size_t max_log_size = 5000;
        // If set to true will enable prevoting stage during election
        bool enable_prevoting = true;
        // If set to true, forward configuration and entries from
        // follower to the leader autmatically. This guarantees
        // add_entry()/modify_config() never throws not_a_leader,
        // but makes timed_out_error more likely.
        bool enable_forwarding = true;
    };

    virtual ~server() {}
    // Add command to replicated log
    // Returned future is resolved depending on wait_type parameter:
    //  'committed' - when the entry is committed
    //  'applied'   - when the entry is applied (happens after it is committed)
    // May fail because of an internal error or because leader changed and an entry was either
    // replaced by the new leader or the server lost track of it. The former will result in
    // dropped_entry exception the later in commit_status_unknown.
    //
    // If forwarding is enabled and this is a follower, and the returned future resolves without exception,
    // this means that the entry is committed/applied locally (depending on the wait type).
    // Applied locally means the local state machine replica applied this command;
    // committed locally means simply that the commit index is beyond this entry's index.
    //
    // The caller may pass a pointer to an abort_source to make the operation abortable.
    // If abort is requested before the operation finishes, the future will contain `raft::request_aborted` exception.
    //
    // Successfull `add_entry` with `wait_type::committed` does not guarantee that `state_machine::apply` will be called
    // locally for this entry. Between the commit and the application we may receive a snapshot containing this entry,
    // so the state machine's state 'jumps' forward in time, skipping the entry application.
    // However, for `wait_type::applied`, we guarantee that the entry will be applied locally with `state_machine::apply`.
    // If a snapshot causes the state machine to jump over the entry, `add_entry` will return `commit_status_unknown`
    // (even if the snapshot included that entry).
    virtual future<> add_entry(command command, wait_type type, seastar::abort_source* as = nullptr) = 0;

    // Set a new cluster configuration. If the configuration is
    // identical to the previous one does nothing.
    //
    // Does not preempt before adding entry to this server's
    // in-memory log.
    //
    // Provided node_info is passed to rpc::add_server() for each
    // new server and rpc::remove_server() is called for each
    // departing server.
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
    // If abort is requested before the operation finishes, the future will contain `raft::request_aborted` exception.
    virtual future<> set_configuration(config_member_set c_new, seastar::abort_source* as = nullptr) = 0;

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
    virtual future<> modify_config(std::vector<config_member> add,
        std::vector<server_id> del, seastar::abort_source* as = nullptr) = 0;

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
    virtual future<> abort() = 0;

    // Return Raft protocol current term.
    virtual term_t get_current_term() const = 0;

    // May be called before attempting a read from the local state
    // machine. The read should proceed only after the returned
    // future has resolved successfully.
    //
    // The caller may pass a pointer to an abort_source to make the operation abortable.
    // If abort is requested before the operation finishes, the future will contain `raft::request_aborted` exception.
    virtual future<> read_barrier(seastar::abort_source* as = nullptr) = 0;

    // Initiate leader stepdown process.
    // If the node is not a leader returns not_a_leader exception.
    // In case of a timeout returns timeout_error.
    virtual future<> stepdown(logical_clock::duration timeout) = 0;

    // Register metrics for this server. Metric are global but their names
    // depend on the server's ID, so it is possible to register metrics
    // of two servers iff their IDs are different.
    virtual void register_metrics() = 0;

    // Ad hoc functions for testing
    virtual void wait_until_candidate() = 0;
    virtual future<> wait_election_done() = 0;
    virtual future<> wait_log_idx_term(std::pair<index_t, term_t> idx_log) = 0;
    virtual std::pair<index_t, term_t> log_last_idx_term() = 0;
    virtual void elapse_election() = 0;
    virtual bool is_leader() = 0;
    virtual void tick() = 0;
    // Server id of this server
    virtual raft::server_id id() const = 0;
};

std::unique_ptr<server> create_server(server_id uuid, std::unique_ptr<rpc> rpc,
        std::unique_ptr<state_machine> state_machine, std::unique_ptr<persistence> persistence,
        seastar::shared_ptr<failure_detector> failure_detector, server::configuration config);

} // namespace raft

