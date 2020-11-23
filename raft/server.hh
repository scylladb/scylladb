/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */
#pragma once
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
        // max number of entries of in-memory part of the log after
        // which requests are stopped to be addmitted unill the log
        // is shrunk back by snapshoting. It has to be greater than
        // snapshot_threshold otherwise submition of new entries will
        // deadlock. 
        size_t max_log_length = 5000;
    };

    virtual ~server() {}
    // Add command to replicated log
    // Returned future is resolved depending on wait_type parameter:
    //  'committed' - when the entry is committed
    //  'applied'   - when the entry is applied (happens after it is committed)
    // The function has to be called on a leader, throws not_a_leader exception otherwise.
    // May fail because of internal error or because leader changed and an entry was replaced
    // by another leader. In the later case dropped_entry exception will be returned.
    virtual future<> add_entry(command command, wait_type type) = 0;

    // Add new server to a cluster. If a node is already a member
    // of the cluster does nothing. Provided node_info is passed to
    // rpc::new_node() on each node in a cluster as it learns
    // about joining node. Connection info can be passed there.
    // Can be called on a leader only, otherwise throws not_a_leader.
    // Cannot be called until previous add/remove server completes
    // otherwise conf_change_in_progress exception is thrown.
    virtual future<> add_server(server_id id, server_info node_info, clock_type::duration timeout) = 0;

    // Remove a server from the cluster. If the server is not a member
    // of the cluster does nothing. Can be called on a leader only
    // otherwise throws not_a_leader.
    // Cannot be called until previous add/remove server completes
    // otherwise conf_change_in_progress exception is thrown.
    virtual future<> remove_server(server_id id, clock_type::duration timeout) = 0;

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
    // If called not on a leader throws not_a_leader error.
    // After calling this function and resolving the returned
    // future:
    //
    // 1) The result of all completed
    //    add_entries(wait_type::applied) can be observed by
    //    direct access to the local state machine. 
    // 2) A subsequent add_entry() is likely to find this
    //    server still in the leader role.
    // 3) If the caller ensures that writes to the state machine
    //    are linearised and the current term didn't change
    //    between read_barrier() and add_entry(), (@sa
    //    get_current_term()), a pair of read from the state
    //    machine and add_entry() will be linearised as well.
    //
    // To sum up, @read_barrier() can be used as a poor man
    // distributed Compare-And-Swap:
    //
    // lock()
    // term_t term = get_current_term()
    // co_await read_barrier()
    // ... Read previous value from the state machine ...
    // ... Create a new value ...
    // if (term == get_current_term())) {
    //      co_await add_entry();
    // }
    // unlock()
    virtual future<> read_barrier() = 0;

    // Ad hoc functions for testing
    virtual future<> elect_me_leader() = 0;
    virtual void elapse_election() = 0;
    virtual bool is_leader() = 0;
    virtual void tick() = 0;
};

std::unique_ptr<server> create_server(server_id uuid, std::unique_ptr<rpc> rpc,
        std::unique_ptr<state_machine> state_machine, std::unique_ptr<storage> storage,
        seastar::shared_ptr<failure_detector> failure_detector, server::configuration config);

} // namespace raft

