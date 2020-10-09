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

#include <seastar/core/condition-variable.hh>
#include "raft.hh"
#include "progress.hh"
#include "log.hh"

namespace raft {

// State of the FSM that needs logging & sending.
struct fsm_output {
    term_t term;
    server_id vote;
    std::vector<log_entry_ptr> log_entries;
    std::vector<std::pair<server_id, rpc_message>> messages;
    // Entries to apply.
    std::vector<log_entry_ptr> committed;
};

struct fsm_config {
    // max size of appended entries in bytes
    size_t append_request_threshold;
};

// 3.4 Leader election
// If a follower receives no communication over a period of
// time called the election timeout, then it assumes there is
// no viable leader and begins an election to choose a new
// leader.
static constexpr logical_clock::duration ELECTION_TIMEOUT = logical_clock::duration{10};

// 3.3 Raft Basics
// At any given time each server is in one of three states:
// leader, follower, or candidate.
// In normal operation there is exactly one leader and all of the
// other servers are followers. Followers are passive: they issue
// no requests on their own but simply respond to requests from
// leaders and candidates. The leader handles all client requests
// (if a client contacts a follower, the follower redirects it to
// the leader). The third state, candidate, is used to elect a new
// leader.
class follower {};
class candidate {};
class leader {};

// Raft protocol finite state machine
//
// Most libraries separate themselves from implementations by
// providing an API to the environment of the Raft protocol, such
// as the database, the write ahead log and the RPC to peers.

// This callback based design has some  drawbacks:

// - some callbacks may be defined in blocking model; e.g.
//  writing log entries to disk, or persisting the current
//  term in the database; Seastar has no blocking IO and
//  would have to emulate it with fibers;
// - the API calls are spread over the state machine
//  implementation, which makes reasoning about the correctness
//  more difficult (what happens if the library is is accessed
//  concurrently by multiple users, which of these accesses have
//  to be synchronized; what if the callback fails, is the state
//  machine handling the error correctly?)
// - while using callbacks allow testing without a real network or disk,
//  it still complicates it, since one has to implement meaningful
//  mocks for most of the APIs.
//
// Seastar Raft instead implements an instance of Raft as
// in-memory state machine with a catch-all API step(message)
// method. The method handles any kind of input and performs the
// needed state machine state transitions. To get state machine output
// poll_output() function has to be called. This call produces an output
// object, which encapsulates a list of actions that must be
// performed until the next poll_output() call can be made. The time is
// represented with a logical timer. The client is responsible for
// periodically invoking tick() method, which advances the state
// machine time and allows it to track such events as election or
// heartbeat timeouts.
class fsm {
    // id of this node
    server_id _my_id;
    // id of the current leader
    server_id _current_leader;
    // What state the server is in. The default is follower.
    std::variant<follower, candidate, leader> _state;
    // _current_term, _voted_for && _log are persisted in storage
    // The latest term the server has seen.
    term_t _current_term;
    // Candidate id that received a vote in the current term (or
    // nil if none).
    server_id _voted_for;
    // Index of the highest log entry known to be committed.
    // Currently not persisted.
    index_t _commit_idx = index_t(0);
    // Log entries; each entry contains a command for state machine,
    // and the term when the entry was received by the leader.
    log _log;
    // A possibly shared server failure detector.
    failure_detector& _failure_detector;
    // fsm configuration
    fsm_config _config;

    // Stores the last state observed by get_output().
    // Is updated with the actual state of the FSM after
    // fsm_output is created.
    struct last_observed_state {
        term_t _current_term;
        server_id _voted_for;
        index_t _commit_idx;

        bool is_equal(const fsm& fsm) const {
            return _current_term == fsm._current_term && _voted_for == fsm._voted_for &&
                _commit_idx == fsm._commit_idx;
        }

        void advance(const fsm& fsm) {
            _current_term = fsm._current_term;
            _voted_for = fsm._voted_for;
            _commit_idx = fsm._commit_idx;
        }
    } _observed;

    logical_clock _clock;
    // Start of the current election epoch - a time point relative
    // to which we expire election timeout.
    logical_clock::time_point _last_election_time = logical_clock::min();
    // A random value in range [election_timeout, 2 * election_timeout),
    // reset on each term change.
    logical_clock::duration _randomized_election_timeout = ELECTION_TIMEOUT;
    // Votes received during an election round. Available only in
    // candidate state.
    std::optional<votes> _votes;

    // A state for each follower, maintained only on the leader.
    std::optional<tracker> _tracker;
    // Holds all replies to AppendEntries RPC which are not
    // yet sent out. If AppendEntries request is accepted, we must
    // withhold a reply until the respective entry is persisted in
    // the log. Otherwise, e.g. when we receive AppendEntries with
    // an older term, we may reject it immediately.
    // Either way all replies are appended to this queue first.
    //
    // 3.3 Raft Basics
    // If a server receives a request with a stale term number, it
    // rejects the request.
    // TLA+ line 328
    std::vector<std::pair<server_id, rpc_message>> _messages;

    // Currently used configuration, may be different from
    // the committed during a configuration change.
    configuration _current_config;

    // Signaled when there is a IO event to process.
    seastar::condition_variable _sm_events;
    // Called when one of the replicas advances its match index
    // so it may be the case that some entries are committed now.
    // Signals _sm_events.
    void check_committed();
    // Check if the randomized election timeout has expired.
    bool is_past_election_timeout() const {
        return _clock.now() - _last_election_time >= _randomized_election_timeout;
    }

    // A helper to send any kind of RPC message.
    template <typename Message>
    void send_to(server_id to, Message&& m) {
        static_assert(std::is_rvalue_reference<decltype(m)>::value, "must be rvalue");
        _messages.push_back(std::make_pair(to, std::move(m)));
        _sm_events.signal();
    }

    // A helper to update the FSM's current term.
    void update_current_term(term_t current_term);

    void check_is_leader() const {
        if (!is_leader()) {
            throw not_a_leader(_current_leader);
        }
    }

    void become_candidate();

    void become_follower(server_id leader);

    // Controls whether the follower has been responsive recently,
    // so it makes sense to send more data to it.
    bool can_send_to(const follower_progress& progress);
    // Replicate entries to a follower. If there are no entries to send
    // and allow_empty is true, send a heartbeat.
    void replicate_to(follower_progress& progress, bool allow_empty);
    void replicate();
    void append_entries(server_id from, append_request_recv&& append_request);
    void append_entries_reply(server_id from, append_reply&& reply);

    void request_vote(server_id from, vote_request&& vote_request);
    void request_vote_reply(server_id from, vote_reply&& vote_reply);

    // Called on a follower with a new known leader commit index.
    // Advances the follower's commit index up to all log-stable
    // entries, known to be committed.
    void advance_commit_idx(index_t leader_commit_idx);
    // Called after log entries in FSM output are considered persisted.
    // Produces new FSM output.
    void advance_stable_idx(index_t idx);
    // Tick implementation on a leader
    void tick_leader();

    // Set cluster configuration
    void set_configuration(const configuration& config) {
        _current_config = config;
        // We unconditionally access _current_config
        // to identify which entries are committed.
        assert(_current_config.servers.size() > 0);
        if (is_leader()) {
            _tracker->set_configuration(_current_config.servers, _log.next_idx());
        } else if (is_candidate()) {
            _votes->set_configuration(_current_config.servers);
        }
    }
public:
    explicit fsm(server_id id, term_t current_term, server_id voted_for, log log,
            failure_detector& failure_detector, fsm_config conf);

    bool is_leader() const {
        return std::holds_alternative<leader>(_state);
    }
    bool is_follower() const {
        return std::holds_alternative<follower>(_state);
    }
    bool is_candidate() const {
        return std::holds_alternative<candidate>(_state);
    }

    void become_leader();

    // Add an entry to in-memory log. The entry has to be
    // committed to the persistent Raft log afterwards.
    template<typename T> const log_entry& add_entry(T command);

    // Wait until there is, and return state machine output that
    // needs to be handled.
    // This includes a list of the entries that need
    // to be logged. The logged entries are eventually
    // discarded from the state machine after snapshotting.
    future<fsm_output> poll_output();

    // Get state machine output, if there is any. Doesn't
    // wait. It is public for use in testing.
    // May throw on allocation failure, but leaves state machine
    // in the same state in that case
    fsm_output get_output();

    // Called to advance virtual clock of the protocol state machine.
    void tick();

    // Feed one Raft RPC message into the state machine.
    // Advances the state machine state and generates output,
    // accessible via poll_output().
    template <typename Message>
    void step(server_id from, Message&& msg);

    void stop();

    // @sa can_read()
    term_t get_current_term() const {
        return _current_term;
    }

    // How much time has passed since last election or last
    // time we heard from a valid leader.
    logical_clock::duration election_elapsed() const {
        return _clock.now() - _last_election_time;
    }

    // Should be called on leader only, throws otherwise.
    // Returns true if the current leader has at least one entry
    // committed and a quorum of followers was alive in the last
    // tick period.
    bool can_read();

    friend std::ostream& operator<<(std::ostream& os, const fsm& f);
};

template <typename Message>
void fsm::step(server_id from, Message&& msg) {
    static_assert(std::is_rvalue_reference<decltype(msg)>::value, "must be rvalue");
    // 4.1. Safety
    // Servers process incoming RPC requests without consulting
    // their current configurations.

    // 3.3. Raft basics.
    //
    // Current terms are exchanged whenever servers
    // communicate; if one server’s current term is smaller
    // than the other’s, then it updates its current term to
    // the larger value. If a candidate or leader discovers
    // that its term is out of date, it immediately reverts to
    // follower state. If a server receives a request with
    // a stale term number, it rejects the request.
    if (msg.current_term > _current_term) {
        logger.trace("{} [term: {}] received a message with higher term from {} [term: {}]",
            _my_id, _current_term, from, msg.current_term);

        if constexpr (std::is_same_v<Message, append_request_recv>) {
            become_follower(from);
        } else {
            if constexpr (std::is_same_v<Message, vote_request>) {
                if (_current_leader != server_id{} && election_elapsed() < ELECTION_TIMEOUT) {
                    // 4.2.3 Disruptive servers
                    // If a server receives a RequestVote request
                    // within the minimum election timeout of
                    // hearing from a current leader, it does not
                    // update its term or grant its vote.
                    logger.trace("{} [term: {}] not granting a vote within a minimum election timeout, elapsed {}",
                        _my_id, _current_term, election_elapsed());
                    return;
                }
            }
            become_follower(server_id{});
        }
        update_current_term(msg.current_term);

    } else if (msg.current_term < _current_term) {
        if constexpr (std::is_same_v<Message, append_request_recv>) {
            // Instructs the leader to step down.
            append_reply reply{_current_term, _commit_idx, append_reply::rejected{msg.prev_log_idx, _log.last_idx()}};
            send_to(from, std::move(reply));
        } else {
            // Ignore other cases
            logger.trace("{} [term: {}] ignored a message with lower term from {} [term: {}]",
                _my_id, _current_term, from, msg.current_term);
        }
        return;

    } else /* _current_term == msg.current_term */ {
        if constexpr (std::is_same_v<Message, append_request_recv> ||
                      std::is_same_v<Message, install_snapshot>) {
            if (is_candidate()) {
                // 3.4 Leader Election
                // While waiting for votes, a candidate may receive an AppendEntries
                // RPC from another server claiming to be leader. If the
                // leader’s term (included in its RPC) is at least as large as the
                // candidate’s current term, then the candidate recognizes the
                // leader as legitimate and returns to follower state.
                become_follower(from);
            } else if (_current_leader == server_id{}) {
                // Earlier we changed our term to match a candidate's
                // term. Now we get the first message from the
                // newly elected leader. Keep track of the current
                // leader to avoid starting an election if the
                // leader becomes idle.
                _current_leader = from;
            }
            assert(_current_leader == from);
        }
    }

    auto visitor = [this, from, msg = std::move(msg)](auto state) mutable {
        using State = decltype(state);

        if constexpr (std::is_same_v<Message, append_request_recv>) {
            // Got AppendEntries RPC from self
            append_entries(from, std::move(msg));
        } else if constexpr (std::is_same_v<Message, append_reply>) {
            if constexpr (!std::is_same_v<State, leader>) {
                // Ignore stray reply if we're not a leader.
                return;
            }
            append_entries_reply(from, std::move(msg));
        } else if constexpr (std::is_same_v<Message, vote_request>) {
            request_vote(from, std::move(msg));
        } else if constexpr (std::is_same_v<Message, vote_reply>) {
            if constexpr (!std::is_same_v<State, candidate>) {
                // Ignore stray reply if we're not a candidate.
                return;
            }
            request_vote_reply(from, std::move(msg));
        }
    };

    std::visit(visitor, _state);
}

} // namespace raft

