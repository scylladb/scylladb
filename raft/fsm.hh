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
#include <seastar/core/on_internal_error.hh>
#include "raft.hh"
#include "tracker.hh"
#include "log.hh"

namespace raft {

// State of the FSM that needs logging & sending.
struct fsm_output {
    std::optional<std::pair<term_t, server_id>> term_and_vote;
    std::vector<log_entry_ptr> log_entries;
    std::vector<std::pair<server_id, rpc_message>> messages;
    // Entries to apply.
    std::vector<log_entry_ptr> committed;
    std::optional<snapshot> snp;
    // Latest configuration obtained from the log in case it has changed
    // since last fsm output poll.
    std::optional<server_address_set> rpc_configuration;

    // True if there is no new output
    bool empty() const {
        return !term_and_vote &&
            log_entries.size() == 0 && messages.size() == 0 &&
            committed.size() == 0 && !snp &&
            !rpc_configuration;
    }
};

struct fsm_config {
    // max size of appended entries in bytes
    size_t append_request_threshold;
    // Max number of entries of in-memory part of the log after
    // which requests are stopped to be admitted until the log
    // is shrunk back by a snapshot. Should be greater than
    // whatever the default number of trailing log entries
    // is configured by the snapshot, otherwise the state
    // machine will deadlock.
    size_t max_log_size;
    // If set to true will enable prevoting stage during election
    bool enable_prevoting;
};

class fsm;

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
struct follower {
    server_id current_leader;
};
struct candidate {
     // Votes received during an election round.
    votes votes;
    // True if the candidate in prevote state
    bool is_prevote;
    candidate(configuration configuration, bool prevote) :
               votes(std::move(configuration)), is_prevote(prevote) {}
};
struct leader {
    // A state for each follower
    tracker tracker;
    // Used to acces new leader to set semaphore exception
    const fsm& fsm;
    // Used to limit log size
    seastar::semaphore log_limiter_semaphore;
    // True if the leader is in the process of transferring the leadership
    bool stepdown = false;
    // True it timeout_now was already sent to one of the followers
    bool timeout_now_sent = false;
    leader(size_t max_log_size, const class fsm& fsm_) : fsm(fsm_), log_limiter_semaphore(max_log_size) {}
    ~leader();
};

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
    // What state the server is in. The default is follower.
    std::variant<follower, candidate, leader> _state;
    // _current_term, _voted_for && _log are persisted in persistence
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
        snapshot _snapshot;
        index_t _last_conf_idx;
        term_t _last_term;

        bool is_equal(const fsm& fsm) const {
            return _current_term == fsm._current_term && _voted_for == fsm._voted_for &&
                _commit_idx == fsm._commit_idx && _snapshot.id == fsm._log.get_snapshot().id &&
                _last_conf_idx == fsm._log.last_conf_idx() &&
                _last_term == fsm._log.last_term();
        }

        void advance(const fsm& fsm) {
            _current_term = fsm._current_term;
            _voted_for = fsm._voted_for;
            _commit_idx = fsm._commit_idx;
            _snapshot = fsm._log.get_snapshot();
            _last_conf_idx = fsm._log.last_conf_idx();
            _last_term = fsm._log.last_term();
        }
    } _observed;

    logical_clock _clock;
    // Start of the current election epoch - a time point relative
    // to which we expire election timeout.
    logical_clock::time_point _last_election_time = logical_clock::min();
    // A random value in range [election_timeout + 1, 2 * election_timeout),
    // reset on each term change. For testing, it's necessary to have the value
    // at election_timeout without becoming a candidate.
    logical_clock::duration _randomized_election_timeout = ELECTION_TIMEOUT + logical_clock::duration{1};

private:
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

    // Signaled when there is a IO event to process.
    seastar::condition_variable _sm_events;

    server_id current_leader() const {
        if (is_leader()) {
            return _my_id;
        } else if (is_candidate()) {
            return {};
        } else {
            return follower_state().current_leader;
        }
    }
    // Called when one of the replicas advances its match index
    // so it may be the case that some entries are committed now.
    // Signals _sm_events. May resign leadership if we committed
    // a configuration change.
    void maybe_commit();
    // Check if the randomized election timeout has expired.
    bool is_past_election_timeout() const {
        return election_elapsed() >= _randomized_election_timeout;
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
            throw not_a_leader(current_leader());
        }
    }

    void become_leader();

    void become_candidate(bool is_prevote, bool is_leadership_transfer = false);

    // Controls whether the follower has been responsive recently,
    // so it makes sense to send more data to it.
    bool can_send_to(const follower_progress& progress);
    // Replicate entries to a follower. If there are no entries to send
    // and allow_empty is true, send a heartbeat.
    void replicate_to(follower_progress& progress, bool allow_empty);
    void replicate();
    void append_entries(server_id from, append_request&& append_request);

    // Precondition: `is_leader() && reply.current_term == _current_term`
    void append_entries_reply(server_id from, append_reply&& reply);

    void request_vote(server_id from, vote_request&& vote_request);
    void request_vote_reply(server_id from, vote_reply&& vote_reply);

    void install_snapshot_reply(server_id from, snapshot_reply&& reply);

    // Called on a follower with a new known leader commit index.
    // Advances the follower's commit index up to all log-stable
    // entries, known to be committed.
    void advance_commit_idx(index_t leader_commit_idx);
    // Called after log entries in FSM output are considered persisted.
    // Produces new FSM output.
    void advance_stable_idx(index_t idx);
    // Tick implementation on a leader
    void tick_leader();

    void reset_election_timeout();

    candidate& candidate_state() {
        return std::get<candidate>(_state);
    }

    const candidate& candidate_state() const {
        return std::get<candidate>(_state);
    }

    follower& follower_state() {
        return std::get<follower>(_state);
    }

    const follower& follower_state() const {
        return std::get<follower>(_state);
    }

    void send_timeout_now(server_id);
protected: // For testing

    void become_follower(server_id leader);

    leader& leader_state() {
        return std::get<leader>(_state);
    }

    const leader& leader_state() const {
        return std::get<leader>(_state);
    }

    log& get_log() {
        return _log;
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
    bool is_prevote_candidate() const {
        return is_candidate() && std::get<candidate>(_state).is_prevote;
    }
    index_t log_last_idx() const {
        return _log.last_idx();
    }
    term_t log_last_term() const {
        return _log.last_term();
    }

    // Call this function to wait for the number of log entries to
    // go below  max_log_size.
    future<> wait_max_log_size();

    // Return current configuration. Throws if not a leader.
    const configuration& get_configuration() const;

    // Add an entry to in-memory log. The entry has to be
    // committed to the persistent Raft log afterwards.
    template<typename T> const log_entry& add_entry(T command);

    // Wait until there is, and return state machine output that
    // needs to be handled.
    // This includes a list of the entries that need
    // to be logged. The logged entries are eventually
    // discarded from the state machine after applying a snapshot.
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

    template <typename Message>
    void step(server_id from, const leader& s, Message&& msg);
    template <typename Message>
    void step(server_id from, const candidate& s, Message&& msg);
    template <typename Message>
    void step(server_id from, const follower& s, Message&& msg);

    // This function can be called on a leader only.
    // When called it makes the leader to stop accepting
    // new requests and waits for one of the voting followers
    // to be fully up-to-date. When such follower appears it
    // sends timeout_now rpc to it and makes it initiate new election.
    // Can be used for leader stepdown if new configuration does not contain
    // current leader.
    void transfer_leadership();

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

    // This call will update the log to point to the new snapshot
    // and will truncate the log prefix up to (snp.idx - trailing)
    // entry. Returns false if the snapshot is older than existing one.
    bool apply_snapshot(snapshot snp, size_t traling);

    size_t in_memory_log_size() const {
        return _log.in_memory_size();
    };

    server_id id() const { return _my_id; }

    friend std::ostream& operator<<(std::ostream& os, const fsm& f);
    friend leader;
};

template <typename Message>
void fsm::step(server_id from, const leader& s, Message&& msg) {
    if constexpr (std::is_same_v<Message, append_request>) {
        // We are here if we got AppendEntries RPC with our term
        // but this is impossible since we are the leader and
        // locally applied entries do not go via the RPC. Just ignore it.
    } else if constexpr (std::is_same_v<Message, append_reply>) {
        append_entries_reply(from, std::move(msg));
    } else if constexpr (std::is_same_v<Message, vote_request>) {
        request_vote(from, std::move(msg));
    } else if constexpr (std::is_same_v<Message, install_snapshot>) {
        send_to(from, snapshot_reply{.current_term = _current_term,
                     .success = false });
    } else if constexpr (std::is_same_v<Message, snapshot_reply>) {
        install_snapshot_reply(from, std::move(msg));
    }
}

template <typename Message>
void fsm::step(server_id from, const candidate& c, Message&& msg) {
    if constexpr (std::is_same_v<Message, vote_request>) {
        request_vote(from, std::move(msg));
    } else if constexpr (std::is_same_v<Message, vote_reply>) {
        request_vote_reply(from, std::move(msg));
    } else if constexpr (std::is_same_v<Message, install_snapshot>) {
        send_to(from, snapshot_reply{.current_term = _current_term,
                     .success = false });
    }
}

template <typename Message>
void fsm::step(server_id from, const follower& c, Message&& msg) {
    if constexpr (std::is_same_v<Message, append_request>) {
        append_entries(from, std::move(msg));
    } else if constexpr (std::is_same_v<Message, vote_request>) {
        request_vote(from, std::move(msg));
    } else if constexpr (std::is_same_v<Message, install_snapshot>) {
        send_to(from, snapshot_reply{.current_term = _current_term,
                    .success = apply_snapshot(std::move(msg.snp), 0)});
    } else if constexpr (std::is_same_v<Message, timeout_now>) {
        // Leadership transfers never use pre-vote; we know we are not
        // recovering from a partition so there is no need for the
        // extra round trip.
        become_candidate(false, true);
    }
}

template <typename Message>
void fsm::step(server_id from, Message&& msg) {
    if (from == _my_id) {
        on_internal_error(logger, "fsm cannot process messages from itself");
    }
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
        server_id leader{};

        logger.trace("{} [term: {}] received a message with higher term from {} [term: {}]",
            _my_id, _current_term, from, msg.current_term);

        if constexpr (std::is_same_v<Message, append_request> ||
                      std::is_same_v<Message, install_snapshot>) {
            leader = from;
        } else {
            if constexpr (std::is_same_v<Message, vote_request>) {
                if (current_leader() != server_id{} && election_elapsed() < ELECTION_TIMEOUT && !msg.force) {
                    // 4.2.3 Disruptive servers
                    // If a server receives a RequestVote request
                    // within the minimum election timeout of
                    // hearing from a current leader, it does not
                    // update its term or grant its vote.
                    // Unless `force` flag is set which indicates that the current leader
                    // wants to stepdown.
                    logger.trace("{} [term: {}] not granting a vote within a minimum election timeout, elapsed {} (current leader = {})",
                        _my_id, _current_term, election_elapsed(), current_leader());
                    return;
                }
            }
        }
        bool ignore_term = false;
        if constexpr (std::is_same_v<Message, vote_request>) {
            // Do not update term on prevote request
            ignore_term = msg.is_prevote;
        } else if constexpr (std::is_same_v<Message, vote_reply>) {
            // We send pre-vote requests with a term in our future. If the
            // pre-vote is granted, we will increment our term when we get a
            // quorum. If it is not, the term comes from the node that
            // rejected our vote so we should become a follower at the new
            // term.
            ignore_term = msg.is_prevote && msg.vote_granted;
        }

        if (!ignore_term) {
            become_follower(leader);
            update_current_term(msg.current_term);
        }
    } else if (msg.current_term < _current_term) {
        if constexpr (std::is_same_v<Message, append_request>) {
            // Instructs the leader to step down.
            append_reply reply{_current_term, _commit_idx, append_reply::rejected{msg.prev_log_idx, _log.last_idx()}};
            send_to(from, std::move(reply));
        } else if constexpr (std::is_same_v<Message, install_snapshot>) {
            send_to(from, snapshot_reply{.current_term = _current_term,
                    .success = false});
        } else if constexpr (std::is_same_v<Message, vote_request>) {
            if (msg.is_prevote) {
                send_to(from, vote_reply{_current_term, false, true});
            }
        } else {
            // Ignore other cases
            logger.trace("{} [term: {}] ignored a message with lower term from {} [term: {}]",
                _my_id, _current_term, from, msg.current_term);
        }
        return;

    } else /* _current_term == msg.current_term */ {
        if constexpr (std::is_same_v<Message, append_request> ||
                      std::is_same_v<Message, install_snapshot>) {
            if (is_candidate()) {
                // 3.4 Leader Election
                // While waiting for votes, a candidate may receive an AppendEntries
                // RPC from another server claiming to be leader. If the
                // leader’s term (included in its RPC) is at least as large as the
                // candidate’s current term, then the candidate recognizes the
                // leader as legitimate and returns to follower state.
                become_follower(from);
            } else if (current_leader() == server_id{}) {
                // Earlier we changed our term to match a candidate's
                // term. Now we get the first message from the
                // newly elected leader. Keep track of the current
                // leader to avoid starting an election if the
                // leader becomes idle.
                follower_state().current_leader = from;
            }
            if (current_leader() != from) {
                on_internal_error_noexcept(logger, "Got append request or install snpaphot from unexpected leader");
            }
        }
    }

    auto visitor = [this, from, msg = std::move(msg)](const auto& state) mutable {
        step(from, state, std::move(msg));
    };

    std::visit(visitor, _state);
}

} // namespace raft

