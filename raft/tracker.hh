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

namespace raft {

// Leader's view of each follower, including self.
class follower_progress {
public:
    // Id of this server
    const server_id id;
    // Index of the next log entry to send to this server.
    // Invariant: next_idx > match_idx.
    index_t next_idx;
    // Index of the highest log entry known to be replicated to this server.
    // More specifically, this is the greatest `last_new_idx` received from this follower
    // in an `accepted` message. As long as the follower remains in our term we know
    // that its log must match with ours up to (and including) `match_idx`.
    index_t match_idx = index_t(0);
    // Index that we know to be committed by the follower
    index_t commit_idx = index_t(0);
    // True if the follower is voting one
    bool can_vote = true;

    enum class state {
        // In this state only one append entry is send until matching index is found
        PROBE,
        // In this state multiple append entries are sent optimistically
        PIPELINE,
        // In this state snapshot is been transfered
        SNAPSHOT
    };
    state state = state::PROBE;
    // true if a packet was sent already in a probe mode
    bool probe_sent = false;
    // number of in flight still un-acked append entries requests
    size_t in_flight = 0;
    static constexpr size_t max_in_flight = 10;

    // Check if a reject packet should be ignored because it was delayed or reordered.
    // This is not 100% accurate (may return false negatives) and should only be relied on
    // for liveness optimizations, not for safety.
    //
    // Precondition: we are the leader and `r.current_term` is equal to our term (`_current_term`).
    // Postcondition: if the function returns `false` it is guaranteed that:
    // 1. `match_idx < r.non_matching_idx`.
    // 2. `match_idx < r.last_idx + 1`.
    // 3. If we're in PROBE mode then `next_idx == r.non_matching_idx + 1`.
    bool is_stray_reject(const append_reply::rejected& r);

    void become_probe();
    void become_pipeline();
    void become_snapshot(index_t snp_idx);

    void accepted(index_t idx) {
        // AppendEntries replies can arrive out of order.
        match_idx = std::max(idx, match_idx);
        // idx may be smaller if we increased next_idx
        // optimistically in pipeline mode.
        next_idx = std::max(idx + index_t{1}, next_idx);
    }

    // Return true if a new replication record can be sent to the follower.
    bool can_send_to();

    follower_progress(server_id id_arg, index_t next_idx_arg)
        : id(id_arg), next_idx(next_idx_arg)
    {}
};

using progress = std::unordered_map<server_id, follower_progress>;

class tracker: private progress {
    std::unordered_set<server_id> _current_voters;
    std::unordered_set<server_id> _previous_voters;

    // Hide size() function we inherited from progress since
    // it is never right to use it directly in case of joint config
    size_t size() const {
        assert(false);
    }
public:
    using progress::begin, progress::end, progress::cbegin, progress::cend, progress::size;

    // Return progress for a follower
    // May return nullptr if the follower is not part of the current
    // configuration any more. This may happen when handling
    // messages from removed followers.
    follower_progress* find(server_id dst) {
        auto it = this->progress::find(dst);
        return  it == this->progress::end() ? nullptr : &it->second;
    }
    void set_configuration(const configuration& configuration, index_t next_idx);
    // Calculate the current commit index based on the current
    // simple or joint quorum.
    index_t committed(index_t prev_commit_idx);

    class activity_tracker {
        tracker& _tracker;
        size_t _cur = 0;
        size_t _prev = 0;
        activity_tracker(tracker& t) : _tracker(t) {}
    public:
        void operator()(server_id id) {
            _cur += _tracker._current_voters.contains(id);
            _prev += _tracker._previous_voters.contains(id);
        }

        operator bool() const {
            bool active = _cur >= _tracker._current_voters.size()/2 + 1;
            if (!_tracker._previous_voters.empty()) {
                active &= _prev >= _tracker._previous_voters.size()/2 + 1;
            }
            return active;
        }
        friend tracker;
    };

    activity_tracker get_activity_tracker() {
        return activity_tracker(*this);
    }

    friend activity_tracker;
};

// Possible leader election outcomes.
enum class vote_result {
    // We haven't got enough responses yet, either because
    // the servers haven't voted or responses failed to arrive.
    UNKNOWN = 0,
    // This candidate has won the election
    WON,
    // The quorum of servers has voted against this candidate
    LOST,
};

std::ostream& operator<<(std::ostream& os, const vote_result& v);

// State of election in a single quorum
class election_tracker {
    // All eligible voters
    std::unordered_set<server_id> _suffrage;
    // Votes collected
    std::unordered_set<server_id> _responded;
    size_t _granted = 0;
public:
    election_tracker(const server_address_set& configuration) {
        for (const auto& a : configuration) {
            if (a.can_vote) {
                _suffrage.emplace(a.id);
            }
        }
    }

    bool register_vote(server_id from, bool granted) {
        if (_suffrage.find(from) == _suffrage.end()) {
            return false;
        }
        if (_responded.emplace(from).second) {
            // Have not counted this vote yet
            _granted += static_cast<int>(granted);
        }
        return true;
    }
    vote_result tally_votes() const {
        auto quorum = _suffrage.size() / 2 + 1;
        if (_granted >= quorum) {
            return vote_result::WON;
        }
        assert(_responded.size() <= _suffrage.size());
        auto unknown = _suffrage.size() - _responded.size();
        return _granted + unknown >= quorum ? vote_result::UNKNOWN : vote_result::LOST;
    }
    friend std::ostream& operator<<(std::ostream& os, const election_tracker& v);
};

// Candidate's state specific to election
class votes {
    server_address_set _voters;
    election_tracker _current;
    std::optional<election_tracker> _previous;
public:
    votes(configuration configuration);

    const server_address_set& voters() const {
        return _voters;
    }

    void register_vote(server_id from, bool granted);
    vote_result tally_votes() const;

    friend std::ostream& operator<<(std::ostream& os, const votes& v);
};

} // namespace raft

