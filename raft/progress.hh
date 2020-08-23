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
#include "logical_clock.hh"

namespace raft {

// Leader's view of each follower, including self.
class follower_progress {
public:
    // Id of this server
    server_id id;
    // Index of the next log entry to send to this server.
    index_t next_idx;
    // Index of the highest log entry known to be replicated to this
    // server.
    index_t match_idx = index_t(0);
    // Index that we know to be committed by the follower
    index_t commit_idx = index_t(0);

    enum class state {
        // In this state only one append entry is send until matching index is found
        PROBE,
        // In this state multiple append entries are sent optimistically
        PIPELINE,
        // In this state snapshot is been transfered
        SNAPSHOT
    };
    state state = state::PROBE;
    // number of in flight still un-acked append entries requests
    size_t in_flight = 0;
    static constexpr size_t max_in_flight = 10;

    // Set when a message is sent to the follower.
    // Used to decide if a separate keep alive message is needed
    // within this tick.
    // In probe mode, used to limit the amount of entries sent to
    // the follower.
    logical_clock::time_point last_append_time = logical_clock::min();

    // check if a reject packet should be ignored because it was delayed
    // or reordered
    bool is_stray_reject(const append_reply::rejected&);

    void become_probe();
    void become_pipeline();
    void become_snapshot();

    // Return true if a new replication record can be sent to the follower.
    bool can_send_to(logical_clock::time_point now);

    follower_progress(server_id id_arg, index_t next_idx_arg)
        : id(id_arg), next_idx(next_idx_arg)
    {}
};

using progress = std::unordered_map<server_id, follower_progress>;

class tracker: private progress {
    // Copy of this server's id
    server_id _my_id;
public:
    using progress::begin, progress::end, progress::cbegin, progress::cend, progress::size;

    explicit tracker(server_id my_id)
            : _my_id(my_id)
    {}

    // Return progress for a follower
    follower_progress& find(server_id dst) {
        return this->progress::find(dst)->second;
    }
    void set_configuration(const std::vector<server_address>& servers, index_t next_idx);

    // Calculate the current commit index based on the current
    // simple or joint quorum.
    index_t committed(index_t prev_commit_idx);
};

// Possible leader election outcomes.
enum class vote_result {
    // We haven't got enough responses yet, either because
    // the servers haven't voted or responses failed to arrive.
    UNKNOWN,
    // This candidate has won the election
    WON,
    // The quorum of servers has voted against this candidate
    LOST,
};

// Candidate's state specific to election
class votes {
    size_t _cluster_size = 1;
    // Number of responses to RequestVote RPC.
    // The candidate always votes for self.
    size_t _responded = 1;
    // Number of granted votes.
    // The candidate always votes for self.
    size_t _granted = 1;
public:
    void set_configuration(const std::vector<server_address>& servers) {
        _cluster_size = servers.size();
    }

    void register_vote(server_id from, bool granted) {
        _responded++;
        if (granted) {
            _granted++;
        }
    }

    vote_result tally_votes() const {
        auto quorum = _cluster_size / 2 + 1;
        if (_granted >= quorum) {
            return vote_result::WON;
        }
        assert(_responded <= _cluster_size);
        auto unknown = _cluster_size - _responded;
        return _granted + unknown >= quorum ? vote_result::UNKNOWN : vote_result::LOST;
    }

    friend std::ostream& operator<<(std::ostream& os, const votes& v);
};

} // namespace raft

