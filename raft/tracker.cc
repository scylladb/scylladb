/*
 * Copyright (C) 2020-present ScyllaDB
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
#include "tracker.hh"
#include <seastar/core/coroutine.hh>

namespace raft {

bool follower_progress::is_stray_reject(const append_reply::rejected& rejected) {
    // By precondition, we are the leader and `rejected.current_term` is equal to our term.
    // By definition of `match_idx` we know that at some point all entries up to and including
    // `match_idx` were the same in our log and the follower's log; ...
    if (rejected.non_matching_idx <= match_idx) {
        // ... in particular, entry `rejected.non_matching_idx` (which is <= `match_idx`) at some point
        // was the same in our log and the follower's log, but `rejected` claims they are different.
        // A follower cannot change an entry unless it enters a different term, but `rejected.current_term`
        // is equal to our term. Thus `rejected` must be stray.
        return true;
    }
    if (rejected.last_idx < match_idx) {
        // ... in particular, at some point the follower had to have an entry with index `rejected.last_idx + 1`
        // (because `rejected.last_idx < match_idx implies rejected.last_idx + 1 <= match_idx)
        // but `rejected` claims it doesn't have such entry now.
        // A follower cannot truncate a suffix of its log unless it enters a different term,
        // but `rejected.current_term` is equal to our term. Thus `rejected` must be stray.
        return true;
    }

    switch (state) {
    case follower_progress::state::PIPELINE:
        break;
    case follower_progress::state::PROBE:
        // In PROBE state we send a single append request `req` with `req.prev_log_idx == next_idx - 1`.
        // When the follower generates a rejected response `r`, it sets `r.non_matching_idx = req.prev_log_idx`.
        // Thus the reject either satisfies `rejected.non_matching_idx == next_idx - 1` or is stray.
        if (rejected.non_matching_idx != index_t(next_idx - 1)) {
            return true;
        }
        break;
    case follower_progress::state::SNAPSHOT:
        // any reject during snapshot transfer is stray one
        return true;
    default:
        assert(false);
    }
    return false;
}

void follower_progress::become_probe() {
    state = state::PROBE;
    probe_sent = false;
}

void follower_progress::become_pipeline() {
    if (state != state::PIPELINE) {
        // If a previous request was accepted, move to "pipeline" state
        // since we now know the follower's log state.
        state = state::PIPELINE;
        in_flight = 0;
    }
}

void follower_progress::become_snapshot(index_t snp_idx) {
    state = state::SNAPSHOT;
    // If snapshot transfer succeeds, start replicating from the
    // next index, otherwise we will learn the follower's index
    // again by sending a probe request.
    next_idx = snp_idx + index_t{1};
}

bool follower_progress::can_send_to() {
    switch (state) {
    case state::PROBE:
        return !probe_sent;
    case state::PIPELINE:
        // allow `max_in_flight` outstanding indexes
        // FIXME: make it smarter
        return in_flight < follower_progress::max_in_flight;
    case state::SNAPSHOT:
        // In this state we are waiting
        // for a snapshot to be transferred
        // before starting to sync the log.
        return false;
    }
    assert(false);
    return false;
}

// If this is called when a tracker is just created, the current
// progress is empty and we should simply crate an instance for
// each follower.
// When switching configurations, we should preserve progress
// for existing followers, crate progress for new, and remove
// progress for non-members (to make sure we don't send noise
// messages to them).
void tracker::set_configuration(const configuration& configuration, index_t next_idx) {
    _current_voters.clear();
    _previous_voters.clear();

    // Swap out the current progress and then re-add
    // only those entries which are still present.
    progress old_progress = std::move(*this);

    auto emplace_simple_config = [&](const server_address_set& config, std::unordered_set<server_id>& voter_ids) {
        for (const auto& s : config) {
            if (s.can_vote) {
                voter_ids.emplace(s.id);
            }
            auto newp = this->progress::find(s.id);
            if (newp != this->progress::end()) {
                // Processing joint configuration and already added
                // an entry for this id.
                continue;
            }
            auto oldp = old_progress.find(s.id);
            if (oldp != old_progress.end()) {
                newp = this->progress::emplace(s.id, std::move(oldp->second)).first;
            } else {
                newp = this->progress::emplace(s.id, follower_progress{s.id, next_idx}).first;
            }
            newp->second.can_vote = s.can_vote;
        }
    };
    emplace_simple_config(configuration.current, _current_voters);
    if (configuration.is_joint()) {
        emplace_simple_config(configuration.previous, _previous_voters);
    }
}

// A sorted array of node match indexes used to find
// the pivot which serves as commit index of the group.
class match_vector {
    std::vector<index_t> _match;
    // How many elements in the match array have a match index
    // larger than the previous commit index.
    size_t _count = 0;
    index_t _prev_commit_idx;
public:
    explicit match_vector(index_t prev_commit_idx, size_t reserve_size)
            : _prev_commit_idx(prev_commit_idx) {
        _match.reserve(reserve_size);
    }

    void push_back(index_t match_idx) {
        if (match_idx > _prev_commit_idx) {
            _count++;
        }
        _match.push_back(match_idx);
    }
    bool committed() const {
        return _count >= _match.size()/2 + 1;
    }
    index_t commit_idx() {
        logger.trace("check committed count {} cluster size {}", _count, _match.size());
        // The index of the pivot node is selected so that all nodes
        // with a larger match index plus the pivot form a majority,
        // for example:
        // cluster size  pivot node     majority
        // 1             0              1
        // 2             0              2
        // 3             1              2
        // 4             1              3
        // 5             2              3
        //
        auto pivot = (_match.size() - 1) / 2;
        std::nth_element(_match.begin(), _match.begin() + pivot, _match.end());
        return _match[pivot];
    }
};

index_t tracker::committed(index_t prev_commit_idx) {
    match_vector current(prev_commit_idx, _current_voters.size());

    if (!_previous_voters.empty()) {
        match_vector previous(prev_commit_idx, _previous_voters.size());

        for (const auto& [id, p] : *this) {
            if (_current_voters.contains(p.id)) {
                current.push_back(p.match_idx);
            }
            if (_previous_voters.contains(p.id)) {
                previous.push_back(p.match_idx);
            }
        }
        if (!current.committed() || !previous.committed()) {
            return prev_commit_idx;
        }
        return std::min(current.commit_idx(), previous.commit_idx());
    } else {
        for (const auto& [id, p] : *this) {
            if (_current_voters.contains(p.id)) {
                current.push_back(p.match_idx);
            }
        }
        if (!current.committed()) {
            return prev_commit_idx;
        }
        return current.commit_idx();
    }
}

votes::votes(configuration configuration)
        :_voters(configuration.current)
        , _current(configuration.current) {

    if (configuration.is_joint()) {
        _previous.emplace(configuration.previous);
        _voters.insert(configuration.previous.begin(), configuration.previous.end());
    }
    // Filter out non voting members
    std::erase_if(_voters, [] (const server_address& s) { return !s.can_vote; });
}

void votes::register_vote(server_id from, bool granted) {
    bool registered = false;

    if (_current.register_vote(from, granted)) {
        registered = true;
    }
    if (_previous && _previous->register_vote(from, granted)) {
        registered = true;
    }
    // We can get an outdated vote from a node that is now non-voting member.
    // Such vote should be ignored.
    if (!registered) {
        logger.info("Got a vote from unregistered server {} during election", from);
    }
}

vote_result votes::tally_votes() const {
    if (_previous) {
        auto previous_result = _previous->tally_votes();
        if (previous_result != vote_result::WON) {
            return previous_result;
        }
    }
    return _current.tally_votes();
}

std::ostream& operator<<(std::ostream& os, const election_tracker& v) {
    os << "responded: " << v._responded.size() << ", ";
    os << "granted: " << v._granted;
    return os;
}


std::ostream& operator<<(std::ostream& os, const votes& v) {
    os << "current: " << v._current << std::endl;
    if (v._previous) {
        os << "previous: " << v._previous.value() << std::endl;
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, const vote_result& v) {
    static const char *n;
    switch (v) {
    case vote_result::UNKNOWN:
        n = "UNKNOWN";
        break;
    case vote_result::WON:
        n = "WON";
        break;
    case vote_result::LOST:
        n = "LOST";
        break;
    }
    os << n;
    return os;
}

} // end of namespace raft
