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
#include "progress.hh"
#include <seastar/core/coroutine.hh>

namespace raft {

bool follower_progress::is_stray_reject(const append_reply::rejected& rejected) {
    switch (state) {
    case follower_progress::state::PIPELINE:
        if (rejected.non_matching_idx <= match_idx) {
            // If rejected index is smaller that matched it means this is a stray reply
            return true;
        }
        break;
    case follower_progress::state::PROBE:
        // In the probe state the reply is only valid if it matches next_idx - 1, since only
        // one append request is outstanding.
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
}

void follower_progress::become_pipeline() {
    if (state != state::PIPELINE) {
        // If a previous request was accepted, move to "pipeline" state
        // since we now know the follower's log state.
        state = state::PIPELINE;
        in_flight = 0;
    }
}

void follower_progress::become_snapshot() {
    state = state::SNAPSHOT;
}

bool follower_progress::can_send_to(logical_clock::time_point now) {
    switch (state) {
    case state::PROBE:
        return now - last_append_time >= logical_clock::duration{1};
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

void tracker::set_configuration(const std::vector<server_address>& servers, index_t next_idx) {
    for (auto& s : servers) {
        if (this->progress::find(s.id) != this->progress::end()) {
            continue;
        }
        this->progress::emplace(s.id, follower_progress{s.id, next_idx});
    }
}

index_t tracker::committed(index_t prev_commit_idx) {
    std::vector<index_t> match;
    size_t count = 0;

    for (const auto& [id, p] : *this) {
        logger.trace("committed {}: {} {}", p.id, p.match_idx, prev_commit_idx);
        if (p.match_idx > prev_commit_idx) {
            count++;
        }
        match.push_back(p.match_idx);
    }
    logger.trace("check committed count {} cluster size {}", count, match.size());
    if (count < match.size()/2 + 1) {
        return prev_commit_idx;
    }
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
    auto pivot = (match.size() - 1) / 2;
    std::nth_element(match.begin(), match.begin() + pivot, match.end());
    return match[pivot];
}

std::ostream& operator<<(std::ostream& os, const votes& v) {
    os << "responded: " << v._responded << ", ";
    os << "granted: " << v._granted;
    return os;
}

} // end of namespace raft
