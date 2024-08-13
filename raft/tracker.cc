/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include "utils/assert.hh"
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
        if (rejected.non_matching_idx != next_idx - index_t(1)) {
            return true;
        }
        break;
    case follower_progress::state::SNAPSHOT:
        // any reject during snapshot transfer is stray one
        return true;
    default:
        SCYLLA_ASSERT(false);
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
    SCYLLA_ASSERT(false);
    return false;
}

// If this is called when a tracker is just created, the current
// progress is empty and we should simply create an instance for
// each follower.
// When switching configurations, we should preserve progress
// for existing followers, create progress for new, and remove
// progress for non-members (to make sure we don't send noise
// messages to them).
void tracker::set_configuration(const configuration& configuration, index_t next_idx) {
    _current_voters.clear();
    _previous_voters.clear();

    // Swap out the current progress and then re-add
    // only those entries which are still present.
    progress old_progress = std::move(*this);

    auto emplace_simple_config = [&](const config_member_set& config, std::unordered_set<server_id>& voter_ids) {
        for (const auto& s : config) {
            if (s.can_vote) {
                voter_ids.emplace(s.addr.id);
            }
            auto newp = this->progress::find(s.addr.id);
            if (newp != this->progress::end()) {
                // Processing joint configuration and already added
                // an entry for this id.
                continue;
            }
            auto oldp = old_progress.find(s.addr.id);
            if (oldp != old_progress.end()) {
                newp = this->progress::emplace(s.addr.id, std::move(oldp->second)).first;
            } else {
                newp = this->progress::emplace(s.addr.id, follower_progress{s.addr.id, next_idx}).first;
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
template<typename Index>
class match_vector {
    std::vector<Index> _match;
    // How many elements in the match array have a match index
    // larger than the previous commit index.
    size_t _count = 0;
    Index _prev_commit_idx;
public:
    explicit match_vector(Index prev_commit_idx, size_t reserve_size)
            : _prev_commit_idx(prev_commit_idx) {
        _match.reserve(reserve_size);
    }

    void push_back(Index match_idx) {
        if (match_idx > _prev_commit_idx) {
            _count++;
        }
        _match.push_back(match_idx);
    }
    bool committed() const {
        return _count >= _match.size()/2 + 1;
    }
    Index commit_idx() {
        logger.trace("{}: check committed count {} cluster size {}", std::is_same_v<Index, index_t> ? "commit" : "read barrier", _count, _match.size());
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

template<typename T>
T tracker::committed(T prev_commit_idx) {
    auto push_idx = [] (match_vector<T>& v, const follower_progress& p) {
        if constexpr (std::is_same_v<T, index_t>) {
            v.push_back(p.match_idx);
        } else {
            v.push_back(p.max_acked_read);
        }
    };
    match_vector<T> current(prev_commit_idx, _current_voters.size());

    if (!_previous_voters.empty()) {
        match_vector<T> previous(prev_commit_idx, _previous_voters.size());

        for (const auto& [id, p] : *this) {
            if (_current_voters.contains(p.id)) {
                push_idx(current, p);
            }
            if (_previous_voters.contains(p.id)) {
                push_idx(previous, p);
            }
        }
        if (!current.committed() || !previous.committed()) {
            return prev_commit_idx;
        }
        return std::min(current.commit_idx(), previous.commit_idx());
    } else {
        for (const auto& [id, p] : *this) {
            if (_current_voters.contains(p.id)) {
                push_idx(current, p);
            }
        }
        if (!current.committed()) {
            return prev_commit_idx;
        }
        return current.commit_idx();
    }
}

template index_t tracker::committed(index_t);
template read_id tracker::committed(read_id);

votes::votes(configuration configuration)
        : _current(configuration.current) {
    for (auto* cfg: {&configuration.previous, &configuration.current}) {
        for (auto& srv: *cfg) {
            if (srv.can_vote) {
                _voters.insert(srv.addr);
            }
        }
    }

    if (configuration.is_joint()) {
        _previous.emplace(configuration.previous);
    }
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

} // end of namespace raft

auto fmt::formatter<raft::election_tracker>::format(const raft::election_tracker& v, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "responded: {}, granted: {}",
                          v._responded.size(), v._granted);
}

auto fmt::formatter<raft::votes>::format(const raft::votes& v, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    auto out = ctx.out();
    out = fmt::format_to(out, "current: {}\n", v._current);
    if (v._previous) {
        out = fmt::format_to(out, "previous: {}\n", v._previous.value());
    }
    return out;
}

auto fmt::formatter<raft::vote_result>::format(const raft::vote_result& v, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    std::string_view name;
    using enum raft::vote_result;
    switch (v) {
    case UNKNOWN:
        name = "UNKNOWN";
        break;
    case WON:
        name = "WON";
        break;
    case LOST:
        name = "LOST";
        break;
    }
    return formatter<string_view>::format(name, ctx);
}
