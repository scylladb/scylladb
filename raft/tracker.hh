/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include "utils/assert.hh"
#include <seastar/core/condition-variable.hh>
#include <fmt/core.h>
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
    // Highest read id the follower replied to
    read_id max_acked_read = read_id{0};

    // True if the follower is a voting one
    bool can_vote = true;

    enum class state {
        // In this state only one append entry is send until matching index is found
        PROBE,
        // In this state multiple append entries are sent optimistically
        PIPELINE,
        // In this state snapshot has been transferred
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
        SCYLLA_ASSERT(false);
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
    template<typename Index> Index committed(Index prev_commit_idx);

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

// State of election in a single quorum
class election_tracker {
    // All eligible voters
    std::unordered_set<server_id> _suffrage;
    // Votes collected
    std::unordered_set<server_id> _responded;
    size_t _granted = 0;
public:
    election_tracker(const config_member_set& configuration) {
        for (const auto& s : configuration) {
            if (s.can_vote) {
                _suffrage.emplace(s.addr.id);
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
        SCYLLA_ASSERT(_responded.size() <= _suffrage.size());
        auto unknown = _suffrage.size() - _responded.size();
        return _granted + unknown >= quorum ? vote_result::UNKNOWN : vote_result::LOST;
    }
    friend fmt::formatter<election_tracker>;
};

// Candidate's state specific to election
class votes {
    server_address_set _voters;
    election_tracker _current;
    std::optional<election_tracker> _previous;
public:
    votes(configuration configuration);

    // A server is a member of this set iff
    // it is a voter in the current or previous configuration.
    const server_address_set& voters() const {
        return _voters;
    }

    void register_vote(server_id from, bool granted);
    vote_result tally_votes() const;

    friend fmt::formatter<votes>;
};

} // namespace raft

template <> struct fmt::formatter<raft::election_tracker> : fmt::formatter<string_view> {
    auto format(const raft::election_tracker& v, fmt::format_context& ctx) const  -> decltype(ctx.out());
};

template <> struct fmt::formatter<raft::votes> : fmt::formatter<string_view> {
    auto format(const raft::votes& v, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <> struct fmt::formatter<raft::vote_result> : fmt::formatter<string_view> {
    auto format(const raft::vote_result& v, fmt::format_context& ctx) const -> decltype(ctx.out());
};
