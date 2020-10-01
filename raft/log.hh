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
// This class represents the Raft log in memory.
//
// The value of the first index is 1.
// New entries are added at the back.
//
// Entries are persisted locally after they are added.  Entries may be
// dropped from the beginning by snapshotting and from the end by
// a new leader replacing stale entries. Any exception thrown by
// any function leaves the log in a consistent state.
class log {
    // Snapshot of the prefix of the log.
    snapshot _snapshot;
    // We need something that can be truncated from both sides.
    // std::deque move constructor is not nothrow hence cannot be used
    log_entries _log;
    // Index of the last stable (persisted) entry in the log.
    index_t _stable_idx = index_t(0);

private:
    void truncate_head(index_t i);
    void truncate_tail(index_t i);
    log_entry_ptr& get_entry(index_t);
    index_t start_idx() const;
public:
    log() = default ;
    explicit log(log_entries log) : _log(std::move(log)) { stable_to(index_t(_log.size())); };
    log(snapshot snp, log_entries log) : _snapshot(std::move(snp)), _log(std::move(log)) { stable_to(last_idx()); }
    explicit log(snapshot snp) : _snapshot(std::move(snp)) {}
    // The index here the global raft log index, not related to a snapshot.
    // It is a programming error to call the function with an index that points into the snapshot,
    // the function will abort()
    log_entry_ptr& operator[](size_t i);
    // Add an entry to the log.
    void emplace_back(log_entry&& e);
    // Mark all entries up to this index as stable.
    void stable_to(index_t idx);
    // Return true if in memory log is empty.
    bool empty() const;
    // 3.6.1 Election restriction.
    // The voter denies its vote if its own log is more up-to-date
    // than that of the candidate.
    bool is_up_to_date(index_t idx, term_t term) const;
    index_t next_idx() const;
    index_t last_idx() const;
    index_t stable_idx() const {
        return _stable_idx;
    }
    term_t last_term() const;

    // The function returns current snapshot state of the log
    const snapshot& get_snapshot() const {
        return _snapshot;
    }

    void apply_snapshot(snapshot&& snp);

    // 3.5
    // Raft maintains the following properties, which
    // together constitute the Log Matching Property:
    // * If two entries in different logs have the same index and
    // term, then they store the same command.
    // * If two entries in different logs have the same index and
    // term, then the logs are identical in all preceding entries.
    //
    // The first property follows from the fact that a leader
    // creates at most one entry with a given log index in a given
    // term, and log entries never change their position in the
    // log. The second property is guaranteed by a consistency
    // check performed by AppendEntries. When sending an
    // AppendEntries RPC, the leader includes the index and term
    // of the entry in its log that immediately precedes the new
    // entries. If the follower does not find an entry in its log
    // with the same index and term, then it refuses the new
    // entries.
    //
    // @retval first is true - there is a match, term value is irrelevant
    // @retval first is false - the follower's log doesn't match the leader's
    //                          and non matching term is in second
    std::pair<bool, term_t> match_term(index_t idx, term_t term) const;

    // Called on a follower to append entries from a leader.
    // @retval return an index of last appended entry
    index_t maybe_append(std::vector<log_entry>&& entries);

    friend std::ostream& operator<<(std::ostream& os, const log& l);
};

}
