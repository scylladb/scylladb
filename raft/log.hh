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
    // Raft log index of the first entry in the log.
    // Usually it's simply _snapshot.idx + 1,
    // but if apply_snapshot() with non-zero trailing was used,
    // it may point at an entry older than the snapshot.
    // If the log is empty, same as next_idx()
    index_t _first_idx;
    // Index of the last stable (persisted) entry in the log.
    index_t _stable_idx = index_t(0);
    // Log index of the last configuration change.
    //
    // Is used to:
    // - prevent accepting a new configuration change while
    // there is a change in progress.
    // - revert the state machine to the previous configuration if
    // the log is truncated while there is an uncommitted change
    //
    // Seastar Raft uses a joint consensus approach to
    // configuration changes, when each change is represented as
    // two log entries: one for C_old + C_new and another for
    // C_new. This index is therefore updated twice per change.
    // It's used to track when the log entry for C_old + C_new is
    // committed (_commit_idx >= _last_conf_idx &&
    // _configuration.is_joint()) and automatically append a new
    // log entry for C_new.
    //
    // While the index is used only in leader and candidate
    // states, it is maintained in all states to avoid scanning
    // the log backwards during election.
    index_t _last_conf_idx = index_t{0};
    // The previous value of _last_conf_idx, to avoid scanning
    // the log backwards after truncate().
    index_t _prev_conf_idx = index_t{0};
private:
    // Drop uncommitted log entries not present on the leader.
    void truncate_uncommitted(index_t i);
    // A helper used to find the last configuration entry in the
    // log after it's been loaded from disk.
    void init_last_conf_idx();
    log_entry_ptr& get_entry(index_t);
public:
    explicit log(snapshot snp, log_entries log = {})
            : _snapshot(std::move(snp)), _log(std::move(log)) {
        if (_log.empty()) {
            _first_idx = _snapshot.idx + index_t{1};
        } else {
            _first_idx = _log[0]->idx;
            // All log entries following the snapshot must
            // be present, otherwise we will not be able to
            // perform an initial state transfer.
            assert(_first_idx <= _snapshot.idx + 1);
        }
        // The snapshot index is at least 0, so _first_idx
        // is at least 1
        assert(_first_idx > 0);
        stable_to(last_idx());
        init_last_conf_idx();
    }
    // The index here the global raft log index, not related to a snapshot.
    // It is a programming error to call the function with an index that points into the snapshot,
    // the function will abort()
    log_entry_ptr& operator[](size_t i);
    // Add an entry to the log.
    void emplace_back(log_entry_ptr&& e);
    // Mark all entries up to this index as stable.
    void stable_to(index_t idx);
    // Return true if in memory log is empty.
    bool empty() const;
    // 3.6.1 Election restriction.
    // The voter denies its vote if its own log is more up-to-date
    // than that of the candidate.
    bool is_up_to_date(index_t idx, term_t term) const;
    index_t next_idx() const;
    // Return index of the last entry. If the log is empty,
    // return the index of the last entry in the snapshot.
    index_t last_idx() const;
    index_t last_conf_idx() const {
        return _last_conf_idx ? _last_conf_idx : _snapshot.idx;
    }
    index_t stable_idx() const {
        return _stable_idx;
    }
    // Return the term of the last entry in the log,
    // or the snapshot term if the log is empty.
    // Used in elections to not vote for a candidate with
    // a less recent term.
    term_t last_term() const;
    // Return the number of log entries in memory
    size_t in_memory_size() const {
        return _log.size();
    }

    // The function returns current snapshot state of the log
    const snapshot& get_snapshot() const {
        return _snapshot;
    }

    // This call will update the log to point to the new snaphot
    // and will truncate the log prefix up to (snp.idx - trailing)
    // entry. Return value specifies how many log entries were dropped
    size_t apply_snapshot(snapshot&& snp, size_t trailing);

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
    // Return term number of the entry matching the index. If the
    // entry is not in the log and does not match snapshot index,
    // return an empty optional.
    // Used to validate the log matching rule.
    std::optional<term_t> term_for(index_t idx) const;

    // Return the latest configuration present in the log.
    // This would be either the entry at last_conf_idx()
    // or, if it's not set, the snapshot configuration.
    // The returned reference is only valid until the next
    // operation on the log.
    const configuration& get_configuration() const;

    // Called on a follower to append entries from a leader.
    // @retval return an index of last appended entry
    index_t maybe_append(std::vector<log_entry_ptr>&& entries);

    friend std::ostream& operator<<(std::ostream& os, const log& l);
};

}
