/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include "utils/assert.hh"
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
    snapshot_descriptor _snapshot;
    // In-memory log data.
    log_entries _log;
    // Max command size, is used to calculate memory usage.
    size_t _max_command_size;
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
    size_t _memory_usage;
private:
    // Drop uncommitted log entries not present on the leader.
    void truncate_uncommitted(index_t i);
    // A helper used to find the last configuration entry in the
    // log after it's been loaded from disk.
    void init_last_conf_idx();
    log_entry_ptr& get_entry(index_t);
    const log_entry_ptr& get_entry(index_t) const;
    size_t range_memory_usage(log_entries::iterator first, log_entries::iterator last) const;
public:
    explicit log(snapshot_descriptor snp, log_entries log = {}, size_t max_command_size = sizeof(log_entry))
            : _snapshot(std::move(snp)), _log(std::move(log)), _max_command_size(max_command_size) {
        if (_log.empty()) {
            _first_idx = _snapshot.idx + index_t{1};
        } else {
            _first_idx = _log[0]->idx;
            // All log entries following the snapshot must
            // be present, otherwise we will not be able to
            // perform an initial state transfer.
            SCYLLA_ASSERT(_first_idx <= _snapshot.idx + index_t{1});
        }
        _memory_usage = range_memory_usage(_log.begin(), _log.end());
        // The snapshot index is at least 0, so _first_idx
        // is at least 1
        SCYLLA_ASSERT(_first_idx > index_t{0});
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
    // Returns memory usage of the log entries in bytes
    size_t memory_usage() const {
        return _memory_usage;
    }

    // The function returns current snapshot state of the log
    const snapshot_descriptor& get_snapshot() const {
        return _snapshot;
    }

    // This call will update the log to point to the new snapshot and will truncate the log prefix so that
    // the number of remaining applied entries is <= max_trailing_entries and their total
    // size is <= max_trailing_bytes.
    // Please note: the number of remaining items includes the snapshot index item, so a trailing entry
    // index is smaller or equal to the snapshot index. In case there are no trailing entries, the
    // first index in the log will be the snapshot index + 1, even if the log contains no entries, only
    // the snapshot.
    // Return: the value that specifies the size in bytes of the dropped log entries and the first index
    //         in the log after truncation
    std::tuple<size_t, index_t> apply_snapshot(snapshot_descriptor&& snp, size_t max_trailing_entries,
        size_t max_trailing_bytes);

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

    // Return the last configuration entry with index smaller than or equal to `idx`.
    // Precondition: `last_idx()` >= `idx` >= `get_snapshot().idx`;
    // there is no way in general to learn configurations before the last snapshot.
    const configuration& last_conf_for(index_t idx) const;

    // Return the previous configuration, if available (otherwise return nullptr).
    // The returned pointer, if not null, is only valid until the next operation on the log.
    const configuration* get_prev_configuration() const;

    // Called on a follower to append entries from a leader.
    // @retval return an index of last appended entry
    index_t maybe_append(std::vector<log_entry_ptr>&& entries);

    friend fmt::formatter<log>;

    // The log keeps track of the memory it uses. This function returns the number
    // of bytes that will be marked as used when a log_entry is added to the log.
    // This logic should match the handling of log_limiter_semaphore,
    // which is currently incremented only for command, but not for configuration and log_entry::dummy.
    // This is why zero is returned for other log_entry elements
    // and why this function has been kept separate from entry_size,
    // which returns non-zero for configuration.
    template <typename T>
    requires std::is_same_v<T, log_entry> ||
             std::is_same_v<T, command> || std::is_same_v<T, configuration> || std::is_same_v<T, log_entry::dummy>
    static inline size_t memory_usage_of(const T& v, size_t max_command_size) {
        if constexpr(std::is_same_v<T, command>) {
            // We account for sizeof(log_entry) for "small" commands,
            // since the overhead of log_entries can take up significant memory.
            return max_command_size > sizeof(log_entry) && v.size() < max_command_size - sizeof(log_entry)
                ? v.size() + sizeof(log_entry)
                : v.size();
        }
        if constexpr(std::is_same_v<T, log_entry>) {
            if (const auto* c = get_if<command>(&v.data); c != nullptr) {
                return memory_usage_of(*c, max_command_size);
            }
        }
        return 0;
    }
};

}

template <> struct fmt::formatter<raft::log> : fmt::formatter<string_view> {
    auto format(const raft::log&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
