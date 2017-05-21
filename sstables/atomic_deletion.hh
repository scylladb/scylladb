/*
 * Copyright (C) 2016 ScyllaDB
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

// The atomic deletion manager solves the problem of orchestrating
// the deletion of files that must be deleted as a group, where each
// shard has different groups, and all shards delete a file for it to
// be deleted.  For example,
//
//  shard 0: delete "A"
//     we can't delete anything because shard 1 hasn't agreed yet.
//  shard 1: delete "A" and B"
//     shard 1 agrees to delete "A", but we can't delete it yet,
//     because shard 1 requires that it be deleted together with "B",
//     and shard 0 hasn't agreed to delete "B" yet.
//  shard 0: delete "B" and "C"
//     shards 0 and 1 now both agree to delete "A" and "B", but shard 0
//     doesn't allow us to delete "B" without "C".
//  shard 1: delete "C"
//     finally, we can delete "A", "B", and "C".

#include "log.hh"
#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/reactor.hh> // for shard_id
#include <unordered_set>
#include <unordered_map>
#include <vector>

#include "seastarx.hh"

namespace sstables {

struct sstable_to_delete {
    sstable_to_delete(sstring name, bool shared) : name(std::move(name)), shared(shared) {}
    sstring name;
    bool shared = false;
    friend std::ostream& operator<<(std::ostream& os, const sstable_to_delete& std);
};

class atomic_deletion_cancelled : public std::exception {
    std::string _msg;
public:
    explicit atomic_deletion_cancelled(std::vector<sstring> names);
    template <typename StringRange>
    explicit atomic_deletion_cancelled(StringRange range)
            : atomic_deletion_cancelled(std::vector<sstring>{range.begin(), range.end()}) {
    }
    const char* what() const noexcept override;
};

class atomic_deletion_manager {
    logging::logger _deletion_logger{"sstable-deletion"};
    using shards_agreeing_to_delete_sstable_type = std::unordered_set<shard_id>;
    using sstables_to_delete_atomically_type = std::set<sstring>;
    struct pending_deletion {
        sstables_to_delete_atomically_type names;
        std::unordered_set<lw_shared_ptr<promise<>>> completions;
    };
    bool _atomic_deletions_cancelled = false;
    // map from sstable name to a set of sstables that must be deleted atomically, including itself
    std::unordered_map<sstring, lw_shared_ptr<pending_deletion>> _atomic_deletion_sets;
    std::unordered_map<sstring, shards_agreeing_to_delete_sstable_type> _shards_agreeing_to_delete_sstable;
    unsigned _shard_count;
    std::function<future<> (std::vector<sstring> sstables)> _delete_sstables;
public:
    atomic_deletion_manager(unsigned shard_count,
            std::function<future<> (std::vector<sstring> sstables)> delete_sstables);
    future<> delete_atomically(std::vector<sstable_to_delete> atomic_deletion_set, unsigned deleting_shard);
    void cancel_atomic_deletions();
};

}
