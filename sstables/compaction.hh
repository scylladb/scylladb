/*
 * Copyright (C) 2015 ScyllaDB
 *
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

#include "database_fwd.hh"
#include "shared_sstable.hh"
#include "gc_clock.hh"
#include "compaction_weight_registration.hh"
#include <seastar/core/thread.hh>
#include <functional>

namespace sstables {

    struct compaction_descriptor {
        // List of sstables to be compacted.
        std::vector<sstables::shared_sstable> sstables;
        // Level of sstable(s) created by compaction procedure.
        int level;
        // Threshold size for sstable(s) to be created.
        uint64_t max_sstable_bytes;
        // Holds ownership of a weight assigned to this compaction iff it's a regular one.
        stdx::optional<compaction_weight_registration> weight_registration;

        compaction_descriptor() = default;

        explicit compaction_descriptor(std::vector<sstables::shared_sstable> sstables, int level = 0, uint64_t max_sstable_bytes = std::numeric_limits<uint64_t>::max())
            : sstables(std::move(sstables))
            , level(level)
            , max_sstable_bytes(max_sstable_bytes) {}
    };

    struct resharding_descriptor {
        std::vector<sstables::shared_sstable> sstables;
        uint64_t max_sstable_bytes;
        shard_id reshard_at;
        uint32_t level;
    };

    enum class compaction_type {
        Compaction = 0,
        Cleanup = 1,
        Validation = 2,
        Scrub = 3,
        Index_build = 4,
        Reshard = 5,
    };

    static inline sstring compaction_name(compaction_type type) {
        switch (type) {
        case compaction_type::Compaction:
            return "COMPACTION";
        case compaction_type::Cleanup:
            return "CLEANUP";
        case compaction_type::Validation:
            return "VALIDATION";
        case compaction_type::Scrub:
            return "SCRUB";
        case compaction_type::Index_build:
            return "INDEX_BUILD";
        case compaction_type::Reshard:
            return "RESHARD";
        default:
            throw std::runtime_error("Invalid Compaction Type");
        }
    }

    struct compaction_info {
        compaction_type type = compaction_type::Compaction;
        sstring ks;
        sstring cf;
        size_t sstables = 0;
        uint64_t start_size = 0;
        uint64_t end_size = 0;
        uint64_t total_partitions = 0;
        uint64_t total_keys_written = 0;
        int64_t ended_at;
        std::vector<shared_sstable> new_sstables;
        sstring stop_requested;
        bool tracking = true;

        bool is_stop_requested() const {
            return stop_requested.size() > 0;
        }

        void stop(sstring reason) {
            stop_requested = std::move(reason);
        }

        void stop_tracking() {
            tracking = false;
        }
    };

    // Compact a list of N sstables into M sstables.
    // Returns info about the finished compaction, which includes vector to new sstables.
    //
    // creator is used to get a sstable object for a new sstable that will be written.
    // max_sstable_size is a relaxed limit size for a sstable to be generated.
    // Example: It's okay for the size of a new sstable to go beyond max_sstable_size
    // when writing its last partition.
    // sstable_level will be level of the sstable(s) to be created by this function.
    // If cleanup is true, mutation that doesn't belong to current node will be
    // cleaned up, log messages will inform the user that compact_sstables runs for
    // cleaning operation, and compaction history will not be updated.
    future<compaction_info> compact_sstables(sstables::compaction_descriptor descriptor,
            column_family& cf, std::function<shared_sstable()> creator, bool cleanup = false);

    // Compacts a set of N shared sstables into M sstables. For every shard involved,
    // i.e. which owns any of the sstables, a new unshared sstable is created.
    future<std::vector<shared_sstable>> reshard_sstables(std::vector<shared_sstable> sstables,
            column_family& cf, std::function<shared_sstable(shard_id)> creator,
        uint64_t max_sstable_size, uint32_t sstable_level);

    // Return list of expired sstables for column family cf.
    // A sstable is fully expired *iff* its max_local_deletion_time precedes gc_before and its
    // max timestamp is lower than any other relevant sstable.
    // In simpler words, a sstable is fully expired if all of its live cells with TTL is expired
    // and possibly doesn't contain any tombstone that covers cells in other sstables.
    std::unordered_set<sstables::shared_sstable>
    get_fully_expired_sstables(column_family& cf, const std::vector<sstables::shared_sstable>& compacting, gc_clock::time_point gc_before);
}
