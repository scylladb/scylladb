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

#include "sstables.hh"
#include <functional>

namespace sstables {

    struct compaction_descriptor {
        // List of sstables to be compacted.
        std::vector<sstables::shared_sstable> sstables;
        // Level of sstable(s) created by compaction procedure.
        int level = 0;
        // Threshold size for sstable(s) to be created.
        uint64_t max_sstable_bytes = std::numeric_limits<uint64_t>::max();

        compaction_descriptor() = default;

        compaction_descriptor(std::vector<sstables::shared_sstable> sstables, int level, long max_sstable_bytes)
            : sstables(std::move(sstables))
            , level(level)
            , max_sstable_bytes(max_sstable_bytes) {}

        compaction_descriptor(std::vector<sstables::shared_sstable> sstables)
            : sstables(std::move(sstables)) {}
    };

    enum class compaction_type {
        Compaction = 0,
        Cleanup = 1,
        Validation = 2,
        Scrub = 3,
        Index_build = 4,
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
        default:
            throw std::runtime_error("Invalid Compaction Type");
        }
    }

    struct compaction_info {
        compaction_type type;
        sstring ks;
        sstring cf;
        size_t sstables = 0;
        uint64_t start_size = 0;
        uint64_t end_size = 0;
        uint64_t total_partitions = 0;
        uint64_t total_keys_written = 0;
        std::vector<shared_sstable> new_sstables;
        sstring stop_requested;

        bool is_stop_requested() const {
            return stop_requested.size() > 0;
        }

        void stop(sstring reason) {
            stop_requested = std::move(reason);
        }
    };

    // Compact a list of N sstables into M sstables.
    // Returns a vector with newly created sstables(s).
    //
    // creator is used to get a sstable object for a new sstable that will be written.
    // max_sstable_size is a relaxed limit size for a sstable to be generated.
    // Example: It's okay for the size of a new sstable to go beyond max_sstable_size
    // when writing its last partition.
    // sstable_level will be level of the sstable(s) to be created by this function.
    // If cleanup is true, mutation that doesn't belong to current node will be
    // cleaned up, log messages will inform the user that compact_sstables runs for
    // cleaning operation, and compaction history will not be updated.
    future<std::vector<shared_sstable>> compact_sstables(std::vector<shared_sstable> sstables,
            column_family& cf, std::function<shared_sstable()> creator,
            uint64_t max_sstable_size, uint32_t sstable_level, bool cleanup = false);

    // Return the most interesting bucket applying the size-tiered strategy.
    std::vector<sstables::shared_sstable>
    size_tiered_most_interesting_bucket(lw_shared_ptr<sstable_list> candidates);

    std::vector<sstables::shared_sstable>
    size_tiered_most_interesting_bucket(const std::list<sstables::shared_sstable>& candidates);
}
