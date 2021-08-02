/*
 * Copyright (C) 2015-present ScyllaDB
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
#include "sstables/shared_sstable.hh"
#include "compaction/compaction_descriptor.hh"
#include "gc_clock.hh"
#include "compaction_weight_registration.hh"
#include "service/priority_manager.hh"
#include "utils/UUID.hh"
#include <seastar/core/thread.hh>

class flat_mutation_reader;

namespace sstables {

    class pretty_printed_data_size {
        uint64_t _size;
    public:
        pretty_printed_data_size(uint64_t size) : _size(size) {}
        friend std::ostream& operator<<(std::ostream&, pretty_printed_data_size);
    };

    class pretty_printed_throughput {
        uint64_t _size;
        std::chrono::duration<float> _duration;
    public:
        pretty_printed_throughput(uint64_t size, std::chrono::duration<float> dur) : _size(size), _duration(std::move(dur)) {}
        friend std::ostream& operator<<(std::ostream&, pretty_printed_throughput);
    };

    sstring compaction_name(compaction_type type);
    compaction_type to_compaction_type(sstring type_name);

    struct compaction_info {
        compaction_type type = compaction_type::Compaction;
        table* cf = nullptr;
        sstring ks_name;
        sstring cf_name;
        size_t sstables = 0;
        uint64_t start_size = 0;
        uint64_t end_size = 0;
        uint64_t total_partitions = 0;
        uint64_t total_keys_written = 0;
        int64_t ended_at;
        std::vector<shared_sstable> new_sstables;
        sstring stop_requested;
        bool tracking = true;
        utils::UUID run_identifier;
        utils::UUID compaction_uuid;
        struct replacement {
            const std::vector<shared_sstable> removed;
            const std::vector<shared_sstable> added;
        };
        std::vector<replacement> pending_replacements;

        bool is_stop_requested() const noexcept {
            return !stop_requested.empty();
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
    // replacer will replace old sstables by new ones in the column family.
    // max_sstable_size is a relaxed limit size for a sstable to be generated.
    // Example: It's okay for the size of a new sstable to go beyond max_sstable_size
    // when writing its last partition.
    // sstable_level will be level of the sstable(s) to be created by this function.
    // If descriptor.cleanup is true, mutation that doesn't belong to current node will be
    // cleaned up, log messages will inform the user that compact_sstables runs for
    // cleaning operation, and compaction history will not be updated.
    future<compaction_info> compact_sstables(sstables::compaction_descriptor descriptor, column_family& cf);

    // Return list of expired sstables for column family cf.
    // A sstable is fully expired *iff* its max_local_deletion_time precedes gc_before and its
    // max timestamp is lower than any other relevant sstable.
    // In simpler words, a sstable is fully expired if all of its live cells with TTL is expired
    // and possibly doesn't contain any tombstone that covers cells in other sstables.
    std::unordered_set<sstables::shared_sstable>
    get_fully_expired_sstables(column_family& cf, const std::vector<sstables::shared_sstable>& compacting, gc_clock::time_point gc_before);

    // For tests, can drop after we virtualize sstables.
    flat_mutation_reader make_scrubbing_reader(flat_mutation_reader rd, compaction_options::scrub::mode scrub_mode);

    // For tests, can drop after we virtualize sstables.
    future<bool> scrub_validate_mode_validate_reader(flat_mutation_reader rd, const compaction_info& info);
}
