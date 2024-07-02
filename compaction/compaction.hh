/*
 * Copyright (C) 2015-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "sstables/shared_sstable.hh"
#include "compaction/compaction_descriptor.hh"
#include "gc_clock.hh"
#include "utils/UUID.hh"
#include "table_state.hh"
#include <seastar/core/abort_source.hh>

using namespace compaction;

namespace sstables {

bool is_eligible_for_compaction(const sstables::shared_sstable& sst) noexcept;

// Return the name of the compaction type
// as used over the REST api, e.g. "COMPACTION" or "CLEANUP".
sstring compaction_name(compaction_type type);

// Reverse map the name of the compaction type
// as used over the REST api, e.g. "COMPACTION" or "CLEANUP",
// to the compaction_type enum code.
compaction_type to_compaction_type(sstring type_name);

// Return a string representing the compaction type
// as a verb for logging purposes, e.g. "Compact" or "Cleanup".
std::string_view to_string(compaction_type type);

struct compaction_info {
    utils::UUID compaction_uuid;
    compaction_type type = compaction_type::Compaction;
    sstring ks_name;
    sstring cf_name;
    uint64_t total_partitions = 0;
    uint64_t total_keys_written = 0;
};

struct compaction_data {
    uint64_t compaction_size = 0;
    uint64_t total_partitions = 0;
    uint64_t total_keys_written = 0;
    sstring stop_requested;
    abort_source abort;
    utils::UUID compaction_uuid;
    unsigned compaction_fan_in = 0;
    struct replacement {
        const std::vector<shared_sstable> removed;
        const std::vector<shared_sstable> added;
    };
    std::vector<replacement> pending_replacements;

    bool is_stop_requested() const noexcept {
        return !stop_requested.empty();
    }

    void stop(sstring reason) {
        if (!abort.abort_requested()) {
            stop_requested = std::move(reason);
            abort.request_abort();
        }
    }
};

struct compaction_stats {
    std::chrono::time_point<db_clock> ended_at;
    uint64_t start_size = 0;
    uint64_t end_size = 0;
    uint64_t validation_errors = 0;
    // Bloom filter checks during max purgeable calculation
    uint64_t bloom_filter_checks = 0;

    compaction_stats& operator+=(const compaction_stats& r) {
        ended_at = std::max(ended_at, r.ended_at);
        start_size += r.start_size;
        end_size += r.end_size;
        validation_errors += r.validation_errors;
        bloom_filter_checks += r.bloom_filter_checks;
        return *this;
    }
    friend compaction_stats operator+(const compaction_stats& l, const compaction_stats& r) {
        auto tmp = l;
        tmp += r;
        return tmp;
    }
};

struct compaction_result {
    std::vector<sstables::shared_sstable> new_sstables;
    compaction_stats stats;
};

class read_monitor_generator;

class compaction_progress_monitor {
    std::unique_ptr<read_monitor_generator> _generator = nullptr;
    uint64_t _progress = 0;
public:
    void set_generator(std::unique_ptr<read_monitor_generator> generator);
    void reset_generator();
    // Returns number of bytes processed with _generator.
    uint64_t get_progress() const;

    friend class compaction;
    friend future<compaction_result> scrub_sstables_validate_mode(sstables::compaction_descriptor, compaction_data&, table_state&, compaction_progress_monitor&);
};

// Compact a list of N sstables into M sstables.
// Returns info about the finished compaction, which includes vector to new sstables.
//
// compaction_descriptor is responsible for specifying the type of compaction, and influencing
// compaction behavior through its available member fields.
future<compaction_result> compact_sstables(sstables::compaction_descriptor descriptor, compaction_data& cdata, table_state& table_s, compaction_progress_monitor& progress_monitor);

// Return list of expired sstables for column family cf.
// A sstable is fully expired *iff* its max_local_deletion_time precedes gc_before and its
// max timestamp is lower than any other relevant sstable.
// In simpler words, a sstable is fully expired if all of its live cells with TTL is expired
// and possibly doesn't contain any tombstone that covers cells in other sstables.
std::unordered_set<sstables::shared_sstable>
get_fully_expired_sstables(const table_state& table_s, const std::vector<sstables::shared_sstable>& compacting, gc_clock::time_point gc_before);

// For tests, can drop after we virtualize sstables.
mutation_reader make_scrubbing_reader(mutation_reader rd, compaction_type_options::scrub::mode scrub_mode, uint64_t& validation_errors);

}
