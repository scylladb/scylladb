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
#include "compaction_weight_registration.hh"
#include "service/priority_manager.hh"
#include "utils/UUID.hh"
#include "table_state.hh"
#include <seastar/core/thread.hh>
#include <seastar/core/abort_source.hh>

using namespace compaction;

namespace sstables {

bool is_eligible_for_compaction(const sstables::shared_sstable& sst) noexcept;

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

// Return the name of the compaction type
// as used over the REST api, e.g. "COMPACTION" or "CLEANUP".
sstring compaction_name(compaction_type type);

// Reverse map the name of the compaction type
// as used over the REST api, e.g. "COMPACTION" or "CLEANUP",
// to the compaction_type enum code.
compaction_type to_compaction_type(sstring type_name);

// Return a string respresenting the compaction type
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

    compaction_stats& operator+=(const compaction_stats& r) {
        ended_at = std::max(ended_at, r.ended_at);
        start_size += r.start_size;
        end_size += r.end_size;
        validation_errors += r.validation_errors;
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

// Compact a list of N sstables into M sstables.
// Returns info about the finished compaction, which includes vector to new sstables.
//
// compaction_descriptor is responsible for specifying the type of compaction, and influencing
// compaction behavior through its available member fields.
future<compaction_result> compact_sstables(sstables::compaction_descriptor descriptor, compaction_data& cdata, table_state& table_s);

// Return list of expired sstables for column family cf.
// A sstable is fully expired *iff* its max_local_deletion_time precedes gc_before and its
// max timestamp is lower than any other relevant sstable.
// In simpler words, a sstable is fully expired if all of its live cells with TTL is expired
// and possibly doesn't contain any tombstone that covers cells in other sstables.
std::unordered_set<sstables::shared_sstable>
get_fully_expired_sstables(const table_state& table_s, const std::vector<sstables::shared_sstable>& compacting, gc_clock::time_point gc_before);

// For tests, can drop after we virtualize sstables.
flat_mutation_reader_v2 make_scrubbing_reader(flat_mutation_reader_v2 rd, compaction_type_options::scrub::mode scrub_mode, uint64_t& validation_errors);

}
