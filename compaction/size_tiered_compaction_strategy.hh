/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "compaction_strategy_impl.hh"
#include "sstables/shared_sstable.hh"

#include <chrono>

namespace compaction {

// Note: the SizeTieredCompactionStrategy class name is deprecated and is now
// just an alias of IncrementalCompactionStrategy (see make_compaction_strategy()),
// so size-tiered compaction is no longer a compaction strategy a table can be
// configured with, and this is no longer a compaction_strategy_impl. What
// remains is the size-tiered bucketing logic, which is still used internally,
// per time window by TWCS and for level 0 by LCS.

class size_tiered_backlog_tracker;

class size_tiered_compaction_strategy_options {
public:
    static constexpr uint64_t DEFAULT_MIN_SSTABLE_SIZE = 50L * 1024L * 1024L;
    static constexpr std::chrono::seconds DEFAULT_MIN_SSTABLE_AGE = std::chrono::hours(1);
    static constexpr double DEFAULT_BUCKET_LOW = 0.5;
    static constexpr double DEFAULT_BUCKET_HIGH = 1.5;
    static constexpr double DEFAULT_COLD_READS_TO_OMIT = 0.05;
    static constexpr auto MIN_SSTABLE_SIZE_KEY = "min_sstable_size";
    static constexpr auto MIN_SSTABLE_AGE_KEY = "min_sstable_age";
    static constexpr auto BUCKET_LOW_KEY = "bucket_low";
    static constexpr auto BUCKET_HIGH_KEY = "bucket_high";
    static constexpr auto COLD_READS_TO_OMIT_KEY = "cold_reads_to_omit";
private:
    uint64_t min_sstable_size = DEFAULT_MIN_SSTABLE_SIZE;
    std::chrono::seconds min_sstable_age = DEFAULT_MIN_SSTABLE_AGE;
    double bucket_low = DEFAULT_BUCKET_LOW;
    double bucket_high = DEFAULT_BUCKET_HIGH;
    double cold_reads_to_omit =  DEFAULT_COLD_READS_TO_OMIT;
public:
    size_tiered_compaction_strategy_options(const std::map<sstring, sstring>& options);

    size_tiered_compaction_strategy_options();
    size_tiered_compaction_strategy_options(const size_tiered_compaction_strategy_options&) = default;
    size_tiered_compaction_strategy_options(size_tiered_compaction_strategy_options&&) = default;
    size_tiered_compaction_strategy_options& operator=(const size_tiered_compaction_strategy_options&) = default;
    size_tiered_compaction_strategy_options& operator=(size_tiered_compaction_strategy_options&&) = default;

    static void validate(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options);

    friend class size_tiered_compaction_strategy;
};

class size_tiered_compaction_strategy {
    size_tiered_compaction_strategy_options _options;

    // Return a list of pair of shared_sstable and its respective size.
    static std::vector<std::pair<sstables::shared_sstable, uint64_t>> create_sstable_and_length_pairs(const std::vector<sstables::shared_sstable>& sstables);

    std::vector<std::vector<sstables::shared_sstable>> get_buckets(const std::vector<sstables::shared_sstable>& sstables) const;

    // Maybe return a bucket of sstables to compact
    std::vector<sstables::shared_sstable>
    most_interesting_bucket(std::vector<std::vector<sstables::shared_sstable>> buckets, unsigned min_threshold, unsigned max_threshold);

    static bool is_bucket_interesting(const std::vector<sstables::shared_sstable>& bucket, int min_threshold) {
        return bucket.size() >= size_t(min_threshold);
    }
public:
    size_tiered_compaction_strategy() = default;

    explicit size_tiered_compaction_strategy(const size_tiered_compaction_strategy_options& options);

    // Group files of similar size into buckets.
    static std::vector<std::vector<sstables::shared_sstable>> get_buckets(const std::vector<sstables::shared_sstable>& sstables, size_tiered_compaction_strategy_options options);

    std::vector<compaction_descriptor> get_cleanup_compaction_jobs(compaction_group_view& table_s, std::vector<sstables::shared_sstable> candidates) const;

    static int64_t estimated_pending_compactions(const std::vector<sstables::shared_sstable>& sstables,
        int min_threshold, int max_threshold, size_tiered_compaction_strategy_options options);

    // Return the most interesting bucket for a set of sstables
    static std::vector<sstables::shared_sstable>
    most_interesting_bucket(const std::vector<sstables::shared_sstable>& candidates, int min_threshold, int max_threshold,
        size_tiered_compaction_strategy_options options = {});

    compaction_descriptor get_reshaping_job(std::vector<sstables::shared_sstable> input, schema_ptr schema, reshape_config cfg) const;

    friend class ::compaction::size_tiered_backlog_tracker;
};

}
