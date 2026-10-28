/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "compaction_strategy_impl.hh"

#include <chrono>

namespace compaction {

class incremental_backlog_tracker;

class incremental_compaction_strategy_options {
public:
    static constexpr uint64_t DEFAULT_MIN_SSTABLE_SIZE = 50L * 1024L * 1024L;
    static constexpr std::chrono::seconds DEFAULT_MIN_SSTABLE_AGE = std::chrono::hours(1);
    static constexpr double DEFAULT_BUCKET_LOW = 0.7071;
    static constexpr double DEFAULT_BUCKET_HIGH = 1.4142;

    static constexpr auto MIN_SSTABLE_SIZE_KEY = "min_sstable_size";
    static constexpr auto MIN_SSTABLE_AGE_KEY = "min_sstable_age";
    static constexpr auto BUCKET_LOW_KEY = "bucket_low";
    static constexpr auto BUCKET_HIGH_KEY = "bucket_high";
private:
    uint64_t min_sstable_size = DEFAULT_MIN_SSTABLE_SIZE;
    std::chrono::seconds min_sstable_age = DEFAULT_MIN_SSTABLE_AGE;
    double bucket_low = DEFAULT_BUCKET_LOW;
    double bucket_high = DEFAULT_BUCKET_HIGH;
public:
    incremental_compaction_strategy_options(const std::map<sstring, sstring>& options);

    incremental_compaction_strategy_options() {
        min_sstable_size = DEFAULT_MIN_SSTABLE_SIZE;
        min_sstable_age = DEFAULT_MIN_SSTABLE_AGE;
        bucket_low = DEFAULT_BUCKET_LOW;
        bucket_high = DEFAULT_BUCKET_HIGH;
    }

    static void validate(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options);

    friend class incremental_compaction_strategy;
};

using sstable_run_and_length = std::pair<sstables::frozen_sstable_run, uint64_t>;
using sstable_run_bucket_and_length = std::pair<std::vector<sstables::frozen_sstable_run>, uint64_t>;

class incremental_compaction_strategy : public compaction_strategy_impl {
    incremental_compaction_strategy_options _options;

    using size_bucket_t = std::vector<sstables::frozen_sstable_run>;
public:
    static constexpr int32_t DEFAULT_MAX_FRAGMENT_SIZE_IN_MB = 1000;
    static constexpr auto FRAGMENT_SIZE_OPTION = "sstable_size_in_mb";
    static constexpr auto SPACE_AMPLIFICATION_GOAL_OPTION = "space_amplification_goal";
private:
    size_t _fragment_size = DEFAULT_MAX_FRAGMENT_SIZE_IN_MB*1024*1024;
    std::optional<double> _space_amplification_goal;
    static std::vector<sstable_run_and_length> create_run_and_length_pairs(const std::vector<sstables::frozen_sstable_run>& runs);

    std::vector<std::vector<sstables::frozen_sstable_run>> get_buckets(const std::vector<sstables::frozen_sstable_run>& runs) const {
        return get_buckets(runs, _options);
    }

    static uint64_t avg_size(std::vector<sstables::frozen_sstable_run>& runs);

    static bool is_bucket_interesting(const std::vector<sstables::frozen_sstable_run>& bucket, size_t min_threshold);

    bool is_any_bucket_interesting(const std::vector<std::vector<sstables::frozen_sstable_run>>& buckets, size_t min_threshold) const;

    compaction_descriptor find_garbage_collection_job(const compaction_group_view& t, std::vector<size_bucket_t>& buckets);

    static void sort_run_bucket_by_first_key(size_bucket_t& bucket, size_t max_elements, const schema_ptr& schema);
public:
    incremental_compaction_strategy() = default;

    incremental_compaction_strategy(const std::map<sstring, sstring>& options);

    // For strategies that apply ICS to a subset of their sstables, e.g. a time window, and
    // use it to reshape or clean up that subset. Tombstone compaction options are left at
    // their defaults, as these jobs do not consult them.
    incremental_compaction_strategy(incremental_compaction_strategy_options options, uint64_t fragment_size);

    // The fragment size, in bytes, the given options ask for, and its validation, for the
    // strategies that write ICS runs and accept the FRAGMENT_SIZE_OPTION.
    static uint64_t parse_fragment_size(const std::map<sstring, sstring>& options);
    static void validate_fragment_size_option(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options);

    static void validate_options(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options);

    // Group runs of similar size into buckets.
    static std::vector<std::vector<sstables::frozen_sstable_run>> get_buckets(const std::vector<sstables::frozen_sstable_run>& runs, const incremental_compaction_strategy_options& options);

    // Of the given buckets, return the one with the most runs among those holding at least
    // min_threshold of them, trimmed to max_threshold runs. Empty if none qualifies.
    static std::vector<sstables::frozen_sstable_run>
    most_interesting_bucket(std::vector<std::vector<sstables::frozen_sstable_run>> buckets, size_t min_threshold, size_t max_threshold);

    // Bucket the given runs by size and return the most interesting bucket, as above.
    // Used by strategies that apply size-tiering to a subset of their sstables, e.g. a
    // time window. Unlike get_sstables_for_compaction(), min_threshold is always honored.
    static std::vector<sstables::frozen_sstable_run>
    most_interesting_bucket(const std::vector<sstables::frozen_sstable_run>& runs, size_t min_threshold, size_t max_threshold,
            const incremental_compaction_strategy_options& options);

    // The number of compactions needed to bring the given runs down to below min_threshold per bucket.
    static int64_t estimated_pending_compactions(const std::vector<sstables::frozen_sstable_run>& runs, size_t min_threshold, size_t max_threshold,
            const incremental_compaction_strategy_options& options);

    static std::vector<sstables::shared_sstable> runs_to_sstables(std::vector<sstables::frozen_sstable_run> runs);
    static std::vector<sstables::frozen_sstable_run> sstables_to_runs(std::vector<sstables::shared_sstable> sstables);

    virtual future<compaction_descriptor> get_sstables_for_compaction(compaction_group_view& t, strategy_control& control) override;

    virtual std::vector<compaction_descriptor> get_cleanup_compaction_jobs(compaction_group_view& t, std::vector<sstables::shared_sstable> candidates) const override;

    virtual compaction_descriptor get_major_compaction_job(compaction_group_view& t, std::vector<sstables::shared_sstable> candidates) override;

    virtual future<int64_t> estimated_pending_compactions(compaction_group_view& t) const override;

    virtual compaction_strategy_type type() const override {
        return compaction_strategy_type::incremental;
    }

    virtual std::unique_ptr<compaction_backlog_tracker::impl> make_backlog_tracker() const override;

    virtual compaction_descriptor get_reshaping_job(std::vector<sstables::shared_sstable> input, schema_ptr schema, reshape_config cfg) const override;

    virtual std::unique_ptr<sstables::sstable_set_impl> make_sstable_set(const compaction_group_view& ts) const override;

    friend class compaction::incremental_backlog_tracker;
};

// Returns true whether any tiny sstable run, i.e. a run smaller than option.min_sstable_size, was written within the last min_sstable_age.
template <std::ranges::range Range>
requires std::convertible_to<std::ranges::range_value_t<Range>, sstables::shared_sstable> || std::convertible_to<std::ranges::range_value_t<Range>, sstables::frozen_sstable_run>
bool tiny_sstables_written_recently(uint64_t min_sstable_size, db_clock::duration min_sstable_age, const Range& sstable_runs) {
    return std::ranges::any_of(sstable_runs, [min_sstable_size, min_sstable_age, now = db_clock::now()] (const std::ranges::range_value_t<Range>& r) {
        return r->data_size() < min_sstable_size && r->data_file_write_time() > (now - min_sstable_age);
    });
}

}
