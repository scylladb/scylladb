/*
 * Copyright (C) 2019 ScyllaDB
 *
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "compaction_strategy_impl.hh"
#include <boost/range/algorithm.hpp>

class incremental_backlog_tracker;

namespace sstables {

class incremental_compaction_strategy_options {
public:
    static constexpr uint64_t DEFAULT_MIN_SSTABLE_SIZE = 50L * 1024L * 1024L;
    static constexpr double DEFAULT_BUCKET_LOW = 0.5;
    static constexpr double DEFAULT_BUCKET_HIGH = 1.5;

    static constexpr auto MIN_SSTABLE_SIZE_KEY = "min_sstable_size";
    static constexpr auto BUCKET_LOW_KEY = "bucket_low";
    static constexpr auto BUCKET_HIGH_KEY = "bucket_high";
private:
    uint64_t min_sstable_size = DEFAULT_MIN_SSTABLE_SIZE;
    double bucket_low = DEFAULT_BUCKET_LOW;
    double bucket_high = DEFAULT_BUCKET_HIGH;
public:
    incremental_compaction_strategy_options(const std::map<sstring, sstring>& options);

    incremental_compaction_strategy_options() {
        min_sstable_size = DEFAULT_MIN_SSTABLE_SIZE;
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

    static std::vector<std::vector<sstables::frozen_sstable_run>> get_buckets(const std::vector<sstables::frozen_sstable_run>& runs, const incremental_compaction_strategy_options& options);

    std::vector<std::vector<sstables::frozen_sstable_run>> get_buckets(const std::vector<sstables::frozen_sstable_run>& runs) const {
        return get_buckets(runs, _options);
    }

    std::vector<sstables::frozen_sstable_run>
    most_interesting_bucket(std::vector<std::vector<sstables::frozen_sstable_run>> buckets, size_t min_threshold, size_t max_threshold);

    uint64_t avg_size(std::vector<sstables::frozen_sstable_run>& runs) const;

    static bool is_bucket_interesting(const std::vector<sstables::frozen_sstable_run>& bucket, size_t min_threshold);

    bool is_any_bucket_interesting(const std::vector<std::vector<sstables::frozen_sstable_run>>& buckets, size_t min_threshold) const;

    compaction_descriptor find_garbage_collection_job(const table_state& t, std::vector<size_bucket_t>& buckets);

    static std::vector<shared_sstable> runs_to_sstables(std::vector<frozen_sstable_run> runs);
    static std::vector<frozen_sstable_run> sstables_to_runs(std::vector<shared_sstable> sstables);
    static void sort_run_bucket_by_first_key(size_bucket_t& bucket, size_t max_elements, const schema_ptr& schema);
public:
    incremental_compaction_strategy() = default;

    incremental_compaction_strategy(const std::map<sstring, sstring>& options);

    static void validate_options(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options);

    virtual compaction_descriptor get_sstables_for_compaction(table_state& t, strategy_control& control) override;

    virtual std::vector<compaction_descriptor> get_cleanup_compaction_jobs(table_state& t, std::vector<shared_sstable> candidates) const override;

    virtual compaction_descriptor get_major_compaction_job(table_state& t, std::vector<sstables::shared_sstable> candidates) override;

    virtual int64_t estimated_pending_compactions(table_state& t) const override;

    virtual compaction_strategy_type type() const override {
        return compaction_strategy_type::incremental;
    }

    virtual std::unique_ptr<compaction_backlog_tracker::impl> make_backlog_tracker() const override;

    virtual compaction_descriptor get_reshaping_job(std::vector<shared_sstable> input, schema_ptr schema, reshape_config cfg) const override;

    virtual std::unique_ptr<sstable_set_impl> make_sstable_set(schema_ptr schema) const override;

    friend class ::incremental_backlog_tracker;
};

}
