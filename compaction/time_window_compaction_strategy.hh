/*
 * Copyright (C) 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "compaction_strategy_impl.hh"
#include "size_tiered_compaction_strategy.hh"
#include "timestamp.hh"
#include "sstables/shared_sstable.hh"

namespace sstables {

extern logging::logger clogger;

using namespace std::chrono_literals;

class time_window_backlog_tracker;

class time_window_compaction_strategy_options {
public:
    static constexpr std::chrono::seconds DEFAULT_COMPACTION_WINDOW_UNIT = 86400s;
    static constexpr int DEFAULT_COMPACTION_WINDOW_SIZE = 1;
    static constexpr std::chrono::seconds DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS() { return 600s; }

    static constexpr auto TIMESTAMP_RESOLUTION_KEY = "timestamp_resolution";
    static constexpr auto COMPACTION_WINDOW_UNIT_KEY = "compaction_window_unit";
    static constexpr auto COMPACTION_WINDOW_SIZE_KEY = "compaction_window_size";
    static constexpr auto EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY = "expired_sstable_check_frequency_seconds";

    static const std::unordered_map<sstring, std::chrono::seconds> valid_window_units;

    enum class timestamp_resolutions {
        microsecond,
        millisecond,
    };
    static const std::unordered_map<sstring, timestamp_resolutions> valid_timestamp_resolutions;
private:
    std::chrono::seconds sstable_window_size = DEFAULT_COMPACTION_WINDOW_UNIT * DEFAULT_COMPACTION_WINDOW_SIZE;
    db_clock::duration expired_sstable_check_frequency = DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS();
    timestamp_resolutions timestamp_resolution = timestamp_resolutions::microsecond;
    bool enable_optimized_twcs_queries{true};
public:
    time_window_compaction_strategy_options(const time_window_compaction_strategy_options&);
    time_window_compaction_strategy_options(time_window_compaction_strategy_options&&);
    time_window_compaction_strategy_options(const std::map<sstring, sstring>& options);

    static void validate(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options);
public:
    std::chrono::seconds get_sstable_window_size() const { return sstable_window_size; }

    friend class time_window_compaction_strategy;
    friend class time_window_backlog_tracker;
};

struct time_window_compaction_strategy_state {
    int64_t estimated_remaining_tasks = 0;
    db_clock::time_point last_expired_check;
    // As api::timestamp_type is an int64_t, a primitive type, it must be initialized here.
    api::timestamp_type highest_window_seen = 0;
    // Keep track of all recent active windows that still need to be compacted into a single SSTable
    std::unordered_set<api::timestamp_type> recent_active_windows;
};

class time_window_compaction_strategy : public compaction_strategy_impl {
    time_window_compaction_strategy_options _options;
    size_tiered_compaction_strategy_options _stcs_options;
public:
    // The maximum amount of buckets we segregate data into when writing into sstables.
    // To prevent an explosion in the number of sstables we cap it.
    // Better co-locate some windows into the same sstables than OOM.
    static constexpr uint64_t max_data_segregation_window_count = 100;
    static constexpr float reshape_target_space_overhead = 0.1f;

    using bucket_t = std::vector<shared_sstable>;
    enum class bucket_compaction_mode { none, size_tiered, major };
public:
    time_window_compaction_strategy(const std::map<sstring, sstring>& options);
    virtual compaction_descriptor get_sstables_for_compaction(table_state& table_s, strategy_control& control) override;

    virtual std::vector<compaction_descriptor> get_cleanup_compaction_jobs(table_state& table_s, std::vector<shared_sstable> candidates) const override;

    static void validate_options(const std::map<sstring, sstring>& options, std::map<sstring, sstring>& unchecked_options);
private:
    time_window_compaction_strategy_state& get_state(table_state& table_s) const;

    static api::timestamp_type
    to_timestamp_type(time_window_compaction_strategy_options::timestamp_resolutions resolution, int64_t timestamp_from_sstable) {
        switch (resolution) {
        case time_window_compaction_strategy_options::timestamp_resolutions::microsecond:
            return api::timestamp_type(timestamp_from_sstable);
        case time_window_compaction_strategy_options::timestamp_resolutions::millisecond:
            return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::milliseconds(timestamp_from_sstable)).count();
        default:
            throw std::runtime_error("Timestamp resolution invalid for TWCS");
        };
    }

    // Returns true if bucket is the last, most active one.
    bool is_last_active_bucket(api::timestamp_type bucket_key, api::timestamp_type now) const {
        return bucket_key >= now;
    }

    // Returns which compaction type should be performed on a given window bucket.
    bucket_compaction_mode
    compaction_mode(const time_window_compaction_strategy_state&, const bucket_t& bucket, api::timestamp_type bucket_key, api::timestamp_type now, size_t min_threshold) const;

    std::vector<shared_sstable>
    get_next_non_expired_sstables(table_state& table_s, strategy_control& control, std::vector<shared_sstable> non_expiring_sstables, gc_clock::time_point compaction_time);

    std::vector<shared_sstable> get_compaction_candidates(table_state& table_s, strategy_control& control, std::vector<shared_sstable> candidate_sstables);
public:
    // Find the lowest timestamp for window of given size
    static api::timestamp_type
    get_window_lower_bound(std::chrono::seconds sstable_window_size, api::timestamp_type timestamp);

    // Group files with similar max timestamp into buckets.
    // @return A pair, where the left element is the bucket representation (map of timestamp to sstablereader),
    // and the right is the highest timestamp seen
    static std::pair<std::map<api::timestamp_type, std::vector<shared_sstable>>, api::timestamp_type>
    get_buckets(std::vector<shared_sstable> files, const time_window_compaction_strategy_options& options);

    std::vector<shared_sstable>
    newest_bucket(table_state& table_s, strategy_control& control, std::map<api::timestamp_type, std::vector<shared_sstable>> buckets,
        int min_threshold, int max_threshold, api::timestamp_type now);

    static std::vector<shared_sstable>
    trim_to_threshold(std::vector<shared_sstable> bucket, int max_threshold);

    static int64_t
    get_window_for(const time_window_compaction_strategy_options& options, api::timestamp_type ts) {
        return get_window_lower_bound(options.sstable_window_size, to_timestamp_type(options.timestamp_resolution, ts));
    }

    static api::timestamp_type
    get_window_size(const time_window_compaction_strategy_options& options) {
        return api::timestamp_type(std::chrono::duration_cast<std::chrono::microseconds>(options.get_sstable_window_size()).count());
    }
private:
    void update_estimated_compaction_by_tasks(time_window_compaction_strategy_state& state,
        std::map<api::timestamp_type, std::vector<shared_sstable>>& tasks,
        int min_threshold, int max_threshold);

    friend class time_window_backlog_tracker;
public:
    virtual int64_t estimated_pending_compactions(table_state& table_s) const override {
        return get_state(table_s).estimated_remaining_tasks;
    }

    virtual compaction_strategy_type type() const override {
        return compaction_strategy_type::time_window;
    }

    virtual std::unique_ptr<sstable_set_impl> make_sstable_set(schema_ptr schema) const override;

    virtual std::unique_ptr<compaction_backlog_tracker::impl> make_backlog_tracker() const override;

    virtual uint64_t adjust_partition_estimate(const mutation_source_metadata& ms_meta, uint64_t partition_estimate, schema_ptr s) const override;

    virtual reader_consumer_v2 make_interposer_consumer(const mutation_source_metadata& ms_meta, reader_consumer_v2 end_consumer) const override;

    virtual bool use_interposer_consumer() const override {
        return true;
    }

    virtual compaction_descriptor get_reshaping_job(std::vector<shared_sstable> input, schema_ptr schema, reshape_config cfg) const override;
};

}
