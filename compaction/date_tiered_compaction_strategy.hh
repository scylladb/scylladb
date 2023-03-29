/*
 * Copyright (C) 2016-present-2017 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <map>
#include <chrono>
#include <algorithm>
#include <vector>
#include <iterator>
#include "sstables/sstables.hh"
#include "compaction.hh"
#include "timestamp.hh"
#include "cql3/statements/property_definitions.hh"
#include "compaction_strategy_impl.hh"

static constexpr double DEFAULT_MAX_SSTABLE_AGE_DAYS = 365;
static constexpr int64_t DEFAULT_BASE_TIME_SECONDS = 60;

struct duration_conversor {
    // Convert given duration to TargetDuration and return value as timestamp.
    template <typename TargetDuration, typename SourceDuration>
    static api::timestamp_type convert(SourceDuration d) {
        return std::chrono::duration_cast<TargetDuration>(d).count();
    }

    // Convert given duration to duration that is represented by the string
    // target_duration, and return value as timestamp.
    template <typename SourceDuration>
    static api::timestamp_type convert(const sstring& target_duration, SourceDuration d) {
        if (target_duration == "HOURS") {
            return convert<std::chrono::hours>(d);
        } else if (target_duration == "MICROSECONDS") {
            return convert<std::chrono::microseconds>(d);
        } else if (target_duration == "MILLISECONDS") {
            return convert<std::chrono::milliseconds>(d);
        } else if (target_duration == "MINUTES") {
            return convert<std::chrono::minutes>(d);
        } else if (target_duration == "NANOSECONDS") {
            return convert<std::chrono::nanoseconds>(d);
        } else if (target_duration == "SECONDS") {
            return convert<std::chrono::seconds>(d);
        } else {
            throw std::runtime_error(format("target duration {} is not available", target_duration));
        }
    }
};

class date_tiered_compaction_strategy_options {
    const sstring DEFAULT_TIMESTAMP_RESOLUTION = "MICROSECONDS";
    const sstring TIMESTAMP_RESOLUTION_KEY = "timestamp_resolution";
    const sstring MAX_SSTABLE_AGE_KEY = "max_sstable_age_days";
    const sstring BASE_TIME_KEY = "base_time_seconds";

    api::timestamp_type max_sstable_age;
    api::timestamp_type base_time;
public:
    date_tiered_compaction_strategy_options(const std::map<sstring, sstring>& options);

    date_tiered_compaction_strategy_options();
private:

    friend class date_tiered_manifest;
};

class date_tiered_manifest {
    date_tiered_compaction_strategy_options _options;
public:
    static logging::logger logger;

    date_tiered_manifest() = delete;

    date_tiered_manifest(const std::map<sstring, sstring>& options)
        : _options(options) {}

    std::vector<sstables::shared_sstable>
    get_next_sstables(table_state& table_s, std::vector<sstables::shared_sstable>& uncompacting, gc_clock::time_point compaction_time);

    int64_t get_estimated_tasks(table_state& table_s) const;
private:
    std::vector<sstables::shared_sstable>
    get_next_non_expired_sstables(table_state& table_s, std::vector<sstables::shared_sstable>& non_expiring_sstables, gc_clock::time_point compaction_time);

    std::vector<sstables::shared_sstable>
    get_compaction_candidates(table_state& table_s, std::vector<sstables::shared_sstable> candidate_sstables, int64_t now, int base);

    /**
     * Gets the timestamp that DateTieredCompactionStrategy considers to be the "current time".
     * @return the maximum timestamp across all SSTables.
     */
    static int64_t get_now(lw_shared_ptr<const sstables::sstable_list> shared_set);

    /**
     * Removes all sstables with max timestamp older than maxSSTableAge.
     * @return a list of sstables with the oldest sstables excluded
     */
    static std::vector<sstables::shared_sstable>
    filter_old_sstables(std::vector<sstables::shared_sstable> sstables, api::timestamp_type max_sstable_age, int64_t now);

    /**
     *
     * @param sstables
     * @return
     */
    static std::vector<std::pair<sstables::shared_sstable,int64_t>>
    create_sst_and_min_timestamp_pairs(const std::vector<sstables::shared_sstable>& sstables);

    /**
     * A target time span used for bucketing SSTables based on timestamps.
     */
    struct target {
        // How big a range of timestamps fit inside the target.
        int64_t size;
        // A timestamp t hits the target iff t / size == divPosition.
        int64_t div_position;

        target() = delete;
        target(int64_t size, int64_t div_position) : size(size), div_position(div_position) {}

        /**
         * Compares the target to a timestamp.
         * @param timestamp the timestamp to compare.
         * @return a negative integer, zero, or a positive integer as the target lies before, covering, or after than the timestamp.
         */
        int compare_to_timestamp(int64_t timestamp) {
            auto ts1 = div_position;
            auto ts2 = timestamp / size;
            return (ts1 > ts2 ? 1 : (ts1 == ts2 ? 0 : -1));
        }

        /**
         * Tells if the timestamp hits the target.
         * @param timestamp the timestamp to test.
         * @return <code>true</code> iff timestamp / size == divPosition.
         */
        bool on_target(int64_t timestamp) {
            return compare_to_timestamp(timestamp) == 0;
        }

        /**
         * Gets the next target, which represents an earlier time span.
         * @param base The number of contiguous targets that will have the same size. Targets following those will be <code>base</code> times as big.
         * @return
         */
        target next_target(int base)
        {
            if (div_position % base > 0) {
                return target(size, div_position - 1);
            } else {
                return target(size * base, div_position / base - 1);
            }
        }
    };


    /**
     * Group files with similar min timestamp into buckets. Files with recent min timestamps are grouped together into
     * buckets designated to short timespans while files with older timestamps are grouped into buckets representing
     * longer timespans.
     * @param files pairs consisting of a file and its min timestamp
     * @param timeUnit
     * @param base
     * @param now
     * @return a list of buckets of files. The list is ordered such that the files with newest timestamps come first.
     *         Each bucket is also a list of files ordered from newest to oldest.
     */
    std::vector<std::vector<sstables::shared_sstable>>
    get_buckets(std::vector<std::pair<sstables::shared_sstable,int64_t>>&& files, api::timestamp_type time_unit, int base, int64_t now) const {
        // Sort files by age. Newest first.
        std::sort(files.begin(), files.end(), [] (auto& i, auto& j) {
            return i.second > j.second;
        });

        std::vector<std::vector<sstables::shared_sstable>> buckets;
        auto target = get_initial_target(now, time_unit);
        auto it = files.begin();

        while (it != files.end()) {
            bool finish = false;
            while (!target.on_target(it->second)) {
                // If the file is too new for the target, skip it.
                if (target.compare_to_timestamp(it->second) < 0) {
                    it++;
                    if (it == files.end()) {
                        finish = true;
                        break;
                    }
                } else { // If the file is too old for the target, switch targets.
                    target = target.next_target(base);
                }
            }
            if (finish) {
                break;
            }

            std::vector<sstables::shared_sstable> bucket;
            while (target.on_target(it->second)) {
                bucket.push_back(it->first);
                it++;
                if (it == files.end()) {
                    break;
                }
            }
            buckets.push_back(bucket);
        }

        return buckets;
    }

    target get_initial_target(uint64_t now, int64_t time_unit) const {
        return target(time_unit, now / time_unit);
    }

    /**
     * @param buckets list of buckets, sorted from newest to oldest, from which to return the newest bucket within thresholds.
     * @param minThreshold minimum number of sstables in a bucket to qualify.
     * @param maxThreshold maximum number of sstables to compact at once (the returned bucket will be trimmed down to this).
     * @return a bucket (list) of sstables to compact.
     */
    std::vector<sstables::shared_sstable>
    newest_bucket(std::vector<std::vector<sstables::shared_sstable>>& buckets, int min_threshold, int max_threshold,
            int64_t now, api::timestamp_type base_time) {

        // If the "incoming window" has at least minThreshold SSTables, choose that one.
        // For any other bucket, at least 2 SSTables is enough.
        // In any case, limit to maxThreshold SSTables.
        target incoming_window = get_initial_target(now, base_time);
        for (auto& bucket : buckets) {
            auto min_timestamp = bucket.front()->get_stats_metadata().min_timestamp;
            if (bucket.size() >= size_t(min_threshold) ||
                    (bucket.size() >= 2 && !incoming_window.on_target(min_timestamp))) {
                trim_to_threshold(bucket, max_threshold);
                return bucket;
            }
        }
        return {};
    }


    /**
     * @param bucket list of sstables, ordered from newest to oldest by getMinTimestamp().
     * @param maxThreshold maximum number of sstables in a single compaction task.
     * @return A bucket trimmed to the <code>maxThreshold</code> newest sstables.
     */
    static void trim_to_threshold(std::vector<sstables::shared_sstable>& bucket, int max_threshold) {
        // Trim the oldest sstables off the end to meet the maxThreshold
        bucket.resize(std::min(bucket.size(), size_t(max_threshold)));
    }
};

namespace sstables {

class date_tiered_compaction_strategy : public compaction_strategy_impl {
    date_tiered_manifest _manifest;
public:
    date_tiered_compaction_strategy(const std::map<sstring, sstring>& options);
    virtual compaction_descriptor get_sstables_for_compaction(table_state& table_s, strategy_control& control, std::vector<sstables::shared_sstable> candidates) override;

    virtual int64_t estimated_pending_compactions(table_state& table_s) const override {
        return _manifest.get_estimated_tasks(table_s);
    }

    virtual compaction_strategy_type type() const override {
        return compaction_strategy_type::date_tiered;
    }

    virtual std::unique_ptr<compaction_backlog_tracker::impl> make_backlog_tracker() const override;
};

}
