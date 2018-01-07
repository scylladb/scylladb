/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2016-2017 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include <map>
#include <chrono>
#include <algorithm>
#include <vector>
#include <iterator>
#include "sstables.hh"
#include "compaction.hh"
#include "timestamp.hh"
#include "cql3/statements/property_definitions.hh"

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
            throw std::runtime_error(sprint("target duration %s is not available", target_duration));
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
    date_tiered_compaction_strategy_options(const std::map<sstring, sstring>& options) {
        using namespace cql3::statements;

        auto tmp_value = get_value(options, TIMESTAMP_RESOLUTION_KEY);
        auto target_unit = tmp_value ? tmp_value.value() : DEFAULT_TIMESTAMP_RESOLUTION;

        tmp_value = get_value(options, MAX_SSTABLE_AGE_KEY);
        auto fractional_days = property_definitions::to_double(MAX_SSTABLE_AGE_KEY, tmp_value, DEFAULT_MAX_SSTABLE_AGE_DAYS);
        int64_t max_sstable_age_in_hours = std::lround(fractional_days * 24);
        max_sstable_age = duration_conversor::convert(target_unit, std::chrono::hours(max_sstable_age_in_hours));

        tmp_value = get_value(options, BASE_TIME_KEY);
        auto base_time_seconds = property_definitions::to_long(BASE_TIME_KEY, tmp_value, DEFAULT_BASE_TIME_SECONDS);
        base_time = duration_conversor::convert(target_unit, std::chrono::seconds(base_time_seconds));
    }

    date_tiered_compaction_strategy_options() {
        auto max_sstable_age_in_hours = int64_t(DEFAULT_MAX_SSTABLE_AGE_DAYS * 24);
        max_sstable_age = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::hours(max_sstable_age_in_hours)).count();
        base_time = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::seconds(DEFAULT_BASE_TIME_SECONDS)).count();
    }
private:
    static std::experimental::optional<sstring> get_value(const std::map<sstring, sstring>& options, const sstring& name) {
        auto it = options.find(name);
        if (it == options.end()) {
            return std::experimental::nullopt;
        }
        return it->second;
    }

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
    get_next_sstables(column_family& cf, std::vector<sstables::shared_sstable>& uncompacting, gc_clock::time_point gc_before) {
        if (cf.get_sstables()->empty()) {
            return {};
        }

        // Find fully expired SSTables. Those will be included no matter what.
        auto expired = get_fully_expired_sstables(cf, uncompacting, gc_before);

        if (!expired.empty()) {
            auto is_expired = [&] (const sstables::shared_sstable& s) { return expired.find(s) != expired.end(); };
            uncompacting.erase(boost::remove_if(uncompacting, is_expired), uncompacting.end());
        }

        auto compaction_candidates = get_next_non_expired_sstables(cf, uncompacting, gc_before);
        if (!expired.empty()) {
            compaction_candidates.insert(compaction_candidates.end(), expired.begin(), expired.end());
        }
        return compaction_candidates;
    }

    int64_t get_estimated_tasks(column_family& cf) const {
        int base = cf.schema()->min_compaction_threshold();
        int64_t now = get_now(cf);
        std::vector<sstables::shared_sstable> sstables;
        int64_t n = 0;

        sstables.reserve(cf.sstables_count());
        for (auto& entry : *cf.get_sstables()) {
            sstables.push_back(entry);
        }
        auto candidates = filter_old_sstables(sstables, _options.max_sstable_age, now);
        auto buckets = get_buckets(create_sst_and_min_timestamp_pairs(candidates), _options.base_time, base, now);

        for (auto& bucket : buckets) {
            if (bucket.size() >= size_t(cf.schema()->min_compaction_threshold())) {
                n += std::ceil(double(bucket.size()) / cf.schema()->max_compaction_threshold());
            }
        }
        return n;
    }
private:
    std::vector<sstables::shared_sstable>
    get_next_non_expired_sstables(column_family& cf, std::vector<sstables::shared_sstable>& non_expiring_sstables, gc_clock::time_point gc_before) {
        int base = cf.schema()->min_compaction_threshold();
        int64_t now = get_now(cf);
        auto most_interesting = get_compaction_candidates(cf, non_expiring_sstables, now, base);

        return most_interesting;

        // FIXME: implement functionality below that will look for a single sstable with worth dropping tombstone,
        // iff strategy didn't find anything to compact. So it's not essential.
#if 0
        // if there is no sstable to compact in standard way, try compacting single sstable whose droppable tombstone
        // ratio is greater than threshold.

        List<SSTableReader> sstablesWithTombstones = Lists.newArrayList();
        for (SSTableReader sstable : nonExpiringSSTables)
        {
            if (worthDroppingTombstones(sstable, gcBefore))
                sstablesWithTombstones.add(sstable);
        }
        if (sstablesWithTombstones.isEmpty())
            return Collections.emptyList();

        return Collections.singletonList(Collections.min(sstablesWithTombstones, new SSTableReader.SizeComparator()));
#endif
    }

    std::vector<sstables::shared_sstable>
    get_compaction_candidates(column_family& cf, std::vector<sstables::shared_sstable> candidate_sstables, int64_t now, int base) {
        int min_threshold = cf.schema()->min_compaction_threshold();
        int max_threshold = cf.schema()->max_compaction_threshold();
        auto candidates = filter_old_sstables(candidate_sstables, _options.max_sstable_age, now);

        auto buckets = get_buckets(create_sst_and_min_timestamp_pairs(candidates), _options.base_time, base, now);

        return newest_bucket(buckets, min_threshold, max_threshold, now, _options.base_time);
    }

    /**
     * Gets the timestamp that DateTieredCompactionStrategy considers to be the "current time".
     * @return the maximum timestamp across all SSTables.
     */
    static int64_t get_now(column_family& cf) {
        int64_t max_timestamp = 0;
        for (auto& sst : *cf.get_sstables()) {
            int64_t candidate = sst->get_stats_metadata().max_timestamp;
            max_timestamp = candidate > max_timestamp ? candidate : max_timestamp;
        }
        return max_timestamp;
    }

    /**
     * Removes all sstables with max timestamp older than maxSSTableAge.
     * @return a list of sstables with the oldest sstables excluded
     */
    static std::vector<sstables::shared_sstable>
    filter_old_sstables(std::vector<sstables::shared_sstable> sstables, api::timestamp_type max_sstable_age, int64_t now) {
        if (max_sstable_age == 0) {
            return sstables;
        }
        int64_t cutoff = now - max_sstable_age;

        sstables.erase(std::remove_if(sstables.begin(), sstables.end(), [cutoff] (auto& sst) {
            return sst->get_stats_metadata().max_timestamp < cutoff;
        }), sstables.end());

        return sstables;
    }

    /**
     *
     * @param sstables
     * @return
     */
    static std::vector<std::pair<sstables::shared_sstable,int64_t>>
    create_sst_and_min_timestamp_pairs(const std::vector<sstables::shared_sstable>& sstables) {
        std::vector<std::pair<sstables::shared_sstable,int64_t>> sstable_min_timestamp_pairs;
        sstable_min_timestamp_pairs.reserve(sstables.size());
        for (auto& sst : sstables) {
            sstable_min_timestamp_pairs.emplace_back(sst, sst->get_stats_metadata().min_timestamp);
        }
        return sstable_min_timestamp_pairs;
    }

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
    compaction_backlog_tracker _backlog_tracker;
public:
    date_tiered_compaction_strategy(const std::map<sstring, sstring>& options);
    virtual compaction_descriptor get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) override {
        auto gc_before = gc_clock::now() - cfs.schema()->gc_grace_seconds();
        auto sstables = _manifest.get_next_sstables(cfs, candidates, gc_before);

        if (!sstables.empty()) {
            date_tiered_manifest::logger.debug("datetiered: Compacting {} out of {} sstables", sstables.size(), candidates.size());
            return sstables::compaction_descriptor(std::move(sstables));
        }

        // filter out sstables which droppable tombstone ratio isn't greater than the defined threshold.
        auto e = boost::range::remove_if(candidates, [this, &gc_before] (const sstables::shared_sstable& sst) -> bool {
            return !worth_dropping_tombstones(sst, gc_before);
        });
        candidates.erase(e, candidates.end());
        if (candidates.empty()) {
            return sstables::compaction_descriptor();
        }
        // find oldest sstable which is worth dropping tombstones because they are more unlikely to
        // shadow data from other sstables, and it also tends to be relatively big.
        auto it = std::min_element(candidates.begin(), candidates.end(), [] (auto& i, auto& j) {
            return i->get_stats_metadata().min_timestamp < j->get_stats_metadata().min_timestamp;
        });
        return sstables::compaction_descriptor({ *it });
    }

    virtual int64_t estimated_pending_compactions(column_family& cf) const override {
        return _manifest.get_estimated_tasks(cf);
    }

    virtual compaction_strategy_type type() const {
        return compaction_strategy_type::date_tiered;
    }

    virtual compaction_backlog_tracker& get_backlog_tracker() override {
        return _backlog_tracker;
    }
};

}
