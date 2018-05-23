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
 * Copyright (C) 2017 ScyllaDB
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

#include "compaction_strategy_impl.hh"
#include "compaction.hh"
#include "size_tiered_compaction_strategy.hh"
#include "timestamp.hh"
#include "exceptions/exceptions.hh"
#include <boost/range/algorithm/partial_sort.hpp>
#include <boost/range/adaptors.hpp>

namespace sstables {

extern logging::logger clogger;

using namespace std::chrono_literals;

class time_window_backlog_tracker;

class time_window_compaction_strategy_options {
public:
    static constexpr std::chrono::seconds DEFAULT_COMPACTION_WINDOW_UNIT(int window_size) { return window_size * 86400s; }
    static constexpr int DEFAULT_COMPACTION_WINDOW_SIZE = 1;
    static constexpr std::chrono::seconds DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS() { return 600s; }

    static constexpr auto TIMESTAMP_RESOLUTION_KEY = "timestamp_resolution";
    static constexpr auto COMPACTION_WINDOW_UNIT_KEY = "compaction_window_unit";
    static constexpr auto COMPACTION_WINDOW_SIZE_KEY = "compaction_window_size";
    static constexpr auto EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY = "expired_sstable_check_frequency_seconds";
private:
    const std::unordered_map<sstring, std::chrono::seconds> valid_window_units = { { "MINUTES", 60s }, { "HOURS", 3600s }, { "DAYS", 86400s } };

    enum class timestamp_resolutions {
        microsecond,
        millisecond,
    };
    const std::unordered_map<sstring, timestamp_resolutions> valid_timestamp_resolutions = {
        { "MICROSECONDS", timestamp_resolutions::microsecond },
        { "MILLISECONDS", timestamp_resolutions::millisecond },
    };

    std::chrono::seconds sstable_window_size = DEFAULT_COMPACTION_WINDOW_UNIT(DEFAULT_COMPACTION_WINDOW_SIZE);
    db_clock::duration expired_sstable_check_frequency = DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS();
    timestamp_resolutions timestamp_resolution = timestamp_resolutions::microsecond;
public:
    time_window_compaction_strategy_options(const std::map<sstring, sstring>& options) {
        std::chrono::seconds window_unit;

        auto it = options.find(COMPACTION_WINDOW_UNIT_KEY);
        if (it != options.end()) {
            auto valid_window_units_it = valid_window_units.find(it->second);
            if (valid_window_units_it == valid_window_units.end()) {
                throw exceptions::syntax_exception(sstring("Invalid window unit ") + it->second + " for " + COMPACTION_WINDOW_UNIT_KEY);
            }
            window_unit = valid_window_units_it->second;
        }

        it = options.find(COMPACTION_WINDOW_SIZE_KEY);
        if (it != options.end()) {
            try {
                sstable_window_size = std::stoi(it->second) * window_unit;
            } catch (const std::exception& e) {
                throw exceptions::syntax_exception(sstring("Invalid integer value ") + it->second + " for " + COMPACTION_WINDOW_SIZE_KEY);
            }
        }

        it = options.find(EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY);
        if (it != options.end()) {
            try {
                expired_sstable_check_frequency = std::chrono::seconds(std::stol(it->second));
            } catch (const std::exception& e) {
                throw exceptions::syntax_exception(sstring("Invalid long value ") + it->second + "for " + EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY);
            }
        }

        it = options.find(TIMESTAMP_RESOLUTION_KEY);
        if (it != options.end()) {
            if (!valid_timestamp_resolutions.count(it->second)) {
                throw exceptions::syntax_exception(sstring("Invalid timestamp resolution ") + it->second + "for " + TIMESTAMP_RESOLUTION_KEY);
            } else {
                timestamp_resolution = valid_timestamp_resolutions.at(it->second);
            }
        }
    }

    std::chrono::seconds get_sstable_window_size() const { return sstable_window_size; }

    friend class time_window_compaction_strategy;
    friend class time_window_backlog_tracker;
};

using timestamp_type = api::timestamp_type;

class time_window_compaction_strategy : public compaction_strategy_impl {
    time_window_compaction_strategy_options _options;
    int64_t _estimated_remaining_tasks = 0;
    db_clock::time_point _last_expired_check;
    timestamp_type _highest_window_seen;
    size_tiered_compaction_strategy_options _stcs_options;
    compaction_backlog_tracker _backlog_tracker;
public:
    time_window_compaction_strategy(const std::map<sstring, sstring>& options);
    virtual compaction_descriptor get_sstables_for_compaction(column_family& cf, std::vector<shared_sstable> candidates) override {
        auto gc_before = gc_clock::now() - cf.schema()->gc_grace_seconds();

        if (candidates.empty()) {
            return compaction_descriptor();
        }

        // Find fully expired SSTables. Those will be included no matter what.
        std::unordered_set<shared_sstable> expired;

        if (db_clock::now() - _last_expired_check > _options.expired_sstable_check_frequency) {
            clogger.debug("TWCS expired check sufficiently far in the past, checking for fully expired SSTables");
            expired = get_fully_expired_sstables(cf, candidates, gc_before);
            _last_expired_check = db_clock::now();
        } else {
            clogger.debug("TWCS skipping check for fully expired SSTables");
        }

        if (!expired.empty()) {
            auto is_expired = [&] (const shared_sstable& s) { return expired.find(s) != expired.end(); };
            candidates.erase(boost::remove_if(candidates, is_expired), candidates.end());
        }

        auto compaction_candidates = get_next_non_expired_sstables(cf, std::move(candidates), gc_before);
        if (!expired.empty()) {
            compaction_candidates.insert(compaction_candidates.end(), expired.begin(), expired.end());
        }
        return compaction_descriptor(std::move(compaction_candidates));
    }
private:
    static timestamp_type
    to_timestamp_type(time_window_compaction_strategy_options::timestamp_resolutions resolution, int64_t timestamp_from_sstable) {
        switch (resolution) {
        case time_window_compaction_strategy_options::timestamp_resolutions::microsecond:
            return timestamp_type(timestamp_from_sstable);
        case time_window_compaction_strategy_options::timestamp_resolutions::millisecond:
            return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::milliseconds(timestamp_from_sstable)).count();
        default:
            throw std::runtime_error("Timestamp resolution invalid for TWCS");
        };
    }

    std::vector<shared_sstable>
    get_next_non_expired_sstables(column_family& cf, std::vector<shared_sstable> non_expiring_sstables, gc_clock::time_point gc_before) {
        auto most_interesting = get_compaction_candidates(cf, non_expiring_sstables);

        if (!most_interesting.empty()) {
            return most_interesting;
        }

        // if there is no sstable to compact in standard way, try compacting single sstable whose droppable tombstone
        // ratio is greater than threshold.
        auto e = boost::range::remove_if(non_expiring_sstables, [this, &gc_before] (const shared_sstable& sst) -> bool {
            return !worth_dropping_tombstones(sst, gc_before);
        });
        non_expiring_sstables.erase(e, non_expiring_sstables.end());
        if (non_expiring_sstables.empty()) {
            return {};
        }
        auto it = boost::min_element(non_expiring_sstables, [] (auto& i, auto& j) {
            return i->get_stats_metadata().min_timestamp < j->get_stats_metadata().min_timestamp;
        });
        return { *it };
    }

    std::vector<shared_sstable> get_compaction_candidates(column_family& cf, std::vector<shared_sstable> candidate_sstables) {
        auto p = get_buckets(std::move(candidate_sstables), _options);
        // Update the highest window seen, if necessary
        _highest_window_seen = std::max(_highest_window_seen, p.second);

        update_estimated_compaction_by_tasks(p.first, cf.schema()->min_compaction_threshold());

        return newest_bucket(std::move(p.first), cf.schema()->min_compaction_threshold(), cf.schema()->max_compaction_threshold(),
            _options.sstable_window_size, _highest_window_seen, _stcs_options);
    }
public:
    // Find the lowest timestamp for window of given size
    static timestamp_type
    get_window_lower_bound(std::chrono::seconds sstable_window_size, timestamp_type timestamp) {
        using namespace std::chrono;
        auto timestamp_in_sec = duration_cast<seconds>(microseconds(timestamp)).count();

        // mask out window size from timestamp to get lower bound of its window
        auto window_lower_bound_in_sec = seconds(timestamp_in_sec - (timestamp_in_sec % sstable_window_size.count()));

        return timestamp_type(duration_cast<microseconds>(window_lower_bound_in_sec).count());
    }

    // Group files with similar max timestamp into buckets.
    // @return A pair, where the left element is the bucket representation (map of timestamp to sstablereader),
    // and the right is the highest timestamp seen
    static std::pair<std::map<timestamp_type, std::vector<shared_sstable>>, timestamp_type>
    get_buckets(std::vector<shared_sstable> files, time_window_compaction_strategy_options& options) {
        std::map<timestamp_type, std::vector<shared_sstable>> buckets;

        timestamp_type max_timestamp = 0;
        // Create map to represent buckets
        // For each sstable, add sstable to the time bucket
        // Where the bucket is the file's max timestamp rounded to the nearest window bucket
        for (auto&& f : files) {
            timestamp_type ts = to_timestamp_type(options.timestamp_resolution, f->get_stats_metadata().max_timestamp);
            timestamp_type lower_bound = get_window_lower_bound(options.sstable_window_size, ts);
            buckets[lower_bound].push_back(std::move(f));
            max_timestamp = std::max(max_timestamp, lower_bound);
        }

        return std::make_pair(std::move(buckets), max_timestamp);
    }

    static std::vector<shared_sstable>
    newest_bucket(std::map<timestamp_type, std::vector<shared_sstable>> buckets, int min_threshold, int max_threshold,
            std::chrono::seconds sstable_window_size, timestamp_type now, size_tiered_compaction_strategy_options& stcs_options) {
        // If the current bucket has at least minThreshold SSTables, choose that one.
        // For any other bucket, at least 2 SSTables is enough.
        // In any case, limit to maxThreshold SSTables.

        for (auto&& key_bucket : buckets | boost::adaptors::reversed) {
            auto key = key_bucket.first;
            auto& bucket = key_bucket.second;

            clogger.trace("Key {}, now {}", key, now);

            if (bucket.size() >= size_t(min_threshold) && key >= now) {
                // If we're in the newest bucket, we'll use STCS to prioritize sstables
                auto stcs_interesting_bucket = size_tiered_compaction_strategy::most_interesting_bucket(bucket, min_threshold, max_threshold, stcs_options);

                // If the tables in the current bucket aren't eligible in the STCS strategy, we'll skip it and look for other buckets
                if (!stcs_interesting_bucket.empty()) {
                    return stcs_interesting_bucket;
                }
            } else if (bucket.size() >= 2 && key < now) {
                clogger.debug("bucket size {} >= 2 and not in current bucket, compacting what's here", bucket.size());
                return trim_to_threshold(std::move(bucket), max_threshold);
            } else {
                clogger.debug("No compaction necessary for bucket size {} , key {}, now {}", bucket.size(), key, now);
            }
        }
        return {};
    }

    static std::vector<shared_sstable>
    trim_to_threshold(std::vector<shared_sstable> bucket, int max_threshold) {
        auto n = std::min(bucket.size(), size_t(max_threshold));
        // Trim the largest sstables off the end to meet the maxThreshold
        boost::partial_sort(bucket, bucket.begin() + n, [] (auto& i, auto& j) {
            return i->ondisk_data_size() < j->ondisk_data_size();
        });
        bucket.resize(n);
        return bucket;
    }
private:
    void update_estimated_compaction_by_tasks(std::map<timestamp_type, std::vector<shared_sstable>>& tasks, int min_threshold) {
        int64_t n = 0;
        timestamp_type now = _highest_window_seen;

        for (auto task : tasks) {
            auto key = task.first;

            // For current window, make sure it's compactable
            auto count = task.second.size();
            if (key >= now && count >= size_t(min_threshold)) {
                n++;
            } else if (key < now && count >= 2) {
                n++;
            }
        }
        _estimated_remaining_tasks = n;
    }

    friend class time_window_backlog_tracker;
public:
    virtual int64_t estimated_pending_compactions(column_family& cf) const override {
        return _estimated_remaining_tasks;
    }

    virtual compaction_strategy_type type() const {
        return compaction_strategy_type::time_window;
    }

    virtual compaction_backlog_tracker& get_backlog_tracker() override {
        return _backlog_tracker;
    }
};

}
