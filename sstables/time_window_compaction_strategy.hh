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
 * Copyright (C) 2017-present ScyllaDB
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
#include "sstables/sstables.hh"
#include "service/priority_manager.hh"

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

    std::chrono::seconds sstable_window_size = DEFAULT_COMPACTION_WINDOW_UNIT * DEFAULT_COMPACTION_WINDOW_SIZE;
    db_clock::duration expired_sstable_check_frequency = DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS();
    timestamp_resolutions timestamp_resolution = timestamp_resolutions::microsecond;
public:
    time_window_compaction_strategy_options(const time_window_compaction_strategy_options&);
    time_window_compaction_strategy_options(time_window_compaction_strategy_options&&);
    time_window_compaction_strategy_options(const std::map<sstring, sstring>& options);

    std::chrono::seconds get_sstable_window_size() const { return sstable_window_size; }

    friend class time_window_compaction_strategy;
    friend class time_window_backlog_tracker;
};

using timestamp_type = api::timestamp_type;

class time_window_compaction_strategy : public compaction_strategy_impl {
    time_window_compaction_strategy_options _options;
    int64_t _estimated_remaining_tasks = 0;
    db_clock::time_point _last_expired_check;
    // As timestamp_type is an int64_t, a primitive type, it must be initialized here.
    timestamp_type _highest_window_seen = 0;
    // Keep track of all recent active windows that still need to be compacted into a single SSTable
    std::unordered_set<timestamp_type> _recent_active_windows;
    size_tiered_compaction_strategy_options _stcs_options;
    compaction_backlog_tracker _backlog_tracker;
public:
    // The maximum amount of buckets we segregate data into when writing into sstables.
    // To prevent an explosion in the number of sstables we cap it.
    // Better co-locate some windows into the same sstables than OOM.
    static constexpr uint64_t max_data_segregation_window_count = 100;

    using bucket_t = std::vector<shared_sstable>;
    enum class bucket_compaction_mode { none, size_tiered, major };
public:
    time_window_compaction_strategy(const std::map<sstring, sstring>& options);
    virtual compaction_descriptor get_sstables_for_compaction(column_family& cf, std::vector<shared_sstable> candidates) override;
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

    // Returns true if bucket is the last, most active one.
    bool is_last_active_bucket(timestamp_type bucket_key, timestamp_type now) const {
        return bucket_key >= now;
    }

    // Returns which compaction type should be performed on a given window bucket.
    bucket_compaction_mode
    compaction_mode(const bucket_t& bucket, timestamp_type bucket_key, timestamp_type now, size_t min_threshold) const;

    std::vector<shared_sstable>
    get_next_non_expired_sstables(column_family& cf, std::vector<shared_sstable> non_expiring_sstables, gc_clock::time_point gc_before);

    std::vector<shared_sstable> get_compaction_candidates(column_family& cf, std::vector<shared_sstable> candidate_sstables);
public:
    // Find the lowest timestamp for window of given size
    static timestamp_type
    get_window_lower_bound(std::chrono::seconds sstable_window_size, timestamp_type timestamp);

    // Group files with similar max timestamp into buckets.
    // @return A pair, where the left element is the bucket representation (map of timestamp to sstablereader),
    // and the right is the highest timestamp seen
    static std::pair<std::map<timestamp_type, std::vector<shared_sstable>>, timestamp_type>
    get_buckets(std::vector<shared_sstable> files, time_window_compaction_strategy_options& options);

    std::vector<shared_sstable>
    newest_bucket(std::map<timestamp_type, std::vector<shared_sstable>> buckets, int min_threshold, int max_threshold,
            std::chrono::seconds sstable_window_size, timestamp_type now, size_tiered_compaction_strategy_options& stcs_options);

    static std::vector<shared_sstable>
    trim_to_threshold(std::vector<shared_sstable> bucket, int max_threshold);

    static int64_t
    get_window_for(const time_window_compaction_strategy_options& options, api::timestamp_type ts) {
        return get_window_lower_bound(options.sstable_window_size, to_timestamp_type(options.timestamp_resolution, ts));
    }

    static api::timestamp_type
    get_window_size(const time_window_compaction_strategy_options& options) {
        return timestamp_type(std::chrono::duration_cast<std::chrono::microseconds>(options.get_sstable_window_size()).count());
    }
private:
    void update_estimated_compaction_by_tasks(std::map<timestamp_type, std::vector<shared_sstable>>& tasks,
        int min_threshold, int max_threshold);

    friend class time_window_backlog_tracker;
public:
    virtual int64_t estimated_pending_compactions(column_family& cf) const override {
        return _estimated_remaining_tasks;
    }

    virtual compaction_strategy_type type() const {
        return compaction_strategy_type::time_window;
    }

    virtual std::unique_ptr<sstable_set_impl> make_sstable_set(schema_ptr schema) const override;

    virtual compaction_backlog_tracker& get_backlog_tracker() override {
        return _backlog_tracker;
    }

    virtual uint64_t adjust_partition_estimate(const mutation_source_metadata& ms_meta, uint64_t partition_estimate) override;

    virtual reader_consumer make_interposer_consumer(const mutation_source_metadata& ms_meta, reader_consumer end_consumer) override;

    virtual bool use_interposer_consumer() const override {
        return true;
    }

    virtual compaction_descriptor get_reshaping_job(std::vector<shared_sstable> input, schema_ptr schema, const ::io_priority_class& iop, reshape_mode mode) override;
};

}
