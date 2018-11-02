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
 * Copyright (C) 2015 ScyllaDB
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

#include "types.hh"
#include "utils/extremum_tracking.hh"
#include "utils/murmur_hash.hh"
#include "hyperloglog.hh"
#include "db/commitlog/replay_position.hh"
#include <algorithm>

namespace sstables {

static constexpr int TOMBSTONE_HISTOGRAM_BIN_SIZE = 100;

/**
 * ColumnStats holds information about the columns for one partition inside sstable
 */
struct column_stats {
    /** how many atomic cells are there in the partition (every cell in a collection counts)*/
    uint64_t cells_count;
    /** how many columns are there in the partition */
    uint64_t column_count;
    /** how many rows are there in the partition */
    uint64_t rows_count;

    uint64_t start_offset;
    uint64_t partition_size;

    /** the largest/smallest (client-supplied) timestamp in the partition */
    min_max_tracker<api::timestamp_type> timestamp_tracker;
    min_max_tracker<int32_t> local_deletion_time_tracker;
    min_max_tracker<int32_t> ttl_tracker;
    /** histogram of tombstone drop time */
    utils::streaming_histogram tombstone_histogram;

    bool has_legacy_counter_shards;

    column_stats() :
        cells_count(0),
        column_count(0),
        rows_count(0),
        start_offset(0),
        partition_size(0),
        timestamp_tracker(),
        local_deletion_time_tracker(std::numeric_limits<int32_t>::max(), std::numeric_limits<int32_t>::max()),
        ttl_tracker(0, 0),
        tombstone_histogram(TOMBSTONE_HISTOGRAM_BIN_SIZE),
        has_legacy_counter_shards(false)
        {
    }

    void reset() {
        *this = column_stats();
    }

    void update_timestamp(api::timestamp_type value) {
        timestamp_tracker.update(value);
    }
    void update_local_deletion_time(int32_t value) {
        local_deletion_time_tracker.update(value);
    }
    void update_ttl(int32_t value) {
        ttl_tracker.update(value);
    }
};

class metadata_collector {
public:
    static constexpr double NO_COMPRESSION_RATIO = -1.0;

    static hll::HyperLogLog hyperloglog(int p, int sp) {
        // FIXME: hll::HyperLogLog doesn't support sparse format, so ignoring parameters by the time being.
        return hll::HyperLogLog();
    }
private:
    // EH of 150 can track a max value of 1697806495183, i.e., > 1.5PB
    utils::estimated_histogram _estimated_row_size{150};
    // EH of 114 can track a max value of 2395318855, i.e., > 2B cells
    utils::estimated_histogram _estimated_cells_count{114};
    db::replay_position _replay_position;
    min_max_tracker<api::timestamp_type> _timestamp_tracker;
    uint64_t _repaired_at = 0;
    min_max_tracker<int32_t> _local_deletion_time_tracker;
    min_max_tracker<int32_t> _ttl_tracker;
    double _compression_ratio = NO_COMPRESSION_RATIO;
    std::set<int> _ancestors;
    utils::streaming_histogram _estimated_tombstone_drop_time{TOMBSTONE_HISTOGRAM_BIN_SIZE};
    int _sstable_level = 0;
    std::vector<bytes_opt> _min_column_names;
    std::vector<bytes_opt> _max_column_names;
    bool _has_legacy_counter_shards = false;
    uint64_t _columns_count = 0;
    uint64_t _rows_count = 0;

    /**
     * Default cardinality estimation method is to use HyperLogLog++.
     * Parameter here(p=13, sp=25) should give reasonable estimation
     * while lowering bytes required to hold information.
     * See CASSANDRA-5906 for detail.
     */
    hll::HyperLogLog _cardinality = hyperloglog(13, 25);
private:
    /*
     * Convert a vector of bytes into a disk array of disk_string<uint16_t>.
     */
    static void convert(disk_array<uint32_t, disk_string<uint16_t>>&to, std::vector<bytes_opt>&& from) {
        for (auto i = 0U; i < from.size(); i++) {
            if (!from[i]) {
                break;
            }
            disk_string<uint16_t> s;
            s.value = std::move(from[i].value());
            to.elements.push_back(std::move(s));
        }
    }
public:
    void add_key(bytes_view key) {
        long hashed = utils::murmur_hash::hash2_64(key, 0);
        _cardinality.offer_hashed(hashed);
    }

    void add_row_size(uint64_t row_size) {
        _estimated_row_size.add(row_size);
    }

    void add_cells_count(uint64_t cells_count) {
        _estimated_cells_count.add(cells_count);
    }

    void merge_tombstone_histogram(utils::streaming_histogram& histogram) {
        _estimated_tombstone_drop_time.merge(histogram);
    }

    /**
     * Ratio is compressed/uncompressed and it is
     * if you have 1.x then compression isn't helping
     */
    void add_compression_ratio(uint64_t compressed, uint64_t uncompressed) {
        _compression_ratio = (double) compressed/uncompressed;
    }

    void set_replay_position(const db::replay_position & rp) {
        _replay_position = rp;
    }

    void set_repaired_at(uint64_t repaired_at) {
        _repaired_at = repaired_at;
    }

    void add_ancestor(int generation) {
        _ancestors.insert(generation);
    }

    std::set<int> ancestors() const {
        return _ancestors;
    }

    void sstable_level(int sstable_level) {
        _sstable_level = sstable_level;
    }

    std::vector<bytes_opt>& min_column_names() {
        return _min_column_names;
    }

    std::vector<bytes_opt>& max_column_names() {
        return _max_column_names;
    }

    void update_has_legacy_counter_shards(bool has_legacy_counter_shards) {
        _has_legacy_counter_shards = _has_legacy_counter_shards || has_legacy_counter_shards;
    }

    void update(column_stats&& stats) {
        _timestamp_tracker.update(stats.timestamp_tracker);
        _local_deletion_time_tracker.update(stats.local_deletion_time_tracker);
        _ttl_tracker.update(stats.ttl_tracker);
        add_row_size(stats.partition_size);
        add_cells_count(stats.cells_count);
        merge_tombstone_histogram(stats.tombstone_histogram);
        update_has_legacy_counter_shards(stats.has_legacy_counter_shards);
        _columns_count += stats.column_count;
        _rows_count += stats.rows_count;
    }

    void construct_compaction(compaction_metadata& m) {
        if (!_ancestors.empty()) {
            m.ancestors.elements = utils::chunked_vector<uint32_t>(_ancestors.begin(), _ancestors.end());
        }
        auto cardinality = _cardinality.get_bytes();
        m.cardinality.elements = utils::chunked_vector<uint8_t>(cardinality.get(), cardinality.get() + cardinality.size());
    }

    void construct_stats(stats_metadata& m) {
        m.estimated_row_size = std::move(_estimated_row_size);
        m.estimated_cells_count = std::move(_estimated_cells_count);
        m.position = _replay_position;
        m.min_timestamp = _timestamp_tracker.min();
        m.max_timestamp = _timestamp_tracker.max();
        m.min_local_deletion_time = _local_deletion_time_tracker.min();
        m.max_local_deletion_time = _local_deletion_time_tracker.max();
        m.min_ttl = _ttl_tracker.min();
        m.max_ttl = _ttl_tracker.max();
        m.compression_ratio = _compression_ratio;
        m.estimated_tombstone_drop_time = std::move(_estimated_tombstone_drop_time);
        m.sstable_level = _sstable_level;
        m.repaired_at = _repaired_at;
        convert(m.min_column_names, std::move(_min_column_names));
        convert(m.max_column_names, std::move(_max_column_names));
        m.has_legacy_counter_shards = _has_legacy_counter_shards;
        m.columns_count = _columns_count;
        m.rows_count = _rows_count;
    }
};

}


