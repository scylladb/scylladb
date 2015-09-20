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
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
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
#include "utils/murmur_hash.hh"
#include "hyperloglog.hh"
#include "db/commitlog/replay_position.hh"
#include <algorithm>

namespace sstables {

static constexpr int TOMBSTONE_HISTOGRAM_BIN_SIZE = 100;

class min_long_tracker {
    uint64_t _default_value;
    bool _is_set = false;
    uint64_t _value;
public:
    min_long_tracker() {}
    min_long_tracker(uint64_t default_value) {
        _default_value = default_value;
    }

    void update(uint64_t value) {
        if (!_is_set) {
            _value = value;
            _is_set = true;
        } else {
            if (value < _value) {
                _value = value;
            }
        }
    }

    uint64_t get() {
        if (_is_set) {
            return _value;
        }
        return _default_value;
    }
};

class max_long_tracker {
    uint64_t _default_value;
    bool _is_set = false;
    uint64_t _value;
public:
    max_long_tracker() {}
    max_long_tracker(uint64_t default_value) {
        _default_value = default_value;
    }

    void update(uint64_t value) {
        if (!_is_set) {
            _value = value;
            _is_set = true;
        } else {
            if (value > _value) {
                _value = value;
            }
        }
    }

    uint64_t get() {
        if (_is_set) {
            return _value;
        }
        return _default_value;
    }
};

class max_int_tracker {
    int _default_value;
    bool _is_set = false;
    int _value;
public:
    max_int_tracker() {}
    max_int_tracker(int default_value) {
        _default_value = default_value;
    }

    void update(int value) {
        if (!_is_set) {
            _value = value;
            _is_set = true;
        } else {
            if (value > _value) {
                _value = value;
            }
        }
    }

    int get() {
        if (_is_set) {
            return _value;
        }
        return _default_value;
    }
};

/**
 * ColumnStats holds information about the columns for one row inside sstable
 */
struct column_stats {
    /** how many columns are there in the row */
    uint64_t column_count;

    uint64_t start_offset;
    uint64_t row_size;

    /** the largest (client-supplied) timestamp in the row */
    min_long_tracker min_timestamp;
    max_long_tracker max_timestamp;
    max_int_tracker max_local_deletion_time;
    /** histogram of tombstone drop time */
    streaming_histogram tombstone_histogram;

    /** max and min column names according to comparator */
    std::vector<bytes> min_column_names;
    std::vector<bytes> max_column_names;

    bool has_legacy_counter_shards;

    column_stats() :
        column_count(0),
        start_offset(0),
        row_size(0),
        min_timestamp(min_long_tracker(std::numeric_limits<uint64_t>::min())),
        max_timestamp(max_long_tracker(std::numeric_limits<uint64_t>::max())),
        max_local_deletion_time(max_int_tracker(std::numeric_limits<int>::max())),
        tombstone_histogram(TOMBSTONE_HISTOGRAM_BIN_SIZE),
        has_legacy_counter_shards(false)
        {
    }

    void reset() {
        *this = column_stats();
    }

    void update_min_timestamp(uint64_t potential_min) {
        min_timestamp.update(potential_min);
    }
    void update_max_timestamp(uint64_t potential_max) {
        max_timestamp.update(potential_max);
    }
    void update_max_local_deletion_time(int potential_value) {
        max_local_deletion_time.update(potential_value);
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
    estimated_histogram _estimated_row_size{150};
    // EH of 114 can track a max value of 2395318855, i.e., > 2B columns
    estimated_histogram _estimated_column_count{114};
    db::replay_position _replay_position;
    uint64_t _min_timestamp = std::numeric_limits<uint64_t>::max();
    uint64_t _max_timestamp = std::numeric_limits<uint64_t>::min();
    uint64_t _repaired_at = 0;
    int _max_local_deletion_time = std::numeric_limits<int>::min();
    double _compression_ratio = NO_COMPRESSION_RATIO;
    std::set<int> _ancestors;
    streaming_histogram _estimated_tombstone_drop_time{TOMBSTONE_HISTOGRAM_BIN_SIZE};
    int _sstable_level = 0;
    std::vector<bytes> _min_column_names;
    std::vector<bytes> _max_column_names;
    bool _has_legacy_counter_shards = false;

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
    static void convert(disk_array<uint32_t, disk_string<uint16_t>>&to, std::vector<bytes>&& from) {
        to.elements.resize(from.size());
        for (auto i = 0U; i < from.size(); i++) {
            to.elements[i].value = std::move(from[i]);
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

    void add_column_count(uint64_t column_count) {
        _estimated_column_count.add(column_count);
    }

    void merge_tombstone_histogram(streaming_histogram& histogram) {
        _estimated_tombstone_drop_time.merge(histogram);
    }

    /**
     * Ratio is compressed/uncompressed and it is
     * if you have 1.x then compression isn't helping
     */
    void add_compression_ratio(uint64_t compressed, uint64_t uncompressed) {
        _compression_ratio = (double) compressed/uncompressed;
    }

    void update_min_timestamp(uint64_t potential_min) {
        _min_timestamp = std::min(_min_timestamp, potential_min);
    }

    void update_max_timestamp(uint64_t potential_max) {
        _max_timestamp = std::max(_max_timestamp, potential_max);
    }

    void update_max_local_deletion_time(int max_local_deletion_time) {
        _max_local_deletion_time = std::max(_max_local_deletion_time, max_local_deletion_time);
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

    void sstable_level(int sstable_level) {
        _sstable_level = sstable_level;
    }

    void update_min_column_names(std::vector<bytes>&& min_column_names) {
        if (min_column_names.size() > 0) {
            column_name_helper::merge_min_components(_min_column_names, std::move(min_column_names));
        }
    }

    void update_max_column_names(std::vector<bytes>&& max_column_names) {
        if (max_column_names.size() > 0) {
            column_name_helper::merge_max_components(_max_column_names, std::move(max_column_names));
        }
    }

    void update_has_legacy_counter_shards(bool has_legacy_counter_shards) {
        _has_legacy_counter_shards = _has_legacy_counter_shards || has_legacy_counter_shards;
    }

    void update(column_stats&& stats) {
        update_min_timestamp(stats.min_timestamp.get());
        update_max_timestamp(stats.max_timestamp.get());
        update_max_local_deletion_time(stats.max_local_deletion_time.get());
        add_row_size(stats.row_size);
        add_column_count(stats.column_count);
        merge_tombstone_histogram(stats.tombstone_histogram);
        update_min_column_names(std::move(stats.min_column_names));
        update_max_column_names(std::move(stats.max_column_names));
        update_has_legacy_counter_shards(stats.has_legacy_counter_shards);
    }

    void construct_compaction(compaction_metadata& m) {
        if (!_ancestors.empty()) {
            m.ancestors.elements = std::deque<uint32_t>(_ancestors.begin(), _ancestors.end());
        }
        auto cardinality = _cardinality.get_bytes();
        m.cardinality.elements = std::deque<uint8_t>(cardinality.get(), cardinality.get() + cardinality.size());
    }

    void construct_stats(stats_metadata& m) {
        m.estimated_row_size = std::move(_estimated_row_size);
        m.estimated_column_count = std::move(_estimated_column_count);
        m.position = _replay_position;
        m.min_timestamp = _min_timestamp;
        m.max_timestamp = _max_timestamp;
        m.max_local_deletion_time = _max_local_deletion_time;
        m.compression_ratio = _compression_ratio;
        m.estimated_tombstone_drop_time = std::move(_estimated_tombstone_drop_time);
        m.sstable_level = _sstable_level;
        m.repaired_at = _repaired_at;
        convert(m.min_column_names, std::move(_min_column_names));
        convert(m.max_column_names, std::move(_max_column_names));
        m.has_legacy_counter_shards = _has_legacy_counter_shards;
    }
};

}


