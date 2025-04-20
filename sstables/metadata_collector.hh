/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "sstables/types.hh"
#include "sstables/component_type.hh"
#include "timestamp.hh"
#include "utils/extremum_tracking.hh"
#include "utils/murmur_hash.hh"
#include "hyperloglog.hh"
#include "db/commitlog/replay_position.hh"
#include "mutation/position_in_partition.hh"
#include "locator/host_id.hh"


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
    /** how many rows (including range tombstone markers) are there in the partition */
    uint64_t rows_count;
    /** how many range tombstones are there in the partition */
    uint64_t range_tombstones_count;
    /** how many dead rows are there in the partition */
    uint64_t dead_rows_count;

    uint64_t start_offset;
    uint64_t partition_size;

    /** the largest/smallest (client-supplied) timestamp in the partition */
    min_max_tracker<api::timestamp_type> timestamp_tracker;
    /** the largest/smallest (client-supplied) timestamp of live data in the partition, for the purpose of tombstone garbage collection **/
    min_tracker<api::timestamp_type> min_live_timestamp_tracker;
    /** the largest/smallest (client-supplied) timestamp of live data that would shadow shadowable tomebstones in the partition,
     ** for the purpose of tombstone garbage collection of shadowable tombstones **/
    min_tracker<api::timestamp_type> min_live_row_marker_timestamp_tracker;

    min_max_tracker<int32_t> local_deletion_time_tracker;
    min_max_tracker<int32_t> ttl_tracker;
    /** histogram of tombstone drop time */
    utils::streaming_histogram tombstone_histogram;

    bool has_legacy_counter_shards;
    bool capped_local_deletion_time = false;

    column_stats() :
        cells_count(0),
        column_count(0),
        rows_count(0),
        range_tombstones_count(0),
        dead_rows_count(0),
        start_offset(0),
        partition_size(0),
        min_live_timestamp_tracker(api::max_timestamp),
        min_live_row_marker_timestamp_tracker(api::max_timestamp),
        tombstone_histogram(TOMBSTONE_HISTOGRAM_BIN_SIZE),
        has_legacy_counter_shards(false)
        {
    }

    void reset() {
        *this = column_stats();
    }

    void update_timestamp(api::timestamp_type value, is_live is_live) {
        timestamp_tracker.update(value);
        if (is_live) {
            min_live_timestamp_tracker.update(value);
        }
    }

    void update_live_row_marker_timestamp(api::timestamp_type value) {
        min_live_row_marker_timestamp_tracker.update(value);
    }

    void update_local_deletion_time(int32_t value) {
        local_deletion_time_tracker.update(value);
    }
    void update_local_deletion_time_and_tombstone_histogram(gc_clock::time_point value) {
        bool capped;
        int32_t ldt = adjusted_local_deletion_time(value, capped);
        local_deletion_time_tracker.update(ldt);
        tombstone_histogram.update(ldt);
        capped_local_deletion_time |= capped;
    }
    void update_ttl(int32_t value) {
        ttl_tracker.update(value);
    }
    void update_ttl(gc_clock::duration value) {
        ttl_tracker.update(gc_clock::as_int32(value));
    }
    void do_update(const tombstone& t) {
        update_timestamp(t.timestamp, is_live::no);
        update_local_deletion_time_and_tombstone_histogram(t.deletion_time);
    }
    void update(const tombstone& t) {
        if (t) {
            do_update(t);
        }
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
    const schema& _schema;
    component_name _name;
    locator::host_id _host_id;
    // EH of 150 can track a max value of 1697806495183, i.e., > 1.5PB
    utils::estimated_histogram _estimated_partition_size{150};
    // EH of 114 can track a max value of 2395318855, i.e., > 2B cells
    utils::estimated_histogram _estimated_cells_count{114};
    db::replay_position _replay_position;
    min_max_tracker<api::timestamp_type> _timestamp_tracker;
    min_tracker<api::timestamp_type> _min_live_timestamp_tracker;
    min_tracker<api::timestamp_type> _min_live_row_marker_timestamp_tracker;
    min_max_tracker<int32_t> _local_deletion_time_tracker{std::numeric_limits<int32_t>::max(), std::numeric_limits<int32_t>::max()};
    min_max_tracker<int32_t> _ttl_tracker{0, 0};
    double _compression_ratio = NO_COMPRESSION_RATIO;
    utils::streaming_histogram _estimated_tombstone_drop_time{TOMBSTONE_HISTOGRAM_BIN_SIZE};
    int _sstable_level = 0;
    std::optional<position_in_partition> _min_clustering_pos;
    std::optional<position_in_partition> _max_clustering_pos;
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
    void convert(disk_array<uint32_t, disk_string<uint16_t>>&to, const std::optional<position_in_partition>& from);
public:
    explicit metadata_collector(const schema& schema, component_name name, const locator::host_id& host_id)
        : _schema(schema)
        , _name(name)
        , _host_id(host_id)
        , _min_live_timestamp_tracker(api::max_timestamp)
        , _min_live_row_marker_timestamp_tracker(api::max_timestamp)
    {
        if (!schema.clustering_key_size()) {
            _min_clustering_pos.emplace(position_in_partition_view::before_all_clustered_rows());
            _max_clustering_pos.emplace(position_in_partition_view::after_all_clustered_rows());
        }
    }

    const schema& get_schema() {
        return _schema;
    }

    void add_key(bytes_view key) {
        long hashed = utils::murmur_hash::hash2_64(key, 0);
        _cardinality.offer_hashed(hashed);
    }

    void add_partition_size(uint64_t partition_size) {
        _estimated_partition_size.add(partition_size);
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

    void set_sstable_level(int sstable_level) {
        _sstable_level = sstable_level;
    }

    void update_has_legacy_counter_shards(bool has_legacy_counter_shards) {
        _has_legacy_counter_shards = _has_legacy_counter_shards || has_legacy_counter_shards;
    }

    // pos must be in the clustered region
    void update_min_max_components(position_in_partition_view pos);

    void update(column_stats&& stats) {
        _timestamp_tracker.update(stats.timestamp_tracker);
        _min_live_timestamp_tracker.update(stats.min_live_timestamp_tracker);
        _min_live_row_marker_timestamp_tracker.update(stats.min_live_row_marker_timestamp_tracker);
        _local_deletion_time_tracker.update(stats.local_deletion_time_tracker);
        _ttl_tracker.update(stats.ttl_tracker);
        add_partition_size(stats.partition_size);
        add_cells_count(stats.cells_count);
        merge_tombstone_histogram(stats.tombstone_histogram);
        update_has_legacy_counter_shards(stats.has_legacy_counter_shards);
        _columns_count += stats.column_count;
        _rows_count += stats.rows_count;
    }

    void construct_compaction(compaction_metadata& m) {
        auto cardinality = _cardinality.get_bytes();
        m.cardinality.elements = utils::chunked_vector<uint8_t>(cardinality.get(), cardinality.get() + cardinality.size());
    }

    void construct_stats(stats_metadata& m) {
        m.estimated_partition_size = std::move(_estimated_partition_size);
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
        convert(m.min_column_names, _min_clustering_pos);
        convert(m.max_column_names, _max_clustering_pos);
        m.has_legacy_counter_shards = _has_legacy_counter_shards;
        m.columns_count = _columns_count;
        m.rows_count = _rows_count;
        m.originating_host_id = _host_id;
    }

    scylla_metadata::ext_timestamp_stats::map_type get_ext_timestamp_stats() {
        return scylla_metadata::ext_timestamp_stats::map_type{
            { ext_timestamp_stats_type::min_live_timestamp, _min_live_timestamp_tracker.get() },
            { ext_timestamp_stats_type::min_live_row_marker_timestamp, _min_live_row_marker_timestamp_tracker.get() },
        };
    }
};

}
