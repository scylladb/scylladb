/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "sstables/types.hh"
#include "utils/streaming_histogram.hh"
#include "utils/estimated_histogram.hh"
#include "locator/host_id.hh"

namespace sstables {

struct stats_metadata : public metadata_base<stats_metadata> {
    // NOTE: on-disk serialization order is defined by describe_type() below and
    // is independent of this declaration order. Fields are grouped here to pack
    // the sub-8-byte scalars together and avoid padding holes.
    utils::estimated_histogram estimated_partition_size;
    utils::estimated_histogram estimated_cells_count;
    utils::streaming_histogram estimated_tombstone_drop_time;
    db::replay_position position;
    db::replay_position commitlog_lower_bound; // 3_x only
    int64_t min_timestamp;
    int64_t max_timestamp;
    double compression_ratio;
    // There is not meaningful value to put in this field, since we have no
    // incremental repair. Before we have it, let's set it to 0.
    // According to architecture/sstable/sstable3/sstables-3-statistics.rst,
    // the repaired_at is a int64_t value.
    int64_t repaired_at = 0;
    int64_t columns_count; // 3_x only
    int64_t rows_count; // 3_x only
    // Sub-8-byte scalars grouped together to avoid padding holes.
    int32_t min_local_deletion_time; // 3_x only
    int32_t max_local_deletion_time;
    int32_t min_ttl; // 3_x only
    int32_t max_ttl; // 3_x only
    uint32_t sstable_level;
    bool has_legacy_counter_shards;
    disk_array<uint32_t, disk_string<uint16_t>> min_column_names;
    disk_array<uint32_t, disk_string<uint16_t>> max_column_names;
    disk_array<uint32_t, commitlog_interval> commitlog_intervals; // 3_x only
    std::optional<locator::host_id> originating_host_id; // 3_11_11 and later (me format)

    template <typename Describer>
    auto describe_type(sstable_version_types v, Describer f) {
        switch (v) {
        case sstable_version_types::mt:
        case sstable_version_types::ms:
        case sstable_version_types::me:
            return f(
                estimated_partition_size,
                estimated_cells_count,
                position,
                min_timestamp,
                max_timestamp,
                min_local_deletion_time,
                max_local_deletion_time,
                min_ttl,
                max_ttl,
                compression_ratio,
                estimated_tombstone_drop_time,
                sstable_level,
                repaired_at,
                min_column_names,
                max_column_names,
                has_legacy_counter_shards,
                columns_count,
                rows_count,
                commitlog_lower_bound,
                commitlog_intervals,
                originating_host_id
            );
        case sstable_version_types::mc:
        case sstable_version_types::md:
            return f(
                estimated_partition_size,
                estimated_cells_count,
                position,
                min_timestamp,
                max_timestamp,
                min_local_deletion_time,
                max_local_deletion_time,
                min_ttl,
                max_ttl,
                compression_ratio,
                estimated_tombstone_drop_time,
                sstable_level,
                repaired_at,
                min_column_names,
                max_column_names,
                has_legacy_counter_shards,
                columns_count,
                rows_count,
                commitlog_lower_bound,
                commitlog_intervals
            );
        case sstable_version_types::ka:
        case sstable_version_types::la:
            return f(
                estimated_partition_size,
                estimated_cells_count,
                position,
                min_timestamp,
                max_timestamp,
                max_local_deletion_time,
                compression_ratio,
                estimated_tombstone_drop_time,
                sstable_level,
                repaired_at,
                min_column_names,
                max_column_names,
                has_legacy_counter_shards
            );
        }
        // Should never reach here - compiler will complain if switch above does not cover all sstable versions
        abort();
    }
};

}
