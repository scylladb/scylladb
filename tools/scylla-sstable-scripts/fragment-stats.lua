--
-- Copyright (C) 2022-present ScyllaDB
--
-- SPDX-License-Identifier: AGPL-3.0-or-later
--

-- Creates simple statistics of the fragments in the sstable
--
-- Prints the number of each fragment type as well as the total fragment count
-- and stats for the partition with the most fragments.

function new_stats(key)
    return {
        partition_key = key,
        total = 0,
        partition = 0,
        static_row = 0,
        clustering_row = 0,
        range_tombstone_change = 0,
    }
end

total_stats = new_stats(nil)

function inc_stat(stats, field)
    stats[field] = stats[field] + 1
    stats.total = stats.total + 1
    total_stats[field] = total_stats[field] + 1
    total_stats.total = total_stats.total + 1
end

function consume_sstable_start(sst)
    max_partition_stats = new_stats(nil)
    if sst then
        current_sst_filename = sst.filename
    else
        current_sst_filename = nil
    end
end

function consume_partition_start(ps)
    current_partition_stats = new_stats(ps.key)
    inc_stat(current_partition_stats, "partition")
end

function consume_static_row(sr)
    inc_stat(current_partition_stats, "static_row")
end

function consume_clustering_row(cr)
    inc_stat(current_partition_stats, "clustering_row")
end

function consume_range_tombstone_change(crt)
    inc_stat(current_partition_stats, "range_tombstone_change")
end

function consume_partition_end()
    if current_partition_stats.total > max_partition_stats.total then
        max_partition_stats = current_partition_stats
    end
end

function consume_sstable_end()
    if current_sst_filename then
        print(string.format("Stats for sstable %s:", current_sst_filename))
    else
        print("Stats for stream:")
    end
    print(string.format("\t%d fragments in %d partitions - %d static rows, %d clustering rows and %d range tombstone changes",
        total_stats.total,
        total_stats.partition,
        total_stats.static_row,
        total_stats.clustering_row,
        total_stats.range_tombstone_change))
    print(string.format("\tPartition with max number of fragments (%d): %s - %d static rows, %d clustering rows and %d range tombstone changes",
        max_partition_stats.total,
        max_partition_stats.partition_key,
        max_partition_stats.static_row,
        max_partition_stats.clustering_row,
        max_partition_stats.range_tombstone_change))
end
