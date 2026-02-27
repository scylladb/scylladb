/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/shard_id.hh>
#include <seastar/core/sstring.hh>
#include <vector>
#include <unordered_map>
#include "sstables/basic_info.hh"
#include "utils/UUID.hh"

namespace db {

struct compaction_history_entry {
    utils::UUID id;
    shard_id shard_id;
    sstring ks;
    sstring cf;
    sstring compaction_type;
    int64_t started_at = 0;
    int64_t compacted_at = 0;
    int64_t bytes_in = 0;
    int64_t bytes_out = 0;
    // Key: number of rows merged
    // Value: counter
    std::unordered_map<int32_t, int64_t> rows_merged;
    std::vector<sstables::basic_info> sstables_in;
    std::vector<sstables::basic_info> sstables_out;
    int64_t total_tombstone_purge_attempt = 0;
    int64_t total_tombstone_purge_failure_due_to_overlapping_with_memtable = 0;
    int64_t total_tombstone_purge_failure_due_to_overlapping_with_uncompacting_sstable = 0;
};

}
