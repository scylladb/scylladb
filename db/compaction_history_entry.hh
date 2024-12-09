/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include <seastar/core/sstring.hh>
#include <unordered_map>
#include "utils/UUID.hh"

namespace db {

struct compaction_history_entry {
    utils::UUID id;
    sstring ks;
    sstring cf;
    int64_t compacted_at = 0;
    int64_t bytes_in = 0;
    int64_t bytes_out = 0;
    // Key: number of rows merged
    // Value: counter
    std::unordered_map<int32_t, int64_t> rows_merged;
};

}
