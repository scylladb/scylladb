/*
 * Copyright (C) 2023 ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>

struct partition_index_cache_stats {
    uint64_t hits = 0; // Number of times entry was found ready
    uint64_t misses = 0; // Number of times entry was not found
    uint64_t blocks = 0; // Number of times entry was not ready (>= misses)
    uint64_t evictions = 0; // Number of times entry was evicted
    uint64_t populations = 0; // Number of times entry was inserted
    uint64_t used_bytes = 0; // Number of bytes entries occupy in memory
};
