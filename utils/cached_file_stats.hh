/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>

struct cached_file_stats {
    uint64_t page_hits = 0;
    uint64_t page_misses = 0;
    uint64_t page_evictions = 0;
    uint64_t page_populations = 0;
    uint64_t cached_bytes = 0;
    uint64_t bytes_in_std = 0; // memory used by active temporary_buffer:s
};
