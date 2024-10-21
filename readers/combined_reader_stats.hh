/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <vector>

struct combined_reader_statistics {
    // Histogram describing a distribution of clustering keys. The vector
    // gathers a number of clustering keys merged (value) from a given
    // number of sstable files (index). The length of the vector is equal
    // to the number of compacted sstables + 1
    std::vector<int64_t> rows_merged_histogram;
};
