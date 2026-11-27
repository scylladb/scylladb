/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>

namespace sstables::parquet {

/// The memory of the Parquet metadata caches (reader_cache.hh) whose entries
/// live in one LSA region. The cache_tracker that owns the region counts it
/// as index memory, which index_cache_fraction limits.
struct metadata_cache_stats {
    /// The bytes that the entries hold in the region.
    uint64_t used_bytes = 0;
};

} // namespace sstables::parquet
