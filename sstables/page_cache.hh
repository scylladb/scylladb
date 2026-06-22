/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <optional>
#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/shared_ptr.hh>
#include "utils/UUID.hh"

namespace sstables {

/// Abstract interface for an object-storage page cache.
///
/// Caches the raw bytes fetched from object storage by (sstable generation UUID, file offset).
/// Implementations store and retrieve cached pages; on a miss the caller is
/// expected to fall back to the real object-storage request.
class page_cache {
public:
    virtual ~page_cache() = default;

    /// Return cached bytes for the read at (sstable_gen, offset), or nullopt on a miss.
    virtual seastar::future<std::optional<seastar::temporary_buffer<char>>>
    get_page(utils::UUID sstable_gen, int64_t offset) = 0;

    /// Store bytes read at (sstable_gen, offset) in the cache.
    virtual seastar::future<>
    put_page(utils::UUID sstable_gen, int64_t offset, seastar::temporary_buffer<char> data) = 0;

    /// Evict all cached entries for the given SSTable (e.g. after it is deleted).
    virtual seastar::future<>
    evict_sstable(utils::UUID sstable_gen) = 0;
};

using page_cache_ptr = seastar::shared_ptr<page_cache>;

} // namespace sstables
