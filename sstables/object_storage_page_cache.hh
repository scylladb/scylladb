/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// This file is part of the disk page cache of sstables on object storage
// (the 'disk_cache': 'pages' keyspace storage option). It defines the
// interface through which the sstables code uses the cache, which is
// implemented in db/object_storage_page_cache_table.hh.

#pragma once

#include <cstdint>
#include <vector>
#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>
#include "sstables/component_type.hh"
#include "sstables/types.hh"
#include "seastarx.hh"

namespace sstables {

// An abstract interface to the disk page cache of sstables on object
// storage: byte ranges ("pages") of sstable components, which were read from
// the object store and are kept on the node's disk, so that reading them
// again does not need another request to the object store.
//
// The only implementation is db::object_storage_page_cache_table (in
// db/object_storage_page_cache_table.hh), which stores the pages in the
// system_cache.pagecache table. The sstables code uses only this interface,
// so it does not depend on the code that reads and writes tables, and can
// be tested with a fake cache.
//
// Each shard has its own cache, of the sstables that it owns, and all the
// methods work on the cache of the calling shard.
//
// All offsets are positions in the component's file as it is stored on the
// object store. In particular, a compressed Data component is cached in
// compressed chunks, and the offset of a page is the position of its chunk
// in the compressed file - never a position in the uncompressed data.
class object_storage_page_cache {
public:
    struct page {
        uint64_t offset;
        temporary_buffer<char> data;
    };

    virtual ~object_storage_page_cache() = default;

    // Caches a page of the sstable's component, which starts at the given
    // offset. A page already cached at the same offset is replaced.
    virtual future<> put_page(sstable_id sst, component_type component,
            uint64_t offset, temporary_buffer<char> data) = 0;

    // Returns the cached pages of the sstable's component whose start offset
    // is in [from, to), in offset order - but at most max_pages of them, so
    // that reading a large range does not hold all of it in memory at once.
    // To read the rest of the range, call again with "from" just after the
    // offset of the last page returned.
    //
    // "from" and "to" may be any offsets, but only the start offset of each
    // page is compared to them: a page that starts before "to" is returned
    // whole even if it extends beyond "to", while a page that starts before
    // "from" is not returned even if it extends into the range. So "from" is
    // normally the start offset of the first page the caller needs, and "to"
    // any offset after the start offset of the last page it needs.
    virtual future<std::vector<page>> get_pages(sstable_id sst, component_type component,
            uint64_t from, uint64_t to, size_t max_pages) = 0;

    // Drops all the cached pages of the sstable, of all its components.
    virtual future<> evict_sstable(sstable_id sst) = 0;
};

} // namespace sstables
