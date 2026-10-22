/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// This file is part of the disk page cache of sstables on object storage
// (the 'disk_cache': 'pages' keyspace storage option). It defines a
// seastar::file implementation which reads an sstable component through the
// cache, for the components that are read through a seastar::file rather
// than a data_source: the index components.

#pragma once

#include <seastar/core/file.hh>
#include <seastar/core/shared_ptr.hh>
#include "sstables/component_type.hh"
#include "sstables/object_storage_page_cache.hh"
#include "sstables/types.hh"
#include "sstables/types_fwd.hh"
#include "seastarx.hh"

namespace sstables {

// Returns a seastar::file which reads the given sstable component through
// the disk page cache, in fixed-size pages (fixed_size_page_geometry).
// "underlying" is the component's file on the object store, which this file
// reads in a single request whenever a read misses the cache - so, like the
// data_source of object_storage_page_cache_source.hh, a read is served from
// the cache up to its first page that is not cached, and from one request to
// the object store from there to its end.
//
// The sstable index readers read the index components (Partitions, Rows and
// Index) through a file, in aligned 4 KiB pages which they cache in memory
// (see utils::cached_file). So each of their reads from this file is one or
// a few whole pages of the disk page cache.
//
// With populate_disk_cache::no, the file reads the pages which are already
// cached, but doesn't add pages to the cache (see populate_disk_cache).
//
// Only reads go through the cache; other operations are forwarded to the
// underlying file. The returned file keeps the cache alive.
file make_page_cached_file(shared_ptr<object_storage_page_cache> cache, sstable_id sst, component_type component, file underlying,
        populate_disk_cache populate);

} // namespace sstables
