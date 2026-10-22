/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// This file is part of the disk page cache of sstables on object storage
// (the 'disk_cache': 'pages' keyspace storage option). It defines a
// data_source which reads a byte range of an sstable component through the
// cache (object_storage_page_cache.hh), split into pages as defined in
// object_storage_page_cache_geometry.hh.

#pragma once

#include <cstdint>
#include <seastar/core/iostream.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/noncopyable_function.hh>
#include "sstables/component_type.hh"
#include "sstables/object_storage_page_cache.hh"
#include "sstables/object_storage_page_cache_geometry.hh"
#include "sstables/types.hh"
#include "seastarx.hh"

namespace sstables {

// Starts a single request to the object store, which reads the byte range
// [offset, offset + length) of the component, and returns the data as it
// arrives.
using object_storage_range_reader = noncopyable_function<future<data_source>(uint64_t offset, uint64_t length)>;

// The longest skip which a data_source returned by make_page_cached_source()
// reads through, from a request to the object store which has started,
// rather than closing the request. Reading through a skip saves a request,
// but downloads, and caches, pages which the read doesn't need: a read of
// rows far apart in a wide partition - say, WHERE c IN (0, 2999) - reads
// the first row, and then skips to the next, possibly over megabytes. A new
// request takes tens of milliseconds before its first byte arrives, about
// the time it takes a request to return a megabyte or a few, so reading
// through skips of up to 1 MiB costs little time, and few pages.
constexpr uint64_t max_read_through_skip = 1024 * 1024;

// Returns a data_source of the bytes [offset, offset + length) of an sstable
// component, which reads them through the disk page cache:
//
// * The source reads whole pages, in order. The first pages of the range
//   which are cached are read from the cache, in batches of consecutive
//   pages, each of which is a sequential read from the cache's table.
//
// * At the first page which is not cached, the source starts a single
//   request to the object store (read_range), which reads everything from
//   that page to the end of the range - even if some of the following pages
//   are cached. A request to the object store costs the same regardless of
//   its size, so one large request is cheaper than splitting the rest of the
//   range into several requests around the cached pages. The source splits
//   the data that arrives into pages, and caches each of them.
//
// * The range is extended to whole pages (see page_geometry::pages_covering()),
//   so whole pages are read from the object store and cached, but only the
//   requested bytes are returned.
//
// skip() only moves the position: the pages it skips are not read from the
// cache. Once a request to the object store has started, the source keeps
// reading from it, and also caches the pages that a short skip() passed
// over, as they arrive anyway. But after a skip of more than
// max_read_through_skip bytes beyond what the request returned, the source
// closes the request, and continues as at the start of the range: from the
// cache, and with a new request at the first page which isn't cached.
//
// The cache must outlive the returned source.
data_source make_page_cached_source(object_storage_page_cache& cache, sstable_id sst, component_type component,
        shared_ptr<const page_geometry> geometry, uint64_t offset, uint64_t length,
        object_storage_range_reader read_range);

} // namespace sstables
