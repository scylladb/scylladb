/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/util/bool_class.hh>

#include "utils/UUID.hh"

namespace sstables {

using run_id = utils::tagged_uuid<struct run_id_tag>;
using integrity_check = bool_class<class integrity_check_tag>;

// Whether a read of an sstable - of its Data and of its index - may populate
// the disk page cache of sstables on object storage
// (object_storage_page_cache.hh), for an sstable of a keyspace with the
// 'disk_cache' storage option. Every read of such an sstable reads the pages
// which are already in the cache - a cached page is an exact copy of the
// bytes in the object store, which is cheaper to read than sending a request
// to the object store - but only the user's reads without BYPASS CACHE add
// pages to the cache.
// A read which must not populate the cache reads the index through a second
// set of index files, which don't populate it, and so can't use the
// in-memory caches of the index either (see sstable::index_caching()).
// Unlike use_caching, it defaults to populate_disk_cache::no, and only the
// user's reads of a table - table::make_mutation_reader(), directly or through
// the row cache - set it to populate_disk_cache::yes, unless they have BYPASS
// CACHE. Reads with BYPASS CACHE, and background reads such as compaction,
// streaming and view building, read whole sstables, or large parts of them,
// once, so if they populated the cache, they would only replace the pages
// which the user's reads need with pages which nobody will read again.
// So a read with BYPASS CACHE does read the pages which are already in the
// disk page cache - unlike the row cache, which it neither reads nor
// populates.
using populate_disk_cache = bool_class<struct populate_disk_cache_tag>;

using update_sstable_id = bool_class<struct update_sstable_id_tag>;

} // namespace sstables
