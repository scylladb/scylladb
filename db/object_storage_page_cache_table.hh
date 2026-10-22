/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "schema/schema_fwd.hh"

namespace db {

// The disk page cache of sstables on object storage, which is stored in
// the system_cache.pagecache table.
class object_storage_page_cache_table {
public:
    // A node-local keyspace for caches. Unlike the system keyspace, writes
    // to it skip the commitlog, and it may grow very large.
    static constexpr auto SYSTEM_CACHE = "system_cache";
    static constexpr auto PAGECACHE = "pagecache";

    static schema_ptr schema();
};

} // namespace db
