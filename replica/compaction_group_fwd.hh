/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Forward declarations for replica/compaction_group.hh, so that widely-included
// headers (such as replica/database.hh) need not include it and everything it
// pulls in (the compaction manager, sstable sets, etc.)

#include <cstddef>
#include <absl/container/flat_hash_map.h>
#include <seastar/core/shared_ptr.hh>

namespace replica {

class compaction_group;
class storage_group;
class storage_group_manager;

using compaction_group_ptr = seastar::lw_shared_ptr<compaction_group>;
using const_compaction_group_ptr = seastar::lw_shared_ptr<const compaction_group>;
using storage_group_ptr = seastar::lw_shared_ptr<storage_group>;
using storage_group_map = absl::flat_hash_map<size_t, storage_group_ptr, absl::Hash<size_t>>;

} // namespace replica
