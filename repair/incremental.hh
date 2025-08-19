/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "sstables/sstables.hh"
#include "sstables/sstable_set.hh"
#include <seastar/core/shared_ptr.hh>
#include "sstables/shared_sstable.hh"

struct incremental_repair_meta {
    lw_shared_ptr<sstables::sstable_set> sst_set;
    int64_t sstables_repaired_at = 0;
};

namespace repair {

bool is_repaired(int64_t sstables_repaired_at, const sstables::shared_sstable& sst);

}
