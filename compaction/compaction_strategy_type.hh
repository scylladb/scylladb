/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>

namespace compaction {

enum class compaction_strategy_type {
    null,
    // Deprecated: an alias of `incremental`. A table can still be configured
    // with the SizeTieredCompactionStrategy class name, and it is then reported
    // back as such, but the table is compacted by ICS.
    size_tiered,
    leveled,
    time_window,
    in_memory,
    incremental,
};

enum class reshape_mode { strict, relaxed };

struct reshape_config {
    reshape_mode mode;
    const uint64_t free_storage_space;
};

}
