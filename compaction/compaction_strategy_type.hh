/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <cstdint>

namespace sstables {

enum class compaction_strategy_type {
    null,
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
