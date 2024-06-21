/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>

namespace sstables {

enum class compaction_strategy_type {
    null,
    size_tiered,
    leveled,
    time_window,
};

enum class reshape_mode { strict, relaxed };

struct reshape_config {
    reshape_mode mode;
    const uint64_t free_storage_space;
};

}
