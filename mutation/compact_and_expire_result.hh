/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>

struct compact_and_expire_result {
    uint64_t live_cells = 0;
    uint64_t dead_cells = 0;
    uint32_t collection_tombstones = 0;

    operator bool () const noexcept {
        return live_cells;
    }

    compact_and_expire_result& operator+=(const compact_and_expire_result& o) {
        live_cells += o.live_cells;
        dead_cells += o.dead_cells;
        collection_tombstones += o.collection_tombstones;
        return *this;
    }
};
