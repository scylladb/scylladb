/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <fmt/core.h>

struct compact_and_expire_result {
    uint64_t live_cells = 0;
    uint64_t dead_cells = 0;
    uint32_t collection_tombstones = 0;

    bool is_live() const noexcept {
        return live_cells;
    }

    bool operator==(const compact_and_expire_result&) const = default;

    compact_and_expire_result& operator+=(const compact_and_expire_result& o) {
        live_cells += o.live_cells;
        dead_cells += o.dead_cells;
        collection_tombstones += o.collection_tombstones;
        return *this;
    }
};

template <> struct fmt::formatter<compact_and_expire_result> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const compact_and_expire_result& r, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{{live_cells: {}, dead_cells: {}, collection_tombstones: {}}}", r.live_cells, r.dead_cells,
                r.collection_tombstones);
    }
};
