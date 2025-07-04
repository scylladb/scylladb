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
#include "compaction/table_state.hh"

struct incremental_repair_meta {
    lw_shared_ptr<sstables::sstable_set> sst_set;
    int64_t sstables_repaired_at = 0;
};

namespace repair {

enum class sstables_repair_state { unrepaired, repaired };

bool is_repaired(int64_t sstables_repaired_at, const sstables::shared_sstable& sst);

inline bool is_repaired_state(sstables_repair_state state) {
    return state == sstables_repair_state::repaired;
}

}

class table_state_view {
public:
    compaction::table_state* ts;
    repair::sstables_repair_state repair_state;
    compaction::table_state& as_table_state() { return *ts; };
    table_state_view(compaction::table_state* t, repair::sstables_repair_state state) : ts(t), repair_state(state) { }
    std::vector<sstables::shared_sstable> filter_sstables(std::vector<sstables::shared_sstable> sstables) const;
    std::vector<sstables::frozen_sstable_run> filter_sstables(std::vector<sstables::frozen_sstable_run> runs) const;
};

inline table_state_view make_repaired_table_state_view(compaction::table_state* ts) {
    return table_state_view(ts, repair::sstables_repair_state::repaired);
}

inline table_state_view make_unrepaired_table_state_view(compaction::table_state* ts) {
    return table_state_view(ts, repair::sstables_repair_state::unrepaired);
}

template <>
struct fmt::formatter<repair::sstables_repair_state> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const repair::sstables_repair_state x, fmt::format_context& ctx) const  -> decltype(ctx.out());
};
