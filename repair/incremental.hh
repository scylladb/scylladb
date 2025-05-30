/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "sstables/sstables.hh"
#include "sstables/sstable_set.hh"
#include "utils/chunked_vector.hh"
#include <seastar/core/shared_ptr.hh>
#include <unordered_set>
#include "sstables/shared_sstable.hh"

struct incremental_repair_meta {
    lw_shared_ptr<utils::chunked_vector<sstables::sstable_files_snapshot>> sstables;
    lw_shared_ptr<sstables::sstable_set> sst_set;
    int64_t sstables_repaired_at = 0;
};

namespace repair {

enum class sstables_repair_state { unrepaired, repaired, repaired_and_unrepaired };

bool is_repaired(int64_t sstables_repaired_at, const sstables::shared_sstable& sst);

std::vector<sstables::shared_sstable> filter_sstables(std::vector<sstables::shared_sstable> sstables, repair::sstables_repair_state repair_state, int64_t sstables_repaired_at);
std::vector<sstables::frozen_sstable_run> filter_sstables(std::vector<sstables::frozen_sstable_run> runs, repair::sstables_repair_state repair_state, int64_t sstables_repaired_at);

}

template <>
struct fmt::formatter<repair::sstables_repair_state> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const repair::sstables_repair_state x, fmt::format_context& ctx) const  -> decltype(ctx.out());
};
