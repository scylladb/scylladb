/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "repair/incremental.hh"
#include "utils/log.hh"
#include <algorithm>
#include <ranges>

extern logging::logger rlogger;

namespace repair {

std::vector<sstables::shared_sstable> filter_sstables(std::vector<sstables::shared_sstable> sstables, repair::sstables_repair_state repair_state, int64_t sstables_repaired_at) {
    if (repair_state == repair::sstables_repair_state::repaired) {
        auto it = std::ranges::remove_if(sstables, [&](const sstables::shared_sstable& sst) {
            return !repair::is_repaired(sstables_repaired_at, sst);
        });
        sstables.resize(it.begin() - sstables.begin());
    } else if (repair_state == repair::sstables_repair_state::unrepaired) {
        auto it = std::ranges::remove_if(sstables, [&](const sstables::shared_sstable& sst) {
            return repair::is_repaired(sstables_repaired_at, sst) || !sst->being_repaired.uuid().is_null();
        });
        sstables.resize(it.begin() - sstables.begin());
    }
    return sstables;
}

std::vector<sstables::frozen_sstable_run> filter_sstables(std::vector<sstables::frozen_sstable_run> runs, repair::sstables_repair_state repair_state, int64_t sstables_repaired_at) {
    if (repair_state == repair::sstables_repair_state::repaired_and_unrepaired) {
        return runs;
    }
    std::vector<sstables::frozen_sstable_run> tmp;
    for (sstables::frozen_sstable_run& run : runs) {
        sstables::shared_sstable_run r = make_lw_shared<sstables::sstable_run>();
        for (auto& sst : run->all()) {
            bool should_include;
            if (repair_state == repair::sstables_repair_state::repaired) {
                should_include = repair::is_repaired(sstables_repaired_at, sst);
            } else {
                should_include = !repair::is_repaired(sstables_repaired_at, sst) && sst->being_repaired.uuid().is_null();
            }
            if (should_include) {
                auto ok = r->insert(sst);
                if (!ok) {
                    on_internal_error(rlogger, "Failed to insert a sst into a run");
                }
            }
        }
        if (r->data_size()) {
            tmp.push_back(r);
        }
    }
    return tmp;
}

bool is_repaired(int64_t sstables_repaired_at, const sstables::shared_sstable& sst) {
    auto& stats = sst->get_stats_metadata();
    bool repaired = stats.repaired_at !=0 && stats.repaired_at <= sstables_repaired_at;
    rlogger.debug("Checking sst={} repaired_at={} sstables_repaired_at={} repaired={}",
            sst->toc_filename(), stats.repaired_at, sstables_repaired_at, repaired);
    return repaired;
}

}

auto fmt::formatter<repair::sstables_repair_state>::format(repair::sstables_repair_state x, fmt::format_context& ctx) const -> decltype(ctx.out()) {
    std::string_view name;
    switch (x) {
    case repair::sstables_repair_state::repaired:
        name = "repaired";
        break;
    case repair::sstables_repair_state::unrepaired:
        name = "unrepaired";
        break;
    case repair::sstables_repair_state::repaired_and_unrepaired:
        name = "repaired_and_unrepaired";
        break;
    }
    return fmt::format_to(ctx.out(), "{}", name);
}

