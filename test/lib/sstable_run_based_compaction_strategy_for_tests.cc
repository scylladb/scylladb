/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */


#include "test/lib/sstable_run_based_compaction_strategy_for_tests.hh"
#include <boost/range/numeric.hpp>

namespace sstables {

sstable_run_based_compaction_strategy_for_tests::sstable_run_based_compaction_strategy_for_tests() = default;

compaction_descriptor sstable_run_based_compaction_strategy_for_tests::get_sstables_for_compaction(table_state& table_s, strategy_control& control) {
    // Get unique runs from all uncompacting sstables
    std::vector<frozen_sstable_run> runs = table_s.main_sstable_set().all_sstable_runs();

    // Group similar sized runs into a bucket.
    std::map<uint64_t, std::vector<frozen_sstable_run>> similar_sized_runs;
    for (auto& run : runs) {
        bool found = false;
        for (auto& e : similar_sized_runs) {
            if (run->data_size() >= e.first*0.5 && run->data_size() <= e.first*1.5) {
                e.second.push_back(run);
                found = true;
                break;
            }
        }
        if (!found) {
            similar_sized_runs[run->data_size()].push_back(run);
        }
    }
    // Get the most interesting bucket, if any, and return sstables from all its runs.
    for (auto& entry : similar_sized_runs) {
        auto& runs = entry.second;
        if (runs.size() < size_t(table_s.schema()->min_compaction_threshold())) {
            continue;
        }
        auto all = runs
            | std::views::transform([] (auto& run) -> auto& { return run->all(); })
            | std::views::join
            | std::ranges::to<std::vector>();
        return sstables::compaction_descriptor(std::move(all), 0, static_fragment_size_for_run);
    }
    return sstables::compaction_descriptor();
}

int64_t sstable_run_based_compaction_strategy_for_tests::estimated_pending_compactions(table_state& table_s) const {
    throw std::runtime_error("unimplemented");
}

compaction_strategy_type sstable_run_based_compaction_strategy_for_tests::type() const {
    throw std::runtime_error("unimplemented");
}

std::unique_ptr<compaction_backlog_tracker::impl> sstable_run_based_compaction_strategy_for_tests::make_backlog_tracker() const {
    throw std::runtime_error("unimplemented");
}

}
