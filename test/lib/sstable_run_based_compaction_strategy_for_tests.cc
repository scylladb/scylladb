/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include "test/lib/sstable_run_based_compaction_strategy_for_tests.hh"

namespace sstables {

sstable_run_based_compaction_strategy_for_tests::sstable_run_based_compaction_strategy_for_tests() = default;

compaction_descriptor sstable_run_based_compaction_strategy_for_tests::get_sstables_for_compaction(table_state& table_s, strategy_control& control, std::vector<sstables::shared_sstable> uncompacting_sstables) {
    // Get unique runs from all uncompacting sstables
    std::vector<sstable_run> runs = table_s.main_sstable_set().select_sstable_runs(uncompacting_sstables);

    // Group similar sized runs into a bucket.
    std::map<uint64_t, std::vector<sstable_run>> similar_sized_runs;
    for (auto& run : runs) {
        bool found = false;
        for (auto& e : similar_sized_runs) {
            if (run.data_size() >= e.first*0.5 && run.data_size() <= e.first*1.5) {
                e.second.push_back(run);
                found = true;
                break;
            }
        }
        if (!found) {
            similar_sized_runs[run.data_size()].push_back(run);
        }
    }
    // Get the most interesting bucket, if any, and return sstables from all its runs.
    for (auto& entry : similar_sized_runs) {
        auto& runs = entry.second;
        if (runs.size() < size_t(table_s.schema()->min_compaction_threshold())) {
            continue;
        }
        auto all = boost::accumulate(runs, std::vector<shared_sstable>(), [&] (std::vector<shared_sstable>&& v, const sstable_run& run) {
            v.insert(v.end(), run.all().begin(), run.all().end());
            return std::move(v);
        });
        return sstables::compaction_descriptor(std::move(all), default_priority_class(), 0, static_fragment_size_for_run);
    }
    return sstables::compaction_descriptor();
}

int64_t sstable_run_based_compaction_strategy_for_tests::estimated_pending_compactions(table_state& table_s) const {
    throw std::runtime_error("unimplemented");
}

compaction_strategy_type sstable_run_based_compaction_strategy_for_tests::type() const {
    throw std::runtime_error("unimplemented");
}

compaction_backlog_tracker& sstable_run_based_compaction_strategy_for_tests::get_backlog_tracker() {
    throw std::runtime_error("unimplemented");
}

}
