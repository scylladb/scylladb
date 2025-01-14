/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "incremental_backlog_tracker.hh"
#include "sstables/sstables.hh"
#include <boost/range/adaptor/map.hpp>

using namespace sstables;

incremental_backlog_tracker::inflight_component incremental_backlog_tracker::compacted_backlog(const compaction_backlog_tracker::ongoing_compactions& ongoing_compactions) const {
    inflight_component in;
    for (auto& crp : ongoing_compactions) {
        if (!_sstable_runs_contributing_backlog.contains(crp.first->run_identifier())) {
            continue;
        }
        auto compacted = crp.second->compacted();
        in.total_bytes += compacted;
        in.contribution += compacted * log4((crp.first->data_size()));
    }
    return in;
}

incremental_backlog_tracker::backlog_calculation_result
incremental_backlog_tracker::calculate_sstables_backlog_contribution(const std::unordered_map<sstables::run_id, sstable_run>& all, const incremental_compaction_strategy_options& options,  unsigned threshold) {
    int64_t total_backlog_bytes = 0;
    float sstables_backlog_contribution = 0.0f;
    std::unordered_set<sstables::run_id> sstable_runs_contributing_backlog = {};

    if (!all.empty()) {
      auto freeze = [] (const sstable_run& run) { return make_lw_shared<const sstable_run>(run); };
      for (auto& bucket : incremental_compaction_strategy::get_buckets(all | std::views::values | std::views::transform(freeze) | std::ranges::to<std::vector>(), options)) {
        if (!incremental_compaction_strategy::is_bucket_interesting(bucket, threshold)) {
            continue;
        }
        for (const frozen_sstable_run& run_ptr : bucket) {
            auto& run = *run_ptr;
            auto data_size = run.data_size();
            if (data_size > 0) {
                total_backlog_bytes += data_size;
                sstables_backlog_contribution += data_size * log4(data_size);
                sstable_runs_contributing_backlog.insert((*run.all().begin())->run_identifier());
            }
        }
      }
    }
    return backlog_calculation_result{
        .total_backlog_bytes = total_backlog_bytes,
        .sstables_backlog_contribution = sstables_backlog_contribution,
        .sstable_runs_contributing_backlog = std::move(sstable_runs_contributing_backlog),
    };
}

incremental_backlog_tracker::incremental_backlog_tracker(incremental_compaction_strategy_options options) : _options(std::move(options)) {}

double incremental_backlog_tracker::backlog(const compaction_backlog_tracker::ongoing_writes& ow, const compaction_backlog_tracker::ongoing_compactions& oc) const {
    inflight_component compacted = compacted_backlog(oc);

    // Bail out if effective backlog is zero
    if (_total_backlog_bytes <= compacted.total_bytes) {
        return 0;
    }

    // Formula for each SSTable is (Si - Ci) * log(T / Si)
    // Which can be rewritten as: ((Si - Ci) * log(T)) - ((Si - Ci) * log(Si))
    //
    // For the meaning of each variable, please refer to the doc in size_tiered_backlog_tracker.hh

    // Sum of (Si - Ci) for all SSTables contributing backlog
    auto effective_backlog_bytes = _total_backlog_bytes - compacted.total_bytes;

    // Sum of (Si - Ci) * log (Si) for all SSTables contributing backlog
    auto sstables_contribution = _sstables_backlog_contribution - compacted.contribution;
    // This is subtracting ((Si - Ci) * log (Si)) from ((Si - Ci) * log(T)), yielding the final backlog
    auto b = (effective_backlog_bytes * log4(_total_bytes)) - sstables_contribution;
    return b > 0 ? b : 0;
}

// Removing could be the result of a failure of an in progress write, successful finish of a
// compaction, or some one-off operation, like drop
void incremental_backlog_tracker::replace_sstables(const std::vector<sstables::shared_sstable>& old_ssts, const std::vector<sstables::shared_sstable>& new_ssts) {
    auto all = _all;
    auto total_bytes = _total_bytes;
    auto threshold = _threshold;
    auto backlog_calculation_result = incremental_backlog_tracker::backlog_calculation_result{};
    for (auto&& sst : new_ssts) {
    if (sst->data_size() > 0) {
        // note: we don't expect failed insertions since each sstable will be inserted once
        (void)all[sst->run_identifier()].insert(sst);
        total_bytes += sst->data_size();
        // Deduce threshold from the last SSTable added to the set
        threshold = sst->get_schema()->min_compaction_threshold();
    }
    }

    bool exhausted_input_run = false;
    for (auto&& sst : old_ssts) {
    if (sst->data_size() > 0) {
        auto run_identifier = sst->run_identifier();
        all[run_identifier].erase(sst);
        if (all[run_identifier].all().empty()) {
            all.erase(run_identifier);
            exhausted_input_run = true;
        }
        total_bytes -= sst->data_size();
    }
    }
    // Backlog contribution will only be refreshed when an input SSTable run was exhausted by
    // compaction, so to avoid doing it for each exhausted fragment, which would be both
    // overkill and expensive.
    if (exhausted_input_run) {
        backlog_calculation_result = calculate_sstables_backlog_contribution(all, _options, threshold);
    }

    // commit calculations
    std::invoke([&] () noexcept {
        _all = std::move(all);
        _total_bytes = total_bytes;
        _threshold = threshold;

        if (exhausted_input_run) {
            _total_backlog_bytes = backlog_calculation_result.total_backlog_bytes;
            _sstables_backlog_contribution = backlog_calculation_result.sstables_backlog_contribution;
            _sstable_runs_contributing_backlog = std::move(backlog_calculation_result.sstable_runs_contributing_backlog);
        }
    });
}
