/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "incremental_backlog_tracker.hh"
#include "compaction.hh"
#include "sstables/sstables.hh"
#include "sstables/sstable_set.hh"

namespace compaction {

incremental_backlog_tracker::inflight_component incremental_backlog_tracker::compacted_backlog(const backlog_calculation_result& contribution,
        const compaction_backlog_tracker::ongoing_compactions& ongoing_compactions) {
    inflight_component in;
    for (auto& crp : ongoing_compactions) {
        if (!contribution.sstable_runs_contributing_backlog.contains(crp.first->run_identifier())) {
            continue;
        }
        // Ci is left untaxed on purpose: the fixed cost belongs to the sstable existing,
        // not to the compaction reading it, so it's only retired once the sstable leaves
        // the set. Taxing Ci would cancel the tax added to Si for every sstable being
        // compacted, and prorating it would only decay it earlier.
        auto compacted = crp.second->compacted();
        in.total_bytes += compacted;
        in.contribution += compacted * log4((crp.first->data_size()));
    }
    return in;
}

incremental_backlog_tracker::backlog_calculation_result
incremental_backlog_tracker::calculate_sstables_backlog_contribution(const compaction_backlog_source& src, const incremental_compaction_strategy_options& options) {
    return calculate_runs_backlog_contribution(src.sstables_for_backlog()->all_sstable_runs(), src.schema()->min_compaction_threshold(), options);
}

incremental_backlog_tracker::backlog_calculation_result
incremental_backlog_tracker::calculate_runs_backlog_contribution(const std::vector<sstables::frozen_sstable_run>& runs, int min_threshold,
        const incremental_compaction_strategy_options& options) {
    backlog_calculation_result result;

    // Only runs eligible for compaction are accounted for, e.g. the ones still waiting
    // for view building are left out.
    std::vector<sstables::frozen_sstable_run> all;
    for (auto& run : runs) {
        if (is_eligible_for_compaction(run)) {
            result.total_bytes += run->data_size();
            all.push_back(run);
        }
    }

    if (!all.empty()) {
      for (auto& bucket : incremental_compaction_strategy::get_buckets(all, options)) {
        if (!incremental_compaction_strategy::is_bucket_interesting(bucket, min_threshold)) {
            continue;
        }
        for (const sstables::frozen_sstable_run& run_ptr : bucket) {
            auto& run = *run_ptr;
            auto data_size = run.data_size();
            if (data_size > 0) {
                // Si is taxed with the fixed cost of every sstable in the run, where it
                // weighs the work, but not inside the log. See sstable_backlog_fixed_cost.
                auto size = effective_backlog_size(data_size, run.all().size());
                result.total_backlog_bytes += size;
                result.sstables_backlog_contribution += size * log4(data_size);
                result.sstable_runs_contributing_backlog.insert((*run.all().begin())->run_identifier());
            }
        }
      }
    }
    return result;
}

incremental_backlog_tracker::incremental_backlog_tracker(incremental_compaction_strategy_options options) : _options(std::move(options)) {}

double incremental_backlog_tracker::backlog(const compaction_backlog_source& src, const compaction_backlog_tracker::ongoing_writes& ow, const compaction_backlog_tracker::ongoing_compactions& oc) const {
    if (_backlog_dirty) {
        _contribution = calculate_sstables_backlog_contribution(src, _options);
        _backlog_dirty = false;
    }
    return backlog_of(_contribution, oc);
}

double incremental_backlog_tracker::backlog_of(const backlog_calculation_result& contribution, const compaction_backlog_tracker::ongoing_compactions& oc) {
    inflight_component compacted = compacted_backlog(contribution, oc);

    // Bail out if effective backlog is zero
    if (contribution.total_backlog_bytes <= compacted.total_bytes) {
        return 0;
    }

    // Formula for each SSTable is (Si - Ci) * log(T / Si)
    // Which can be rewritten as: ((Si - Ci) * log(T)) - ((Si - Ci) * log(Si))
    //
    // For the meaning of each variable, please refer to the doc in size_tiered_backlog_tracker.hh

    // Sum of (Si - Ci) for all SSTables contributing backlog
    auto effective_backlog_bytes = contribution.total_backlog_bytes - compacted.total_bytes;

    // Sum of (Si - Ci) * log (Si) for all SSTables contributing backlog
    auto sstables_contribution = contribution.sstables_backlog_contribution - compacted.contribution;
    // This is subtracting ((Si - Ci) * log (Si)) from ((Si - Ci) * log(T)), yielding the final backlog
    auto b = (effective_backlog_bytes * log4(contribution.total_bytes)) - sstables_contribution;
    return b > 0 ? b : 0;
}

void incremental_backlog_tracker::replace_sstables(const std::vector<sstables::shared_sstable>& old_ssts, const std::vector<sstables::shared_sstable>& new_ssts) {
    // Defer backlog contribution recalculation to the next backlog() call,
    // avoiding O(N^2) cost when many sstables are added in a batch (e.g. boot).
    _backlog_dirty = true;
}

}
