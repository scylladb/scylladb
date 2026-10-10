/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cmath>

#include "compaction_backlog_manager.hh"
#include "incremental_compaction_strategy.hh"

namespace compaction {

// Backlog for one SSTable run under ICS:
//
//   (1) Bi = Ei * log4 (T / Si),
//
// where Ei is the effective size of the run, Si is the Size of this run, and T is
// the total size of the Table.
//
// To calculate the backlog, we can use the logarithm in any base, but we choose
// 4 as that is the historical minimum for the number of runs being compacted
// together. Although now that minimum could be lifted, this is still a good number
// of runs to aim for in a compaction execution.
//
// T, the total table size, is defined as
//
//   (2) T = Sum(i = 0...N) { Si }.
//
// Ei, the effective size, is defined as
//
//   (3) Ei = Si - Ci,
//
// where Ci is the total amount of bytes already compacted for this run.
// For runs that are not under compaction, Ci = 0 and Si = Ei.
//
// Using the fact that log(a / b) = log(a) - log(b), we rewrite (1) as:
//
//   Bi = Ei log4(T) - Ei log4(Si)
//
// For the entire Table, the Aggregate Backlog (A) is
//
//   A = Sum(i = 0...N) { Ei * log4(T) - Ei * log4(Si) },
//
// which can be expressed as a sum of a table component and a run component:
//
//   A = Sum(i = 0...N) { Ei } * log4(T) - Sum(i = 0...N) { Ei * log4(Si) },
//
// and if we define C = Sum(i = 0...N) { Ci }, then we can write
//
//   A = (T - C) * log4(T) - Sum(i = 0...N) { (Si - Ci)* log4(Si) }.
//
// Because the number of runs can be quite big, we'd like to keep iterations to a minimum.
// We can do that if we rewrite the expression above one more time, yielding:
//
//   (4) A = T * log4(T) - C * log4(T) - (Sum(i = 0...N) { Si * log4(Si) } - Sum(i = 0...N) { Ci * log4(Si) }
//
// When runs are added or removed, we update the static parts of the equation, and
// every time we need to compute the backlog we use the most up-to-date estimate of Ci to
// calculate the compacted parts, having to iterate only over the runs that are compacting,
// instead of all of them. Only runs in a size tier holding at least min_threshold runs
// contribute to the backlog, and the writes in progress are not accounted for.
//
// The runs are taken directly from the sstable set of the compaction group the
// backlog is computed for, rather than being maintained by the tracker on every
// sstable replacement.
class incremental_backlog_tracker final : public compaction_backlog_tracker::impl {
public:
    struct backlog_calculation_result {
        int64_t total_bytes = 0;
        int64_t total_backlog_bytes = 0;
        float sstables_backlog_contribution = 0.0f;
        std::unordered_set<sstables::run_id> sstable_runs_contributing_backlog;
    };
private:
    incremental_compaction_strategy_options _options;

    // Cached backlog contribution, recalculated lazily when _backlog_dirty is set.
    // Marked mutable because it's a cache updated on first backlog() call after a change.
    mutable bool _backlog_dirty = true;
    mutable backlog_calculation_result _contribution;

    struct inflight_component {
        int64_t total_bytes = 0;
        double contribution = 0;
    };

    static inflight_component compacted_backlog(const backlog_calculation_result& contribution, const compaction_backlog_tracker::ongoing_compactions& ongoing_compactions);

public:
    static double log4(double x) {
        static const double inv_log_4 = 1.0f / std::log(4);
        return log(x) * inv_log_4;
    }

    static backlog_calculation_result calculate_sstables_backlog_contribution(const compaction_backlog_source& src, const incremental_compaction_strategy_options& options);

    // The contribution of the given runs, compacted together by ICS, to the backlog. Also used
    // by the strategies applying ICS to a subset of their sstables, e.g. a time window.
    static backlog_calculation_result calculate_runs_backlog_contribution(const std::vector<sstables::frozen_sstable_run>& runs, int min_threshold,
            const incremental_compaction_strategy_options& options);

    // The backlog left of a contribution, given the compactions in progress.
    static double backlog_of(const backlog_calculation_result& contribution, const compaction_backlog_tracker::ongoing_compactions& oc);

    incremental_backlog_tracker(incremental_compaction_strategy_options options);

    virtual double backlog(const compaction_backlog_source& src, const compaction_backlog_tracker::ongoing_writes& ow, const compaction_backlog_tracker::ongoing_compactions& oc) const override;

    // The replaced sstables are already reflected in the group's sstable set, so this
    // only invalidates the cached backlog contribution.
    virtual void replace_sstables(const std::vector<sstables::shared_sstable>& old_ssts, const std::vector<sstables::shared_sstable>& new_ssts) override;
};

// The ICS backlog of a subset of a compaction group's sstables, e.g. a time window or level 0,
// for the strategies that compact such a subset with ICS. Unlike incremental_backlog_tracker,
// it keeps the sstables of the subset itself, as they're replaced, and recalculates their
// contribution to the backlog lazily, on the first backlog() after a change.
class incremental_subset_backlog {
    std::unordered_set<sstables::shared_sstable> _sstables;
    mutable bool _dirty = true;
    mutable incremental_backlog_tracker::backlog_calculation_result _contribution;
public:
    void add(sstables::shared_sstable sst) {
        _sstables.insert(std::move(sst));
        _dirty = true;
    }
    void remove(const sstables::shared_sstable& sst) {
        _dirty |= _sstables.erase(sst) > 0;
    }
    bool empty() const noexcept {
        return _sstables.empty();
    }
    // The backlog of the subset, given the compactions in progress, whose compacted parts are
    // taken off the runs of the subset they read from.
    double backlog(int min_threshold, const incremental_compaction_strategy_options& options,
            const compaction_backlog_tracker::ongoing_compactions& oc) const;
};

}
