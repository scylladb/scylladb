/*
 * Copyright (C) 2018-present ScyllaDB
 */
/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include "compaction_backlog_manager.hh"
#include "size_tiered_compaction_strategy.hh"
#include <cmath>

// Backlog for one SSTable under STCS:
//
//   (1) Bi = Ei * log4 (T / Si),
//
// where Ei is the effective size of the SStable, Si is the Size of this
// SSTable, and T is the total size of the Table.
//
// To calculate the backlog, we can use the logarithm in any base, but we choose
// 4 as that is the historical minimum for the number of SSTables being compacted
// together. Although now that minimum could be lifted, this is still a good number
// of SSTables to aim for in a compaction execution.
//
// T, the total table size, is defined as
//
//   (2) T = Sum(i = 0...N) { Si }.
//
// Ei, the effective size, is defined as
//
//   (3) Ei = Si - Ci,
//
// where Ci is the total amount of bytes already compacted for this table.
// For tables that are not under compaction, Ci = 0 and Si = Ei.
//
// Using the fact that log(a / b) = log(a) - log(b), we rewrite (1) as:
//
//   Bi = Ei log4(T) - Ei log4(Si)
//
// For the entire Table, the Aggregate Backlog (A) is
//
//   A = Sum(i = 0...N) { Ei * log4(T) - Ei * log4(Si) },
//
// which can be expressed as a sum of a table component and a SSTable component:
//
//   A = Sum(i = 0...N) { Ei } * log4(T) - Sum(i = 0...N) { Ei * log4(Si) },
//
// and if we define C = Sum(i = 0...N) { Ci }, then we can write
//
//   A = (T - C) * log4(T) - Sum(i = 0...N) { (Si - Ci)* log4(Si) }.
//
// Because the SSTable number can be quite big, we'd like to keep iterations to a minimum.
// We can do that if we rewrite the expression above one more time, yielding:
//
//   (4) A = T * log4(T) - C * log4(T) - (Sum(i = 0...N) { Si * log4(Si) } - Sum(i = 0...N) { Ci * log4(Si) }
//
// When SSTables are added or removed, we update the static parts of the equation, and
// every time we need to compute the backlog we use the most up-to-date estimate of Ci to
// calculate the compacted parts, having to iterate only over the SSTables that are compacting,
// instead of all of them.
//
// For SSTables that are being currently written, we assume that they are a full SSTable in a
// certain point in time, whose size is the amount of bytes currently written. So all we need
// to do is keep track of them too, and add the current estimate to the static part of (4).
class size_tiered_backlog_tracker final : public compaction_backlog_tracker::impl {
    struct sstables_backlog_contribution {
        double value = 0.0f;
        std::unordered_set<sstables::shared_sstable> sstables;
    };

    sstables::size_tiered_compaction_strategy_options _stcs_options;
    int64_t _total_bytes = 0;
    sstables_backlog_contribution _contrib;
    std::unordered_set<sstables::shared_sstable> _all;

    struct inflight_component {
        uint64_t total_bytes = 0;
        double contribution = 0;
    };

    inflight_component compacted_backlog(const compaction_backlog_tracker::ongoing_compactions& ongoing_compactions) const;

    static double log4(double x) {
        double inv_log_4 = 1.0f / std::log(4);
        return log(x) * inv_log_4;
    }

    static sstables_backlog_contribution calculate_sstables_backlog_contribution(const std::vector<sstables::shared_sstable>& all, const sstables::size_tiered_compaction_strategy_options& stcs_options);
public:
    size_tiered_backlog_tracker(sstables::size_tiered_compaction_strategy_options stcs_options) : _stcs_options(stcs_options) {}

    virtual double backlog(const compaction_backlog_tracker::ongoing_writes& ow, const compaction_backlog_tracker::ongoing_compactions& oc) const override;

    // Removing could be the result of a failure of an in progress write, successful finish of a
    // compaction, or some one-off operation, like drop
    // Provides strong exception safety guarantees.
    virtual void replace_sstables(const std::vector<sstables::shared_sstable>& old_ssts, const std::vector<sstables::shared_sstable>& new_ssts) override;

    int64_t total_bytes() const {
        return _total_bytes;
    }
};
