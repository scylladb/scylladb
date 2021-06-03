/*
 * Copyright (C) 2018-present ScyllaDB
 */
/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once
#include "sstables/compaction_backlog_manager.hh"
#include <cmath>
#include <ctgmath>

// Backlog for one SSTable under STCS:
//
//   (1) Bi = Ei * log4 (T / Ei),
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
//   Bi = Ei log4(T) - Ei log4(Ei)
//
// For the entire Table, the Aggregate Backlog (A) is
//
//   A = Sum(i = 0...N) { Ei * log4(T) - Ei * log4(Ei) },
//
// which can be expressed as a sum of a table component and a SSTable component:
//
//   A = Sum(i = 0...N) { Ei } * log4(T) - Sum(i = 0...N) { Ei * log4(Ei) },
//
// and if we define C = Sum(i = 0...N) { Ci }, then we can write
//
//   A = (T - C) * log4(T) - Sum(i = 0...N) { (Si - Ci)* log4(Ei) }.
//
// Because the SSTable number can be quite big, we'd like to keep iterations to a minimum.
// We can do that if we rewrite the expression above one more time, yielding:
//
//   (4) A = T * log4(T) - C * log4(T) - (Sum(i = 0...N) { Si * log4(Ei) } - Sum(i = 0...N) { Ci * log4(Ei) }
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
    int64_t _total_bytes = 0;
    double _sstables_backlog_contribution = 0.0f;

    struct inflight_component {
        int64_t total_bytes = 0;
        double contribution = 0;
    };

    inflight_component partial_backlog(const compaction_backlog_tracker::ongoing_writes& ongoing_writes) const;

    inflight_component compacted_backlog(const compaction_backlog_tracker::ongoing_compactions& ongoing_compactions) const;

    double log4(double x) const {
        double inv_log_4 = 1.0f / std::log(4);
        return log(x) * inv_log_4;
    }
public:
    virtual double backlog(const compaction_backlog_tracker::ongoing_writes& ow, const compaction_backlog_tracker::ongoing_compactions& oc) const override;

    virtual void add_sstable(sstables::shared_sstable sst) override;

    // Removing could be the result of a failure of an in progress write, successful finish of a
    // compaction, or some one-off operation, like drop
    virtual void remove_sstable(sstables::shared_sstable sst) override;

    int64_t total_bytes() const {
        return _total_bytes;
    }
};
