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

// The only difference to size tiered backlog tracker is that it will calculate
// backlog contribution using total bytes of each sstable run instead of total
// bytes of an individual sstable object.
//
// The runs are taken directly from the sstable set of the compaction group the
// backlog is computed for, rather than being maintained by the tracker on every
// sstable replacement.
class incremental_backlog_tracker final : public compaction_backlog_tracker::impl {
    incremental_compaction_strategy_options _options;

    // Cached backlog contribution fields, recalculated lazily when _backlog_dirty is set.
    // Marked mutable because they are caches updated on the first backlog() or rates()
    // call after a change.
    mutable bool _backlog_dirty = true;
    mutable int64_t _total_bytes = 0;
    mutable int64_t _total_backlog_bytes = 0;
    mutable double _sstables_backlog_contribution = 0.0f;
    mutable std::unordered_set<sstables::run_id> _sstable_runs_contributing_backlog;
    // The shape of the tree, for the compaction rates: the runs eligible for compaction,
    // the tiers they fall into, and the rate at which the data of the tiers ready to be
    // compacted was ingested.
    mutable size_t _runs = 0;
    mutable size_t _tiers = 0;
    mutable utils::rate<utils::units::bytes> _ingestion_rate_from_sstables;

    // URGENCY_FACTOR last reported, which only moves once the tree's shape moved it by
    // more than urgency_hysteresis, so that small changes in the shape don't keep
    // moving the target.
    mutable double _urgency_factor = 1;
    static constexpr double min_urgency_factor = 0.25;
    static constexpr double max_urgency_factor = 4;
    static constexpr double urgency_hysteresis = 1.25;

    // Shortest period the data of a tier is considered ingested over. It keeps a tier
    // whose data was ingested in a burst, or whose span is unknown, from demanding to
    // be compacted at an arbitrary rate.
    static constexpr std::chrono::minutes min_ingestion_span{10};

    struct inflight_component {
        int64_t total_bytes = 0;
        double contribution = 0;
    };

    inflight_component compacted_backlog(const compaction_backlog_tracker::ongoing_compactions& ongoing_compactions) const;

    struct backlog_calculation_result {
        int64_t total_bytes;
        int64_t total_backlog_bytes;
        float sstables_backlog_contribution;
        std::unordered_set<sstables::run_id> sstable_runs_contributing_backlog;
        size_t runs;
        size_t tiers;
        utils::rate<utils::units::bytes> ingestion_rate_from_sstables;
    };

    // Recalculates the cached fields if the sstables changed since.
    void refresh(const compaction_backlog_source& src) const;
    double urgency_factor(const read_amplification_stats& read_amplification, double threshold) const;

public:
    static double log4(double x) {
        static const double inv_log_4 = 1.0f / std::log(4);
        return log(x) * inv_log_4;
    }

    static backlog_calculation_result calculate_sstables_backlog_contribution(const compaction_backlog_source& src, const incremental_compaction_strategy_options& options);

    incremental_backlog_tracker(incremental_compaction_strategy_options options);

    virtual double backlog(const compaction_backlog_source& src, const compaction_backlog_tracker::ongoing_writes& ow, const compaction_backlog_tracker::ongoing_compactions& oc) const override;
    // The target is max(ingestion_rate * (WA - 1), ingestion_rate_from_sstables) * URGENCY_FACTOR.
    //
    // Compaction rewrites ingested data once per tier above the size it's flushed at,
    // log_f(T / F) times, the flush being its first write: WA - 1, with T the size of
    // the runs eligible for compaction, F the mean flush size, no smaller than
    // min_sstable_size, and f min_threshold.
    //
    // ingestion_rate_from_sstables is the rate at which the data of the tiers ready to
    // be compacted was ingested, from the ingestion metadata of their sstables. It
    // covers what the moving average of flushes doesn't, e.g. after a restart.
    //
    // URGENCY_FACTOR compares the number of runs, which range scans read, and the mean
    // number of sstables point reads read, with the most runs the tree holds when no
    // tier is overdue, (tiers - 1) * (f - 1) + f, i.e. f - 1 runs in each tier and one
    // tier reaching f. It's clamped to [0.25, 4].
    virtual compaction_rates rates(const compaction_backlog_source& src) const override;

    // The replaced sstables are already reflected in the group's sstable set, so this
    // only invalidates the cached backlog contribution.
    virtual void replace_sstables(const std::vector<sstables::shared_sstable>& old_ssts, const std::vector<sstables::shared_sstable>& new_ssts) override;
};

}
