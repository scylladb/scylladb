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

    // Whether the sstables changed since _last_result was calculated. Marked mutable, as
    // the cache is updated on the first backlog() or rates() call after a change.
    mutable bool _backlog_dirty = true;
    // A tier with enough runs to compact: the runs its next compaction reads, at most what
    // one compaction takes, the mean size of its runs, the rate at which its data was
    // ingested, from the ingestion metadata of its sstables, and the tiers of runs no
    // smaller than the flushed sstables below it, each a compaction its data went through.
    struct pending_tier {
        size_t runs;
        double run_size;
        utils::rate<utils::units::bytes> ingestion;
        size_t tiers_below;
    };

    // The sstables a point read touched, on average, over the threshold, between
    // 1 / threshold and 1: how far the reads pay for the tiers past the threshold. A read
    // touching threshold sstables, or more, pays for a tier's worth of runs that should
    // have been compacted, and the target stands; a read touching fewer, because the runs
    // hold no version of its key, as under unique keys, pays less, and the target is
    // discounted in proportion, to no less than 1 / threshold, where a tier settles at
    // threshold^2 runs. No reads at all, as in a bulk load, are reads touching nothing.
    static double read_discount(double threshold, double sstables_per_read);
    // The smallest mean run size of the flush tier, the tier of runs the size of the flushed
    // sstables: bucket_low times their mean size, as ICS tiers them.
    static double flush_tier_size(double mean_flush_size, const incremental_compaction_strategy_options& options) {
        return mean_flush_size * options.bucket_low;
    }

    // Shortest period the data of a tier is considered ingested over. It keeps a tier
    // whose data was ingested in a burst, or whose span is unknown, from demanding to
    // be compacted at an arbitrary rate.
    static constexpr std::chrono::minutes min_ingestion_span{10};

    // The refills over which a tier holding more runs than the threshold is asked to
    // compact the excess away, a repayment_refills'th of it every refill: steadily, but
    // not as fast as the tier fills. The tier never asks for less than its inflow, so the
    // excess shrinks every refill; asked to clear it within a single refill, a tier of 16
    // runs at threshold 4 asked for four times its inflow, and a few such tiers more than
    // compaction could do at any shares. The longer the repayment, though, the larger the
    // excess a tier settles at before it asks for what compaction does: in the simulator,
    // over 4 refills, bulk's runs and moderate's sstables per read grew well past what 1
    // left; 2 halves the ask of a tier of 16 runs at threshold 4 and about half the growth.
    static constexpr double repayment_refills = 2;

    // Added to the refill period of every tier, so that a tier refilled within a fraction
    // of a second, e.g. one of tiny runs while tiny sstables are being flushed, asks for no
    // more than the bytes it holds per refill_tax.
    static constexpr std::chrono::seconds refill_tax{1};

    struct inflight_component {
        int64_t total_bytes = 0;
        double contribution = 0;
    };

    inflight_component compacted_backlog(const compaction_backlog_tracker::ongoing_compactions& ongoing_compactions) const;

    struct backlog_calculation_result {
        int64_t total_bytes = 0;
        int64_t total_backlog_bytes = 0;
        float sstables_backlog_contribution = 0.0f;
        std::unordered_set<sstables::run_id> sstable_runs_contributing_backlog;
        // The effective threshold the tiers were found eligible by. It can change while the
        // sstables don't, e.g. with compaction_enforce_min_threshold or once bootstrap ends,
        // so the result is recalculated then too.
        unsigned threshold = 0;
        // The tiers with enough runs to compact: what the target is made of.
        size_t eligible_tiers = 0;
        std::vector<pending_tier> pending_tiers;
        // The mean size of the flushed sstables the tiers were placed by, relative to the
        // flush tier, so rates() places them by the same one.
        double mean_flush_size = 0;
    };
    // The backlog contribution of the sstables and the tiers they fall into, as last
    // calculated, recalculated lazily when _backlog_dirty is set.
    mutable backlog_calculation_result _last_result;

    // The runs a tier has to hold for ICS to compact it, as the strategy finds it now.
    static unsigned effective_min_threshold(const compaction_backlog_source& src) noexcept;

    // Recalculates _last_result if the sstables, or the effective threshold, changed since.
    void refresh(const compaction_backlog_source& src) const;

public:
    static double log4(double x) {
        static const double inv_log_4 = 1.0f / std::log(4);
        return log(x) * inv_log_4;
    }

    static backlog_calculation_result calculate_sstables_backlog_contribution(const compaction_backlog_source& src, const incremental_compaction_strategy_options& options);

    incremental_backlog_tracker(incremental_compaction_strategy_options options);

    virtual double backlog(const compaction_backlog_source& src, const compaction_backlog_tracker::ongoing_writes& ow, const compaction_backlog_tracker::ongoing_compactions& oc) const override;
    // The target is the sum, over the tiers holding at least min_threshold runs, of what
    // each needs to compact by the time the tier has min_threshold new runs, i.e. the next
    // compaction is due: the threshold's worth of runs plus a repayment_refills'th of the
    // runs past it, times run_size, over the tier's refill period, min_threshold * run_size
    // / inflow + refill_tax, times the read discount, insofar as the reads pay for the runs
    // past the threshold. The target is no lower than the
    // ingestion rate while a tier is pending, whatever the reads. A tier below the
    // threshold has nothing to compact and asks for nothing. The tiers stand in for the
    // write amplification: each is one more rewrite of what is ingested.
    //
    // A tier's inflow is the group's flushed bytes when its runs are the size of the
    // flushed sstables, which every flushed byte passes through; a tier of larger runs
    // receives what the compactions of the tiers below kept of them, the flushed bytes times
    // what regular compaction wrote over what it read, once per tier below; a tier of
    // smaller runs only receives the flushes of its size, and its inflow is the rate its
    // data was ingested at, from the ingestion metadata of its sstables, no higher than
    // the flushed bytes. When nothing was flushed within the rate window, e.g. after a
    // restart, that rate stands in for every tier.
    virtual compaction_rates rates(const compaction_backlog_source& src) const override;

    // The replaced sstables are already reflected in the group's sstable set, so this
    // only invalidates the cached backlog contribution.
    virtual void replace_sstables(const std::vector<sstables::shared_sstable>& old_ssts, const std::vector<sstables::shared_sstable>& new_ssts) override;
};

}
