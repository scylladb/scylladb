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
    // the cache is updated on the first backlog() call after a change.
    mutable bool _backlog_dirty = true;

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
    };
    // The backlog contribution of the sstables, as last calculated, recalculated lazily
    // when _backlog_dirty is set.
    mutable backlog_calculation_result _last_result;

    // Recalculates the cached fields if the sstables changed since.
    void refresh(const compaction_backlog_source& src) const;

public:
    static double log4(double x) {
        static const double inv_log_4 = 1.0f / std::log(4);
        return log(x) * inv_log_4;
    }

    static backlog_calculation_result calculate_sstables_backlog_contribution(const compaction_backlog_source& src, const incremental_compaction_strategy_options& options);

    incremental_backlog_tracker(incremental_compaction_strategy_options options);

    virtual double backlog(const compaction_backlog_source& src, const compaction_backlog_tracker::ongoing_writes& ow, const compaction_backlog_tracker::ongoing_compactions& oc) const override;
    // FIXME: compute the target from the strategy's model.
    virtual compaction_rates rates(const compaction_backlog_source& src) const override {
        return achieved_compaction_rates(src);
    }

    // The replaced sstables are already reflected in the group's sstable set, so this
    // only invalidates the cached backlog contribution.
    virtual void replace_sstables(const std::vector<sstables::shared_sstable>& old_ssts, const std::vector<sstables::shared_sstable>& new_ssts) override;
};

}
