/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <algorithm>

#include "incremental_backlog_tracker.hh"
#include "compaction.hh"
#include "sstables/sstables.hh"
#include "sstables/sstable_set.hh"

namespace compaction {

incremental_backlog_tracker::inflight_component incremental_backlog_tracker::compacted_backlog(const compaction_backlog_tracker::ongoing_compactions& ongoing_compactions) const {
    inflight_component in;
    for (auto& crp : ongoing_compactions) {
        if (!_sstable_runs_contributing_backlog.contains(crp.first->run_identifier())) {
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
    auto threshold = src.schema()->min_compaction_threshold();
    int64_t total_bytes = 0;
    int64_t total_backlog_bytes = 0;
    float sstables_backlog_contribution = 0.0f;
    std::unordered_set<sstables::run_id> sstable_runs_contributing_backlog = {};

    // Only runs eligible for compaction are accounted for, e.g. the ones still waiting
    // for view building are left out.
    std::vector<sstables::frozen_sstable_run> all;
    for (auto& run : src.sstables_for_backlog()->all_sstable_runs()) {
        if (is_eligible_for_compaction(run)) {
            total_bytes += run->data_size();
            all.push_back(run);
        }
    }

    size_t tiers = 0;
    utils::rate<utils::units::bytes> ingestion_rate_from_sstables;
    if (!all.empty()) {
      auto buckets = incremental_compaction_strategy::get_buckets(all, options);
      tiers = buckets.size();
      for (auto& bucket : buckets) {
        if (!incremental_compaction_strategy::is_bucket_interesting(bucket, threshold)) {
            continue;
        }
        ingestion_rate_from_sstables = ingestion_rate_from_sstables + compaction::ingestion_rate_from_sstables(bucket, min_ingestion_span);
        for (const sstables::frozen_sstable_run& run_ptr : bucket) {
            auto& run = *run_ptr;
            auto data_size = run.data_size();
            if (data_size > 0) {
                // Si is taxed with the fixed cost of every sstable in the run, where it
                // weighs the work, but not inside the log. See sstable_backlog_fixed_cost.
                auto size = effective_backlog_size(data_size, run.all().size());
                total_backlog_bytes += size;
                sstables_backlog_contribution += size * log4(data_size);
                sstable_runs_contributing_backlog.insert((*run.all().begin())->run_identifier());
            }
        }
      }
    }
    return backlog_calculation_result{
        .total_bytes = total_bytes,
        .total_backlog_bytes = total_backlog_bytes,
        .sstables_backlog_contribution = sstables_backlog_contribution,
        .sstable_runs_contributing_backlog = std::move(sstable_runs_contributing_backlog),
        .runs = all.size(),
        .tiers = tiers,
        .ingestion_rate_from_sstables = ingestion_rate_from_sstables,
    };
}

void incremental_backlog_tracker::refresh(const compaction_backlog_source& src) const {
    if (!_backlog_dirty) {
        return;
    }
    auto result = calculate_sstables_backlog_contribution(src, _options);
    _total_bytes = result.total_bytes;
    _total_backlog_bytes = result.total_backlog_bytes;
    _sstables_backlog_contribution = result.sstables_backlog_contribution;
    _sstable_runs_contributing_backlog = std::move(result.sstable_runs_contributing_backlog);
    _runs = result.runs;
    _tiers = result.tiers;
    _ingestion_rate_from_sstables = result.ingestion_rate_from_sstables;
    _backlog_dirty = false;
}

double incremental_backlog_tracker::urgency_factor(const read_amplification_stats& read_amplification, double threshold) const {
    // The most runs the tree holds when no tier is overdue.
    auto expected_runs = (double(_tiers) - 1) * (threshold - 1) + threshold;
    auto urgency = std::max(_runs / expected_runs, read_amplification.mean_sstables_read() / expected_runs);
    urgency = std::clamp(urgency, min_urgency_factor, max_urgency_factor);
    if (urgency >= _urgency_factor * urgency_hysteresis || urgency <= _urgency_factor / urgency_hysteresis
            || urgency == min_urgency_factor || urgency == max_urgency_factor) {
        _urgency_factor = urgency;
    }
    return _urgency_factor;
}

compaction_rates incremental_backlog_tracker::rates(const compaction_backlog_source& src) const {
    refresh(src);
    auto stats = src.stats();
    double threshold = src.schema()->min_compaction_threshold();

    // Compaction has to process each ingested byte once per rewrite, e.g. at the rate of
    // ingestion when data is rewritten once (WA = 2), as achieved counts its input bytes.
    //
    // T reflects how much of the data survives overwrites, deletes and expiration, so the
    // number of rewrites follows the actual depth of the tree. But it assumes every ingested
    // byte reaches every tier, while each merge keeps only part of its inputs, so the target
    // is too high when little survives: by a factor of rewrites / (1 + s + ... + s^(rewrites - 1))
    // for a fraction s surviving each merge.
    // FIXME: correct by the ratio of output to input bytes of regular compaction, should
    // overwrite-heavy or short-TTL workloads turn out to be compacted too eagerly.
    auto flush_size = std::max(stats.rates.mean_flush_size, double(_options.min_sstable_size));
    auto rewrites = _total_bytes > flush_size ? std::log(_total_bytes / flush_size) / std::log(threshold) : 0;
    auto from_ingestion = stats.rates.flushed_bytes * rewrites;
    auto target = from_ingestion.per_second() >= _ingestion_rate_from_sstables.per_second() ? from_ingestion : _ingestion_rate_from_sstables;

    return compaction_rates{
        .target = target * urgency_factor(stats.read_amplification, threshold),
        .achieved = stats.rates.compacted_bytes,
    };
}

incremental_backlog_tracker::incremental_backlog_tracker(incremental_compaction_strategy_options options) : _options(std::move(options)) {}

double incremental_backlog_tracker::backlog(const compaction_backlog_source& src, const compaction_backlog_tracker::ongoing_writes& ow, const compaction_backlog_tracker::ongoing_compactions& oc) const {
    refresh(src);

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

void incremental_backlog_tracker::replace_sstables(const std::vector<sstables::shared_sstable>& old_ssts, const std::vector<sstables::shared_sstable>& new_ssts) {
    // Defer backlog contribution recalculation to the next backlog() call,
    // avoiding O(N^2) cost when many sstables are added in a batch (e.g. boot).
    _backlog_dirty = true;
}

}
