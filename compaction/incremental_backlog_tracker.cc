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
        if (!_last_result.sstable_runs_contributing_backlog.contains(crp.first->run_identifier())) {
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
    size_t threshold = incremental_compaction_strategy_options::effective_min_threshold(src.schema()->min_compaction_threshold(), src.compaction_enforce_min_threshold());
    size_t max_threshold = src.schema()->max_compaction_threshold();
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

    size_t eligible_tiers = 0;
    std::vector<pending_tier> pending_tiers;
    const double mean_flush_size = src.stats().rates.mean_flush_size;
    if (!all.empty()) {
      auto buckets = incremental_compaction_strategy::get_buckets(all, options);
      // The mean size of the runs of each bucket, ascending, as runs are put in buckets in
      // ascending order of size.
      std::vector<double> run_sizes;
      for (const auto& [bucket, bucket_size] : buckets) {
          run_sizes.push_back(double(bucket_size) / bucket.size());
      }
      // The flush tier, the first whose runs are the size of the flushed sstables or larger,
      // which every flushed byte passes through. The tiers above it are each a compaction
      // their data went through on its way up.
      size_t flush_tier = std::ranges::lower_bound(run_sizes, flush_tier_size(mean_flush_size, options)) - run_sizes.begin();
      for (size_t i = 0; i < buckets.size(); i++) {
        auto& bucket = buckets[i].first;
        if (!incremental_compaction_strategy::is_bucket_interesting(bucket, threshold)) {
            continue;
        }
        eligible_tiers++;
        // The runs the tier's next compaction reads, at most what one compaction takes.
        // Runs being compacted are still in the set and count: the tier has work until
        // the compaction ends.
        auto runs = std::min(bucket.size(), max_threshold);
        pending_tiers.push_back(pending_tier{
            .runs = runs,
            .run_size = run_sizes[i],
            .ingestion = compaction::ingestion_rate_from_sstables(bucket, min_ingestion_span),
            .tiers_below = i > flush_tier ? i - flush_tier : 0,
        });
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
        .eligible_tiers = eligible_tiers,
        .pending_tiers = std::move(pending_tiers),
        .mean_flush_size = mean_flush_size,
    };
}

void incremental_backlog_tracker::refresh(const compaction_backlog_source& src) const {
    if (!_backlog_dirty) {
        return;
    }
    _last_result = calculate_sstables_backlog_contribution(src, _options);
    _backlog_dirty = false;
}

double incremental_backlog_tracker::read_discount(double threshold, double sstables_per_read) {
    return std::clamp(sstables_per_read / threshold, 1 / threshold, 1.0);
}

compaction_rates incremental_backlog_tracker::rates(const compaction_backlog_source& src) const {
    refresh(src);
    auto stats = src.stats();
    double threshold = incremental_compaction_strategy_options::effective_min_threshold(src.schema()->min_compaction_threshold(), src.compaction_enforce_min_threshold());

    // A tier with r runs of size S holds r * S bytes for compaction to read, and is due
    // again once threshold more runs arrive, threshold * S / inflow seconds later. To end
    // its compaction by then compaction has to read
    //
    //     r * S / (threshold * S / inflow + refill_tax)
    //
    // which, but for the tax, is inflow * r / threshold: S cancels, a bigger tier's
    // compaction is longer, but so is its refill. Summed over the tiers with enough runs
    // to compact, it is the rate at which each current compaction ends when the next one
    // of its tier is due: about the inflow per eligible tier, since each holds at least
    // threshold runs, and more in proportion to the runs past the threshold. The tiers are
    // the write amplification as it stands, each one more rewrite of what is ingested,
    // rather than an estimate of it from the size of the table. A tier below the threshold
    // has nothing to compact and asks for nothing, so a table whose tree is in shape has a
    // target of 0. A tier holding more than threshold runs asks for a repayment_refills'th
    // of the excess on top, so that it shrinks by that much every refill:
    //
    //     (threshold + (r - threshold) / repayment_refills) * S / (threshold * S / inflow + refill_tax)
    //
    // Every flushed byte reaches the tier whose runs are the size of the flushed sstables,
    // i.e. no smaller than bucket_low times their mean size, as ICS tiers them, so its
    // inflow is the rate the table flushes at. Less reaches the tiers above it: a
    // compaction keeps one version of each row, so overwrites and deletions shrink what moves up. In
    // round 8 write-heavy, the second and third tiers above the flush tier received 54%
    // and 21% of the flushed bytes; charging them all of it made the target four times
    // the flush rate, which no shares reached, while compaction kept up with what there
    // was to compact. A tier above the flush tier is charged the flushed bytes times what
    // regular compaction kept of what it read, over the last two hours, once per tier below
    // it. That's one ratio for all tiers, though the higher ones keep less: in round 8
    // write-heavy it charged the first tier above 63% instead of 86%, the second 40%
    // instead of 54% and the third 25% instead of 21%. A tier of the tiers below that is
    // momentarily empty isn't counted, and the tier is charged more. A tier of runs smaller
    // than the flushed sstables, which tiny flushes build while ICS keeps them apart from
    // the rest (see tiny_sstables_written_recently), receives only the flushes of its size:
    // its inflow is the rate its own data was ingested at, from its sstables, which can't
    // be above the flush rate. Charging it the whole flush rate would ask for its few bytes
    // to be compacted every fraction of a second. When the window has seen no flush, after
    // a restart or once writes stopped for longer than it covers, the rate each tier's own
    // data was ingested at stands in for all of them. Whatever the inflow, refill_tax keeps
    // a tier from asking for more than its bytes per refill_tax.
    //
    // How much of the runs past the threshold the target asks to be gone by then depends
    // on who pays for them. A point read touches one sstable per run that holds a version
    // of its key, so the sstables a read touches, over the threshold, is how far the reads
    // pay for a tier's worth of excess: see read_discount. While a tier is pending the
    // target never drops below the ingestion rate, whatever the reads: every flushed byte
    // is compacted at least once, so the number of sstables stays bounded while compaction
    // is deferred.
    auto sstables_per_read = stats.read_amplification.mean_sstables_read();
    auto discount = read_discount(threshold, sstables_per_read);
    const auto& flushed = stats.rates.flushed_bytes;
    const bool flushing = flushed.amount() > 0;
    // What a compaction keeps of what it reads; all of it until a regular compaction ended.
    const auto& compacted_input = stats.rates.completed_input_bytes;
    const double kept = compacted_input.amount() > 0 ? std::clamp(stats.rates.completed_output_bytes.amount() / compacted_input.amount(), 0.0, 1.0) : 1.0;
    utils::rate<utils::units::bytes> needed;
    for (const auto& tier : _last_result.pending_tiers) {
        auto inflow = tier.ingestion;
        if (flushing) {
            // The flush tier and those above derive their inflow from the flush rate; a tier of smaller runs, from its own ingestion rate, up to the flush rate.
            // The flush tier is the one the tiers were placed by when last computed, as their tiers_below were.
            bool inflow_from_flush_rate = tier.run_size >= flush_tier_size(_last_result.mean_flush_size, _options);
            if (inflow_from_flush_rate || flushed.per_second() < inflow.per_second()) {
                inflow = flushed;
            }
            if (inflow_from_flush_rate) {
                inflow = inflow * std::pow(kept, tier.tiers_below);
            }
        }
        double per_second = 0;
        if (inflow.per_second() > 0) {
            auto refill = threshold * tier.run_size / inflow.per_second() + std::chrono::duration<double>(refill_tax).count();
            auto runs_for_next_refill = threshold + (tier.runs - threshold) / repayment_refills;
            per_second = runs_for_next_refill * tier.run_size / refill * discount;
            needed = needed + utils::rate<utils::units::bytes>(per_second * inflow.interval().count(), inflow.interval());
        }
    }
    auto target = needed;
    if (flushing) {
        target = _last_result.eligible_tiers == 0 ? flushed * 0.0 : (needed.per_second() > flushed.per_second() ? needed : flushed);
    }

    return compaction_rates{
        .target = target,
        .achieved = stats.rates.compacted_bytes,
        .achieved_long = stats.rates.compacted_bytes_long,
    };
}

incremental_backlog_tracker::incremental_backlog_tracker(incremental_compaction_strategy_options options) : _options(std::move(options)) {}

double incremental_backlog_tracker::backlog(const compaction_backlog_source& src, const compaction_backlog_tracker::ongoing_writes& ow, const compaction_backlog_tracker::ongoing_compactions& oc) const {
    refresh(src);

    inflight_component compacted = compacted_backlog(oc);

    // Bail out if effective backlog is zero
    if (_last_result.total_backlog_bytes <= compacted.total_bytes) {
        return 0;
    }

    // Formula for each SSTable is (Si - Ci) * log(T / Si)
    // Which can be rewritten as: ((Si - Ci) * log(T)) - ((Si - Ci) * log(Si))
    //
    // For the meaning of each variable, please refer to the doc in size_tiered_backlog_tracker.hh

    // Sum of (Si - Ci) for all SSTables contributing backlog
    auto effective_backlog_bytes = _last_result.total_backlog_bytes - compacted.total_bytes;

    // Sum of (Si - Ci) * log (Si) for all SSTables contributing backlog
    auto sstables_contribution = _last_result.sstables_backlog_contribution - compacted.contribution;
    // This is subtracting ((Si - Ci) * log (Si)) from ((Si - Ci) * log(T)), yielding the final backlog
    auto b = (effective_backlog_bytes * log4(_last_result.total_bytes)) - sstables_contribution;
    return b > 0 ? b : 0;
}

void incremental_backlog_tracker::replace_sstables(const std::vector<sstables::shared_sstable>& old_ssts, const std::vector<sstables::shared_sstable>& new_ssts) {
    // Defer backlog contribution recalculation to the next backlog() call,
    // avoiding O(N^2) cost when many sstables are added in a batch (e.g. boot).
    _backlog_dirty = true;
}

}
