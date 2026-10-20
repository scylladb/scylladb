/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "replica/logstor/compaction_policy.hh"
#include "replica/logstor/segment_types.hh"
#include "utils/updateable_value.hh"
#include <cstddef>
#include <cstdint>
#include <list>
#include <optional>
#include <vector>
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_future.hh>

namespace replica::logstor {

class logstor_group;

// Runs automatic compaction on one shard: decides when a job starts, on which group, and with
// which batch limits. compaction_manager_impl owns it and implements its host. It runs from
// start() until stop(), for the lifetime of the segment manager.
class compaction_driver {
public:
    // A batch that one group offers, from a ranking.
    struct candidate {
        logstor_group* group;
        std::vector<log_segment_id> segments;
        compaction_candidate_score score;
    };

    struct ranking {
        // Worst first, as top_compaction_candidates::take_sorted() returns them.
        std::vector<candidate> candidates;
    };

    // What the driver reads from the shard.
    struct shard_sample {
        uint64_t available_segments;
    };

    // What the driver needs from the compaction manager.
    class host {
    public:
        virtual ~host() = default;
        // Whether compaction is enabled and admission is open.
        virtual bool can_submit_compaction() const noexcept = 0;
        // The best `max_candidates` groups by the batch each would run under `limits`.
        virtual future<ranking> find_top_compaction_candidates(const compaction_batch_limits& limits, size_t max_candidates) = 0;
        // Whether the group of a candidate is still registered, idle and enabled. `group` is
        // compared only, because the group can be gone.
        virtual bool can_start(const logstor_group* group) const noexcept = 0;
        virtual future<> submit_normal_compaction(logstor_group&, compaction_batch_limits) = 0;
        virtual shard_sample sample() const noexcept = 0;
    };

    struct config {
        // Upper bound on the batch cap; the cap actually used is derived from the free-segment
        // target by make_compaction_limits().
        size_t max_segments_per_compaction;
        utils::updateable_value<double> trigger_threshold;
    };

    // The segment count is known only after segment_manager_impl is constructed, so it comes
    // with start().
    struct disk_geometry {
        uint64_t segment_count;
        uint64_t segment_size;
    };

    struct stats {
        uint64_t batches_refused{0};
    };

    compaction_driver(host&, config);

    void start(disk_geometry);
    // Starts no more runs. stop() waits for the run in flight.
    void request_stop() noexcept;
    // The caller must call request_stop() first.
    future<> stop();

    // Starts a run if the free segments are below the target and no run is active.
    void schedule();

    // What automatic compaction may take.
    compaction_batch_limits automatic_batch_limits() const noexcept;
    // What a compaction that was asked for explicitly may take. It is separate from
    // automatic_batch_limits(), so that a bound of the automatic policy does not apply to it.
    compaction_batch_limits explicit_batch_limits() const noexcept;

    double shares_demand() const noexcept;
    const stats& get_stats() const noexcept { return _stats; }

private:
    host& _host;
    config _cfg;
    disk_geometry _geometry{};
    bool _stopping{false};
    stats _stats;
    // Registered in start(), because the watermarks need the segment count.
    std::optional<utils::observer<double>> _trigger_threshold_observer;

    // Cached result of make_free_segment_watermarks(). should_run() calls it on every segment
    // allocation, so the cache avoids the same computation on each allocation.
    free_segment_watermarks _free_segment_watermarks{};
    compaction_limits _limits{};

    shared_future<> _completion{make_ready_future<>()};
    // Sized for the static maximum; a run holds back the difference between it and the current
    // limit for its duration, see run().
    seastar::semaphore _sem{max_auto_compaction_parallelism};
    // The scores of the automatic jobs in flight, for the marginal admission gate.
    std::list<compaction_candidate_score> _jobs_in_flight;

    bool active() const noexcept {
        return !_completion.available();
    }

    // Refreshes the cached free segment watermarks and the compaction limits derived from them.
    // Called from start() and whenever the trigger threshold changes; the segment count and the
    // configured batch bound are fixed for the lifetime of the segment manager, so the threshold
    // is the only other input.
    void refresh_free_segment_watermarks() noexcept;
    bool should_run() noexcept;
    future<> run();
    // Whether the marginal admission gate refuses `c`, a batch that would run next to the jobs in
    // flight, against the best batch of its ranking. Counts and logs a refusal.
    bool refuse_marginal(const candidate& c, const compaction_candidate_score& admission_bar);
    // Submits the job of `c`, which holds `units` of the driver's semaphore until it finishes.
    void start_job(candidate c, semaphore_units<> units);
};

} // namespace replica::logstor
