/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "replica/logstor/compaction_policy.hh"
#include "replica/logstor/compaction_rate_controller.hh"
#include "replica/logstor/segment_stats.hh"
#include "replica/logstor/segment_types.hh"
#include "utils/updateable_value.hh"
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <list>
#include <optional>
#include <vector>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/timer.hh>

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

    // What one controller tick reads from the shard.
    struct shard_sample {
        uint64_t available_segments;
        const utilization_histogram& utilization;
        // See segment_stats::dead_segments.
        uint64_t dead_segments;
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
        // Upper bound on the batch cap; the cap actually used is derived from the compaction
        // space floor by make_compaction_limits().
        size_t max_segments_per_compaction;
        // See utilization_threshold_curve.
        utils::updateable_value<double> utilization_threshold;
        utils::updateable_value<double> threshold_anchor;
        utils::updateable_value<double> threshold_ceiling;
        utils::updateable_value<double> min_utilization_threshold;
        seastar::scheduling_group sg;
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
    // Stops the controller and wakes the driver, which then starts no more jobs. stop() waits
    // for the jobs in flight.
    void request_stop() noexcept;
    // The caller must call request_stop() first.
    future<> stop();

    // What automatic compaction may take: the batch cap, and the threshold and the minimum batch
    // the controller applies now.
    compaction_batch_limits automatic_batch_limits() const noexcept;
    // What a compaction that was asked for explicitly may take. It is separate from
    // automatic_batch_limits(), so that a bound of the automatic policy does not apply to it.
    compaction_batch_limits explicit_batch_limits() const noexcept;

    // Tells a driver that waits for candidates that a group can be ranked again: its compaction
    // ended, or compaction on it was enabled again.
    void notify_group_idle() noexcept;

    // How much of the range between the minimum and the maximum shares compaction asks for.
    double shares_demand() const noexcept { return _rate_controller.space_pressure(); }
    const compaction_rate_controller& rate_controller() const noexcept { return _rate_controller; }
    // The threshold curve the configuration asks for now; every input is live-updatable, so it is
    // built where it is used rather than cached.
    utilization_threshold_curve threshold_curve() const noexcept;
    size_t jobs_in_flight() const noexcept { return _jobs_in_flight.size(); }
    const stats& get_stats() const noexcept { return _stats; }

private:
    host& _host;
    config _cfg;
    disk_geometry _geometry{};
    bool _stopping{false};
    stats _stats;

    // compaction_space_floor() for this disk, and the limits derived from it. The segment count and
    // the configured batch bound are fixed for the lifetime of the segment manager, so both are
    // computed once, in start().
    uint64_t _space_floor{0};
    compaction_limits _limits{};

    // The driver, run(): started once, for the lifetime of the shard.
    shared_future<> _completion{make_ready_future<>()};
    // Sized for the static maximum; the driver holds back the difference between it and the derived
    // parallelism, see run().
    seastar::semaphore _sem{max_auto_compaction_parallelism};
    // The scores of the automatic jobs in flight, for the marginal admission gate.
    std::list<compaction_candidate_score> _jobs_in_flight;

    // How long the driver waits when no event wakes it. Each event that can change what the driver
    // waits for signals _work_wanted, so the timeout is only a fallback.
    static constexpr auto wait_timeout = std::chrono::seconds(10);
    // What the driver is waiting for, and the condition variable it waits on. A controller tick
    // signals it when the backlog changes, and notify_group_idle() when a group can be ranked again.
    enum class wait_reason {
        nothing,
        candidates,
    };
    wait_reason _wait{wait_reason::nothing};
    seastar::condition_variable _work_wanted;
    // Counts the calls of notify_group_idle(), so that a call while the driver ranks is not lost.
    uint64_t _idle_notifications{0};
    // What can make a group a candidate: the segments under the threshold, as the backlog estimates
    // them, and the dead segments, which do not wait for a minimum batch and which the estimate
    // cannot see die in the lowest bucket.
    struct candidate_counts {
        double under_threshold;
        uint64_t dead;

        bool operator==(const candidate_counts&) const noexcept = default;
    };
    // As of the last controller tick.
    candidate_counts _candidate_counts{};
    // The counts before the last ranking that came back empty; a driver waiting for candidates is
    // woken when they change. Nothing means "wake on anything".
    std::optional<candidate_counts> _candidates_at_empty_ranking;

    // The rate controller has its own timer, because backlog_controller::adjust() does not call the
    // shares callback while compaction_static_shares sets the shares, and the threshold and the wake
    // of the driver must continue then.
    compaction_rate_controller _rate_controller;
    timer<lowres_clock> _rate_timer;

    void tick_rate_controller() noexcept;
    future<> wait_for_work(wait_reason);
    future<> run();
    // Whether the marginal admission gate refuses `c`, a batch that would run next to the jobs in
    // flight, against the best batch of its ranking. Counts and logs a refusal.
    bool refuse_marginal(const candidate& c, const compaction_candidate_score& admission_bar);
    // Submits the job of `c` under `limits`. It holds `units` of the driver's semaphore until it
    // finishes.
    void start_job(candidate c, compaction_batch_limits limits, semaphore_units<> units);
};

} // namespace replica::logstor
