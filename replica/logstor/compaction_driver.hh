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
#include <seastar/util/noncopyable_function.hh>

namespace replica::logstor {

class logstor_group;

// Runs automatic compaction on one shard: decides when a job starts, on which group, and with
// which batch limits. compaction_manager_impl owns it and implements its host. It runs from
// start() until stop(), for the lifetime of the segment manager.
class compaction_driver {
public:
    // The score of the batch that one group offers, from a ranking. The job selects its batch again.
    struct candidate {
        logstor_group* group;
        compaction_candidate_score score;
    };

    struct ranking {
        // The candidates are worst first, in the order that
        // top_compaction_candidates::take_sorted() returns them.
        std::vector<candidate> candidates;
    };

    // The values that one controller tick reads from the shard.
    struct shard_sample {
        uint64_t available_segments;
        const utilization_histogram& utilization;
        uint64_t dead_segments;
    };

    // The services that the driver needs from the compaction manager.
    class host {
    public:
        // Returns true when a ranking may take the group.
        using group_filter = noncopyable_function<bool (const logstor_group&)>;

        virtual ~host() = default;
        // Returns true when compaction is enabled and admission is open.
        virtual bool can_submit_compaction() const noexcept = 0;
        // Returns the best `max_candidates` of the groups that `filter` accepts, ranked by the
        // batch that each group would run under `limits`.
        virtual future<ranking> find_top_compaction_candidates(const compaction_batch_limits& limits, size_t max_candidates,
                group_filter filter) = 0;
        // Returns true when the group of a candidate is still registered, idle and enabled. The
        // function only compares `group`, because the group can be gone.
        virtual bool can_start(const logstor_group* group) const noexcept = 0;
        virtual future<> submit_normal_compaction(logstor_group&, compaction_batch_limits) = 0;
        virtual shard_sample sample() const noexcept = 0;
    };

    struct config {
        // The space floor as a fraction of the disk, see compaction_space_floor().
        utils::updateable_value<double> space_floor;
        // See utilization_threshold_curve.
        utils::updateable_value<double> utilization_threshold;
        utils::updateable_value<double> min_utilization_threshold;
        seastar::scheduling_group sg;
    };

    // The segment count is known only after segment_manager_impl is constructed, so it comes
    // with start().
    struct disk_geometry {
        uint64_t segment_count;
    };

    struct driver_stats {
        uint64_t batches_refused{0};
    };

    compaction_driver(host&, config);

    void start(disk_geometry);
    // Stops the controller and wakes the driver, which then starts no more jobs. stop() waits
    // for the jobs in flight.
    void request_stop() noexcept;
    // The caller must call request_stop() first.
    future<> stop();

    // Returns the limits of automatic compaction: the batch cap, and the threshold and the minimum
    // batch that the rate controller applies now.
    compaction_batch_limits automatic_batch_limits() const noexcept;
    // Returns the limits of a compaction that was asked for explicitly, which takes any batch with a
    // net gain. They are separate from automatic_batch_limits(), so that a bound of the automatic
    // policy does not apply to such a compaction.
    compaction_batch_limits explicit_batch_limits() const noexcept;

    // Tells a driver that waits for candidates that a group can be ranked again: its compaction
    // ended, compaction on it was enabled again, or its table turned automatic compaction on
    // again.
    void notify_group_idle() noexcept;

    // Returns the part of the range from the minimum to the maximum compaction shares that
    // compaction asks for, in [0, 1].
    double shares_demand() const noexcept { return _rate_controller.space_pressure(); }
    // Returns the threshold curve of the current configuration. Every input is live-updatable, so
    // the curve is built where it is used and not cached.
    utilization_threshold_curve threshold_curve() const noexcept;
    const driver_stats& stats() const noexcept { return _stats; }

private:
    host& _host;
    config _cfg;
    disk_geometry _geometry{};
    bool _stopping{false};
    driver_stats _stats;

    // compaction_space_floor() for this disk, and the limits derived from it. set_space_floor()
    // computes them again when the configuration changes.
    uint64_t _space_floor{0};
    compaction_limits _limits{};
    std::optional<utils::observer<double>> _space_floor_observer;

    // The future of run(), which starts once and runs for the lifetime of the shard.
    shared_future<> _completion{make_ready_future<>()};
    // The slots of the automatic jobs. The size is max_auto_compaction_parallelism, which leaves
    // compaction buffers for a split compaction. The driver holds back the difference between the
    // size and the parallelism that the space floor allows, so that the jobs in flight do not hold
    // more than the space floor.
    seastar::semaphore _job_slots{max_auto_compaction_parallelism};
    semaphore_units<> _held_back{consume_units(_job_slots, 0)};
    // The scores of the automatic jobs in flight, for the marginal admission gate.
    std::list<compaction_candidate_score> _jobs_in_flight;

    // The longest time that the driver waits when no event wakes it. The events signal
    // _work_wanted, and the timeout covers what they do not show: a segment that crosses the
    // threshold inside one bucket of the utilization histogram, which does not change the
    // backlog estimate.
    static constexpr auto wait_timeout = std::chrono::seconds(10);
    // The time that the driver waits after a failed step before it starts again. A failure that
    // repeats, for example under memory pressure, then logs at most once in this time.
    static constexpr auto failure_retry_delay = std::chrono::seconds(1);
    // The reason the driver waits, and the condition variable that it waits on. A controller tick
    // signals it when the backlog changes, notify_group_idle() when a group can be ranked again,
    // and the end of a job when the marginal admission gate waits for one. Nothing signals a wait
    // after a failure, so only the delay or a stop ends it.
    enum class wait_reason {
        nothing,
        candidates,
        failure,
        job_end,
    };
    wait_reason _wait{wait_reason::nothing};
    seastar::condition_variable _work_wanted;
    // Counts the calls of notify_group_idle(), so that a call while the driver ranks is not lost.
    uint64_t _idle_notifications{0};
    // Counts the automatic jobs that ended, for the marginal admission gate.
    uint64_t _jobs_ended{0};
    // The inputs of the selection that can give a group a batch. They are the segments under the
    // threshold, as the backlog estimates them, the dead segments, and the limits that decide how
    // many candidates a batch needs and can take. The dead segments count apart, because they do
    // not wait for a minimum batch, and the estimate does not show when they die in the lowest
    // bucket.
    struct ranking_inputs {
        double under_threshold;
        uint64_t dead;
        size_t min_batch;
        size_t batch_cap;

        bool operator==(const ranking_inputs&) const noexcept = default;
    };
    // The inputs as of the last controller tick.
    ranking_inputs _ranking_inputs{};
    // The state before the last ranking that came back empty. A driver that waits for candidates is
    // woken when the inputs change, and it does not wait when a group became idle during the
    // ranking. An empty value means that any event wakes the driver.
    struct ranking_start {
        ranking_inputs inputs;
        uint64_t idle_notifications;
    };
    std::optional<ranking_start> _empty_ranking_start;

    // The anchor and the ceiling of the threshold curve, as fractions of the disk. 0 selects the
    // default points: the anchor at twice the space floor, and the ceiling one segment above the
    // anchor.
    static constexpr double threshold_curve_anchor = 0;
    static constexpr double threshold_curve_ceiling = 0;

    compaction_rate_controller _rate_controller;
    timer<lowres_clock> _rate_timer;

    // The candidates of the last ranking, worst first, the limits that ranked them, and the best
    // batch of the ranking that copies data, which the marginal admission gate measures against.
    // When `candidates` is empty, the driver has no ranking, and its next step ranks again.
    struct ranking_state {
        std::vector<candidate> candidates;
        compaction_batch_limits limits{};
        std::optional<compaction_candidate_score> bar;
    };

    void set_space_floor(double fraction) noexcept;
    void tick_rate_controller() noexcept;
    future<> wait_for_work(wait_reason, std::chrono::milliseconds timeout = wait_timeout);
    future<> run();
    // Runs one step of the driver: a wait for work, a ranking, or the start of one job.
    future<> run_step(ranking_state& ranking);
    // Ranks the groups into `ranking`. Returns false when no group has a batch, or when the driver
    // stops.
    future<bool> rank(ranking_state& ranking);
    // Waits after an empty ranking until the candidates can have changed.
    future<> wait_for_candidates();
    // Returns the bar that the marginal admission gate measures a candidate of `ranking` against:
    // the best batch of the ranking that copies data, or a job in flight that reclaims better. It
    // returns nothing while no job runs, so that the first job always starts.
    std::optional<compaction_candidate_score> admission_bar(const ranking_state& ranking) const;
    future<> wait_for_job_end();
    // Returns true when the marginal admission gate refuses `c`, a batch that would run next to the
    // jobs in flight, against `admission_bar`. It counts each refusal in batches_refused.
    bool refuse_marginal(const candidate& c, const compaction_candidate_score& admission_bar);
    // Submits the job of `c` under `limits`. The job holds `units` of the driver's semaphore until
    // the job finishes.
    void start_job(candidate c, compaction_batch_limits limits, semaphore_units<> units);
    void on_job_end(std::list<compaction_candidate_score>::iterator job) noexcept;
    // Waits for the jobs still in flight, so that the end of run() is the end of the driver.
    future<> wait_for_jobs_in_flight();
};

} // namespace replica::logstor
