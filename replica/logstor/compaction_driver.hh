/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "replica/logstor/compaction_policy.hh"
#include "utils/updateable_value.hh"
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <list>
#include <optional>
#include <vector>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_future.hh>
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

    // The values that the driver reads from the shard.
    struct shard_sample {
        uint64_t available_segments;
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
        utils::updateable_value<double> trigger_threshold;
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
    // Wakes the driver, which then starts no more jobs. stop() waits for the jobs in flight.
    void request_stop() noexcept;
    // The caller must call request_stop() first.
    future<> stop();

    // Wakes the driver when it waits for the free segments to fall below the target, and they are
    // below the target now.
    void schedule();

    // Returns the limits of automatic compaction.
    compaction_batch_limits automatic_batch_limits() const noexcept;
    // Returns the limits of a compaction that was asked for explicitly, which takes any batch with a
    // net gain. They are separate from automatic_batch_limits(), so that a bound of the automatic
    // policy does not apply to such a compaction.
    compaction_batch_limits explicit_batch_limits() const noexcept;

    // Tells a driver that waits for candidates that a group can be ranked again: its compaction
    // ended, or compaction on it was enabled again.
    void notify_group_idle() noexcept;

    double shares_demand() const noexcept;
    const driver_stats& stats() const noexcept { return _stats; }

private:
    host& _host;
    config _cfg;
    disk_geometry _geometry{};
    bool _stopping{false};
    driver_stats _stats;
    // Registered in start(), because the watermarks need the segment count.
    std::optional<utils::observer<double>> _trigger_threshold_observer;

    // Cached result of make_free_segment_watermarks(). should_run() calls it on every segment
    // allocation, so the cache avoids the same computation on each allocation.
    free_segment_watermarks _free_segment_watermarks{};
    // Whether the free level is in the band where automatic compaction runs, see
    // auto_compaction_wanted().
    bool _running{false};
    // The limits that the free-segment target gives when the driver starts. The driver computes
    // them only once, so a later change of the trigger threshold does not change them.
    compaction_limits _limits{};

    // The future of run(), which starts once and runs for the lifetime of the shard.
    shared_future<> _completion{make_ready_future<>()};
    // The slots of the automatic jobs. The size is max_auto_compaction_parallelism, which leaves
    // compaction buffers for a split compaction. The driver holds back the difference between the
    // size and the parallelism that the free-segment target allows.
    seastar::semaphore _job_slots{max_auto_compaction_parallelism};
    semaphore_units<> _held_back{consume_units(_job_slots, 0)};
    // The scores of the automatic jobs in flight, for the marginal admission gate.
    std::list<compaction_candidate_score> _jobs_in_flight;

    // The longest time that the driver waits when no event wakes it.
    static constexpr auto wait_timeout = std::chrono::seconds(10);
    // The time that the driver waits after a ranking that found no batch, when no group becomes
    // idle. Writes can give a group a batch, and nothing signals that, so the driver ranks again
    // after this time.
    static constexpr auto empty_ranking_delay = std::chrono::milliseconds(100);
    // The time that the driver waits after a failed step before it starts again. A failure that
    // repeats, for example under memory pressure, then logs at most once in this time.
    static constexpr auto failure_retry_delay = std::chrono::seconds(1);
    // The reason the driver waits, and the condition variable that it waits on. schedule() signals
    // it when the free segments fall below the target, and notify_group_idle() when a group can be
    // ranked again. Nothing signals a wait after a failure, so only the delay or a stop ends it.
    enum class wait_reason {
        nothing,
        trigger,
        candidates,
        failure,
    };
    wait_reason _wait{wait_reason::nothing};
    seastar::condition_variable _work_wanted;
    // Counts the calls of notify_group_idle(), so that a call while the driver ranks is not lost.
    uint64_t _idle_notifications{0};
    // The state before the last ranking that came back empty. The driver does not wait for
    // candidates when a group became idle during the ranking.
    struct ranking_start {
        uint64_t idle_notifications;
    };
    std::optional<ranking_start> _empty_ranking_start;

    // The candidates of the last ranking, worst first, and the best batch of the ranking that
    // copies data, which the marginal admission gate measures against. When `candidates` is empty,
    // the driver has no ranking, and its next step ranks again.
    struct ranking_state {
        std::vector<candidate> candidates;
        std::optional<compaction_candidate_score> bar;
    };

    // Refreshes the cached free segment watermarks. Called from start() and whenever the trigger
    // threshold changes.
    void refresh_free_segment_watermarks() noexcept;
    bool should_run() noexcept;
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
    // Submits the job of `c`, which holds `units` of the driver's semaphore until it finishes.
    void start_job(candidate c, semaphore_units<> units);
    void on_job_end(std::list<compaction_candidate_score>::iterator job) noexcept;
    // Waits for the jobs still in flight, so that the end of run() is the end of the driver.
    future<> wait_for_jobs_in_flight();
};

} // namespace replica::logstor
