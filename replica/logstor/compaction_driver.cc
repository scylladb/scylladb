/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/compaction_driver.hh"
#include "replica/logstor/compaction.hh"
#include <exception>
#include <optional>
#include <utility>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/log.hh>

namespace replica::logstor {

extern seastar::logger logstor_logger;

compaction_driver::compaction_driver(host& h, config cfg)
    : _host(h)
    , _cfg(std::move(cfg))
    , _rate_timer([this] { tick_rate_controller(); })
{}

void compaction_driver::start(disk_geometry geometry) {
    _geometry = geometry;
    set_space_floor(_cfg.space_floor());
    _space_floor_observer.emplace(_cfg.space_floor.observe([this] (double fraction) {
        logstor_logger.info("Updating logstor compaction space floor to {}", fraction);
        set_space_floor(fraction);
    }));

    // Tick once now, so that the state of the controller is defined from the start.
    tick_rate_controller();
    _rate_timer.arm_periodic(compaction_rate_control_period);
    // The driver runs for the lifetime of the shard. The candidates decide when a job starts, and
    // not the free level.
    _completion = shared_future(with_scheduling_group(_cfg.sg, [this] {
        return run();
    }).handle_exception([] (std::exception_ptr ep) {
        logstor_logger.warn("Automatic logstor compaction failed: {}. Ignored", ep);
    }));
}

void compaction_driver::request_stop() noexcept {
    _stopping = true;
    _rate_timer.cancel();
    _work_wanted.broken();
}

void compaction_driver::set_space_floor(double fraction) noexcept {
    if (fraction > max_space_floor_fraction) {
        logstor_logger.warn("logstor_compaction_space_floor {} is above the maximum of {}. The maximum is used",
                fraction, max_space_floor_fraction);
    }
    _space_floor = compaction_space_floor(_geometry.segment_count, fraction);
    _limits = make_compaction_limits(_space_floor, max_segments_per_compaction);

    // The jobs in flight keep their slots. When the parallelism decreases, the semaphore goes below
    // zero, and no job starts until enough of them end. After the stop, run() waits for all the slots.
    if (!_stopping) {
        const auto held_back = max_auto_compaction_parallelism - _limits.auto_parallelism;
        if (_held_back.count() > held_back) {
            _held_back.return_units(_held_back.count() - held_back);
        } else {
            _held_back.adopt(consume_units(_sem, held_back - _held_back.count()));
        }
    }

    logstor_logger.debug("Compaction space floor {} segments, compaction parallelism {}, up to {} segments per compaction",
            _space_floor, _limits.auto_parallelism, _limits.batch_cap);
}

future<> compaction_driver::stop() {
    auto f = co_await coroutine::as_future(_completion.get_future());
    f.ignore_ready_future();
}

compaction_batch_limits compaction_driver::automatic_batch_limits() const noexcept {
    return compaction_batch_limits{
        .batch_cap = _limits.batch_cap,
        .min_batch = _rate_controller.min_batch(),
        .max_utilization = _rate_controller.utilization_threshold(),
    };
}

compaction_batch_limits compaction_driver::explicit_batch_limits() const noexcept {
    return compaction_batch_limits{
        .batch_cap = _limits.batch_cap,
    };
}

utilization_threshold_curve compaction_driver::threshold_curve() const noexcept {
    return make_utilization_threshold_curve(_geometry.segment_count, _space_floor,
            _cfg.utilization_threshold(), _cfg.threshold_anchor(),
            _cfg.threshold_ceiling(), _cfg.min_utilization_threshold());
}

void compaction_driver::tick_rate_controller() noexcept {
    const auto shard = _host.sample();

    _rate_controller.tick({
        .available_segments = shard.available_segments,
        .curve = threshold_curve(),
        .utilization = shard.utilization,
    });
    _candidate_counts = candidate_counts{
        .under_threshold = _rate_controller.backlog().candidate_segments,
        .dead = shard.dead_segments,
    };

    // A driver that found nothing to compact is told when the shard's candidates are not the ones
    // it looked at, or when the controller is switched on.
    if (_wait == wait_reason::candidates && _rate_controller.enabled()
            && _candidate_counts != _candidates_at_empty_ranking) {
        _work_wanted.signal();
    }
}

void compaction_driver::notify_group_idle() noexcept {
    ++_idle_notifications;
    if (_wait == wait_reason::candidates) {
        _work_wanted.signal();
    }
}

future<> compaction_driver::wait_for_work(wait_reason reason) {
    _wait = reason;
    auto f = co_await coroutine::as_future(_work_wanted.wait(wait_timeout));
    _wait = wait_reason::nothing;
    if (f.failed()) {
        // A timeout, or the condition variable broken by request_stop(); the loop re-checks either way.
        f.ignore_ready_future();
    }
}

future<> compaction_driver::run() {
    std::vector<candidate> pending;
    // The limits that ranked `pending`. Its jobs select their batches by these limits and not by
    // the limits of a later tick, so that a job selects by the rule its candidate was ranked by.
    compaction_batch_limits pending_limits{};
    // What the marginal-admission gate below measures a candidate against: the best batch of the
    // ranking that produced `pending` that copies data.
    std::optional<compaction_candidate_score> admission_bar;

    while (!_stopping) {
        if (!_host.can_submit_compaction() || !_rate_controller.enabled()) {
            // Compaction is disabled, or the threshold is. As far as the controller is concerned
            // that is the same as having nothing to reclaim.
            pending.clear();
            _candidates_at_empty_ranking.reset();
            co_await wait_for_work(wait_reason::candidates);
            continue;
        }

        auto units = co_await get_units(_sem, 1);

        if (_stopping) {
            break;
        }

        if (pending.empty()) {
            // The policy is read once per ranking, so that every group is ranked by the same rule.
            const auto limits = automatic_batch_limits();
            // Read before the ranking, which can yield, so that a change that a tick sees during the
            // ranking wakes the driver.
            const auto candidates_before_ranking = _candidate_counts;
            const auto idle_notifications_before_ranking = _idle_notifications;
            auto ranked = co_await _host.find_top_compaction_candidates(limits, _limits.auto_parallelism);
            if (_stopping) {
                break;
            }
            pending = std::move(ranked.candidates);
            if (pending.empty()) {
                // No group has a batch under the threshold with a net gain, or each such group is
                // compacting, as on a shard with one tablet while its job runs. The driver waits
                // until the backlog changes or a group can be ranked again.
                _candidates_at_empty_ranking = candidates_before_ranking;
                units.return_all();
                // A group that became idle while the ranking ran can have a batch now.
                if (_idle_notifications == idle_notifications_before_ranking) {
                    co_await wait_for_work(wait_reason::candidates);
                }
                continue;
            }
            pending_limits = limits;
            admission_bar.reset();
        }

        auto c = std::move(pending.back());
        pending.pop_back();

        // The candidates come best first, so the first one that copies data is the best batch that
        // copies data. A batch of dead segments has an infinite efficiency, so it does not set the
        // bar: no batch that copies data can reach a fraction of it.
        if (!admission_bar && c.score.live_bytes != 0) {
            admission_bar = c.score;
        }

        if (!_host.can_start(c.group)) {
            continue;
        }

        if (!_jobs_in_flight.empty()) {
            // A job in flight that reclaims better than this ranking's best also sets the bar. The
            // ranking skips a group that is compacting, so without it, a ranking made while the
            // best group compacts would admit the next best group however poor it is.
            auto bar = admission_bar;
            for (const auto& job : _jobs_in_flight) {
                if (job.live_bytes != 0 && (!bar || *bar < job)) {
                    bar = job;
                }
            }
            if (bar && refuse_marginal(c, *bar)) {
                // The ranking is best first, so no later candidate passes either, and the bar does
                // not change until a job ends. Wait for one job to end, and then rank again. The
                // wait counts the jobs that end, and not the free slots, because a change of the
                // space floor also changes the slots.
                pending.clear();
                units.return_all();
                const auto jobs_ended = _jobs_ended;
                while (_jobs_ended == jobs_ended && !_stopping) {
                    co_await wait_for_work(wait_reason::job_end);
                }
                continue;
            }
        }

        // A live update can disable compaction while the driver waits for a slot or for a ranking.
        // Check again, so that no job starts after the driver sees it.
        if (!_host.can_submit_compaction() || !_rate_controller.enabled()) {
            pending.clear();
            continue;
        }

        start_job(std::move(c), pending_limits, std::move(units));
    }

    // Wait for the jobs still in flight, which is what makes this future the driver's completion.
    _held_back.return_all();
    co_await get_units(_sem, max_auto_compaction_parallelism);
}

bool compaction_driver::refuse_marginal(const candidate& c, const compaction_candidate_score& admission_bar) {
    // A parallel job can take only a group that is not compacting. When one tablet holds the
    // garbage of the shard, the other jobs rewrite groups with almost no garbage, so a parallel job
    // must reclaim almost as well as the best batch.
    if (c.score.efficiency_at_least(admission_bar, compaction_marginal_admission_ratio)) {
        return false;
    }
    ++_stats.batches_refused;
    return true;
}

void compaction_driver::start_job(candidate c, compaction_batch_limits limits, semaphore_units<> units) {
    auto job = _jobs_in_flight.insert(_jobs_in_flight.end(), c.score);
    // The slot is released only once the job is done, which is what wakes the driver up to submit
    // the next candidate.
    (void)_host.submit_normal_compaction(*c.group, limits).handle_exception([] (std::exception_ptr ep) {
        logstor_logger.warn("Automatic logstor compaction failed: {}. Ignored", ep);
    }).finally([this, job, units = std::move(units)] {
        _jobs_in_flight.erase(job);
        ++_jobs_ended;
        if (_wait == wait_reason::job_end) {
            _work_wanted.signal();
        }
        notify_group_idle();
    });
}

} // namespace replica::logstor
