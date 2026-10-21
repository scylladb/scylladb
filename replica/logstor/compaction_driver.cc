/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/compaction_driver.hh"
#include "replica/logstor/compaction.hh"
#include <algorithm>
#include <exception>
#include <optional>
#include <ranges>
#include <utility>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/log.hh>

namespace replica::logstor {

extern seastar::logger logstor_logger;

// Returns true when automatic compaction may take the group. A table can turn it off, for example
// with nodetool disableautocompaction.
static bool automatic_compaction_allowed(const logstor_group& group) noexcept {
    return !group.automatic_compaction_disabled();
}

// Returns the best of the `scores` that copy data, or nothing when no score copies data. A batch of
// dead segments has an infinite efficiency, so it does not set the bar, because no batch that
// copies data can reach a fraction of an infinite efficiency.
template <std::ranges::input_range Scores>
static std::optional<compaction_candidate_score> best_copying_score(Scores&& scores) {
    std::optional<compaction_candidate_score> best;
    for (const compaction_candidate_score& score : scores) {
        if (score.live_bytes != 0 && (!best || *best < score)) {
            best = score;
        }
    }
    return best;
}

compaction_driver::compaction_driver(host& h, config cfg)
    : _host(h)
    , _cfg(std::move(cfg))
    , _rate_timer([this] { tick_rate_controller(); })
{}

void compaction_driver::start(disk_geometry geometry) {
    _geometry = geometry;
    set_space_floor(_cfg.space_floor());
    _space_floor_observer.emplace(_cfg.space_floor.observe([this] (double fraction) {
        logstor_logger.info("Updating the compaction space floor to {}", fraction);
        set_space_floor(fraction);
    }));

    tick_rate_controller();
    _rate_timer.arm_periodic(compaction_rate_control_period);
    _completion = shared_future(with_scheduling_group(_cfg.sg, [this] {
        return run();
    }).handle_exception([] (std::exception_ptr ep) {
        logstor_logger.warn("The automatic compaction driver stopped after a failure: {}", ep);
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
    _limits = make_compaction_limits(_space_floor);

    // The jobs in flight keep their slots. When the parallelism decreases, the semaphore goes below
    // zero, and no job starts until enough of them end. After the stop, run() waits for all the slots.
    if (!_stopping) {
        const auto held_back = max_auto_compaction_parallelism - _limits.auto_parallelism;
        if (_held_back.count() > held_back) {
            _held_back.return_units(_held_back.count() - held_back);
        } else {
            _held_back.adopt(consume_units(_job_slots, held_back - _held_back.count()));
        }
    }

    logstor_logger.debug("Compaction space floor {} segments, compaction parallelism {}, up to {} segments per compaction",
            _space_floor, _limits.auto_parallelism, _limits.batch_cap);
}

future<> compaction_driver::stop() {
    return _completion.get_future();
}

compaction_batch_limits compaction_driver::automatic_batch_limits() const noexcept {
    return compaction_batch_limits{
        .batch_cap = _limits.batch_cap,
        .min_batch = _rate_controller.min_batch(),
        .utilization_threshold = _rate_controller.utilization_threshold(),
    };
}

compaction_batch_limits compaction_driver::explicit_batch_limits() const noexcept {
    return compaction_batch_limits{
        .batch_cap = _limits.batch_cap,
        .min_batch = 1,
        .utilization_threshold = 1,
    };
}

utilization_threshold_curve compaction_driver::threshold_curve() const noexcept {
    return make_utilization_threshold_curve(_geometry.segment_count, _space_floor, _cfg.utilization_threshold(),
            threshold_curve_anchor, threshold_curve_ceiling, _cfg.min_utilization_threshold());
}

void compaction_driver::tick_rate_controller() noexcept {
    const auto shard = _host.sample();

    _rate_controller.tick({
        .available_segments = shard.available_segments,
        .curve = threshold_curve(),
        .utilization = shard.utilization,
    });
    _ranking_inputs = ranking_inputs{
        .under_threshold = _rate_controller.backlog().candidate_segments,
        .dead = shard.dead_segments,
        .min_batch = _rate_controller.min_batch(),
        .batch_cap = _limits.batch_cap,
    };

    // A driver that found nothing to compact wakes up when the inputs of the selection differ from
    // the inputs of the ranking that found nothing.
    if (_wait == wait_reason::candidates && (!_empty_ranking_start || _ranking_inputs != _empty_ranking_start->inputs)) {
        _work_wanted.signal();
    }
}

void compaction_driver::notify_group_idle() noexcept {
    ++_idle_notifications;
    if (_wait == wait_reason::candidates) {
        _work_wanted.signal();
    }
}

future<> compaction_driver::wait_for_work(wait_reason reason, std::chrono::milliseconds timeout) {
    _wait = reason;
    auto f = co_await coroutine::as_future(_work_wanted.wait(timeout));
    _wait = wait_reason::nothing;
    if (f.failed()) {
        // The wait ended by a timeout or because request_stop() broke the condition variable, and
        // the loop checks the conditions again in both cases.
        f.ignore_ready_future();
    }
}

future<> compaction_driver::run() {
    ranking_state ranking;

    while (!_stopping) {
        auto f = co_await coroutine::as_future(run_step(ranking));
        if (f.failed()) {
            // A failure of one step, for example a bad_alloc in a ranking, must not stop automatic
            // compaction on the shard until a restart. So the driver drops its ranking, waits, and
            // starts again.
            logstor_logger.warn("Automatic compaction failed: {}. It starts again in {}",
                    f.get_exception(), failure_retry_delay);
            ranking = {};
            _empty_ranking_start.reset();
            co_await wait_for_work(wait_reason::failure, failure_retry_delay);
        }
    }

    co_await wait_for_jobs_in_flight();
}

future<> compaction_driver::run_step(ranking_state& ranking) {
    if (!_host.can_submit_compaction()) {
        // Compaction is disabled on the shard. The driver treats this like a shard with nothing to
        // reclaim.
        ranking = {};
        _empty_ranking_start.reset();
        co_await wait_for_work(wait_reason::candidates);
        co_return;
    }

    auto slot = co_await get_units(_job_slots, 1);
    if (_stopping) {
        co_return;
    }

    if (ranking.candidates.empty() && !co_await rank(ranking)) {
        slot.return_all();
        co_await wait_for_candidates();
        co_return;
    }

    // set_space_floor() can change the limits after the ranking, while rank() yields or while the
    // candidates wait for a slot. A job with the old batch cap can hold more than its part of the
    // new space floor, so the driver ranks again.
    if (ranking.limits.batch_cap != _limits.batch_cap) {
        ranking = {};
        co_return;
    }
    // A lower parallelism can leave the slot of this step above the new limit. The slot goes back,
    // and the semaphore keeps the next step waiting until enough jobs end.
    if (_jobs_in_flight.size() >= _limits.auto_parallelism) {
        co_return;
    }

    auto c = std::move(ranking.candidates.back());
    ranking.candidates.pop_back();
    // can_start() confirms that the group is still registered, so the driver can read it.
    if (!_host.can_start(c.group) || !automatic_compaction_allowed(*c.group)) {
        co_return;
    }

    if (auto bar = admission_bar(ranking); bar && refuse_marginal(c, *bar)) {
        // The driver takes the candidates best first, so no later candidate passes either, and the
        // bar does not change until a job ends. Wait for one job to end, and then rank again.
        ranking = {};
        slot.return_all();
        co_await wait_for_job_end();
        co_return;
    }

    start_job(std::move(c), ranking.limits, std::move(slot));
}

future<bool> compaction_driver::rank(ranking_state& ranking) {
    // The policy is read once per ranking, so that every group is ranked by the same rule.
    const auto limits = automatic_batch_limits();
    // The driver reads these inputs before the ranking, because the ranking can yield, so that a
    // change that a tick sees during the ranking, or a group that becomes idle during the ranking,
    // wakes the driver.
    const auto before = ranking_start{
        .inputs = _ranking_inputs,
        .idle_notifications = _idle_notifications,
    };
    auto ranked = co_await _host.find_top_compaction_candidates(limits, _limits.auto_parallelism,
            automatic_compaction_allowed);
    if (_stopping) {
        co_return false;
    }
    if (ranked.candidates.empty()) {
        // No group has a batch under the threshold with a net gain, or each such group is
        // compacting, as on a shard with one compaction group while its job runs.
        _empty_ranking_start = before;
        co_return false;
    }

    // The bar is computed first, because the initializer of `candidates` moves the candidates.
    auto bar = best_copying_score(ranked.candidates | std::views::transform(&candidate::score));
    ranking = {
        .candidates = std::move(ranked.candidates),
        .limits = limits,
        .bar = bar,
    };
    co_return true;
}

future<> compaction_driver::wait_for_candidates() {
    // A group that became idle during the ranking can have a batch now.
    if (_empty_ranking_start && _idle_notifications != _empty_ranking_start->idle_notifications) {
        co_return;
    }
    co_await wait_for_work(wait_reason::candidates);
}

std::optional<compaction_candidate_score> compaction_driver::admission_bar(const ranking_state& ranking) const {
    if (_jobs_in_flight.empty()) {
        return std::nullopt;
    }
    // A job in flight that reclaims better than the best of the ranking also sets the bar. The
    // ranking skips a group that is compacting, so without the jobs in flight, a ranking made while
    // the best group compacts would admit the next best group, even a very poor one.
    return std::max(ranking.bar, best_copying_score(_jobs_in_flight));
}

future<> compaction_driver::wait_for_job_end() {
    // The wait counts the jobs that end, and not the free slots, because a change of the space
    // floor also changes the slots.
    const auto jobs_ended = _jobs_ended;
    while (_jobs_ended == jobs_ended && !_stopping) {
        co_await wait_for_work(wait_reason::job_end);
    }
}

bool compaction_driver::refuse_marginal(const candidate& c, const compaction_candidate_score& admission_bar) {
    // A parallel job can take only a group that is not compacting. When one group holds the dead
    // records of the shard, the other jobs rewrite groups with almost no dead records, so a
    // parallel job must reclaim almost as well as the best batch.
    if (c.score.efficiency_at_least(admission_bar, compaction_marginal_admission_ratio)) {
        return false;
    }
    ++_stats.batches_refused;
    return true;
}

void compaction_driver::start_job(candidate c, compaction_batch_limits limits, semaphore_units<> units) {
    auto job = _jobs_in_flight.insert(_jobs_in_flight.end(), c.score);
    (void)_host.submit_normal_compaction(*c.group, limits).handle_exception([] (std::exception_ptr ep) {
        logstor_logger.debug("An automatic compaction job failed: {}. Ignored", ep);
    }).finally([this, job, units = std::move(units)] {
        on_job_end(job);
    });
}

void compaction_driver::on_job_end(std::list<compaction_candidate_score>::iterator job) noexcept {
    _jobs_in_flight.erase(job);
    ++_jobs_ended;
    if (_wait == wait_reason::job_end) {
        _work_wanted.signal();
    }
    notify_group_idle();
}

future<> compaction_driver::wait_for_jobs_in_flight() {
    _held_back.return_all();
    co_await get_units(_job_slots, max_auto_compaction_parallelism);
}

} // namespace replica::logstor
