/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/compaction_driver.hh"
#include "replica/logstor/compaction.hh"
#include <exception>
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

compaction_driver::compaction_driver(host& h, config cfg)
    : _host(h)
    , _cfg(std::move(cfg))
{}

void compaction_driver::start(disk_geometry geometry) {
    _geometry = geometry;
    refresh_free_segment_watermarks();
    _limits = make_compaction_limits(_free_segment_watermarks);

    _held_back.adopt(consume_units(_job_slots, max_auto_compaction_parallelism - _limits.auto_parallelism));

    logstor_logger.debug("Compaction parallelism {}, up to {} segments per compaction",
            _limits.auto_parallelism, _limits.batch_cap);

    _trigger_threshold_observer.emplace(_cfg.trigger_threshold.observe([this] (double new_threshold) {
        logstor_logger.debug("Compaction trigger threshold changed to {}", new_threshold);
        refresh_free_segment_watermarks();
        schedule();
    }));
    _completion = shared_future(with_scheduling_group(_cfg.sg, [this] {
        return run();
    }).handle_exception([] (std::exception_ptr ep) {
        logstor_logger.warn("The automatic compaction driver stopped after a failure: {}", ep);
    }));
}

void compaction_driver::request_stop() noexcept {
    _stopping = true;
    _work_wanted.broken();
}

future<> compaction_driver::stop() {
    return _completion.get_future();
}

compaction_batch_limits compaction_driver::automatic_batch_limits() const noexcept {
    return compaction_batch_limits{
        .batch_cap = _limits.batch_cap,
    };
}

compaction_batch_limits compaction_driver::explicit_batch_limits() const noexcept {
    return compaction_batch_limits{
        .batch_cap = _limits.batch_cap,
    };
}

double compaction_driver::shares_demand() const noexcept {
    return compaction_shares_pressure(_host.sample().available_segments, _free_segment_watermarks);
}

void compaction_driver::refresh_free_segment_watermarks() noexcept {
    _free_segment_watermarks = make_free_segment_watermarks(_geometry.segment_count, _cfg.trigger_threshold());

    logstor_logger.debug("Free segment target {} (stop at {})",
            _free_segment_watermarks.low, _free_segment_watermarks.high);
}

bool compaction_driver::should_run() noexcept {
    const auto available_segments = _host.sample().available_segments;
    const auto watermarks = _free_segment_watermarks;
    const auto wanted = auto_compaction_wanted(_running, available_segments, watermarks);

    if (_running && !wanted) {
        logstor_logger.debug("Stopping auto compaction: available segments {} is above high watermark {}",
                available_segments, watermarks.high);
    } else if (!_running && wanted) {
        logstor_logger.debug("Starting auto compaction: available segments {} is below the free segment target {}",
                available_segments, watermarks.low);
    }
    _running = wanted;
    return wanted;
}

void compaction_driver::schedule() {
    if (_wait == wait_reason::trigger && should_run()) {
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
            co_await wait_for_work(wait_reason::failure, failure_retry_delay);
        }
    }

    co_await wait_for_jobs_in_flight();
}

future<> compaction_driver::run_step(ranking_state& ranking) {
    if (!_host.can_submit_compaction() || !should_run()) {
        // Compaction is disabled on the shard, or the free segments are above the target. The
        // driver waits until a segment allocation finds them below the target.
        ranking = {};
        co_await wait_for_work(wait_reason::trigger);
        co_return;
    }

    auto slot = co_await get_units(_job_slots, 1);
    if (_stopping || !should_run()) {
        co_return;
    }

    if (ranking.candidates.empty() && !co_await rank(ranking)) {
        slot.return_all();
        co_await wait_for_work(wait_reason::candidates, empty_ranking_delay);
        co_return;
    }

    auto c = std::move(ranking.candidates.back());
    ranking.candidates.pop_back();
    // can_start() confirms that the group is still registered, so the driver can read it.
    if (!_host.can_start(c.group) || !automatic_compaction_allowed(*c.group)) {
        co_return;
    }

    start_job(std::move(c), std::move(slot));
}

future<bool> compaction_driver::rank(ranking_state& ranking) {
    auto ranked = co_await _host.find_top_compaction_candidates(automatic_batch_limits(), _limits.auto_parallelism,
            automatic_compaction_allowed);
    if (_stopping) {
        co_return false;
    }
    if (ranked.candidates.empty()) {
        co_return false;
    }

    ranking = {.candidates = std::move(ranked.candidates)};
    co_return true;
}

void compaction_driver::start_job(candidate c, semaphore_units<> units) {
    (void)_host.submit_normal_compaction(*c.group, automatic_batch_limits()).handle_exception([] (std::exception_ptr ep) {
        logstor_logger.warn("Automatic logstor compaction failed: {}. Ignored", ep);
    }).finally([units = std::move(units)] {});
}

future<> compaction_driver::wait_for_jobs_in_flight() {
    _held_back.return_all();
    co_await get_units(_job_slots, max_auto_compaction_parallelism);
}

} // namespace replica::logstor
