/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/compaction_driver.hh"
#include <chrono>
#include <exception>
#include <utility>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/log.hh>

namespace replica::logstor {

extern seastar::logger logstor_logger;

compaction_driver::compaction_driver(host& h, config cfg)
    : _host(h)
    , _cfg(std::move(cfg))
{}

void compaction_driver::start(disk_geometry geometry) {
    _geometry = geometry;
    refresh_free_segment_watermarks();
    _trigger_threshold_observer.emplace(_cfg.trigger_threshold.observe([this] (double new_threshold) {
        logstor_logger.debug("Compaction trigger threshold changed to {}", new_threshold);
        refresh_free_segment_watermarks();
        schedule();
    }));
}

void compaction_driver::request_stop() noexcept {
    _stopping = true;
}

future<> compaction_driver::stop() {
    auto f = co_await coroutine::as_future(_completion.get_future());
    f.ignore_ready_future();
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
    _limits = make_compaction_limits(_free_segment_watermarks, _cfg.max_segments_per_compaction);

    logstor_logger.debug("Free segment target {} (stop at {}), compaction parallelism {}, up to {} segments per compaction",
            _free_segment_watermarks.low, _free_segment_watermarks.high,
            _limits.auto_parallelism, _limits.batch_cap);
}

bool compaction_driver::should_run() noexcept {
    const auto running = active();

    if (_stopping) {
        if (running) {
            logstor_logger.debug("Stopping auto compaction: admission closed");
        }
        return false;
    }

    const auto available_segments = _host.sample().available_segments;
    const auto watermarks = _free_segment_watermarks;

    if (!auto_compaction_wanted(running, available_segments, watermarks)) {
        if (running) {
            logstor_logger.debug("Stopping auto compaction: available segments {} is above high watermark {}",
                    available_segments, watermarks.high);
        }
        return false;
    }

    if (!running) {
        logstor_logger.debug("Starting auto compaction: available segments {} is below the free segment target {}",
                available_segments, watermarks.low);
    }
    return true;
}

void compaction_driver::schedule() {
    if (_host.can_submit_compaction() && should_run() && !active()) {
        _completion = shared_future(run().handle_exception([] (std::exception_ptr ep) {
            logstor_logger.warn("Automatic logstor compaction failed: {}. Ignored", ep);
        }));
    }
}

future<> compaction_driver::run() {
    // The parallelism is snapshotted for the whole run. It only changes with the free-segment
    // target, which is a rarely moved live-updatable setting, and holding back the difference to
    // the semaphore's static size for the duration of the run is what lets the drain below wait
    // for exactly the jobs this run submitted.
    const auto parallelism = _limits.auto_parallelism;
    auto held_back = co_await get_units(_sem, max_auto_compaction_parallelism - parallelism);

    std::vector<candidate> pending;

    while (_host.can_submit_compaction() && should_run()) {
        auto units = co_await get_units(_sem, 1);

        if (!_host.can_submit_compaction() || !should_run()) {
            break;
        }

        if (pending.empty()) {
            auto ranked = co_await _host.find_top_compaction_candidates(automatic_batch_limits(), parallelism);
            pending = std::move(ranked.candidates);
            if (pending.empty()) {
                co_await seastar::sleep(std::chrono::milliseconds(100));
                continue;
            }
        }

        auto c = std::move(pending.back());
        pending.pop_back();

        if (!_host.can_start(c.group)) {
            continue;
        }

        start_job(std::move(c), std::move(units));
    }

    // Wait for submitted jobs to complete.
    co_await get_units(_sem, parallelism);
}

void compaction_driver::start_job(candidate c, semaphore_units<> units) {
    // The slot is released only once the job is done, which is what wakes this fiber up to
    // submit the next candidate.
    (void)_host.submit_normal_compaction(*c.group, automatic_batch_limits()).handle_exception([] (std::exception_ptr ep) {
        logstor_logger.warn("Automatic logstor compaction failed: {}. Ignored", ep);
    }).finally([units = std::move(units)] {});
}

} // namespace replica::logstor
