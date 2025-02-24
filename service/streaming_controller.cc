/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <exception>
#include <seastar/core/coroutine.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/smp.hh>
#include <stdexcept>

#include "service/streaming_controller.hh"
#include "seastar/core/abort_source.hh"
#include "utils/log.hh"

namespace service {

static logging::logger logger("streaming_controller");

streaming_controller::streaming_controller(config cfg, utils::disk_space_monitor& dsm, abort_source& as)
    : _cfg(std::move(cfg))
    , _disk_space_monitor(dsm)
    , _abort_source(as)
    , _sem(1)
    , _observe([this] { return adjust_if_needed(); })
    , _min_shares_observer(_cfg.min_shares.observe(_observe.make_observer()))
    , _max_shares_observer(_cfg.max_shares.observe(_observe.make_observer()))
    , _disk_utilization_threshold_observer(_cfg.disk_utilization_threshold.observe(_observe.make_observer()))
    , _monitor_disk_space(dsm.listen([this] (const utils::disk_space_monitor&) { return adjust_if_needed(); }))
    , _abort_subscription(as.subscribe([this] () noexcept { abort(); }))
{
    if (_abort_subscription) {
        // awaited indirectly in stop()
        (void)_observe.trigger();
    } else {
        abort();
    }
}

void streaming_controller::abort() noexcept {
    _min_shares_observer.disconnect();
    _max_shares_observer.disconnect();
    _disk_utilization_threshold_observer.disconnect();
    _monitor_disk_space.disconnect();
}

future<> streaming_controller::stop() {
    abort();
    co_await _observe.join();
}

void streaming_controller::validate_config() const {
    auto min_shares = _cfg.min_shares.get();
    auto max_shares = _cfg.max_shares.get();
    auto threshold = _cfg.disk_utilization_threshold.get();
    if (min_shares <= 0) {
        throw std::runtime_error(format("min_shares={} must be greater than 0", min_shares));
    }
    if (max_shares <= 0) {
        throw std::runtime_error(format("max_shares={} must be greater than 0", max_shares));
    }
    if (max_shares < min_shares) {
        throw std::runtime_error(format("max_shares={} must be greater than or equal to min_shares={}", max_shares, min_shares));
    }
    if (threshold < 0 || threshold > 1) {
        throw std::runtime_error(format("disk_utilization_threshold={} must be greater than 0 and less than 1", threshold));
    }
}

future<> streaming_controller::adjust_if_needed() const {
    if (_abort_source.abort_requested()) {
        return make_ready_future();
    }
    try {
        validate_config();
    } catch (...) {
        logger.error("{}", std::current_exception());
        return make_ready_future();
    }
    unsigned min_shares = _cfg.min_shares.get();
    unsigned shares = min_shares;
    unsigned max_shares = _cfg.max_shares.get();
    float threshold = _cfg.disk_utilization_threshold.get();
    float disk_utilization = _disk_space_monitor.disk_utilization();
    float util_factor = disk_utilization - threshold;
    if (util_factor > 0) {
        util_factor /= (1. - threshold);
        shares += std::ceil((max_shares - shares) * util_factor);
    }
    logger.debug("min_shares={} max_shares={} disk_utilization_threshold={} disk_utilization={}: adjusted shares={}",
        min_shares, max_shares, threshold, disk_utilization, shares);
    if (shares == _cfg.streaming_sg.get_shares()) {
        logger.debug("Streaming shares kept as {}", shares);
        return make_ready_future();
    }
    logger.info("Adjusting streaming shares to {}{}", shares,
            util_factor > 0 ? fmt::format(" due to high disk utilization of {:.1f}%%", disk_utilization * 100) : "");
    return smp::invoke_on_all([shares, sg = _cfg.streaming_sg] () mutable {
        sg.set_shares(shares);
    });
}

} // namespace service
