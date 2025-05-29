/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#include "replica/out_of_space_controller.hh"
#include "utils/log.hh"

namespace replica {

static logging::logger logger("out_of_space_controller");

out_of_space_controller::out_of_space_controller(utils::disk_space_monitor& dsm, config cfg, abort_source& as)
    : _cfg(std::move(cfg))
    , _abort_source(as)
    , _dsm(dsm)
    , _dsm_subscription(dsm.listen([this](const utils::disk_space_monitor& dsm) -> future<> {
        if (_abort_source.abort_requested()) {
            co_return;
        }

        const float current_disk_utilization = dsm.disk_utilization();
        if (current_disk_utilization < 0.0f) {
            co_return;
        }

        logger.debug("current disk utilization={}", current_disk_utilization);

        const bool old = _critical_disk_utilization_threshold_reached;
        _critical_disk_utilization_threshold_reached = current_disk_utilization > std::clamp(_cfg.critical_disk_utilization_threshold(), 0.0f, 1.0f);

        if (old == _critical_disk_utilization_threshold_reached) {
            co_return co_await notify_only_newly_registered();
        }

        static constexpr auto msg_template = "{} the critical disk utilization level ({:.1f}%%). Current disk utilization {:.1f}%%";
        if (_critical_disk_utilization_threshold_reached) {
            logger.warn(msg_template, "Reached", _cfg.critical_disk_utilization_threshold() * 100, current_disk_utilization * 100);
        } else {
            logger.info(msg_template, "Dropped below", _cfg.critical_disk_utilization_threshold() * 100, current_disk_utilization * 100);
        }

        co_await notify();
    }))
    {
    }

future<> out_of_space_controller::stop() {
    return make_ready_future<>();
}

future<> out_of_space_controller::notify() {
    for (auto& sub : _subscriptions) {
        co_await sub(reached_critical_disk_utilization(_critical_disk_utilization_threshold_reached));
    }
}

future<> out_of_space_controller::notify_only_newly_registered() {
    for (auto& sub : _subscriptions | std::views::filter([] (const subscription& sub) { return sub._registered_not_triggered; })) {
        co_await sub(reached_critical_disk_utilization(_critical_disk_utilization_threshold_reached));
    }
}

auto out_of_space_controller::subscribe(subscription_callback_type cb) -> subscription {
    auto sub = subscription(*this, std::move(cb));
    _dsm.trigger_poll();
    return sub;
}


} // namespace replica