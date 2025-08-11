/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "service/qos/effective_service_level_controller.hh"

#include "service/qos/service_level_controller.hh"

namespace qos {

effective_service_level_controller::effective_service_level_controller(
        service_level_controller& sl_controller, auth::service& auth_service)
    : _sl_controller(sl_controller)
    , _auth_service(auth_service)
    , _stop_gate("effective_service_level_controller_stop_gate")
{
    _sl_controller.register_effective_service_level_controller(*this);
}

effective_service_level_controller::~effective_service_level_controller() noexcept {
    // The corresponding `service_level_controller` should've already have
    // this `effective_service_level_controller` unregistered by now.
    SCYLLA_ASSERT(_sl_controller._esl_controller == nullptr);
}

future<> effective_service_level_controller::stop() {
    SCYLLA_ASSERT(_sl_controller._esl_controller == this);
    _sl_controller.unregister_effective_service_level_controller();

    co_await _stop_gate.close();
}

void effective_service_level_controller::register_subscriber(effective_service_level_event_subscriber* subscriber) {
    _subscribers.add(subscriber);
}

future<> effective_service_level_controller::unregister_subscriber(effective_service_level_event_subscriber* subscriber) {
    const auto _ = seastar::gate::holder{_stop_gate};
    return _subscribers.remove(subscriber);
}

void effective_service_level_controller::clear_cache() {
    _mapping_cache.clear();
}

future<> effective_service_level_controller::notify_cache_reloaded() {
    const auto _ = seastar::gate::holder{_stop_gate};

    co_await _subscribers.for_each([] (effective_service_level_event_subscriber* subscriber) -> future<> {
        return subscriber->on_effective_service_levels_cache_reloaded();
    });
}

} // namespace qos
