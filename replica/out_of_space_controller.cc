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

future<> set_database_in_critical_disk_utilization_mode(sharded<database>& db, bool enabled) {
    co_await replica::database::set_in_critical_disk_utilization_mode(db, enabled);
}

future<> set_compaction_manager_disabled(sharded<compaction_manager>& cm, bool disabled) {
    if (disabled) {
        co_await cm.invoke_on_all([] (compaction_manager& cm) { return cm.drain(); });
    } else {
        co_await cm.invoke_on_all([] (compaction_manager& cm) { cm.enable(); });
    }
}

future<> set_repair_service_disabled(sharded<repair_service>& rs, bool disabled) {
    co_await repair_service::set_enable_repair_tablet_rpc(rs, !disabled);
}

out_of_space_controller::out_of_space_controller(utils::disk_space_monitor& dsm, sharded<database>& db, sharded<compaction_manager>& cm,
                                                 sharded<repair_service>& rs, abort_source& as, config cfg)
    : _db(db)
    , _cm(cm)
    , _rs(rs)
    , _cfg(std::move(cfg))
    , _abort_source(as)
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
            co_return;
        }

        static constexpr auto msg_template = "{} the critical disk utilization level ({:.1f}%%). Current disk utilization {:.1f}%%";
        if (_critical_disk_utilization_threshold_reached) {
            logger.warn(msg_template, "Reached", _cfg.critical_disk_utilization_threshold() * 100, current_disk_utilization * 100);
        } else {
            logger.info(msg_template, "Dropped below", _cfg.critical_disk_utilization_threshold() * 100, current_disk_utilization * 100);
        }

        co_await set_database_in_critical_disk_utilization_mode(_db, _critical_disk_utilization_threshold_reached);
        co_await set_compaction_manager_disabled(_cm, _critical_disk_utilization_threshold_reached);
        co_await set_repair_service_disabled(_rs, _critical_disk_utilization_threshold_reached);
    }))
    {
        // Trigger poll to ensure a registered callback is called immediately
        dsm.trigger_poll();
    }

future<> out_of_space_controller::stop() {
    return make_ready_future<>();
}

} // namespace replica