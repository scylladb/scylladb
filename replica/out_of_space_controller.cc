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

future<void> set_user_table_writes_disabled(sharded<database>& db, bool disabled)
{
    logger.info("{} user table writes", disabled ? "disabling" : "enabling");
    co_await replica::database::disable_user_table_writes_on_all_shards(db, disabled);
}

future<void> set_compaction_manager_disabled(sharded<compaction_manager>& cm, bool disabled)
{
    logger.info("{} compaction manager", disabled ? "disabling" : "enabling");
    auto cm_fh = disabled
                ? [](compaction_manager& cm) { return cm.drain(); }
                : [](compaction_manager& cm) { cm.enable(); return make_ready_future<>(); };
    co_await cm.invoke_on_all(cm_fh);
}

future<void> set_repair_service_disabled(sharded<repair_service>& rs, bool disabled)
{
    logger.info("{} repair service", disabled ? "disabling" : "enabling");
    auto rs_fh = disabled
                ? [](repair_service& rs) { return rs.drain(); }
                : [](repair_service& rs) { rs.enable(); return make_ready_future<>(); };
    co_await rs.invoke_on_all(rs_fh);
}

out_of_space_controller::out_of_space_controller(config cfg, utils::disk_space_monitor& dsm, abort_source& as)
    :_cfg(std::move(cfg))
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
        logger.info(msg_template, _critical_disk_utilization_threshold_reached ? "Reached" : "Dropped below",
                    _cfg.critical_disk_utilization_threshold()*100, current_disk_utilization * 100);

        co_await set_user_table_writes_disabled(_cfg.db, _critical_disk_utilization_threshold_reached);
        co_await set_compaction_manager_disabled(_cfg.cm, _critical_disk_utilization_threshold_reached);
        co_await set_repair_service_disabled(_cfg.rs, _critical_disk_utilization_threshold_reached);
    }))
    {}

future<> out_of_space_controller::stop() {
    logger.info("Controller stopped");
    return make_ready_future<>();
}

} // namespace replica