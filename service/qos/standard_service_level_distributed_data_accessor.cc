/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "standard_service_level_distributed_data_accessor.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/system_keyspace.hh"
#include "seastar/core/shared_ptr.hh"
#include "gms/feature_service.hh"

namespace qos {

static logging::logger dda_logger("service_level_distributed_data_accessor");

static const sstring SERVICE_LEVEL_UPGRADE_KEY = "service_level_upgrade_status";
static const sstring SL_UPGRADE_DONE_VALUE = "sl_upgraded_v2";

standard_service_level_distributed_data_accessor::standard_service_level_distributed_data_accessor(db::system_distributed_keyspace &sys_dist_ks, gms::feature_service& feat) 
: _sys_dist_ks(sys_dist_ks)
, _feat(feat)
, _v2_upgraded(false) {}

future<> standard_service_level_distributed_data_accessor::init() {
    auto upgrade_status = co_await db::system_keyspace::get_scylla_local_param(SERVICE_LEVEL_UPGRADE_KEY);
    
    if (upgrade_status && *upgrade_status == SL_UPGRADE_DONE_VALUE) {
        _v2_upgraded = true;
    } else {
        if (_data_accessor_upgrade.available()) {
            _data_accessor_upgrade = upgrade_loop();
        }
    }
}

future<> standard_service_level_distributed_data_accessor::stop() {
    if (!_data_accessor_upgrade_aborter.abort_requested()) {
        _data_accessor_upgrade_aborter.request_abort();
    }

    return std::exchange(_data_accessor_upgrade, make_ready_future<>()).then_wrapped([] (future<> f) {
        try {
            f.get();
        } catch (const broken_semaphore& ignored) {
        } catch (const sleep_aborted& ignored) {
        } catch (const exceptions::unavailable_exception& ignored) {
        } catch (const exceptions::read_timeout_exception& ignored) {
        }
    });
}

future<qos::service_levels_info> standard_service_level_distributed_data_accessor::get_service_levels() const {
    return (_v2_upgraded) ? _sys_dist_ks.get_service_levels_v2() : _sys_dist_ks.get_service_levels();
}

future<qos::service_levels_info> standard_service_level_distributed_data_accessor::get_service_level(sstring service_level_name) const {
    return (_v2_upgraded) ? _sys_dist_ks.get_service_level_v2(service_level_name) : _sys_dist_ks.get_service_level(service_level_name);
}

future<> standard_service_level_distributed_data_accessor::set_service_level(sstring service_level_name, qos::service_level_options slo) const {
    if (!_v2_upgraded) {
        co_await _sys_dist_ks.set_service_level(service_level_name, slo);
    }
    co_await _sys_dist_ks.set_service_level_v2(service_level_name, slo);
}

future<> standard_service_level_distributed_data_accessor::drop_service_level(sstring service_level_name) const {
    if (!_v2_upgraded) {
        co_await _sys_dist_ks.drop_service_level(service_level_name);
    }
    co_await _sys_dist_ks.drop_service_level_v2(service_level_name);
}

future<> standard_service_level_distributed_data_accessor::upgrade_loop() {
    return seastar::async([this] {
        static std::chrono::duration<float> wait_time = std::chrono::seconds(10);

        while (!_sys_dist_ks.started() || !_feat.service_levels_table_v2) {
            try {
                sleep_abortable<steady_clock_type>(
                    std::chrono::duration_cast<steady_clock_type::duration>(wait_time),
                    _data_accessor_upgrade_aborter
                ).get();
            } catch (const sleep_aborted& e) {
                dda_logger.info("update_from_distributed_data: upgrade waiting loop aborted");
                return;
            }
        }

        _sys_dist_ks.migrate_service_levels_to_v2().get();
        db::system_keyspace::set_scylla_local_param(SERVICE_LEVEL_UPGRADE_KEY, SL_UPGRADE_DONE_VALUE).get();
        _v2_upgraded = true;
    });
}

}
