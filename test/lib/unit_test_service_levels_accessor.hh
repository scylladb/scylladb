/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "seastar/core/future.hh"
#include "service/qos/service_level_controller.hh"
#include "service/qos/qos_common.hh"
#include "db/system_distributed_keyspace.hh"
#include <seastar/core/sleep.hh>
#pragma once

namespace qos {

/**
 *  This class is a helper for unit testing. It implements the service level distributed
 *  accessor interface in order to be used in the unit testing environment. The advantage
 *  of this class over the standard implementation is that it makes sure that updates are
 *  Immediately propagated to the underlying service level controller.
 */

class unit_test_service_levels_accessor final : public service_level_controller::service_level_distributed_data_accessor {
        sharded<service_level_controller> &_sl_controller;
        sharded<db::system_distributed_keyspace> &_sys_dist_ks;

        bool _v2_upgraded;
public:
        unit_test_service_levels_accessor(sharded<service_level_controller>& sl_controller, sharded<db::system_distributed_keyspace> &sys_dist_ks, bool v2_upgraded)
                : _sl_controller(sl_controller)
                , _sys_dist_ks(sys_dist_ks)
                , _v2_upgraded(v2_upgraded)
        {}

        virtual future<> init() {
            return make_ready_future<>();
        }
        virtual future<> stop() {
            return make_ready_future<>();
        }
        virtual future<qos::service_levels_info> get_service_levels() const {
            return (_v2_upgraded) ? _sys_dist_ks.local().get_service_levels_v2() : _sys_dist_ks.local().get_service_levels();
        }
        virtual future<qos::service_levels_info> get_service_level(sstring service_level_name) const {
            return (_v2_upgraded) ? _sys_dist_ks.local().get_service_level_v2(service_level_name) :  _sys_dist_ks.local().get_service_level(service_level_name);
        }
        virtual future<> set_service_level(sstring service_level_name, qos::service_level_options slo) const {
            future<> v1 = (_v2_upgraded) ? make_ready_future<>() : _sys_dist_ks.local().set_service_level(service_level_name, slo);
            
            return v1.then([this, service_level_name, slo] () {
                return _sys_dist_ks.local().set_service_level_v2(service_level_name, slo).then([this] () {
                    return _sl_controller.invoke_on_all(&service_level_controller::update_service_levels_from_distributed_data);
                });
            });
        }
        virtual future<> drop_service_level(sstring service_level_name) const {
            future<> v1 = (_v2_upgraded) ? make_ready_future<>() : _sys_dist_ks.local().drop_service_level(service_level_name);

            return v1.then([this, service_level_name] () {
                return _sys_dist_ks.local().drop_service_level_v2(service_level_name).then([this] () {
                    return _sl_controller.invoke_on_all(&service_level_controller::update_service_levels_from_distributed_data);
                });
            });
        }
};

}
