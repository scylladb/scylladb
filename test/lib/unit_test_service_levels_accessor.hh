/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "service/qos/service_level_controller.hh"
#include "service/qos/qos_common.hh"
#include "db/system_distributed_keyspace.hh"
#pragma once

namespace qos {

/**
 *  This class is a helper for unit testing. It implements the service level distributed
 *  accessor interface in order to be used in the unit testing environment. The advantage
 *  of this class over the standard implementation is that it makes sure that updates are
 *  Immediately propagated to the underlying service level controller.
 */
class unit_test_service_levels_accessor : public service_level_controller::service_level_distributed_data_accessor {
        sharded<service_level_controller> &_sl_controller;
        sharded<db::system_distributed_keyspace> &_sys_dist_ks;
public:
        unit_test_service_levels_accessor(sharded<service_level_controller>& sl_controller, sharded<db::system_distributed_keyspace> &sys_dist_ks)
                : _sl_controller(sl_controller)
                , _sys_dist_ks(sys_dist_ks)
        {}
        virtual future<qos::service_levels_info> get_service_levels() const {
            return _sys_dist_ks.local().get_service_levels();
        }
        virtual future<qos::service_levels_info> get_service_level(sstring service_level_name) const {
            return _sys_dist_ks.local().get_service_level(service_level_name);
        }
        virtual future<> set_service_level(sstring service_level_name, qos::service_level_options slo) const {
            return _sys_dist_ks.local().set_service_level(service_level_name, slo).then([this] () {
                return _sl_controller.invoke_on_all(&service_level_controller::update_service_levels_from_distributed_data);
            });

        }
        virtual future<> drop_service_level(sstring service_level_name) const {
            return _sys_dist_ks.local().drop_service_level(service_level_name).then([this] () {
                return _sl_controller.invoke_on_all(&service_level_controller::update_service_levels_from_distributed_data);
            });
        }
};

}
