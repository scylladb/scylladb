/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "service/qos/service_level_controller.hh"
#include "service/qos/qos_common.hh"
#include "db/system_distributed_keyspace.hh"
#include "service/raft/raft_group0_client.hh"
#include "test/lib/unit_test_raft_service_levels_accessor.hh"
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
        virtual future<qos::service_levels_info> get_service_levels() const override {
            return _sys_dist_ks.local().get_service_levels();
        }
        virtual future<qos::service_levels_info> get_service_level(sstring service_level_name) const override {
            return _sys_dist_ks.local().get_service_level(service_level_name);
        }
        virtual future<> set_service_level(sstring service_level_name, qos::service_level_options slo, std::optional<service::group0_guard>, abort_source&) const override {
            return _sys_dist_ks.local().set_service_level(service_level_name, slo).then([this] () {
                return _sl_controller.invoke_on_all(&service_level_controller::update_service_levels_from_distributed_data);
            });

        }
        virtual future<> drop_service_level(sstring service_level_name, std::optional<service::group0_guard>, abort_source&) const override {
            return _sys_dist_ks.local().drop_service_level(service_level_name).then([this] () {
                return _sl_controller.invoke_on_all(&service_level_controller::update_service_levels_from_distributed_data);
            });
        }

        virtual bool is_v2() const override {
            return false;
        }
        virtual ::shared_ptr<service_level_distributed_data_accessor> upgrade_to_v2(cql3::query_processor& qp, service::raft_group0_client& group0_client) const override {
            return ::static_pointer_cast<service_level_controller::service_level_distributed_data_accessor>(
                ::make_shared<unit_test_raft_service_levels_accessor>(qp, group0_client, _sl_controller));
        }
};

}
