/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "standard_service_level_distributed_data_accessor.hh"
#include "db/system_distributed_keyspace.hh"
#include "service/qos/raft_service_level_distributed_data_accessor.hh"
#include "service/raft/raft_group0_client.hh"

namespace qos {

standard_service_level_distributed_data_accessor::standard_service_level_distributed_data_accessor(db::system_distributed_keyspace &sys_dist_ks):
_sys_dist_ks(sys_dist_ks) {
}

future<qos::service_levels_info> standard_service_level_distributed_data_accessor::get_service_levels() const {
    return _sys_dist_ks.get_service_levels();
}

future<qos::service_levels_info> standard_service_level_distributed_data_accessor::get_service_level(sstring service_level_name) const {
    return _sys_dist_ks.get_service_level(service_level_name);
}

future<> standard_service_level_distributed_data_accessor::set_service_level(sstring service_level_name, qos::service_level_options slo, service::group0_batch&) const {
    return _sys_dist_ks.set_service_level(service_level_name, slo);
}

future<> standard_service_level_distributed_data_accessor::drop_service_level(sstring service_level_name, service::group0_batch&) const {
    return _sys_dist_ks.drop_service_level(service_level_name);
}

bool standard_service_level_distributed_data_accessor::is_v2() const {
    return false;
}

::shared_ptr<service_level_controller::service_level_distributed_data_accessor> standard_service_level_distributed_data_accessor::upgrade_to_v2(cql3::query_processor& qp, service::raft_group0_client& group0_client) const {
    return ::static_pointer_cast<service_level_controller::service_level_distributed_data_accessor>(
                ::make_shared<raft_service_level_distributed_data_accessor>(qp, group0_client));
}

}
