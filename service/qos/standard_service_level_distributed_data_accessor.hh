/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "seastarx.hh"
#include "service/raft/raft_group0_client.hh"
#include "service_level_controller.hh"


namespace db {
    class system_distributed_keyspace;
}
namespace qos {
class standard_service_level_distributed_data_accessor : public service_level_controller::service_level_distributed_data_accessor,
         public ::enable_shared_from_this<standard_service_level_distributed_data_accessor> {
private:
    db::system_distributed_keyspace& _sys_dist_ks;
public:
    standard_service_level_distributed_data_accessor(db::system_distributed_keyspace &sys_dist_ks);
    virtual future<qos::service_levels_info> get_service_levels() const override;
    virtual future<qos::service_levels_info> get_service_level(sstring service_level_name) const override;
    virtual future<> set_service_level(sstring service_level_name, qos::service_level_options slo, std::optional<service::group0_guard> guard) const override;
    virtual future<> drop_service_level(sstring service_level_name, std::optional<service::group0_guard> guard) const override;
};
}
