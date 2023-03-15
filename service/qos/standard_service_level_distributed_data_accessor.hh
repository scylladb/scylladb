/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "seastarx.hh"
#include "service_level_controller.hh"


namespace db {
    class system_distributed_keyspace;
}
namespace gms {
    class feature_service;
}
namespace qos {

class standard_service_level_distributed_data_accessor : public service_level_controller::service_level_distributed_data_accessor,
         public ::enable_shared_from_this<standard_service_level_distributed_data_accessor> {
    using service_level_controller_dda_ptr = ::shared_ptr<service_level_controller::service_level_distributed_data_accessor>;

    db::system_distributed_keyspace& _sys_dist_ks;
    gms::feature_service& _feat;
    bool _v2_upgraded;

    future<> _data_accessor_upgrade = make_ready_future();
    abort_source _data_accessor_upgrade_aborter;
public:
    standard_service_level_distributed_data_accessor(db::system_distributed_keyspace &sys_dist_ks, gms::feature_service& feat);
    virtual future<> init() override;
    virtual future<> stop() override;
    
    virtual future<qos::service_levels_info> get_service_levels() const override;
    virtual future<qos::service_levels_info> get_service_level(sstring service_level_name) const override;
    virtual future<> set_service_level(sstring service_level_name, qos::service_level_options slo) const override;
    virtual future<> drop_service_level(sstring service_level_name) const override;

private:
    future<> upgrade_loop();
};

}
