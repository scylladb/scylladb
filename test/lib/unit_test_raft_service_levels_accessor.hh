/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "service/qos/service_level_controller.hh"
#include "service/qos/raft_service_level_distributed_data_accessor.hh"

namespace qos {

class unit_test_raft_service_levels_accessor : public raft_service_level_distributed_data_accessor {
    sharded<service_level_controller>& _sl_controller;

public:
    unit_test_raft_service_levels_accessor(cql3::query_processor& qp, service::raft_group0_client& group0_client, sharded<service_level_controller>& sl_controller)
            : raft_service_level_distributed_data_accessor(qp, group0_client)
            , _sl_controller(sl_controller) {}
    
    virtual future<> set_service_level(sstring service_level_name, qos::service_level_options slo, service::group0_batch& mc) const override {
        co_await raft_service_level_distributed_data_accessor::set_service_level(std::move(service_level_name), std::move(slo), mc);
        co_await _sl_controller.invoke_on_all(&service_level_controller::update_service_levels_from_distributed_data);
    }

    virtual future<> drop_service_level(sstring service_level_name, service::group0_batch& mc) const override {
        co_await raft_service_level_distributed_data_accessor::drop_service_level(std::move(service_level_name), mc);
        co_await _sl_controller.invoke_on_all(&service_level_controller::update_service_levels_from_distributed_data);
    }

    virtual future<> commit_mutations(service::group0_batch&& mc, abort_source& as) const override {
        co_await raft_service_level_distributed_data_accessor::commit_mutations(std::move(mc), as);
        co_await _sl_controller.invoke_on_all(&service_level_controller::update_service_levels_from_distributed_data);
    }

    virtual ::shared_ptr<service_level_distributed_data_accessor> upgrade_to_v2(cql3::query_processor& qp, service::raft_group0_client& group0_client) const override {
        return nullptr;
    }
};

}
