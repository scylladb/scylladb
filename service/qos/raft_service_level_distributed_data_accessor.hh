/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/abort_source.hh>
#include "seastarx.hh"
#include "service/raft/raft_group0_client.hh"
#include "service_level_controller.hh"
#include "mutation/mutation.hh"

namespace cql3 {
class query_processor;
}

namespace service {
class raft_group0_client;
}

namespace qos {

class raft_service_level_distributed_data_accessor : public service_level_controller::service_level_distributed_data_accessor
                                                   , public ::enable_shared_from_this<raft_service_level_distributed_data_accessor> {
private:
    cql3::query_processor& _qp;
    service::raft_group0_client& _group0_client;

public:
    raft_service_level_distributed_data_accessor(cql3::query_processor& qp, service::raft_group0_client& group0_client);

    virtual future<qos::service_levels_info> get_service_levels() const override;
    virtual future<qos::service_levels_info> get_service_level(sstring service_level_name) const override;
    virtual future<> set_service_level(sstring service_level_name, qos::service_level_options slo, service::group0_batch& mc) const override;
    virtual future<> drop_service_level(sstring service_level_name, service::group0_batch& mc) const override;
    virtual future<> commit_mutations(service::group0_batch&& mc, abort_source& as) const override;

    virtual bool is_v2() const override;
    virtual ::shared_ptr<service_level_distributed_data_accessor> upgrade_to_v2(cql3::query_processor& qp, service::raft_group0_client& group0_client) const override;
};

}
