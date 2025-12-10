/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include "locator/abstract_replication_strategy.hh"
#include "message/messaging_service.hh"
#include "service/raft/raft_group_registry.hh"
#include "cql3/query_processor.hh"

namespace service {

class sc_groups_manager: public peering_sharded_service<sc_groups_manager> {
    class state_machine_impl;
    class rpc_impl;

    netw::messaging_service& _ms;
    raft_group_registry& _raft_gr;
    cql3::query_processor& _qp;

    future<> start_raft_server(table_id table_id, locator::tablet_id tablet_id,
        raft::group_id gid, raft::server_id server_id, const locator::tablet_info& tablet_info);
public:
    sc_groups_manager(netw::messaging_service& ms, raft_group_registry& raft_gr,
        cql3::query_processor& qp);

    using erms_map = std::unordered_map<table_id, locator::effective_replication_map_ptr>;
    future<> start_raft_servers(const erms_map& erms, locator::host_id host_id);

    future<> stop();
};

}