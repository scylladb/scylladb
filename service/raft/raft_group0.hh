/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once
#include "service/raft/raft_group_registry.hh"
#include "service/raft/discovery.hh"
#include "service/raft/messaging.hh"

namespace cql3 { class query_processor; }

namespace gms { class gossiper; }

namespace service {

class migration_manager;

class raft_group0 {
public:
    seastar::gate _shutdown_gate;
    seastar::abort_source& _abort_source;
    raft_group_registry& _raft_gr;
    netw::messaging_service& _ms;
    gms::gossiper& _gossiper;
    cql3::query_processor& _qp;
    service::migration_manager& _mm;
    // Status of leader discovery. Initially there is no group 0,
    // and the variant contains no state. During initial cluster
    // bootstrap a discovery object is created, which is then
    // substituted by group0 id when a leader is discovered or
    // created.
    std::variant<std::monostate, service::discovery, raft::group_id> _group0;

    raft_server_for_group create_server_for_group(raft::group_id id, raft::server_address my_addr);
    future<raft::server_address> load_or_create_my_addr();
    // A helper function to discover Raft Group 0 leader in
    // absence of running group 0 server.
    future<group0_info> discover_group0(raft::server_address my_addr);
public:
    raft_group0(seastar::abort_source& abort_source,
        service::raft_group_registry& raft_gr,
        netw::messaging_service& ms,
        gms::gossiper& gs,
        cql3::query_processor& qp,
        migration_manager& mm);

    future<> abort();

    // Join this node to the cluster-wide Raft group
    // Called during bootstrap. Is idempotent - it
    // does nothing if already done, or resumes from the
    // unifinished state if aborted. The result is that
    // raft service has group 0 running.
    future<> join_group0();

    // Remove the node from the cluster-wide raft group.
    // This procedure is idempotent. In case of replace node,
    // it removes the replaced node from the group, since
    // it can't do it by itself (it's dead).
    future<> leave_group0(std::optional<gms::inet_address> host = {});

    // Handle peer_exchange RPC
    future<group0_peer_exchange> peer_exchange(discovery::peer_list peers);
};

} // end of namespace service
