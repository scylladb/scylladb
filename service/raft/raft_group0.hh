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
class raft_group0_client;

// Wrapper for `discovery` which persists the learned peers on disk.
class persistent_discovery {
    discovery _discovery;
    cql3::query_processor& _qp;
    seastar::gate _gate;

public:
    using peer_list = discovery::peer_list;
    using tick_output = discovery::tick_output;

    // See `discovery::discovery`.
    // The provided seed list will be extended with already known persisted peers.
    // `self` must be the same across restarts.
    static future<persistent_discovery> make(raft::server_address self, peer_list seeds, cql3::query_processor&);

    // Run the discovery algorithm to find information about group 0.
    future<group0_info> run(
        netw::messaging_service&,
        gate::holder pause_shutdown,
        abort_source&,
        raft::server_address my_addr);

    // Must be called and waited for before destroying the object.
    // Must not be called concurrently with `run`.
    // Can be called concurrently with `request`.
    future<> stop();

    // See `discovery::request`.
    future<std::optional<peer_list>> request(peer_list peers);

private:
    // See `discovery::response`.
    void response(raft::server_address from, const peer_list& peers);

    // See `discovery::tick`.
    future<tick_output> tick();

    persistent_discovery(raft::server_address, const peer_list&, cql3::query_processor&);
};

class raft_group0 {
    seastar::gate _shutdown_gate;
    seastar::abort_source& _abort_source;
    raft_group_registry& _raft_gr;
    netw::messaging_service& _ms;
    gms::gossiper& _gossiper;
    cql3::query_processor& _qp;
    service::migration_manager& _mm;
    raft_group0_client& _client;

    // Status of leader discovery. Initially there is no group 0,
    // and the variant contains no state. During initial cluster
    // bootstrap a discovery object is created, which is then
    // substituted by group0 id when a leader is discovered or
    // created.
    std::variant<std::monostate, service::persistent_discovery, raft::group_id> _group0;

public:
    raft_group0(seastar::abort_source& abort_source,
        service::raft_group_registry& raft_gr,
        netw::messaging_service& ms,
        gms::gossiper& gs,
        cql3::query_processor& qp,
        migration_manager& mm,
        raft_group0_client& client);

    future<> abort();

    // Join this node to the cluster-wide Raft group
    // Called during bootstrap. Is idempotent - it
    // does nothing if already done, or resumes from the
    // unifinished state if aborted. The result is that
    // raft service has group 0 running.
    future<> join_group0();

    // After successful bootstrapping, make this node a voting member.
    future<> become_voter();

    // Remove the node from the cluster-wide raft group.
    // This procedure is idempotent. In case of replace node,
    // it removes the replaced node from the group, since
    // it can't do it by itself (it's dead).
    future<> leave_group0(std::optional<gms::inet_address> host = {});

private:
    void init_rpc_verbs();
    future<> uninit_rpc_verbs();

    // Handle peer_exchange RPC
    future<group0_peer_exchange> peer_exchange(discovery::peer_list peers);

    raft_server_for_group create_server_for_group0(raft::group_id id, raft::server_address my_addr);

    // Loads server address for group 0 from disk if present, otherwise randomly generates a new one and persists it.
    // Execute on shard 0 only.
    future<raft::server_address> load_or_create_my_addr();

    // Run the discovery algorithm.
    //
    // Discovers an existing group 0 cluster or elects a server (called a 'leader')
    // responsible for creating a new group 0 cluster if one doesn't exist
    // (in particular, we may become that leader).
    //
    // See 'raft-in-scylla.md', 'Establishing group 0 in a fresh cluster'.
    future<group0_info> discover_group0(raft::server_address my_addr);

    // Start an existing Raft server for the cluster-wide group 0.
    // Assumes the server was already added to the group earlier so we don't attempt to join it again.
    //
    // `group0_id` must be non-null and equal to the ID of group 0 that we joined earlier.
    // The existing group 0 server must not have been started yet after the last restart of Scylla.
    //
    // XXX: perhaps it would be good to make this function callable multiple times,
    // if we want to handle crashes of the group 0 server without crashing the entire Scylla process
    // (we could then try restarting the server internally).
    future<> start_server_for_group0(raft::group_id group0_id);
};

} // end of namespace service
