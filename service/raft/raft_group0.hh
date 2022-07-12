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

namespace db { class system_keyspace; }

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
    // Assumes that the provided services are fully started.
    raft_group0(seastar::abort_source& abort_source,
        service::raft_group_registry& raft_gr,
        netw::messaging_service& ms,
        gms::gossiper& gs,
        cql3::query_processor& qp,
        migration_manager& mm,
        raft_group0_client& client);

    // Call before destroying the object.
    future<> abort();

    // Call during the startup procedure.
    //
    // If the local RAFT feature is enabled, does one of the following:
    // - join group 0 (if we're bootstrapping),
    // - start existing group 0 server (if we bootstrapped before),
    // - (TODO: not implemented yet) prepare us for the upgrade procedure, which will create group 0 later.
    //
    // Cannot be called twice.
    //
    // TODO: specify dependencies on other services: where during startup should we setup group 0?
    future<> setup_group0(db::system_keyspace&, const std::unordered_set<gms::inet_address>& initial_contact_nodes);

    // After successful bootstrapping, make this node a voting member.
    // Precondition: `setup_group0` successfully finished earlier.
    future<> become_voter();

    // Remove ourselves from group 0.
    //
    // Assumes we've finished the startup procedure (`setup_group0()` finished earlier).
    // Assumes to run during decommission, after the node entered LEFT status.
    //
    // FIXME: make it retryable and do nothing if we're not a member.
    // Currently if we call leave_group0 twice, it will get stuck the second time
    // (it will try to forward an entry to a leader but never find the leader).
    // Not sure how easy or hard it is and whether it's a problem worth solving; if decommission crashes,
    // one can simply call `removenode` on another node to make sure we areremoved (from group 0 too).
    future<> leave_group0();

    // Remove `host` from group 0.
    //
    // Assumes that either
    // 1. we've finished bootstrapping and now running a `removenode` operation,
    // 2. or we're currently bootstrapping and replacing an existing node.
    //
    // In both cases, `setup_group0()` must have finished earlier.
    //
    // The provided address may be our own - if we're replacing a node that had the same address as ours.
    // We'll look for the other node's Raft ID in the group 0 configuration.
    future<> remove_from_group0(gms::inet_address host);

private:
    void init_rpc_verbs();
    future<> uninit_rpc_verbs();

    bool joined_group0() const;

    // Handle peer_exchange RPC
    future<group0_peer_exchange> peer_exchange(discovery::peer_list peers);

    raft_server_for_group create_server_for_group0(raft::group_id id, raft::server_address my_addr);

    // Assumes server address for group 0 is already persisted and loads it from disk.
    // It's a fatal error if the address is missing.
    //
    // Execute on shard 0 only.
    //
    // The returned ID is not empty.
    future<raft::server_address> load_my_addr();

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
    future<group0_info> discover_group0(raft::server_address my_addr, const std::vector<raft::server_info>& seeds);

    // Start a Raft server for the cluster-wide group 0 and join it to the group.
    // Called during bootstrap or upgrade.
    //
    // Uses `seeds` as contact points to discover other servers which will be part of group 0.
    //
    // `as_voter` determines whether the server joins as a voter. If `false`, it will join
    // as a non-voter with one exception: if it becomes the 'discovery leader', meaning that
    // it is elected as the server which creates group 0, it will become a voter.
    //
    // The Raft server may already exist on disk (e.g. because we initialized it earlier but then crashed),
    // but it doesn't have to. It may also already be part of group 0, but if not, we will attempt
    // to discover and join it.
    //
    // Persists group 0 ID on disk at the end so subsequent restarts of Scylla process can detect that group 0
    // has already been joined and the server initialized.
    //
    // `as_voter` should be initially `false` when joining an existing cluster; calling `become_voter`
    // will cause it to become a voter. `as_voter` should be `true` when upgrading, when we create
    // a group 0 using all nodes in the cluster.
    //
    // Preconditions: Raft local feature enabled
    // and we haven't initialized group 0 yet after last Scylla start (`joined_group0()` is false).
    // Postcondition: `joined_group0()` is true.
    future<> join_group0(std::vector<raft::server_info> seeds, bool as_voter);

    // Start an existing Raft server for the cluster-wide group 0.
    // Assumes the server was already added to the group earlier so we don't attempt to join it again.
    //
    // Preconditions: `group0_id` must be non-null and equal to the ID of group 0 that we joined earlier.
    // The existing group 0 server must not have been started yet after the last restart of Scylla
    // (`joined_group0()` is false).
    // Postcondition: `joined_group0()` is true.
    //
    // XXX: perhaps it would be good to make this function callable multiple times,
    // if we want to handle crashes of the group 0 server without crashing the entire Scylla process
    // (we could then try restarting the server internally).
    future<> start_server_for_group0(raft::group_id group0_id);
};

} // end of namespace service
