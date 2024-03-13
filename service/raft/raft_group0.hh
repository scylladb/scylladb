/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include "service/raft/raft_group_registry.hh"
#include "service/raft/discovery.hh"
#include "service/raft/group0_fwd.hh"
#include "gms/feature.hh"

namespace cql3 { class query_processor; }

namespace db { class system_keyspace; }

namespace gms { class gossiper; class feature_service; }

namespace service {

extern const char* const raft_upgrade_doc;

class migration_manager;
class raft_group0_client;
class storage_service;

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
    // `my_addr` must be the same across restarts.
    static future<persistent_discovery> make(discovery_peer my_addr, peer_list seeds, cql3::query_processor&);

    // Run the discovery algorithm to find information about group 0.
    future<group0_info> run(
        netw::messaging_service&,
        gate::holder pause_shutdown,
        abort_source&,
        discovery_peer my_addr);

    // Must be called and waited for before destroying the object.
    // Must not be called concurrently with `run`.
    // Can be called concurrently with `request`.
    future<> stop();

    // See `discovery::request`.
    future<std::optional<peer_list>> request(peer_list peers);

private:
    // See `discovery::response`.
    void response(discovery_peer from, const peer_list& peers);

    // See `discovery::tick`.
    future<tick_output> tick();

    persistent_discovery(discovery_peer my_addr, const peer_list&, cql3::query_processor&);
};

// Abstracts away the details of a handshake used to join an existing group 0.
// None of its methods are called if the node creates a new group 0.
class group0_handshaker {
public:
    virtual ~group0_handshaker() {}

    // Called after initial discovery run is finished, but before starting
    // the raft server. This is only called once and is not retried in case
    // it or `post_server_start` fails.
    virtual future<> pre_server_start(const group0_info& info) = 0;

    // Performs a handshake that should result in this node being added to group 0.
    // Called after the raft server is started.
    //
    // - If the function returns true, the node has been added to group 0
    //   successfully.
    // - If the function returns false, this step has failed but should be
    //   retried.
    // - An exception is interpreted as an irrecoverable error and causes
    //   setup_group0 to fail.
    //
    // The implementation can use the abort_source parameter to abort the
    // function if needed.
    virtual future<bool> post_server_start(const group0_info& info, abort_source& as) = 0;
};

class raft_group0 {
    seastar::gate _shutdown_gate;
    seastar::abort_source& _abort_source;
    raft_group_registry& _raft_gr;
    sharded<netw::messaging_service>& _ms;
    gms::gossiper& _gossiper;
    gms::feature_service& _feat;
    db::system_keyspace& _sys_ks;
    raft_group0_client& _client;

    // Status of leader discovery. Initially there is no group 0,
    // and the variant contains no state. During initial cluster
    // bootstrap a discovery object is created, which is then
    // substituted by group0 id when a leader is discovered or
    // created.
    std::variant<std::monostate, service::persistent_discovery, raft::group_id> _group0;

    gms::feature::listener_registration _raft_support_listener;

    seastar::metrics::metric_groups _metrics;
    void register_metrics();

    // Status of the raft group0 for monitoring.
    enum class status_for_monitoring : uint8_t {
        unused = 0,
        normal = 1,
        aborted = 2
    } _status_for_monitoring;

    // Fiber monitoring and logging gaining/losing leadership events in group 0.
    future<> leadership_monitor_fiber();
    future<> _leadership_monitor = make_ready_future<>();
    abort_source _leadership_monitor_as;

public:
    // Passed to `setup_group0` when replacing a node.
    struct replace_info {
        gms::inet_address ip_addr;
        raft::server_id raft_id;
    };

    // Assumes that the provided services are fully started.
    raft_group0(seastar::abort_source& abort_source,
        service::raft_group_registry& raft_gr,
        sharded<netw::messaging_service>& ms,
        gms::gossiper& gs,
        gms::feature_service& feat,
        db::system_keyspace& sys_ks,
        raft_group0_client& client);

    // Initialises RPC verbs on all shards.
    // Call after construction but before using the object.
    future<> start();

    // Call before destroying the object.
    future<> abort();

    // Run the discovery algorithm.
    //
    // Discovers an existing group 0 cluster or elects a server (called a 'leader')
    // responsible for creating a new group 0 cluster if one doesn't exist
    // (in particular, we may become that leader).
    //
    // See 'raft-in-scylla.md', 'Establishing group 0 in a fresh cluster'.
    future<group0_info> discover_group0(const std::vector<gms::inet_address>& seeds, cql3::query_processor& qp);

    // Call during the startup procedure, after gossiping has started.
    //
    // If we're performing the replace operation, pass the IP and Raft ID of the replaced node
    // obtained using the shadow round through the `replace_info` parameter.
    //
    // If the local RAFT feature is enabled, does one of the following:
    // - join group 0 (if we're bootstrapping),
    // - prepare us for the upgrade procedure, which will create group 0 later (if we're upgrading).
    //
    // Cannot be called twice.
    //
    // Also make sure to call `finish_setup_after_join` after the node has joined the cluster and entered NORMAL state.
    future<> setup_group0(db::system_keyspace&, const std::unordered_set<gms::inet_address>& initial_contact_nodes, shared_ptr<group0_handshaker> handshaker,
                          std::optional<replace_info>, service::storage_service& ss, cql3::query_processor& qp, service::migration_manager& mm, bool topology_change_enabled);

    // Call during the startup procedure before networking is enabled.
    //
    // If the local RAFT feature is enabled and we're not in RECOVERY mode:
    // - start group 0 server if it exists,
    // - disable migration_manager schema pulls if we're bootstrapping
    //   or Raft is already fully functioning.
    //
    // Cannot be called twice.
    //
    future<> setup_group0_if_exist(db::system_keyspace&, service::storage_service& ss, cql3::query_processor& qp, service::migration_manager& mm);

    // Call at the end of the startup procedure, after the node entered NORMAL state.
    // `setup_group0()` must have finished earlier.
    //
    // If the node has just bootstrapped, causes the group 0 server to become a voter.
    //
    // If the node has just upgraded, enables a feature listener for the RAFT feature
    // which will start a procedure to create group 0 and switch administrative operations to use it.
    future<> finish_setup_after_join(service::storage_service& ss, cql3::query_processor& qp, service::migration_manager& mm, bool topology_change_enabled);

    // If Raft is disabled or in RECOVERY mode, returns `false`.
    // Otherwise:
    // - waits for the Raft upgrade procedure to finish if it's currently in progress,
    // - performs a Raft read barrier,
    // - returns `true`.
    //
    // This is a prerequisite for performing group 0 configuration operations.
    future<bool> wait_for_raft();

    // Check whether the given Raft server is a member of group 0 configuration
    // according to our current knowledge.
    //
    // If `include_voters_only` is `true`, returns `true` only if the server is a voting member.
    //
    // Precondition: joined_group0(). In particular, this can be called safely
    // if wait_for_raft() was called earlier and returned `true`.
    bool is_member(raft::server_id, bool include_voters_only);

    // Become a non-voter in group 0.
    //
    // Assumes we've finished the startup procedure (`setup_group0()` finished earlier).
    // `wait_for_raft` must've also been called earlier and returned `true`.
    future<> become_nonvoter();

    // Make the given server, other than us, a non-voter in group 0.
    //
    // Assumes we've finished the startup procedure (`setup_group0()` finished earlier).
    // `wait_for_raft` must've also been called earlier and returned `true`.
    future<> make_nonvoter(raft::server_id);

    // Make the given servers, other than us, a non-voter in group 0.
    //
    // Assumes we've finished the startup procedure (`setup_group0()` finished earlier).
    // `wait_for_raft` must've also been called earlier and returned `true`.
    future<> make_nonvoters(const std::unordered_set<raft::server_id>&);

    // Remove ourselves from group 0.
    //
    // Assumes we've finished the startup procedure (`setup_group0()` finished earlier).
    // Assumes to run during decommission, after the node entered LEFT status.
    // `wait_for_raft` must've also been called earlier and returned `true`.
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
    // `wait_for_raft` must've also been called earlier and returned `true`
    future<> remove_from_group0(raft::server_id);

    // Assumes that this node's Raft server ID is already initialized and returns it.
    // It's a fatal error if the id is missing.
    //
    // The returned ID is not empty.
    const raft::server_id& load_my_id();

    // Remove the node from raft config, retries raft::commit_status_unknown.
    // The function can only be called after wait_for_raft() successfully
    // completes and current state of group0 is not RECOVERY.
    future<> remove_from_raft_config(raft::server_id id);

    // Creates a group0 adder which uses the legacy GROUP0_MODIFY_CONFIG RPC.
    // It is meant to be used as a fallback when a proper handshake procedure
    // cannot be used (e.g. when completing the upgrade or group0 procedures
    // or when joining an old cluster which does not support JOIN_NODE RPC).
    shared_ptr<group0_handshaker> make_legacy_handshaker(bool can_vote);

    // Waits until all upgrade to raft group 0 finishes and all nodes switched
    // to use_post_raft_procedures.
    future<> wait_for_all_nodes_to_finish_upgrade(abort_source& as);

    raft_group0_client& client() {
        return _client;
    }

    // Return an instance of group 0. Valid only on shard 0,
    // after boot/upgrade is complete
    raft::server& group0_server() {
        return _raft_gr.group0();
    }

    // Returns a wrapper for the group0_server() with timeouts support.
    raft_server_with_timeouts group0_server_with_timeouts() {
        return _raft_gr.group0_with_timeouts();
    }

    // Returns true after the group 0 server has been started.
    bool joined_group0() const;

    const raft_address_map& address_map() const;
private:
    static void init_rpc_verbs(raft_group0& shard0_this);
    static future<> uninit_rpc_verbs(netw::messaging_service& ms);

    // Stop the group 0 server and remove it from the raft_group_registry.
    future<> stop_group0();

    future<bool> raft_upgrade_complete() const;

    // Handle peer_exchange RPC
    future<group0_peer_exchange> peer_exchange(discovery::peer_list peers);

    raft_server_for_group create_server_for_group0(raft::group_id id, raft::server_id my_id, service::storage_service& ss, cql3::query_processor& qp,
        service::migration_manager& mm, bool topology_change_enabled);

    // Creates or joins group 0 and switches schema/topology changes to use group 0.
    // Can be restarted after a crash. Does nothing if the procedure was already finished once.
    //
    // The main part of the procedure which may block (due to concurrent schema changes or communication with
    // other nodes) runs in background, so it's safe to call `upgrade_to_group0` and wait for it to finish
    // from places which must not block.
    //
    // Precondition: the SUPPORTS_RAFT cluster feature is enabled.
    future<> upgrade_to_group0(service::storage_service& ss, cql3::query_processor& qp, service::migration_manager& mm, bool topology_change_enabled);

    // Blocking part of `upgrade_to_group0`, runs in background.
    future<> do_upgrade_to_group0(group0_upgrade_state start_state, service::storage_service& ss, cql3::query_processor& qp, service::migration_manager& mm, bool topology_change_enabled);

    // Start a Raft server for the cluster-wide group 0 and join it to the group.
    // Called during bootstrap or upgrade.
    //
    // Uses `seeds` as contact points to discover other servers which will be part of group 0.
    //
    // If the server becomes the discovery leader, meaning that is it elected as the server
    // which creates group 0, then it will become a voter. Otherwise it will use the `handshaker`
    // to ask the discovery leader to join the node to the group, and the voting status
    // depends on how exactly this is implemented.
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
    // When joining an existing cluster, the provided handshaker should result in the node being added
    // as a non-voter. When the cluster is being upgraded and group 0 is being created, the node
    // should be joined as a voter.
    //
    // Preconditions: Raft local feature enabled
    // and we haven't initialized group 0 yet after last Scylla start (`joined_group0()` is false).
    // Postcondition: `joined_group0()` is true.
    future<> join_group0(std::vector<gms::inet_address> seeds, shared_ptr<group0_handshaker> handshaker, service::storage_service& ss, cql3::query_processor& qp, service::migration_manager& mm, db::system_keyspace& sys_ks, bool topology_change_enabled);

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
    future<> start_server_for_group0(raft::group_id group0_id, service::storage_service& ss, cql3::query_processor& qp, service::migration_manager& mm, bool topology_change_enabled);

    // Make the given server a non-voter in Raft group 0 configuration.
    // Retries on raft::commit_status_unknown.
    future<> make_raft_config_nonvoter(const std::unordered_set<raft::server_id>&);

    // Returns true if raft is enabled
    future<bool> use_raft();
};

} // end of namespace service
