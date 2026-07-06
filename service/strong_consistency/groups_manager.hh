/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "locator/abstract_replication_strategy.hh"
#include "locator/tablets.hh"
#include "message/messaging_service.hh"
#include "service/raft/raft_group_registry.hh"
#include "cql3/query_processor.hh"
#include "db/commitlog/raft_commitlog_replay_buffer.hh"

#include <seastar/core/abort_source.hh>

namespace db {
class system_keyspace;
class raft_commitlog_replay_buffer;
}

namespace gms {
class gossiper;
}

namespace service {
class migration_manager;
}

namespace service::strong_consistency {

class raft_server;

/// A cache of leader locations for raft groups where this node is not a replica.
/// Populated by the CQL transport layer after a redirect reveals the actual leader.
///
/// Uses a sweep-based eviction strategy tied to token_metadata updates:
/// begin_sweep() before iterating tablets, mark_seen() for each existing group,
/// end_sweep() to evict entries whose groups no longer exist.
class tablet_group_leader_cache {
    struct entry {
        locator::host_id leader;
        bool seen = false;
    };
    std::unordered_map<raft::group_id, entry> _entries;

public:
    void put(raft::group_id group, locator::host_id leader) {
        auto [it, inserted] = _entries.try_emplace(group, entry{leader});
        if (!inserted) {
            it->second.leader = leader;
        }
    }

    std::optional<locator::host_id> get(raft::group_id group) const {
        auto it = _entries.find(group);
        if (it != _entries.end()) {
            return it->second.leader;
        }
        return std::nullopt;
    }

    void erase(raft::group_id group) {
        _entries.erase(group);
    }

    void begin_sweep() {
        for (auto& [_, e] : _entries) {
            e.seen = false;
        }
    }

    void mark_seen(raft::group_id group) {
        auto it = _entries.find(group);
        if (it != _entries.end()) {
            it->second.seen = true;
        }
    }

    void end_sweep() {
        std::erase_if(_entries, [](const auto& p) { return !p.second.seen; });
    }
};

/// A sharded service responsible for the lifecycle and access
/// management of all Raft groups for strongly consistent tablets hosted on this node.
///
/// Listens for token_metadata updates to automatically start Raft servers for tablets newly
/// assigned to this node and schedule the deletion of Raft servers for tablets that have moved away.
///
/// It serves as the entry point for read and write requests via acquire_server() method. It is guaranteed
/// that the raft::server instance and its associated state managed by groups_manager cannot be
/// stopped or destroyed while the returned raft_server object is alive.
///
/// Runs a background fiber (leader_info_updater) per group that monitors the raft::server state
/// and computes the next write timestamp as soon as the server becomes leader.
/// This allows write requests to proceed without waiting for read_barrier(),
/// which would otherwise be needed to compute the timestamp.
class groups_manager : public peering_sharded_service<groups_manager> {
    class state_machine_impl;
    class rpc_impl;
    class stable_timestamp_tracker;

    friend class raft_server;

    struct leader_info {
        // The Raft term this structure describes.
        raft::term_t term;

        // The last timestamp used for mutations in this term.
        api::timestamp_type last_timestamp;
    };

    struct raft_group_state : public boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>> {
        bool has_tablet = false;
        lw_shared_ptr<gate> gate = nullptr;
        raft::server* server = nullptr;

        // Serialized chain of raft::server control operations (start/stop).
        // This serialization handles (rare) cases where a tablet is migrated out
        // before the raft::server has finished initializing, or conversely,
        // when a tablet is migrated back to this node before deinitialization completes.
        // Subsequent operations wait for the previous one to complete.
        shared_future<> server_control_op = make_ready_future<>();

        // Populated only when this node thinks it's a tablet raft group leader.
        std::optional<leader_info> leader_info = std::nullopt;
        condition_variable leader_info_cond = condition_variable();
        future<> leader_info_updater = make_ready_future<>();

        api::timestamp_type stable_timestamp = api::min_timestamp;
    };

    /// Periodically refreshes the stable_timestamp of the raft groups this node leads.
    ///
    /// stable_timestamp of a group is the minimum, taken across all of its replicas,
    /// of the highest timestamp each replica has applied. Equivalently, it is the
    /// highest timestamp such that every replica has already applied all mutations up
    /// to and including it. A leader can therefore safely serve reads at any timestamp
    /// not exceeding it without a read barrier.
    class stable_timestamp_tracker {
        replica::database& _db;
        netw::messaging_service& _ms;
        std::unordered_map<raft::group_id, raft_group_state>& _raft_groups;

        // Drives shutdown of the periodic refresh fiber.
        abort_source _as;
        // The background fiber that periodically refreshes stable_timestamp.
        future<> _fiber = make_ready_future<>();

        // Bumps state.stable_timestamp to candidate if it is greater.
        void advance(raft_group_state& state, api::timestamp_type candidate);

        // A single refresh pass: for every tablet of a group led by this node,
        // collects applied timestamps from all replicas and advances the group's
        // stable_timestamp to the minimum across replicas.
        future<> refresh();

        // The loop driving refresh() on a fixed interval until aborted.
        future<> run();

    public:
        stable_timestamp_tracker(replica::database& db, netw::messaging_service& ms,
            std::unordered_map<raft::group_id, raft_group_state>& raft_groups);

        // Idempotently launches the background refresh fiber (no-op if already
        // running). Called from groups_manager::update() because the
        // strongly_consistent_tables feature may only become enabled after
        // groups_manager::start() has run.
        void start();

        // Requests shutdown and waits for the background fiber to finish.
        future<> stop();
    };

    netw::messaging_service& _ms;
    raft_group_registry& _raft_gr;
    cql3::query_processor& _qp;
    replica::database& _db;
    service::migration_manager& _mm;
    db::system_keyspace& _sys_ks;
    gms::feature_service& _features;
    gms::gossiper& _gossiper;
    db::raft_commitlog_replay_buffer& _raft_replay_buffer;
    std::unordered_map<raft::group_id, raft_group_state> _raft_groups = {};
    boost::intrusive::list<raft_group_state, boost::intrusive::constant_time_size<false>> _starting_groups;
    locator::token_metadata_ptr _pending_tm = nullptr;
    bool _started = false;

    tablet_group_leader_cache _leader_cache;

    stable_timestamp_tracker _stable_timestamp_tracker;

    // Should be called on the shard that hosts the Raft group
    future<> start_raft_group(locator::global_tablet_id tablet,
        raft::group_id group_id,
        locator::token_metadata_ptr tm);

    void schedule_raft_group_deletion(raft::group_id group_id, raft_group_state& group_state);

    void schedule_raft_groups_deletion(bool all);

    future<> leader_info_updater(raft_group_state& state, locator::global_tablet_id tablet, raft::group_id gid);

    void init_messaging_service();
    future<> uninit_messaging_service();

public:
    groups_manager(netw::messaging_service& ms, raft_group_registry& raft_gr,
        cql3::query_processor& qp, replica::database& _db, service::migration_manager& mm, db::system_keyspace& sys_ks,
        gms::feature_service& features, gms::gossiper& gossiper, db::raft_commitlog_replay_buffer& raft_replay_buffer);

    // Called whenever a new token_metadata is published on this shard.
    // Starts raft::server instances for all strongly consistent tablets now
    // residing on this shard, and schedules removal of servers for tablets
    // that have moved away.
    //
    // Note that the method is synchronous: it only initiates these operations
    // and does not wait for their completion.
    void update(locator::token_metadata_ptr new_tm);

    // The raft_server instance is used to submit write commands and perform read_barrier() before reads.
    future<raft_server> acquire_server(table_id table_id, raft::group_id group_id, abort_source& as);

    // Called during node boot. Starts all raft::server instances corresponding
    // to the latest group0 state in the background.
    void start();

    // Called during node shutdown. Waits for all raft::server instances to stop.
    future<> stop();

    future<> wait_for_groups_to_start(lowres_clock::time_point timeout);

    // Sends an RPC to every host that holds a tablet replica of the given table, asking it to wait
    // until the raft groups for those tablets are started and ready to serve queries.
    // For the local node, waits directly without an RPC.
    future<> wait_for_table_raft_groups_on_all_hosts(table_id table, lowres_clock::time_point timeout);

    tablet_group_leader_cache& leader_cache() { return _leader_cache; }

    std::optional<locator::tablet_routing_info_v2> check_tablet_version(
        const replica::table&,
        const dht::token&,
        const locator::tablet_version_block) const;
};

/// A temporary, RAII-style handle to an active Raft group server instance,
/// used to safely submit commands or perform consistency barriers.
///
/// The holder guarantees that the underlying raft::server and its associated state
/// managed by groups_manager cannot be stopped or destroyed while this raft_server object is alive.
/// It ensures that even if a topology change triggers the deletion of the Raft group,
/// the shutdown sequence will wait until this handle is destroyed, preventing use-after-free
/// errors during ongoing operations.
class raft_server {
private:
    groups_manager::raft_group_state& _state;
    gate::holder _holder;

public:
    raft_server(groups_manager::raft_group_state& state, gate::holder holder);

    raft::server& server() {
        return *_state.server;
    }

    // Possible results:
    //   timestamp_with_term - timestamp to use for a new mutation request
    //   raft::not_a_leader - this node is not a leader
    //   need_wait_for_leader - the caller needs to wait on the specified future and then retry `begin_mutate`
    struct timestamp_with_term {
        api::timestamp_type timestamp;
        raft::term_t term;
    };
    struct need_wait_for_leader {
        future<> future;
    };
    using begin_mutate_result = std::variant<timestamp_with_term, raft::not_a_leader, need_wait_for_leader>;
    begin_mutate_result begin_mutate(abort_source&);

    // Possible results:
    //   ok - this node is the leader, proceed with read_barrier() locally
    //   raft::not_a_leader - this node is not a leader, redirect to the leader
    //   need_wait_for_leader - the leader is unknown, the caller needs to wait and retry
    struct ok {};
    using begin_read_result = std::variant<ok, raft::not_a_leader, need_wait_for_leader>;
    begin_read_result begin_read(abort_source&);
};

} // namespace service::strong_consistency
