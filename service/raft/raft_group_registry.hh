/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/gate.hh>

#include "message/messaging_service_fwd.hh"
#include "raft/raft.hh"
#include "raft/server.hh"
#include "service/raft/raft_address_map.hh"

namespace gms { class gossiper; }

namespace service {

class raft_rpc;
class raft_sys_table_storage;
class raft_gossip_failure_detector;

struct raft_group_not_found: public raft::error {
    raft::group_id gid;
    raft_group_not_found(raft::group_id gid_arg)
            : raft::error(format("Raft group {} not found", gid_arg)), gid(gid_arg)
    {}
};

// An entry in the group registry
struct raft_server_for_group {
    raft::group_id gid;
    std::unique_ptr<raft::server> server;
    std::unique_ptr<raft_ticker_type> ticker;
    raft_rpc& rpc;
    raft_sys_table_storage& persistence;
};

// This class is responsible for creating, storing and accessing raft servers.
// It also manages the raft rpc verbs initialization.
//
// `peering_sharded_service` inheritance is used to forward requests
// to the owning shard for a given raft group_id.
class raft_group_registry : public seastar::peering_sharded_service<raft_group_registry> {
private:
    // True if the feature is enabled
    bool _is_enabled;
    netw::messaging_service& _ms;
    // Raft servers along with the corresponding timers to tick each instance.
    // Currently ticking every 100ms.
    std::unordered_map<raft::group_id, raft_server_for_group> _servers;
    // inet_address:es for remote raft servers known to us
    raft_address_map<> _srv_address_mappings;
    // Shard-local failure detector instance shared among all raft groups
    seastar::shared_ptr<raft_gossip_failure_detector> _fd;

    void init_rpc_verbs();
    seastar::future<> uninit_rpc_verbs();
    seastar::future<> stop_servers() noexcept;

    raft_server_for_group& server_for_group(raft::group_id id);

    // Group 0 id, valid only on shard 0 after boot is over
    std::optional<raft::group_id> _group0_id;
public:
    raft_group_registry(bool is_enabled, netw::messaging_service& ms, gms::gossiper& gs);

    // Called manually at start
    seastar::future<> start();
    // Called by sharded<>::stop()
    seastar::future<> stop();

    raft_rpc& get_rpc(raft::group_id gid);

    // Find server for group by group id. Throws exception if
    // there is no such group.
    raft::server& get_server(raft::group_id gid);

    // Return an instance of group 0. Valid only on shard 0,
    // after boot is complete
    raft::server& group0();

    // Start raft server instance, store in the map of raft servers and
    // arm the associated timer to tick the server.
    future<> start_server_for_group(raft_server_for_group grp);
    unsigned shard_for_group(const raft::group_id& gid) const;
    shared_ptr<raft_gossip_failure_detector>& failure_detector() { return _fd; }
    raft_address_map<>& address_map() { return _srv_address_mappings; }

    bool is_enabled() const { return _is_enabled; }
};

} // end of namespace service
