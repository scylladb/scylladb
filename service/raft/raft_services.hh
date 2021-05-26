/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */
#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include "message/messaging_service_fwd.hh"
#include "gms/inet_address.hh"
#include "raft/raft.hh"
#include "raft/server.hh"
#include "service/raft/raft_address_map.hh"

namespace cql3 {

class query_processor;

} // namespace cql3

namespace gms {

class gossiper;

} // namespace gms

class raft_rpc;
class raft_gossip_failure_detector;

// This class is responsible for creating, storing and accessing raft servers.
// It also manages the raft rpc verbs initialization.
//
// `peering_sharded_service` inheritance is used to forward requests
// to the owning shard for a given raft group_id.
class raft_services : public seastar::peering_sharded_service<raft_services> {
public:
    using ticker_type = seastar::timer<lowres_clock>;
    // TODO: should be configurable.
    static constexpr ticker_type::duration tick_interval = std::chrono::milliseconds(100);
private:
    using create_server_result = std::pair<std::unique_ptr<raft::server>, raft_rpc*>;

    netw::messaging_service& _ms;
    cql3::query_processor& _qp;
    // Shard-local failure detector instance shared among all raft groups
    shared_ptr<raft_gossip_failure_detector> _fd;

    struct servers_value_type {
        std::unique_ptr<raft::server> server;
        raft_rpc* rpc;
        ticker_type ticker;
    };
    // Raft servers along with the corresponding timers to tick each instance.
    // Currently ticking every 100ms.
    std::unordered_map<raft::server_id, servers_value_type> _servers;
    // inet_address:es for remote raft servers known to us
    raft_address_map<> _srv_address_mappings;

    void init_rpc_verbs();
    seastar::future<> uninit_rpc_verbs();
    seastar::future<> stop_servers();

    create_server_result create_schema_server(raft::server_id id);

public:

    raft_services(netw::messaging_service& ms, gms::gossiper& gs, cql3::query_processor& qp);

    seastar::future<> init();
    seastar::future<> uninit();

    raft_rpc& get_rpc(raft::server_id id);
    // Start raft server instance, store in the map of raft servers and
    // initialize the associated timer to tick the server.
    future<> add_server(raft::server_id id, create_server_result srv);
    unsigned shard_for_group(const raft::group_id& gid) const;

    // Map raft server_id to inet_address to be consumed by `messaging_service`
    gms::inet_address get_inet_address(raft::server_id id) const;
    // Update inet_address mapping for a raft server with a given id.
    // In case a mapping exists for a given id, it should be equal to the supplied `addr`
    // otherwise the function will throw.
    void update_address_mapping(raft::server_id id, gms::inet_address addr, bool expiring);
    // Remove inet_address mapping for a raft server
    void remove_address_mapping(raft::server_id);
};
