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

#include "raft/raft.hh"
#include "gms/inet_address.hh"
#include "message/messaging_service_fwd.hh"

// Scylla-specific implementation of raft RPC module.
//
// Uses `netw::messaging_service` as an underlying implementation for
// actually sending RPC messages.
class raft_rpc : public raft::rpc {
    uint64_t _group_id;
    raft::server_id _server_id;
    std::unordered_map<raft::server_id, gms::inet_address> _server_addresses;
    netw::messaging_service& _messaging;

public:
    explicit raft_rpc(netw::messaging_service& ms, uint64_t group_id, raft::server_id srv_id);

    future<> send_snapshot(raft::server_id server_id, const raft::install_snapshot& snap) override;
    future<> send_append_entries(raft::server_id id, const raft::append_request& append_request) override;
    future<> send_append_entries_reply(raft::server_id id, const raft::append_reply& reply) override;
    future<> send_vote_request(raft::server_id id, const raft::vote_request& vote_request) override;
    future<> send_vote_reply(raft::server_id id, const raft::vote_reply& vote_reply) override;
    void add_server(raft::server_id id, raft::server_info info) override;
    void remove_server(raft::server_id id) override;
    future<> abort() override;
    // Map raft server_id to inet_address to be consumed by `messaging_service`
    gms::inet_address get_inet_address(raft::server_id id) const;
    // Update inet_address mapping for a raft server with a given id.
    // In case a mapping exists for a given id, it should be equal to the supplied `addr`
    // otherwise the function will throw.
    void update_address_mapping(raft::server_id id, gms::inet_address addr);

    // Dispatchers to the `rpc_server` upon receiving an rpc message
    void append_entries(raft::server_id from, raft::append_request append_request);
    void append_entries_reply(raft::server_id from, raft::append_reply reply);
    void request_vote(raft::server_id from, raft::vote_request vote_request);
    void request_vote_reply(raft::server_id from, raft::vote_reply vote_reply);
    future<> apply_snapshot(raft::server_id from, raft::install_snapshot snp);
};
