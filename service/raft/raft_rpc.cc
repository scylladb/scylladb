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
#include "service/raft/raft_rpc.hh"
#include "gms/inet_address.hh"
#include "gms/inet_address_serializer.hh"
#include "serializer_impl.hh"
#include "message/msg_addr.hh"
#include "message/messaging_service.hh"
#include "db/timeout_clock.hh"

raft_rpc::raft_rpc(netw::messaging_service& ms, uint64_t group_id, raft::server_id srv_id)
    : _group_id(group_id), _server_id(srv_id), _messaging(ms)
{}

future<> raft_rpc::send_snapshot(raft::server_id id, const raft::install_snapshot& snap) {
    return _messaging.send_raft_send_snapshot(
        netw::msg_addr(get_inet_address(id)), db::no_timeout, _group_id, _server_id, id, snap);
}

future<> raft_rpc::send_append_entries(raft::server_id id, const raft::append_request& append_request) {
    return _messaging.send_raft_append_entries(
        netw::msg_addr(get_inet_address(id)), db::no_timeout, _group_id, _server_id, id, append_request);
}

future<> raft_rpc::send_append_entries_reply(raft::server_id id, const raft::append_reply& reply) {
    return _messaging.send_raft_append_entries_reply(
        netw::msg_addr(get_inet_address(id)), db::no_timeout, _group_id, _server_id, id, reply);
}

future<> raft_rpc::send_vote_request(raft::server_id id, const raft::vote_request& vote_request) {
    return _messaging.send_raft_vote_request(
        netw::msg_addr(get_inet_address(id)), db::no_timeout, _group_id, _server_id, id, vote_request);
}

future<> raft_rpc::send_vote_reply(raft::server_id id, const raft::vote_reply& vote_reply) {
    return _messaging.send_raft_vote_reply(
        netw::msg_addr(get_inet_address(id)), db::no_timeout, _group_id, _server_id, id, vote_reply);
}

void raft_rpc::add_server(raft::server_id id, raft::server_info info) {
    // Parse gms::inet_address from server_info
    auto in = ser::as_input_stream(bytes_view(info));
    _server_addresses.emplace(std::pair(id, ser::deserialize(in, boost::type<gms::inet_address>())));
}

void raft_rpc::remove_server(raft::server_id id) {
    _server_addresses.erase(id);
}

future<> raft_rpc::abort() {
    return make_ready_future<>();
}

gms::inet_address raft_rpc::get_inet_address(raft::server_id id) const {
    auto it = _server_addresses.find(id);
    if (it == _server_addresses.end()) {
        throw std::runtime_error(format("Destination raft server not found (group id: {}, server id: {})", _group_id, id));
    }
    return it->second;
}

void raft_rpc::update_address_mapping(raft::server_id id, gms::inet_address addr) {
    _server_addresses[id] = addr;
}

void raft_rpc::append_entries(raft::server_id from, raft::append_request append_request) {
    _client->append_entries(from, std::move(append_request));
}

void raft_rpc::append_entries_reply(raft::server_id from, raft::append_reply reply) {
    _client->append_entries_reply(from, std::move(reply));
}

void raft_rpc::request_vote(raft::server_id from, raft::vote_request vote_request) {
    _client->request_vote(from, vote_request);
}

void raft_rpc::request_vote_reply(raft::server_id from, raft::vote_reply vote_reply) {
    _client->request_vote_reply(from, vote_reply);
}

future<> raft_rpc::apply_snapshot(raft::server_id from, raft::install_snapshot snp) {
    return _client->apply_snapshot(from, std::move(snp));
}
