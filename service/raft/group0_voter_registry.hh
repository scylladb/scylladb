/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "raft/raft.hh"
#include "service/raft/raft_group0.hh"

#include <seastar/core/coroutine.hh>

namespace service {

struct replica_state;

class raft_server_info_accessor {
public:
    virtual ~raft_server_info_accessor() = default;
    [[nodiscard]] virtual const replica_state& find(raft::server_id id) const = 0;
};

class raft_voter_client {
public:
    virtual ~raft_voter_client() = default;
    virtual future<> set_voters_status(const std::unordered_set<raft::server_id>& nodes, can_vote can_vote, abort_source& as) = 0;
};

class group0_voter_registry {

    const raft_server_info_accessor& _server_info_accessor;
    raft_voter_client& _voter_client;

    size_t _max_voters;

public:
    explicit group0_voter_registry(
            const raft_server_info_accessor& server_info_accessor, raft_voter_client& voter_client, size_t max_voters = std::numeric_limits<size_t>::max())
        : _server_info_accessor(server_info_accessor)
        , _voter_client(voter_client)
        , _max_voters(max_voters) {
    }

    // Insert a node to the voter registry
    future<> insert_node(raft::server_id node, abort_source& as);

    // Insert a list of nodes to the voter registry
    future<> insert_nodes(const std::unordered_set<raft::server_id>& nodes, abort_source& as);

    // Remove a node from the voter registry
    future<> remove_node(raft::server_id node, abort_source& as);

    // Remove a list of nodes from the voter registry
    future<> remove_nodes(const std::unordered_set<raft::server_id>& nodes, abort_source& as);
};

inline future<> group0_voter_registry::insert_node(raft::server_id node, abort_source& as) {
    co_return co_await insert_nodes({node}, as);
}

inline future<> group0_voter_registry::remove_node(raft::server_id node, abort_source& as) {
    co_return co_await remove_nodes({node}, as);
}

} // namespace service
