/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "test/lib/scylla_test_case.hh"
#include <seastar/core/coroutine.hh>
#include <utility>

#include "service/raft/group0_voter_registry.hh"
#include "service/raft/raft_group0.hh"
#include "service/topology_state_machine.hh"

namespace {

class server_info_accessor : public service::raft_server_info_accessor {
    std::unordered_map<raft::server_id, service::replica_state> _state;

public:
    // Provide concrete implementations for all pure virtual functions
    [[nodiscard]] const service::replica_state& find(raft::server_id id) const override {
        auto it = _state.find(id);
        if (it != _state.end()) {
            return it->second;
        }
        throw std::runtime_error("server info missing!");
    }

    server_info_accessor& set_info(raft::server_id id, service::replica_state state) {
        _state[id] = std::move(state);
        return *this;
    }
};

class raft_voter_client : public service::raft_voter_client {

    std::unordered_set<raft::server_id> _voters;

public:
    // Provide concrete implementations for all pure virtual functions
    future<> set_voters_status(const std::unordered_set<raft::server_id>& nodes, service::can_vote can_vote, seastar::abort_source& as) override {
        if (can_vote == service::can_vote::yes) {
            _voters.insert(nodes.begin(), nodes.end());
        } else {
            for (const auto& node : nodes) {
                _voters.erase(node);
            }
        }
        return seastar::make_ready_future<>();
    }

    const std::unordered_set<raft::server_id>& voters() const {
        return _voters;
    }
};

} // namespace


SEASTAR_TEST_CASE(test_voter_registry_insert_single_node) {
    server_info_accessor server_info_accessor;
    raft_voter_client voter_client;
    service::group0_voter_registry reg{server_info_accessor, voter_client};

    // Arrange: Create server ids and set the datacenter and rack information.

    const auto id = raft::server_id::create_random_id();

    server_info_accessor.set_info(id, {.datacenter = "dc1", .rack = "rack1"});

    // Act: Add a single node.

    abort_source as;
    co_await reg.insert_node(id, as);

    // Assert: The node has been selected as a voter.

    const auto& voters = voter_client.voters();

    BOOST_CHECK_EQUAL(voters.size(), 1);
    BOOST_CHECK_EQUAL(*voters.begin(), id);
}


SEASTAR_TEST_CASE(test_voter_registry_insert_multiple_nodes) {
    server_info_accessor server_info_accessor;
    raft_voter_client voter_client;
    service::group0_voter_registry reg{server_info_accessor, voter_client};

    // Arrange: Create server ids and set the datacenter and rack information.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    server_info_accessor.set_info(ids[0], {.datacenter = "dc1", .rack = "rack1"});
    server_info_accessor.set_info(ids[1], {.datacenter = "dc2", .rack = "rack2"});

    // Act: Add a set of multiple nodes.

    const auto ids_set = ids | std::ranges::to<std::unordered_set>();

    abort_source as;
    co_await reg.insert_nodes(ids_set, as);

    // Assert: The nodes have been selected as voters.

    const auto& voters = voter_client.voters();

    BOOST_CHECK_EQUAL(voters.size(), 2);
    BOOST_CHECK_EQUAL_COLLECTIONS(voters.begin(), voters.end(), ids_set.begin(), ids_set.end());
}


SEASTAR_TEST_CASE(test_voter_registry_remove_single_node) {
    server_info_accessor server_info_accessor;
    raft_voter_client voter_client;
    service::group0_voter_registry reg{server_info_accessor, voter_client};

    // Arrange: Create server ids and set the datacenter and rack information...

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    server_info_accessor.set_info(ids[0], {.datacenter = "dc1", .rack = "rack1"});
    server_info_accessor.set_info(ids[1], {.datacenter = "dc2", .rack = "rack2"});

    // ... and add them to the voter registry.

    const auto ids_set = ids | std::ranges::to<std::unordered_set>();

    abort_source as;
    co_await reg.insert_nodes(ids_set, as);

    // Act: Remove a single node.

    co_await reg.remove_node(ids[0], as);

    // Assert: The removed node is not amongst the voters.

    const auto& voters = voter_client.voters();

    BOOST_CHECK_EQUAL(voters.size(), 1);
    BOOST_CHECK_EQUAL(*voters.begin(), ids[1]);
}


SEASTAR_TEST_CASE(test_voter_registry_remove_multiple_nodes) {
    server_info_accessor server_info_accessor;
    raft_voter_client voter_client;
    service::group0_voter_registry reg{server_info_accessor, voter_client};

    // Arrange: Create server ids and set the datacenter and rack information...

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    server_info_accessor.set_info(ids[0], {.datacenter = "dc1", .rack = "rack1"});
    server_info_accessor.set_info(ids[1], {.datacenter = "dc2", .rack = "rack2"});
    server_info_accessor.set_info(ids[2], {.datacenter = "dc3", .rack = "rack3"});

    // ... and add them to the voter registry.

    const auto ids_set = ids | std::ranges::to<std::unordered_set>();

    abort_source as;
    co_await reg.insert_nodes(ids_set, as);

    // Act: Remove multiple nodes.

    co_await reg.remove_nodes({ids[0], ids[2]}, as);

    // Assert: The removed nodes are not amongst the voters.

    const auto& voters = voter_client.voters();

    BOOST_CHECK_EQUAL(voters.size(), 1);
    BOOST_CHECK_EQUAL(*voters.begin(), ids[1]);
}


SEASTAR_TEST_CASE(test_voter_registry_voters_limit) {
    constexpr size_t max_voters = 3;

    // Arrange: Set the voters limit.

    server_info_accessor server_info_accessor;
    raft_voter_client voter_client;
    service::group0_voter_registry reg{server_info_accessor, voter_client, max_voters};

    const std::array ids = {
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    server_info_accessor.set_info(ids[0], {.datacenter = "dc1", .rack = "rack1"});
    server_info_accessor.set_info(ids[1], {.datacenter = "dc2", .rack = "rack2"});
    server_info_accessor.set_info(ids[2], {.datacenter = "dc3", .rack = "rack3"});
    server_info_accessor.set_info(ids[3], {.datacenter = "dc4", .rack = "rack4"});

    // Act: Add the nodes.

    const auto ids_set = ids | std::ranges::to<std::unordered_set>();

    abort_source as;
    co_await reg.insert_nodes(ids_set, as);

    // Assert: There is just the "max_limit" voters - not all nodes were selected as voters.

    const auto& voters = voter_client.voters();

    BOOST_CHECK_EQUAL(voters.size(), max_voters);
}


SEASTAR_TEST_CASE(test_voter_registry_keep_limit_on_removal) {
    constexpr size_t max_voters = 3;

    // Arrange: Set the voters limit...

    server_info_accessor server_info_accessor;
    raft_voter_client voter_client;
    service::group0_voter_registry reg{server_info_accessor, voter_client, max_voters};

    const std::array ids = {
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    server_info_accessor.set_info(ids[0], {.datacenter = "dc1", .rack = "rack1"});
    server_info_accessor.set_info(ids[1], {.datacenter = "dc2", .rack = "rack2"});
    server_info_accessor.set_info(ids[2], {.datacenter = "dc3", .rack = "rack3"});
    server_info_accessor.set_info(ids[3], {.datacenter = "dc4", .rack = "rack4"});

    // ... and add the nodes to the voter registry.

    const auto ids_set = ids | std::ranges::to<std::unordered_set>();

    abort_source as;
    co_await reg.insert_nodes(ids_set, as);

    const auto& voters = voter_client.voters();

    // Act: Remove one of the existing voters.

    const auto voter_id = *voters.cbegin();

    co_await reg.remove_node(voter_id, as);

    // Assert: There is still the "max_limit" voters - another node has been selected as a voter.

    BOOST_CHECK_EQUAL(voters.size(), max_voters);
    BOOST_CHECK_NE(*voters.cbegin(), voter_id);
}
