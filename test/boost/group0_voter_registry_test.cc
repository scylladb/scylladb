/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/core/coroutine.hh>
#include <utility>

#include "service/raft/group0_voter_registry.hh"
#include "service/raft/raft_group0.hh"
#include "service/topology_state_machine.hh"

namespace {

// The mock implementation of the raft_server_info_accessor interface.
class server_info_accessor : public service::raft_server_info_accessor {
    std::unordered_map<raft::server_id, service::replica_state> _state;

public:
    const std::unordered_map<raft::server_id, service::replica_state>& get_members() const override {
        return _state;
    }

    [[nodiscard]] const service::replica_state& find(raft::server_id id) const override {
        auto it = _state.find(id);
        if (it == _state.end()) {
            throw std::runtime_error("server info missing!");
        }
        return it->second;
    }

    server_info_accessor& set_info(raft::server_id id, service::replica_state state) {
        _state[id] = std::move(state);
        return *this;
    }
};

// The mock implementation of the raft_voter_client interface.
class raft_voter_client : public service::raft_voter_client {

    std::unordered_set<raft::server_id> _voters;

public:
    [[nodiscard]] bool is_voter(raft::server_id id) const override {
        return _voters.contains(id);
    }

    [[nodiscard]] future<> set_voters_status(const std::unordered_set<raft::server_id>& nodes, service::can_vote can_vote, seastar::abort_source& as) override {
        if (can_vote == service::can_vote::yes) {
            _voters.insert(nodes.begin(), nodes.end());
        } else {
            for (const auto& node : nodes) {
                _voters.erase(node);
            }
        }
        return seastar::make_ready_future<>();
    }

    [[nodiscard]] const std::unordered_set<raft::server_id>& voters() const {
        return _voters;
    }
};

std::tuple<service::group0_voter_registry::instance_ptr, server_info_accessor*, raft_voter_client*> create_registry(
        std::optional<size_t> max_voters = std::nullopt) {
    auto info_accessor = std::make_unique<server_info_accessor>();
    auto voter_client = std::make_unique<raft_voter_client>();

    // The registry takes ownership of the info_accessor and voter_client, but pointers to them
    // are returned for testing purposes (access to the instances is needed to setup and validate).
    auto* info_accessor_ptr = info_accessor.get();
    auto* voter_client_ptr = voter_client.get();

    auto reg = service::group0_voter_registry::create(std::move(info_accessor), std::move(voter_client), max_voters);

    return {std::move(reg), info_accessor_ptr, voter_client_ptr};
}

} // namespace


BOOST_AUTO_TEST_SUITE(group0_voter_registry_test)


SEASTAR_TEST_CASE(single_node_is_selected_as_voter) {
    abort_source as;

    // Arrange: Create the registry.

    auto [reg, info_accessor, voter_client] = create_registry();

    // Act: Add a single node.

    const auto id = raft::server_id::create_random_id();

    info_accessor->set_info(id, {.datacenter = "dc", .rack = "rack"});

    co_await reg->insert_node(id, as);

    // Assert: The node has been selected as a voter.

    const auto& voters = voter_client->voters();

    BOOST_CHECK_EQUAL(voters.size(), 1);
    BOOST_CHECK(voters.contains(id));
}


SEASTAR_TEST_CASE(multiple_nodes_are_selected_as_voters) {
    abort_source as;

    // Arrange: Create the registry.

    auto [reg, info_accessor, voter_client] = create_registry();

    // Act: Add a set of multiple nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    info_accessor->set_info(ids[0], {.datacenter = "dc", .rack = "rack"});
    info_accessor->set_info(ids[1], {.datacenter = "dc", .rack = "rack"});

    const auto ids_set = ids | std::ranges::to<std::unordered_set>();
    co_await reg->insert_nodes(ids_set, as);

    // Assert: The nodes have been selected as voters.

    const auto& voters = voter_client->voters();

    BOOST_CHECK_EQUAL(voters.size(), 2);
    for (const auto& id : ids) {
        BOOST_CHECK(voters.contains(id));
    }
}


SEASTAR_TEST_CASE(can_insert_empty_set_of_nodes) {
    abort_source as;

    // Arrange: Create the registry.

    auto [reg, info_accessor, voter_client] = create_registry();

    // Act: Add an empty set of nodes.

    co_await reg->insert_nodes({}, as);

    // Assert: There are no voters.

    const auto& voters = voter_client->voters();

    BOOST_CHECK_EQUAL(voters.size(), 0);
}


SEASTAR_TEST_CASE(single_node_is_removed_from_voters) {
    abort_source as;

    // Arrange: Create the registry...

    auto [reg, info_accessor, voter_client] = create_registry();

    // ... and add the initial set of nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    info_accessor->set_info(ids[0], {.datacenter = "dc", .rack = "rack"});
    info_accessor->set_info(ids[1], {.datacenter = "dc", .rack = "rack"});

    const auto ids_set = ids | std::ranges::to<std::unordered_set>();
    co_await reg->insert_nodes(ids_set, as);

    // Act: Remove a single node.

    co_await reg->remove_node(ids[0], as);

    // Assert: The removed node is not amongst the voters.

    const auto& voters = voter_client->voters();

    BOOST_CHECK_EQUAL(voters.size(), 1);
    BOOST_CHECK(voters.contains(ids[1]));
}


SEASTAR_TEST_CASE(multiple_nodes_are_removed_from_voters) {
    abort_source as;

    // Arrange: Create the registry.

    auto [reg, info_accessor, voter_client] = create_registry();

    // ... and add the initial set of nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    info_accessor->set_info(ids[0], {.datacenter = "dc", .rack = "rack"});
    info_accessor->set_info(ids[1], {.datacenter = "dc", .rack = "rack"});
    info_accessor->set_info(ids[2], {.datacenter = "dc", .rack = "rack"});

    const auto ids_set = ids | std::ranges::to<std::unordered_set>();
    co_await reg->insert_nodes(ids_set, as);

    // Act: Remove multiple nodes.

    co_await reg->remove_nodes({ids[0], ids[2]}, as);

    // Assert: The removed nodes are not amongst the voters.

    const auto& voters = voter_client->voters();

    BOOST_CHECK_EQUAL(voters.size(), 1);
    BOOST_CHECK(voters.contains(ids[1]));
}


SEASTAR_TEST_CASE(can_remove_empty_set_of_nodes) {
    abort_source as;

    // Arrange: Create the registry.

    auto [reg, info_accessor, voter_client] = create_registry();

    // Act: Remove an empty set of nodes.

    co_await reg->remove_nodes({}, as);

    // Assert: There are no voters.

    const auto& voters = voter_client->voters();

    BOOST_CHECK_EQUAL(voters.size(), 0);
}


SEASTAR_TEST_CASE(insert_limit_is_reached) {
    abort_source as;

    // Arrange: Set the voters limit.

    constexpr size_t max_voters = 3;

    auto [reg, info_accessor, voter_client] = create_registry(max_voters);

    // Act: Add the nodes.

    const std::array ids = {
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    info_accessor->set_info(ids[0], {.datacenter = "dc", .rack = "rack"});
    info_accessor->set_info(ids[1], {.datacenter = "dc", .rack = "rack"});
    info_accessor->set_info(ids[2], {.datacenter = "dc", .rack = "rack"});
    info_accessor->set_info(ids[3], {.datacenter = "dc", .rack = "rack"});

    const auto ids_set = ids | std::ranges::to<std::unordered_set>();
    co_await reg->insert_nodes(ids_set, as);

    // Assert: There is just the "max_limit" voters - not all nodes were selected as voters.

    const auto& voters = voter_client->voters();

    BOOST_CHECK_EQUAL(voters.size(), max_voters);
}


SEASTAR_TEST_CASE(limit_kept_on_voter_removal) {
    abort_source as;

    // Arrange: Set the voters limit...

    constexpr size_t max_voters = 3;

    auto [reg, info_accessor, voter_client] = create_registry(max_voters);

    // ... and add the nodes to the voter registry.

    const std::array ids = {
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    info_accessor->set_info(ids[0], {.datacenter = "dc", .rack = "rack"});
    info_accessor->set_info(ids[1], {.datacenter = "dc", .rack = "rack"});
    info_accessor->set_info(ids[2], {.datacenter = "dc", .rack = "rack"});
    info_accessor->set_info(ids[3], {.datacenter = "dc", .rack = "rack"});

    BOOST_ASSERT(ids.size() > max_voters);

    const auto ids_set = ids | std::ranges::to<std::unordered_set>();
    co_await reg->insert_nodes(ids_set, as);

    // Act: Remove one of the existing voters.

    const auto& voters = voter_client->voters();

    const auto voter_id = *voters.cbegin();
    co_await reg->remove_node(voter_id, as);

    // Assert: There is still the "max_limit" voters - another node has been selected as a voter.

    BOOST_CHECK_EQUAL(voters.size(), max_voters);
    BOOST_CHECK(!voters.contains(voter_id));
}


SEASTAR_TEST_CASE(limit_is_kept_on_multiple_inserts) {
    abort_source as;

    // Arrange: Set the voters limit...

    constexpr size_t max_voters = 3;

    auto [reg, info_accessor, voter_client] = create_registry(max_voters);

    // ... and insert some initial nodes.

    const std::array initial_ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    info_accessor->set_info(initial_ids[0], {.datacenter = "dc", .rack = "rack"});
    info_accessor->set_info(initial_ids[1], {.datacenter = "dc", .rack = "rack"});

    const auto initial_ids_set = initial_ids | std::ranges::to<std::unordered_set>();
    co_await reg->insert_nodes(initial_ids_set, as);

    // Act: Add some other nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    info_accessor->set_info(ids[0], {.datacenter = "dc", .rack = "rack"});
    info_accessor->set_info(ids[1], {.datacenter = "dc", .rack = "rack"});

    const auto ids_set = ids | std::ranges::to<std::unordered_set>();
    co_await reg->insert_nodes(ids_set, as);

    // Assert: There is just the "max_limit" voters - not all nodes were selected as voters.

    const auto& voters = voter_client->voters();

    BOOST_CHECK_EQUAL(voters.size(), max_voters);
}


SEASTAR_TEST_CASE(limit_is_kept_on_removal_and_insert) {
    abort_source as;

    // Arrange: Set the voters limit...

    constexpr size_t max_voters = 3;

    auto [reg, info_accessor, voter_client] = create_registry(max_voters);

    // ... insert some initial nodes...

    const std::array initial_ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    info_accessor->set_info(initial_ids[0], {.datacenter = "dc", .rack = "rack"});
    info_accessor->set_info(initial_ids[1], {.datacenter = "dc", .rack = "rack"});
    info_accessor->set_info(initial_ids[2], {.datacenter = "dc", .rack = "rack"});

    const auto initial_ids_set = initial_ids | std::ranges::to<std::unordered_set>();
    co_await reg->insert_nodes(initial_ids_set, as);

    // ... and remove one of the initial nodes.

    co_await reg->remove_node(initial_ids[0], as);

    // Act: Add some other nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    info_accessor->set_info(ids[0], {.datacenter = "dc", .rack = "rack"});
    info_accessor->set_info(ids[1], {.datacenter = "dc", .rack = "rack"});

    const auto ids_set = ids | std::ranges::to<std::unordered_set>();
    co_await reg->insert_nodes(ids_set, as);

    // Assert: There is just the "max_limit" voters - not all nodes were selected as voters.

    const auto& voters = voter_client->voters();

    BOOST_CHECK_EQUAL(voters.size(), max_voters);
}


SEASTAR_TEST_CASE(voters_are_distributed_across_dcs) {
    abort_source as;

    // Arrange: Set the voters limit.

    constexpr size_t max_voters = 3;

    auto [reg, info_accessor, voter_client] = create_registry(max_voters);

    // Act: Add the nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    info_accessor->set_info(ids[0], {.datacenter = "dc-1", .rack = "rack"});
    info_accessor->set_info(ids[1], {.datacenter = "dc-1", .rack = "rack"});
    info_accessor->set_info(ids[2], {.datacenter = "dc-2", .rack = "rack"});
    info_accessor->set_info(ids[3], {.datacenter = "dc-2", .rack = "rack"});
    info_accessor->set_info(ids[4], {.datacenter = "dc-3", .rack = "rack"});
    info_accessor->set_info(ids[5], {.datacenter = "dc-3", .rack = "rack"});

    const auto ids_set = ids | std::ranges::to<std::unordered_set>();
    co_await reg->insert_nodes(ids_set, as);

    // Assert: There is no duplicate DC across the voters (having 3 voters and 3 DCs).

    const auto& voters = voter_client->voters();

    std::unordered_set<sstring> voters_dcs;
    for (const auto id : voters) {
        const auto& server_info = info_accessor->find(id);
        BOOST_CHECK_MESSAGE(voters_dcs.emplace(server_info.datacenter).second, fmt::format("Duplicate voter in the same DC: {}", server_info.datacenter));
    }
}


SEASTAR_TEST_CASE(voters_are_distributed_across_dcs_across_multiple_inserts) {
    abort_source as;

    // Arrange: Set the voters limit...

    constexpr size_t max_voters = 3;

    auto [reg, info_accessor, voter_client] = create_registry(max_voters);

    // ... and insert some initial nodes.

    const std::array initial_ids = {
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    info_accessor->set_info(initial_ids[0], {.datacenter = "dc-1", .rack = "rack"});
    info_accessor->set_info(initial_ids[1], {.datacenter = "dc-1", .rack = "rack"});
    info_accessor->set_info(initial_ids[2], {.datacenter = "dc-2", .rack = "rack"});
    info_accessor->set_info(initial_ids[3], {.datacenter = "dc-2", .rack = "rack"});

    const auto initial_ids_set = initial_ids | std::ranges::to<std::unordered_set>();
    co_await reg->insert_nodes(initial_ids_set, as);

    // Act: Add another datacenter nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    info_accessor->set_info(ids[0], {.datacenter = "dc-3", .rack = "rack"});
    info_accessor->set_info(ids[1], {.datacenter = "dc-3", .rack = "rack"});

    const auto ids_set = ids | std::ranges::to<std::unordered_set>();
    co_await reg->insert_nodes(ids_set, as);

    // Assert: There is no duplicate DC across the voters (having 3 voters and 3 DCs).

    const auto& voters = voter_client->voters();

    std::unordered_set<sstring> voters_dcs;
    for (const auto id : voters) {
        const auto& server_info = info_accessor->find(id);
        BOOST_CHECK_MESSAGE(voters_dcs.emplace(server_info.datacenter).second, fmt::format("Duplicate voter in the same DC: {}", server_info.datacenter));
    }
}


SEASTAR_TEST_CASE(voters_are_distributed_asymmetrically_across_two_dcs) {
    abort_source as;

    // Arrange: Set the voters limit.

    // Using a larger number of voters than the count of nodes to make sure that the voters are still distributed
    // asymmetrically across the DCs (i.e. one of the DC should still have less voters).

    constexpr size_t max_voters = 7;

    auto [reg, info_accessor, voter_client] = create_registry(max_voters);

    // Act: Add the nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    info_accessor->set_info(ids[0], {.datacenter = "dc-1", .rack = "rack"});
    info_accessor->set_info(ids[1], {.datacenter = "dc-1", .rack = "rack"});
    info_accessor->set_info(ids[2], {.datacenter = "dc-1", .rack = "rack"});
    info_accessor->set_info(ids[3], {.datacenter = "dc-2", .rack = "rack"});
    info_accessor->set_info(ids[4], {.datacenter = "dc-2", .rack = "rack"});
    info_accessor->set_info(ids[5], {.datacenter = "dc-2", .rack = "rack"});

    const auto ids_set = ids | std::ranges::to<std::unordered_set>();
    co_await reg->insert_nodes(ids_set, as);

    // Assert: One of the two DCs has a larger number of voters (asymmetric) - we want to have the chance to survive
    //         a loss of one DC (the one with smaller number of voters).

    const auto& voters = voter_client->voters();

    std::unordered_map<sstring, int> voters_dc_counts;
    for (const auto id : voters) {
        const auto& server_info = info_accessor->find(id);
        ++voters_dc_counts[server_info.datacenter];
    }

    BOOST_CHECK_NE(voters_dc_counts["dc-1"], voters_dc_counts["dc-2"]);
}


SEASTAR_TEST_CASE(voters_match_node_count) {
    abort_source as;

    // Arrange: Set the voters limit.

    constexpr size_t max_voters = 5;

    auto [reg, info_accessor, voter_client] = create_registry(max_voters);

    // Act: Add the nodes.

    const std::array ids = {
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    info_accessor->set_info(ids[0], {.datacenter = "dc-1", .rack = "rack"});
    info_accessor->set_info(ids[1], {.datacenter = "dc-1", .rack = "rack"});
    info_accessor->set_info(ids[2], {.datacenter = "dc-1", .rack = "rack"});
    info_accessor->set_info(ids[3], {.datacenter = "dc-2", .rack = "rack"});

    const auto ids_set = ids | std::ranges::to<std::unordered_set>();
    co_await reg->insert_nodes(ids_set, as);

    // Assert: All nodes became voters (i.e. voter tokens allocated according to the existing nodes).

    const auto& voters = voter_client->voters();

    BOOST_CHECK_EQUAL(voters.size(), ids.size());
}


SEASTAR_TEST_CASE(dc_cannot_have_majority_of_voters) {
    abort_source as;

    // Arrange: Set the voters limit.

    constexpr size_t max_voters = 7;

    auto [reg, info_accessor, voter_client] = create_registry(max_voters);

    // Act: Add the nodes.

    const std::array ids = {
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    info_accessor->set_info(ids[0], {.datacenter = "dc-1", .rack = "rack-1"});
    info_accessor->set_info(ids[1], {.datacenter = "dc-1", .rack = "rack-2"});
    info_accessor->set_info(ids[2], {.datacenter = "dc-2", .rack = "rack"});
    info_accessor->set_info(ids[3], {.datacenter = "dc-3", .rack = "rack"});

    const auto ids_set = ids | std::ranges::to<std::unordered_set>();
    co_await reg->insert_nodes(ids_set, as);

    // Assert: No DC has the majority of voters (i.e. all DCs only have 1 voter).

    const auto& voters = voter_client->voters();

    std::unordered_set<sstring> voters_dcs;
    for (const auto id : voters) {
        const auto& server_info = info_accessor->find(id);
        BOOST_CHECK_MESSAGE(voters_dcs.emplace(server_info.datacenter).second, fmt::format("Disallowed voter majority for the DC: {}", server_info.datacenter));
    }
    BOOST_CHECK_EQUAL(voters.size(), 3);
}


SEASTAR_TEST_CASE(dc_cannot_have_majority_of_voters_on_node_removal) {
    abort_source as;

    // Arrange: Set the voters limit.

    constexpr size_t max_voters = 7;

    auto [reg, info_accessor, voter_client] = create_registry(max_voters);

    // ... and add the nodes to the voter registry.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    info_accessor->set_info(ids[0], {.datacenter = "dc-1", .rack = "rack"});
    info_accessor->set_info(ids[1], {.datacenter = "dc-1", .rack = "rack"});
    info_accessor->set_info(ids[2], {.datacenter = "dc-2", .rack = "rack"});
    info_accessor->set_info(ids[3], {.datacenter = "dc-3", .rack = "rack"});
    info_accessor->set_info(ids[4], {.datacenter = "dc-3", .rack = "rack"});

    const auto ids_set = ids | std::ranges::to<std::unordered_set>();
    co_await reg->insert_nodes(ids_set, as);

    const auto& voters = voter_client->voters();

    // all nodes expected to become voters
    BOOST_CHECK_EQUAL(voters.size(), 5);

    // Act: Remove one voter node from DC-3.

    co_await reg->remove_node(ids[4], as);

    // Assert: No DC has the majority of voters (i.e. a voter has been removed from the DC-1 as well to avoid having majority).

    std::unordered_set<sstring> voters_dcs;
    for (const auto id : voters) {
        const auto& server_info = info_accessor->find(id);
        BOOST_CHECK_MESSAGE(voters_dcs.emplace(server_info.datacenter).second, fmt::format("Disallowed voter majority for the DC: {}", server_info.datacenter));
    }
    // ... this means we only have 3 voters left
    BOOST_CHECK_EQUAL(voters.size(), 3);
}


SEASTAR_TEST_CASE(dc_voters_increased_on_node_addition) {
    abort_source as;

    // Arrange: Set the voters limit...

    constexpr size_t max_voters = 7;

    auto [reg, info_accessor, voter_client] = create_registry(max_voters);

    //... and add some initial nodes to the voter registry.

    const std::array initial_ids = {
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    info_accessor->set_info(initial_ids[0], {.datacenter = "dc-1", .rack = "rack"});
    info_accessor->set_info(initial_ids[1], {.datacenter = "dc-1", .rack = "rack"});
    info_accessor->set_info(initial_ids[2], {.datacenter = "dc-2", .rack = "rack"});
    info_accessor->set_info(initial_ids[3], {.datacenter = "dc-3", .rack = "rack"});

    const auto initial_ids_set = initial_ids | std::ranges::to<std::unordered_set>();
    co_await reg->insert_nodes(initial_ids_set, as);

    // Act: Add another node to DC-3.

    const auto id = raft::server_id::create_random_id();
    info_accessor->set_info(id, {.datacenter = "dc-3", .rack = "rack"});

    co_await reg->insert_node(id, as);

    // Assert: A voter has been added for both DC-3 and DC-1 (still no DC with a majority/tie).

    const auto& voters = voter_client->voters();

    std::unordered_map<sstring, int> voters_dcs;
    for (const auto id : voters) {
        const auto& server_info = info_accessor->find(id);
        ++voters_dcs[server_info.datacenter];
    }
    BOOST_CHECK_EQUAL(voters_dcs["dc-1"], 2);
    BOOST_CHECK_EQUAL(voters_dcs["dc-2"], 1);
    BOOST_CHECK_EQUAL(voters_dcs["dc-3"], 2);
}


SEASTAR_TEST_CASE(voters_distributed_as_two_dc_asymmetric_after_third_dc_removal) {
    abort_source as;

    // Arrange: Set the voters limit...

    constexpr size_t max_voters = 5;

    auto [reg, info_accessor, voter_client] = create_registry(max_voters);

    // ... and add the initial nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    info_accessor->set_info(ids[0], {.datacenter = "dc-1", .rack = "rack"});
    info_accessor->set_info(ids[1], {.datacenter = "dc-1", .rack = "rack"});
    info_accessor->set_info(ids[2], {.datacenter = "dc-1", .rack = "rack"});
    info_accessor->set_info(ids[3], {.datacenter = "dc-2", .rack = "rack"});
    info_accessor->set_info(ids[4], {.datacenter = "dc-3", .rack = "rack"});
    info_accessor->set_info(ids[5], {.datacenter = "dc-3", .rack = "rack"});

    const auto ids_set = ids | std::ranges::to<std::unordered_set>();
    co_await reg->insert_nodes(ids_set, as);

    // Act: Remove the DC-3 nodes.

    co_await reg->remove_nodes({ids[4], ids[5]}, as);

    // Assert: All nodes became voters (i.e. voter tokens allocated according to the existing nodes),
    //         as when only having the two DCs to begin with.

    const auto& voters = voter_client->voters();

    BOOST_CHECK_EQUAL(voters.size(), ids.size() - 2);
}


SEASTAR_TEST_CASE(adaptive_voters_is_low_bound_with_lower_dc_count) {
    abort_source as;

    // Arrange: Set the adaptive voters limit.

    constexpr size_t max_voters = service::group0_voter_registry::MAX_VOTERS_ADAPTIVE;

    auto [reg, info_accessor, voter_client] = create_registry(max_voters);

    // Act: Add the nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    constexpr std::array dcs = {"dc-1", "dc-2", "dc-3"};

    info_accessor->set_info(ids[0], {.datacenter = dcs[0], .rack = "rack"});
    info_accessor->set_info(ids[1], {.datacenter = dcs[0], .rack = "rack"});
    info_accessor->set_info(ids[2], {.datacenter = dcs[0], .rack = "rack"});
    info_accessor->set_info(ids[3], {.datacenter = dcs[1], .rack = "rack"});
    info_accessor->set_info(ids[4], {.datacenter = dcs[1], .rack = "rack"});
    info_accessor->set_info(ids[5], {.datacenter = dcs[1], .rack = "rack"});
    info_accessor->set_info(ids[6], {.datacenter = dcs[1], .rack = "rack"});
    info_accessor->set_info(ids[7], {.datacenter = dcs[2], .rack = "rack"});
    info_accessor->set_info(ids[8], {.datacenter = dcs[2], .rack = "rack"});
    info_accessor->set_info(ids[9], {.datacenter = dcs[2], .rack = "rack"});

    const auto ids_set = ids | std::ranges::to<std::unordered_set>();
    co_await reg->insert_nodes(ids_set, as);

    // Assert: There are more voters than DCs (we're under the low bound).

    const auto& voters = voter_client->voters();

    BOOST_CHECK_GT(voters.size(), dcs.size());
}


SEASTAR_TEST_CASE(adaptive_voters_is_equal_with_regular_dc_count) {
    abort_source as;

    // Arrange: Set the adaptive voters limit.

    constexpr size_t max_voters = service::group0_voter_registry::MAX_VOTERS_ADAPTIVE;

    auto [reg, info_accessor, voter_client] = create_registry(max_voters);

    // Act: Add the nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    constexpr std::array dcs = {"dc-1", "dc-2", "dc-3", "dc-4", "dc-5", "dc-6", "dc-7"};

    info_accessor->set_info(ids[0], {.datacenter = dcs[0], .rack = "rack"});
    info_accessor->set_info(ids[1], {.datacenter = dcs[0], .rack = "rack"});
    info_accessor->set_info(ids[2], {.datacenter = dcs[0], .rack = "rack"});
    info_accessor->set_info(ids[3], {.datacenter = dcs[1], .rack = "rack"});
    info_accessor->set_info(ids[4], {.datacenter = dcs[1], .rack = "rack"});
    info_accessor->set_info(ids[5], {.datacenter = dcs[1], .rack = "rack"});
    info_accessor->set_info(ids[6], {.datacenter = dcs[2], .rack = "rack"});
    info_accessor->set_info(ids[7], {.datacenter = dcs[2], .rack = "rack"});
    info_accessor->set_info(ids[8], {.datacenter = dcs[3], .rack = "rack"});
    info_accessor->set_info(ids[9], {.datacenter = dcs[3], .rack = "rack"});
    info_accessor->set_info(ids[10], {.datacenter = dcs[4], .rack = "rack"});
    info_accessor->set_info(ids[11], {.datacenter = dcs[4], .rack = "rack"});
    info_accessor->set_info(ids[12], {.datacenter = dcs[5], .rack = "rack"});
    info_accessor->set_info(ids[13], {.datacenter = dcs[6], .rack = "rack"});
    info_accessor->set_info(ids[14], {.datacenter = dcs[6], .rack = "rack"});

    const auto ids_set = ids | std::ranges::to<std::unordered_set>();
    co_await reg->insert_nodes(ids_set, as);

    // Assert: The number of voters is equal to the number of DCs.

    const auto& voters = voter_client->voters();

    BOOST_CHECK_EQUAL(voters.size(), dcs.size());

    std::unordered_set<sstring> voters_dcs;
    for (const auto id : voters) {
        const auto& server_info = info_accessor->find(id);
        BOOST_CHECK_MESSAGE(voters_dcs.emplace(server_info.datacenter).second, fmt::format("Duplicate voter in DC: {}", server_info.datacenter));
    }
}


SEASTAR_TEST_CASE(adaptive_voters_is_high_bound_with_higher_dc_count) {
    abort_source as;

    // Arrange: Set the adaptive voters limit.

    constexpr size_t max_voters = service::group0_voter_registry::MAX_VOTERS_ADAPTIVE;

    auto [reg, info_accessor, voter_client] = create_registry(max_voters);

    // Act: Add the nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    constexpr std::array dcs = {"dc-1", "dc-2", "dc-3", "dc-4", "dc-5", "dc-6", "dc-7", "dc-8", "dc-9", "dc-10", "dc-11", "dc-12"};

    info_accessor->set_info(ids[0], {.datacenter = dcs[0], .rack = "rack"});
    info_accessor->set_info(ids[1], {.datacenter = dcs[0], .rack = "rack"});
    info_accessor->set_info(ids[2], {.datacenter = dcs[0], .rack = "rack"});
    info_accessor->set_info(ids[3], {.datacenter = dcs[1], .rack = "rack"});
    info_accessor->set_info(ids[4], {.datacenter = dcs[1], .rack = "rack"});
    info_accessor->set_info(ids[5], {.datacenter = dcs[1], .rack = "rack"});
    info_accessor->set_info(ids[6], {.datacenter = dcs[2], .rack = "rack"});
    info_accessor->set_info(ids[7], {.datacenter = dcs[2], .rack = "rack"});
    info_accessor->set_info(ids[8], {.datacenter = dcs[3], .rack = "rack"});
    info_accessor->set_info(ids[9], {.datacenter = dcs[3], .rack = "rack"});
    info_accessor->set_info(ids[10], {.datacenter = dcs[4], .rack = "rack"});
    info_accessor->set_info(ids[11], {.datacenter = dcs[4], .rack = "rack"});
    info_accessor->set_info(ids[12], {.datacenter = dcs[5], .rack = "rack"});
    info_accessor->set_info(ids[13], {.datacenter = dcs[6], .rack = "rack"});
    info_accessor->set_info(ids[14], {.datacenter = dcs[6], .rack = "rack"});
    info_accessor->set_info(ids[15], {.datacenter = dcs[7], .rack = "rack"});
    info_accessor->set_info(ids[16], {.datacenter = dcs[7], .rack = "rack"});
    info_accessor->set_info(ids[17], {.datacenter = dcs[7], .rack = "rack"});
    info_accessor->set_info(ids[18], {.datacenter = dcs[8], .rack = "rack"});
    info_accessor->set_info(ids[19], {.datacenter = dcs[8], .rack = "rack"});
    info_accessor->set_info(ids[20], {.datacenter = dcs[9], .rack = "rack"});
    info_accessor->set_info(ids[21], {.datacenter = dcs[9], .rack = "rack"});
    info_accessor->set_info(ids[22], {.datacenter = dcs[9], .rack = "rack"});
    info_accessor->set_info(ids[23], {.datacenter = dcs[10], .rack = "rack"});
    info_accessor->set_info(ids[24], {.datacenter = dcs[10], .rack = "rack"});
    info_accessor->set_info(ids[25], {.datacenter = dcs[11], .rack = "rack"});
    info_accessor->set_info(ids[26], {.datacenter = dcs[11], .rack = "rack"});

    const auto ids_set = ids | std::ranges::to<std::unordered_set>();
    co_await reg->insert_nodes(ids_set, as);

    // Assert: The number of voters is equal to the number of DCs.

    const auto& voters = voter_client->voters();

    BOOST_CHECK_LT(voters.size(), dcs.size());

    std::unordered_set<sstring> voters_dcs;
    for (const auto id : voters) {
        const auto& server_info = info_accessor->find(id);
        BOOST_CHECK_MESSAGE(voters_dcs.emplace(server_info.datacenter).second, fmt::format("Duplicate voter in DC: {}", server_info.datacenter));
    }
}


SEASTAR_TEST_CASE(voters_are_distributed_across_racks) {
    abort_source as;

    // Arrange: Set the voters limit.

    constexpr size_t max_voters = 3;

    auto [reg, info_accessor, voter_client] = create_registry(max_voters);

    // Act: Add the nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    info_accessor->set_info(ids[0], {.datacenter = "dc", .rack = "rack-1"});
    info_accessor->set_info(ids[1], {.datacenter = "dc", .rack = "rack-1"});
    info_accessor->set_info(ids[2], {.datacenter = "dc", .rack = "rack-1"});
    info_accessor->set_info(ids[3], {.datacenter = "dc", .rack = "rack-2"});
    info_accessor->set_info(ids[4], {.datacenter = "dc", .rack = "rack-2"});
    info_accessor->set_info(ids[5], {.datacenter = "dc", .rack = "rack-3"});
    info_accessor->set_info(ids[6], {.datacenter = "dc", .rack = "rack-3"});

    const auto ids_set = ids | std::ranges::to<std::unordered_set>();
    co_await reg->insert_nodes(ids_set, as);

    // Assert: There is no duplicate rack across the voters (having 3 voters and 3 racks).

    const auto& voters = voter_client->voters();

    std::unordered_set<sstring> voters_racks;
    for (const auto id : voters) {
        const auto& server_info = info_accessor->find(id);
        BOOST_CHECK_MESSAGE(voters_racks.emplace(server_info.rack).second, fmt::format("Duplicate voter in the same rack: {}", server_info.rack));
    }
}


SEASTAR_TEST_CASE(more_racks_preferred_over_more_nodes_in_two_dc) {
    abort_source as;

    // Arrange: Set the voters limit.

    constexpr size_t max_voters = 5;

    auto [reg, info_accessor, voter_client] = create_registry(max_voters);

    // Act: Add the nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    info_accessor->set_info(ids[0], {.datacenter = "dc-1", .rack = "rack-1"});
    info_accessor->set_info(ids[1], {.datacenter = "dc-1", .rack = "rack-2"});
    info_accessor->set_info(ids[2], {.datacenter = "dc-1", .rack = "rack-2"});
    info_accessor->set_info(ids[3], {.datacenter = "dc-2", .rack = "rack-3"});
    info_accessor->set_info(ids[4], {.datacenter = "dc-2", .rack = "rack-3"});
    info_accessor->set_info(ids[5], {.datacenter = "dc-2", .rack = "rack-3"});
    info_accessor->set_info(ids[6], {.datacenter = "dc-2", .rack = "rack-3"});

    const auto ids_set = ids | std::ranges::to<std::unordered_set>();
    co_await reg->insert_nodes(ids_set, as);

    // Assert: The racks are preferred over the nodes (DC-1 has more racks so it should have more voters, despite DC-2 having more nodes).

    const auto& voters = voter_client->voters();

    std::unordered_map<sstring, size_t> voters_dcs;
    for (const auto id : voters) {
        const auto& server_info = info_accessor->find(id);
        ++voters_dcs[server_info.datacenter];
    }

    BOOST_CHECK_GT(voters_dcs["dc-1"], voters_dcs["dc-2"]);
}


SEASTAR_TEST_CASE(dcs_preferred_over_racks) {
    abort_source as;

    // Arrange: Set the voters limit.

    constexpr size_t max_voters = 5;

    auto [reg, info_accessor, voter_client] = create_registry(max_voters);

    // Act: Add the nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    info_accessor->set_info(ids[0], {.datacenter = "dc-1", .rack = "rack-1"});
    info_accessor->set_info(ids[1], {.datacenter = "dc-1", .rack = "rack-2"});
    info_accessor->set_info(ids[2], {.datacenter = "dc-1", .rack = "rack-3"});
    info_accessor->set_info(ids[3], {.datacenter = "dc-2", .rack = "rack-4"});
    info_accessor->set_info(ids[4], {.datacenter = "dc-3", .rack = "rack-5"});
    info_accessor->set_info(ids[5], {.datacenter = "dc-4", .rack = "rack-6"});
    info_accessor->set_info(ids[6], {.datacenter = "dc-5", .rack = "rack-7"});

    const auto ids_set = ids | std::ranges::to<std::unordered_set>();
    co_await reg->insert_nodes(ids_set, as);

    // Assert: The DCs are preferred over racks when distributing voters (each DC should have a voter).

    const auto& voters = voter_client->voters();

    std::unordered_set<sstring> voters_dcs;
    for (const auto id : voters) {
        const auto& server_info = info_accessor->find(id);
        BOOST_CHECK_MESSAGE(voters_dcs.emplace(server_info.datacenter).second, fmt::format("Duplicate voter in the same DC: {}", server_info.datacenter));
    }
    BOOST_CHECK_EQUAL(voters.size(), max_voters);
}


BOOST_AUTO_TEST_SUITE_END()
