/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <boost/test/unit_test.hpp>

#include "service/raft/group0_voter_calculator.hh"


BOOST_AUTO_TEST_SUITE(group0_voter_calculator_test)


BOOST_AUTO_TEST_CASE(can_handle_empty_set_of_nodes) {

    // Arrange: Create the voter calculator (the default voters limit).

    const service::group0_voter_calculator voter_calc{};

    // Act: Add an empty set of nodes.

    const service::group0_voter_calculator::nodes_list_t nodes = {};

    const auto& [voters_add, voters_del] = voter_calc.calculate_voters(nodes);

    // Assert: There are no voters.

    BOOST_CHECK_EQUAL(voters_add.size(), 0);
    BOOST_CHECK_EQUAL(voters_del.size(), 0);
}


BOOST_AUTO_TEST_CASE(single_node_is_selected_as_voter) {

    // Arrange: Create the voter calculator (the default voters limit).

    const service::group0_voter_calculator voter_calc{};

    // Act: Add a single node.

    const std::array ids = {raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes{
            {ids[0], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = true}},
    };

    const auto& [voters_add, voters_del] = voter_calc.calculate_voters(nodes);

    // Assert: The node has been selected as a voter.

    BOOST_CHECK_EQUAL(voters_add.size(), 1);
    BOOST_CHECK(voters_add.contains(ids[0]));

    BOOST_CHECK_EQUAL(voters_del.size(), 0);
}


BOOST_AUTO_TEST_CASE(multiple_nodes_are_selected_as_voters) {

    // Arrange: Create the voter calculator (the default voters limit).

    const service::group0_voter_calculator voter_calc{};

    // Act: Add a set of multiple nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes{
            {ids[0], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[1], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = true}},
    };

    const auto& [voters_add, voters_del] = voter_calc.calculate_voters(nodes);

    // Assert: The nodes have been selected as voters.

    BOOST_CHECK_EQUAL(voters_add.size(), 2);
    for (const auto id : ids) {
        BOOST_CHECK(voters_add.contains(id));
    }

    BOOST_CHECK_EQUAL(voters_del.size(), 0);
}


BOOST_AUTO_TEST_CASE(single_node_is_removed_from_voters) {

    // Arrange: Create the voter calculator (the default voters limit).

    const service::group0_voter_calculator voter_calc{};

    // Act: Remove a single node (due to alive = false).

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes{
            {ids[0], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = false}},
            {ids[1], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = true}},
    };

    const auto& [voters_add, voters_del] = voter_calc.calculate_voters(nodes);

    // Assert: The removed node removed vrom the voters.

    BOOST_CHECK_EQUAL(voters_add.size(), 0);

    BOOST_CHECK_EQUAL(voters_del.size(), 1);
    BOOST_CHECK(voters_del.contains(ids[0]));
}


BOOST_AUTO_TEST_CASE(no_voters_added_when_all_nodes_are_dead) {

    // Arrange: Create the voter calculator (the default voters limit).

    const service::group0_voter_calculator voter_calc{};

    // Act: Add a set of multiple nodes (all dead).

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = false}},
            {ids[1], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = false}},
    };

    const auto& [voters_add, voters_del] = voter_calc.calculate_voters(nodes);

    // Assert: There are no voters.

    BOOST_CHECK_EQUAL(voters_add.size(), 0);
    BOOST_CHECK_EQUAL(voters_del.size(), 0);
}


BOOST_AUTO_TEST_CASE(multiple_nodes_are_removed_from_voters) {

    // Arrange: Create the voter calculator (the default voters limit).

    const service::group0_voter_calculator voter_calc{};

    // Act: Remove multiple nodes (due to alive = false).

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = false}},
            {ids[1], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[2], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = false}},
    };

    const auto& [voters_add, voters_del] = voter_calc.calculate_voters(nodes);

    // Assert: The removed nodes are not amongst the voters.

    BOOST_CHECK_EQUAL(voters_add.size(), 0);

    BOOST_CHECK_EQUAL(voters_del.size(), 2);
    for (const auto id : voters_del) {
        BOOST_CHECK(!nodes.at(id).is_alive);
    }
}


BOOST_AUTO_TEST_CASE(voter_limit_is_reached) {

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 3;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Add the nodes.

    const std::array ids = {
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[1], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[2], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[3], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = true}},
    };

    const auto& [voters_add, voters_del] = voter_calc.calculate_voters(nodes);

    // Assert: There is just the "max_limit" voters - not all nodes were selected as voters.

    BOOST_CHECK_EQUAL(voters_add.size(), max_voters);
    BOOST_CHECK_EQUAL(voters_del.size(), 0);
}


BOOST_AUTO_TEST_CASE(limit_kept_on_voter_removal) {

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 3;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Remove one of the existing voters.

    const std::array ids = {
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = true}},
            // the node being removed
            {ids[1], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = false}},
            {ids[2], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = true}},
            // the node being added
            {ids[3], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = true}},
    };

    BOOST_ASSERT(nodes.size() > max_voters);

    const auto& [voters_add, voters_del] = voter_calc.calculate_voters(nodes);

    // Assert: There is still the "max_limit" voters - another node has been selected as a voter.

    BOOST_CHECK_EQUAL(voters_add.size(), 1);
    BOOST_CHECK(voters_add.contains(ids[3]));

    BOOST_CHECK_EQUAL(voters_del.size(), 1);
    BOOST_CHECK(voters_del.contains(ids[1]));
}


BOOST_AUTO_TEST_CASE(limit_is_kept_on_multiple_inserts) {

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 3;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Add some further nodes to the initial set of voters.

    const std::array ids = {
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            // the initial set of nodes
            {ids[0], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[1], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = true}},
            // nodes being added
            {ids[2], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[3], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = true}},
    };

    BOOST_ASSERT(nodes.size() > max_voters);

    const auto& [voters_add, voters_del] = voter_calc.calculate_voters(nodes);

    // Assert: One of the nodes has been added as a voter, but only up to the "max_limit" voters.

    BOOST_CHECK_EQUAL(voters_add.size(), 1);
    BOOST_CHECK(voters_add.contains(ids[2]) || voters_add.contains(ids[3]));

    BOOST_CHECK_EQUAL(voters_del.size(), 0);
}


BOOST_AUTO_TEST_CASE(limit_is_kept_on_removal_and_insert) {

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 3;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Remove one of the voter nodes and add some other nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            // the initial set of nodes (first being removed)
            {ids[0], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = false}},
            {ids[1], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[2], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = true}},
            // nodes being added
            {ids[3], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[4], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = true}},
    };

    const auto [voters_add, voters_del] = voter_calc.calculate_voters(nodes);

    // Assert: The removed node has been replaced by another node, but only up to the "max_limit" voters.

    BOOST_CHECK_EQUAL(voters_add.size(), 1);
    BOOST_CHECK(voters_add.contains(ids[3]) || voters_add.contains(ids[4]));

    BOOST_CHECK_EQUAL(voters_del.size(), 1);
    BOOST_CHECK(voters_del.contains(ids[0]));
}


BOOST_AUTO_TEST_CASE(voters_are_distributed_across_dcs) {

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 3;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Add the nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc-1", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[1], {.datacenter = "dc-1", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[2], {.datacenter = "dc-2", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[3], {.datacenter = "dc-2", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[4], {.datacenter = "dc-3", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[5], {.datacenter = "dc-3", .rack = "rack", .is_voter = false, .is_alive = true}},
    };

    const auto& [voters_add, voters_del] = voter_calc.calculate_voters(nodes);

    // Assert: There is no duplicate DC across the voters (having 3 voters and 3 DCs).

    BOOST_CHECK_EQUAL(voters_add.size(), max_voters);
    BOOST_CHECK_EQUAL(voters_del.size(), 0);

    std::unordered_set<sstring> voters_dcs;
    for (const auto id : voters_add) {
        const auto& node = nodes.at(id);
        BOOST_CHECK_MESSAGE(voters_dcs.emplace(node.datacenter).second, fmt::format("Duplicate voter in the same DC: {}", node.datacenter));
    }
}


BOOST_AUTO_TEST_CASE(voters_are_distributed_across_dcs_across_multiple_inserts) {

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 3;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Add another datacenter nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            // the initial set of nodes
            {ids[0], {.datacenter = "dc-1", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[1], {.datacenter = "dc-1", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[2], {.datacenter = "dc-2", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[3], {.datacenter = "dc-2", .rack = "rack", .is_voter = false, .is_alive = true}},
            // nodes being added
            {ids[4], {.datacenter = "dc-3", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[5], {.datacenter = "dc-3", .rack = "rack", .is_voter = false, .is_alive = true}},
    };

    const auto& [voters_add, voters_del] = voter_calc.calculate_voters(nodes);

    // Assert: There is no duplicate DC across the voters (having 3 voters and 3 DCs).

    BOOST_CHECK_EQUAL(voters_add.size(), 1);
    BOOST_CHECK(voters_add.contains(ids[4]) || voters_add.contains(ids[5]));

    BOOST_CHECK_EQUAL(voters_del.size(), 1);
    BOOST_CHECK(voters_del.contains(ids[0]) || voters_del.contains(ids[1]));
}


BOOST_AUTO_TEST_CASE(voters_are_distributed_asymmetrically_across_two_dcs) {

    // Arrange: Set the voters limit and create the voter calculator.

    // Using a larger number of voters than the count of nodes to make sure that the voters are still distributed
    // asymmetrically across the DCs (i.e. one of the DC should still have less voters).

    constexpr size_t max_voters = 7;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Add the nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc-1", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[1], {.datacenter = "dc-1", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[2], {.datacenter = "dc-1", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[3], {.datacenter = "dc-2", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[4], {.datacenter = "dc-2", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[5], {.datacenter = "dc-2", .rack = "rack", .is_voter = false, .is_alive = true}},
    };

    const auto& [voters_add, voters_del] = voter_calc.calculate_voters(nodes);

    // Assert: One of the two DCs has a larger number of voters (asymmetric) - we want to have the chance to survive
    //         a loss of one DC (the one with smaller number of voters).

    BOOST_CHECK_GT(voters_add.size(), 0);
    BOOST_CHECK_EQUAL(voters_del.size(), 0);

    std::unordered_map<sstring, int> voters_dc_counts;
    for (const auto id : voters_add) {
        const auto& node = nodes.at(id);
        ++voters_dc_counts[node.datacenter];
    }

    BOOST_CHECK_GT(voters_dc_counts["dc-1"], 0);
    BOOST_CHECK_GT(voters_dc_counts["dc-2"], 0);
    BOOST_CHECK_NE(voters_dc_counts["dc-1"], voters_dc_counts["dc-2"]);
}


BOOST_AUTO_TEST_CASE(voters_match_node_count) {

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 5;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Add the nodes.

    const std::array ids = {
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc-1", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[1], {.datacenter = "dc-1", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[2], {.datacenter = "dc-1", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[3], {.datacenter = "dc-2", .rack = "rack", .is_voter = false, .is_alive = true}},
    };

    const auto& [voters_add, voters_del] = voter_calc.calculate_voters(nodes);

    // Assert: All nodes became voters (i.e. voter tokens allocated according to the existing nodes).

    BOOST_CHECK_EQUAL(voters_add.size(), nodes.size());
    BOOST_CHECK_EQUAL(voters_del.size(), 0);
}


BOOST_AUTO_TEST_CASE(dc_cannot_have_majority_of_voters) {
    abort_source as;

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 7;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Add the nodes.

    const std::array ids = {
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc-1", .rack = "rack-1", .is_voter = false, .is_alive = true}},
            {ids[1], {.datacenter = "dc-1", .rack = "rack-2", .is_voter = false, .is_alive = true}},
            {ids[2], {.datacenter = "dc-2", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[3], {.datacenter = "dc-3", .rack = "rack", .is_voter = false, .is_alive = true}},
    };

    const auto& [voters_add, voters_del] = voter_calc.calculate_voters(nodes);

    // Assert: No DC has the majority of voters (i.e. all DCs only have 1 voter).

    BOOST_CHECK_EQUAL(voters_add.size(), 3);
    BOOST_CHECK_EQUAL(voters_del.size(), 0);

    std::unordered_set<sstring> voters_dcs;
    for (const auto id : voters_add) {
        const auto& node = nodes.at(id);
        BOOST_CHECK_MESSAGE(voters_dcs.emplace(node.datacenter).second, fmt::format("Disallowed voter majority for the DC: {}", node.datacenter));
    }
}


BOOST_AUTO_TEST_CASE(dc_cannot_have_majority_of_voters_on_node_removal) {
    abort_source as;

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 7;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Remove one voter node from DC-3.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc-1", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[1], {.datacenter = "dc-1", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[2], {.datacenter = "dc-2", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[3], {.datacenter = "dc-3", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[4], {.datacenter = "dc-3", .rack = "rack", .is_voter = true, .is_alive = false}},
    };

    const auto& [voters_add, voters_del] = voter_calc.calculate_voters(nodes);

    // Assert: No DC has the majority of voters (i.e. a voter has been removed from the DC-1 as well to avoid having majority).

    BOOST_CHECK_EQUAL(voters_add.size(), 0);
    BOOST_CHECK_EQUAL(voters_del.size(), 2);

    // the dead node has been removed
    BOOST_CHECK(voters_del.contains(ids[4]));
    // and one of the nodes from DC-1 has been removed as well
    BOOST_CHECK(voters_del.contains(ids[0]) || voters_del.contains(ids[1]));
}


BOOST_AUTO_TEST_CASE(dc_voters_increased_on_node_addition) {
    abort_source as;

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 7;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Add another node to DC-3.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            // the initial set of nodes
            {ids[0], {.datacenter = "dc-1", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[1], {.datacenter = "dc-1", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[2], {.datacenter = "dc-2", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[3], {.datacenter = "dc-3", .rack = "rack", .is_voter = true, .is_alive = true}},
            // the node being added
            {ids[4], {.datacenter = "dc-3", .rack = "rack", .is_voter = false, .is_alive = true}},
    };

    const auto& [voters_add, voters_del] = voter_calc.calculate_voters(nodes);

    // Assert: A voter has been added for both DC-3 and DC-1 (still no DC with a majority/tie).

    BOOST_CHECK_EQUAL(voters_add.size(), 2);
    BOOST_CHECK_EQUAL(voters_del.size(), 0);

    // the new node has been added
    BOOST_CHECK(voters_add.contains(ids[4]));
    // the node from DC-1 has been added as well
    BOOST_CHECK(voters_add.contains(ids[1]));
}


BOOST_AUTO_TEST_CASE(voters_distributed_as_two_dc_asymmetric_after_third_dc_removal) {
    abort_source as;

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 5;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Remove the DC-3 nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc-1", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[1], {.datacenter = "dc-1", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[2], {.datacenter = "dc-1", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[3], {.datacenter = "dc-2", .rack = "rack", .is_voter = true, .is_alive = true}},
            // the nodes being removed
            {ids[4], {.datacenter = "dc-3", .rack = "rack", .is_voter = true, .is_alive = false}},
            {ids[5], {.datacenter = "dc-3", .rack = "rack", .is_voter = true, .is_alive = false}},
    };

    const auto& [voters_add, voters_del] = voter_calc.calculate_voters(nodes);

    // Assert: All nodes became voters (i.e. voter tokens allocated according to the existing nodes),
    //         as when only having the two DCs to begin with.

    BOOST_CHECK_EQUAL(voters_add.size(), 1);
    BOOST_CHECK_EQUAL(voters_del.size(), 2);

    // the non-voter node has been switched to a voter
    BOOST_CHECK(voters_add.contains(ids[1]));

    // the dead nodes have been removed
    for (const auto id : {ids[4], ids[5]}) {
        BOOST_CHECK(voters_del.contains(id));
    }
}


BOOST_AUTO_TEST_CASE(dead_nodes_cannot_become_voters) {

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 3;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Add the nodes (one of them marked dead).

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[1], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = false}},
            {ids[2], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = true}},
    };

    const auto& [voters_add, voters_del] = voter_calc.calculate_voters(nodes);

    // Assert: The dead node is not amongst the voters.

    BOOST_CHECK_EQUAL(voters_add.size(), 2);
    BOOST_CHECK(!voters_add.contains(ids[1]));

    BOOST_CHECK_EQUAL(voters_del.size(), 0);
}


BOOST_AUTO_TEST_CASE(dead_voters_are_removed_on_reload) {

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 3;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Mark one of the nodes as dead and refresh the registry.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[1], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = false}},
            {ids[2], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = true}},
    };

    const auto& [voters_add, voters_del] = voter_calc.calculate_voters(nodes);

    // Assert: The dead node has been removed from the voters.

    BOOST_CHECK_EQUAL(voters_add.size(), 0);

    BOOST_CHECK_EQUAL(voters_del.size(), 1);
    BOOST_CHECK(voters_del.contains(ids[1]));
}


BOOST_AUTO_TEST_CASE(voters_are_distributed_across_racks) {

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 3;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Add the nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc", .rack = "rack-1", .is_voter = false, .is_alive = true}},
            {ids[1], {.datacenter = "dc", .rack = "rack-1", .is_voter = false, .is_alive = true}},
            {ids[2], {.datacenter = "dc", .rack = "rack-1", .is_voter = false, .is_alive = true}},
            {ids[3], {.datacenter = "dc", .rack = "rack-2", .is_voter = false, .is_alive = true}},
            {ids[4], {.datacenter = "dc", .rack = "rack-2", .is_voter = false, .is_alive = true}},
            {ids[5], {.datacenter = "dc", .rack = "rack-3", .is_voter = false, .is_alive = true}},
            {ids[6], {.datacenter = "dc", .rack = "rack-3", .is_voter = false, .is_alive = true}},
    };

    const auto& [voters_add, voters_del] = voter_calc.calculate_voters(nodes);

    // Assert: There is no duplicate rack across the voters (having 3 voters and 3 racks).

    BOOST_CHECK_EQUAL(voters_add.size(), max_voters);
    BOOST_CHECK_EQUAL(voters_del.size(), 0);

    std::unordered_set<sstring> voters_racks;
    for (const auto id : voters_add) {
        const auto& node = nodes.at(id);
        BOOST_CHECK_MESSAGE(voters_racks.emplace(node.rack).second, fmt::format("Duplicate voter in the same rack: {}", node.rack));
    }
}


BOOST_AUTO_TEST_CASE(more_racks_preferred_over_more_nodes_in_two_dc) {

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 3;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Add the nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc-1", .rack = "rack-1", .is_voter = false, .is_alive = true}},
            {ids[1], {.datacenter = "dc-1", .rack = "rack-2", .is_voter = false, .is_alive = true}},
            {ids[2], {.datacenter = "dc-1", .rack = "rack-2", .is_voter = false, .is_alive = true}},
            {ids[3], {.datacenter = "dc-2", .rack = "rack-3", .is_voter = false, .is_alive = true}},
            {ids[4], {.datacenter = "dc-2", .rack = "rack-3", .is_voter = false, .is_alive = true}},
            {ids[5], {.datacenter = "dc-2", .rack = "rack-3", .is_voter = false, .is_alive = true}},
            {ids[6], {.datacenter = "dc-2", .rack = "rack-3", .is_voter = false, .is_alive = true}},
    };

    const auto& [voters_add, voters_del] = voter_calc.calculate_voters(nodes);

    // Assert: The racks are preferred over the nodes (DC-1 has more racks so it should have more voters, despite DC-2 having more nodes).

    BOOST_CHECK_EQUAL(voters_add.size(), max_voters);
    BOOST_CHECK_EQUAL(voters_del.size(), 0);

    std::unordered_map<sstring, size_t> voters_dcs;
    for (const auto id : voters_add) {
        const auto& node = nodes.at(id);
        ++voters_dcs[node.datacenter];
    }

    BOOST_CHECK_GT(voters_dcs["dc-1"], voters_dcs["dc-2"]);
}


BOOST_AUTO_TEST_CASE(dcs_preferred_over_racks) {

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 3;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Add the nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc-1", .rack = "rack-1", .is_voter = false, .is_alive = true}},
            {ids[1], {.datacenter = "dc-1", .rack = "rack-2", .is_voter = false, .is_alive = true}},
            {ids[2], {.datacenter = "dc-1", .rack = "rack-3", .is_voter = false, .is_alive = true}},
            {ids[3], {.datacenter = "dc-2", .rack = "rack-4", .is_voter = false, .is_alive = true}},
            {ids[4], {.datacenter = "dc-3", .rack = "rack-5", .is_voter = false, .is_alive = true}},
            {ids[5], {.datacenter = "dc-4", .rack = "rack-6", .is_voter = false, .is_alive = true}},
            {ids[6], {.datacenter = "dc-5", .rack = "rack-7", .is_voter = false, .is_alive = true}},
    };

    const auto& [voters_add, voters_del] = voter_calc.calculate_voters(nodes);

    // Assert: The DCs are preferred over racks when distributing voters (each DC should have a voter).

    BOOST_CHECK_EQUAL(voters_add.size(), max_voters);
    BOOST_CHECK_EQUAL(voters_del.size(), 0);

    std::unordered_set<sstring> voters_dcs;
    for (const auto id : voters_add) {
        const auto& node = nodes.at(id);
        BOOST_CHECK_MESSAGE(voters_dcs.emplace(node.datacenter).second, fmt::format("Duplicate voter in the same DC: {}", node.datacenter));
    }
}


BOOST_AUTO_TEST_SUITE_END()
