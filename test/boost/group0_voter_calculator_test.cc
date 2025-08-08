/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <boost/test/data/test_case.hpp>
#include <boost/test/unit_test.hpp>

#include "service/raft/group0_voter_calculator.hh"


BOOST_AUTO_TEST_SUITE(group0_voter_calculator_test)


BOOST_AUTO_TEST_CASE(can_handle_empty_set_of_nodes) {

    // Arrange: Create the voter calculator (the default voters limit).

    const service::group0_voter_calculator voter_calc{};

    // Act: Add an empty set of nodes.

    const service::group0_voter_calculator::nodes_list_t nodes = {};

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: No voters are selected when the input node list is empty.

    BOOST_CHECK_EQUAL(voters.size(), 0);
}


BOOST_AUTO_TEST_CASE(single_node_is_selected_as_voter) {

    // Arrange: Create the voter calculator (the default voters limit).

    const service::group0_voter_calculator voter_calc{};

    // Act: Add a single node.

    const std::array ids = {raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes{
            {ids[0], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = true}},
    };

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: The node has been selected as a voter.

    BOOST_CHECK_EQUAL(voters.size(), 1);
    BOOST_CHECK(voters.contains(ids[0]));
}


BOOST_AUTO_TEST_CASE(multiple_nodes_are_selected_as_voters) {

    // Arrange: Create the voter calculator (the default voters limit).

    const service::group0_voter_calculator voter_calc{};

    // Act: Add a set of multiple nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes{
            {ids[0], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[1], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[2], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = true}},
    };

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: The nodes have been selected as voters.

    BOOST_CHECK_EQUAL(voters.size(), ids.size());
    for (const auto id : ids) {
        BOOST_CHECK(voters.contains(id));
    }
}


BOOST_AUTO_TEST_CASE(no_voters_selected_when_all_nodes_are_dead) {

    // Arrange: Create the voter calculator (the default voters limit).

    const service::group0_voter_calculator voter_calc{};

    // Act: Add a set of multiple nodes (all dead).

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = false}},
            {ids[1], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = false}},
    };

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: No voters are selected since all nodes are dead.

    BOOST_CHECK_EQUAL(voters.size(), 0);
}


BOOST_AUTO_TEST_CASE(dead_nonvoters_dont_become_voters) {

    // Arrange: Create the voter calculator (the default voters limit).

    const service::group0_voter_calculator voter_calc{};

    // Act: Remove multiple dead nodes (due to alive = false).

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = false}},
            {ids[1], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[2], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = false}},
            {ids[3], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[4], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = false}},
    };

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: The dead non-voter nodes are not being selected as voters.

    BOOST_CHECK_EQUAL(voters.size(), 3);
    for (const auto id : voters) {
        BOOST_CHECK(nodes.at(id).is_voter);
    }
}


BOOST_AUTO_TEST_CASE(requires_at_least_one_voter) {

    // Arrange/Act: Create the voter calculator with the zero voters limit.
    // Assert: The constructor throws an exception when the voter limit is set to zero.

    BOOST_CHECK_THROW(const service::group0_voter_calculator voter_calc{0}, std::invalid_argument);
}


BOOST_AUTO_TEST_CASE(voter_limit_is_kept) {

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

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: The number of voters does not exceed the specified voter limit.

    BOOST_CHECK_EQUAL(voters.size(), max_voters);
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

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: The voter limit is maintained after removing a voter, and another node is promoted to maintain the limit.

    BOOST_CHECK_EQUAL(voters.size(), max_voters);
    BOOST_CHECK(!voters.contains(ids[1]));
    BOOST_CHECK(voters.contains(ids[3]));
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

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: The voter limit is respected even after adding multiple nodes.

    BOOST_CHECK_EQUAL(voters.size(), max_voters);
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

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: The voter limit is maintained after removing a voter and adding new nodes.

    BOOST_CHECK_EQUAL(voters.size(), max_voters);
    BOOST_CHECK(!voters.contains(ids[0]));
}


BOOST_AUTO_TEST_CASE(existing_voters_are_kept) {

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 3;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Add the nodes (one of them is already a voter).

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[1], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[2], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[3], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[4], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = true}},
    };

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: Existing voters are retained in the voter set.

    BOOST_CHECK_EQUAL(voters.size(), max_voters);

    for (const auto& [id, node] : nodes | std::views::filter([](const auto& node) {
             return node.second.is_voter;
         })) {
        BOOST_CHECK(voters.contains(id));
    }
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

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: Voters are evenly distributed across datacenters, with no duplicate datacenter in the voter set.

    BOOST_CHECK_EQUAL(voters.size(), max_voters);

    std::unordered_set<sstring> voters_dcs;
    for (const auto id : voters) {
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

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: Voters are evenly distributed across datacenters, with no duplicate datacenter in the voter set.

    BOOST_CHECK_EQUAL(voters.size(), max_voters);

    std::unordered_set<sstring> voters_dcs;
    for (const auto id : voters) {
        const auto& node = nodes.at(id);
        BOOST_CHECK_MESSAGE(voters_dcs.emplace(node.datacenter).second, fmt::format("Duplicate voter in the same DC: {}", node.datacenter));
    }
}


BOOST_AUTO_TEST_CASE(voters_are_distributed_asymmetrically_across_two_dcs) {

    // Arrange: Set the voters limit and create the voter calculator.

    // Using a larger number of voters than the count of nodes to make sure that the voters are still distributed
    // asymmetrically across the DCs (i.e., one of the DC should still have less voters).

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

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: Voters are distributed asymmetrically across two datacenters, ensuring fault tolerance
    //         in case one datacenter is lost.

    std::unordered_map<sstring, int> voters_dc_counts;
    for (const auto id : voters) {
        const auto& node = nodes.at(id);
        ++voters_dc_counts[node.datacenter];
    }

    BOOST_CHECK_NE(voters_dc_counts["dc-1"], voters_dc_counts["dc-2"]);
    if (voters_dc_counts["dc-1"] > voters_dc_counts["dc-2"]) {
        BOOST_CHECK_EQUAL(voters_dc_counts["dc-1"], 3);
        BOOST_CHECK_EQUAL(voters_dc_counts["dc-2"], 2);
    } else {
        BOOST_CHECK_EQUAL(voters_dc_counts["dc-1"], 2);
        BOOST_CHECK_EQUAL(voters_dc_counts["dc-2"], 3);
    }
}


BOOST_AUTO_TEST_CASE(voters_match_node_count) {

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 5;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Add the nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc-1", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[1], {.datacenter = "dc-1", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[2], {.datacenter = "dc-1", .rack = "rack", .is_voter = false, .is_alive = true}},
    };

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: All nodes became voters (i.e., voter tokens allocated according to the existing nodes).

    BOOST_CHECK_EQUAL(voters.size(), nodes.size());
}


BOOST_AUTO_TEST_CASE(dc_cannot_have_half_or_more_of_voters) {
    abort_source as;

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 7;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Add the nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc-1", .rack = "rack-1", .is_voter = false, .is_alive = true}},
            {ids[1], {.datacenter = "dc-1", .rack = "rack-2", .is_voter = false, .is_alive = true}},
            {ids[2], {.datacenter = "dc-1", .rack = "rack-2", .is_voter = false, .is_alive = true}},
            {ids[3], {.datacenter = "dc-2", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[4], {.datacenter = "dc-3", .rack = "rack", .is_voter = false, .is_alive = true}},
    };

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: No DC has half or more voters (i.e., all DCs only have 1 voter).

    BOOST_CHECK_EQUAL(voters.size(), 3);

    std::unordered_set<sstring> voters_dcs;
    for (const auto id : voters) {
        const auto& node = nodes.at(id);
        BOOST_CHECK_MESSAGE(voters_dcs.emplace(node.datacenter).second, fmt::format("The DC {} has half or more voters which is not allowed", node.datacenter));
    }
}


BOOST_AUTO_TEST_CASE(dc_cannot_have_half_or_more_of_voters_on_node_removal) {
    abort_source as;

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 7;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Remove one voter node from DC-3.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc-1", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[1], {.datacenter = "dc-1", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[2], {.datacenter = "dc-1", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[3], {.datacenter = "dc-2", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[4], {.datacenter = "dc-3", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[5], {.datacenter = "dc-3", .rack = "rack", .is_voter = false, .is_alive = false}},
    };

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: No DC has half or more voters (i.e., voters have been removed from the DC-1 as well
    //         to avoid having half of the voters).

    BOOST_CHECK_EQUAL(voters.size(), 3);

    std::unordered_set<sstring> voters_dcs;
    for (const auto id : voters) {
        const auto& node = nodes.at(id);
        BOOST_CHECK_MESSAGE(voters_dcs.emplace(node.datacenter).second, fmt::format("The DC {} has half or more voters which is not allowed", node.datacenter));
    }
}


BOOST_AUTO_TEST_CASE(dc_voters_increased_on_node_addition) {
    abort_source as;

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 7;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Add another node to DC-3.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            // the initial set of nodes
            {ids[0], {.datacenter = "dc-1", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[1], {.datacenter = "dc-1", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[2], {.datacenter = "dc-1", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[3], {.datacenter = "dc-2", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[4], {.datacenter = "dc-3", .rack = "rack", .is_voter = true, .is_alive = true}},
            // the node being added
            {ids[5], {.datacenter = "dc-3", .rack = "rack", .is_voter = false, .is_alive = true}},
    };

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: A voter has been added for both DC-3 and DC-1 (still no DC with a majority/tie).

    BOOST_CHECK_EQUAL(voters.size(), 5);

    std::unordered_map<sstring, size_t> voters_dcs;
    for (const auto id : voters) {
        const auto& node = nodes.at(id);
        ++voters_dcs[node.datacenter];
    }

    BOOST_CHECK_EQUAL(voters_dcs["dc-1"], 2);
    BOOST_CHECK_EQUAL(voters_dcs["dc-2"], 1);
    BOOST_CHECK_EQUAL(voters_dcs["dc-3"], 2);
}


BOOST_AUTO_TEST_CASE(all_nodes_become_voters_after_third_dc_removal) {
    abort_source as;

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 5;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Remove the DC-3 nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc-1", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[1], {.datacenter = "dc-1", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[2], {.datacenter = "dc-1", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[3], {.datacenter = "dc-1", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[4], {.datacenter = "dc-2", .rack = "rack", .is_voter = true, .is_alive = true}},
            // the nodes being removed
            {ids[5], {.datacenter = "dc-3", .rack = "rack", .is_voter = true, .is_alive = false}},
            {ids[6], {.datacenter = "dc-3", .rack = "rack", .is_voter = true, .is_alive = false}},
    };

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: All nodes became voters (i.e., voter tokens allocated according to the existing nodes),
    //         as when only having the two DCs to begin with.

    BOOST_CHECK_EQUAL(voters.size(), 5);
}


BOOST_AUTO_TEST_CASE(voters_distributed_as_two_dc_asymmetric_after_third_dc_removal) {
    abort_source as;

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 7;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Remove the DC-3 nodes.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc-1", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[1], {.datacenter = "dc-1", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[2], {.datacenter = "dc-1", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[3], {.datacenter = "dc-2", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[4], {.datacenter = "dc-2", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[5], {.datacenter = "dc-2", .rack = "rack", .is_voter = true, .is_alive = true}},
            // the nodes being removed
            {ids[6], {.datacenter = "dc-3", .rack = "rack", .is_voter = false, .is_alive = false}},
            {ids[7], {.datacenter = "dc-3", .rack = "rack", .is_voter = false, .is_alive = false}},
    };

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: The voters are distributed asymmetrically across the two DCs, ensuring fault tolerance
    //         in case one datacenter is lost.

    BOOST_CHECK_EQUAL(voters.size(), 5);

    std::unordered_map<sstring, size_t> voters_dcs;
    for (const auto id : voters) {
        const auto& node = nodes.at(id);
        ++voters_dcs[node.datacenter];
    }

    BOOST_CHECK_NE(voters_dcs["dc-1"], voters_dcs["dc-2"]);
    if (voters_dcs["dc-1"] > voters_dcs["dc-2"]) {
        BOOST_CHECK_EQUAL(voters_dcs["dc-1"], 3);
        BOOST_CHECK_EQUAL(voters_dcs["dc-2"], 2);
    } else {
        BOOST_CHECK_EQUAL(voters_dcs["dc-1"], 2);
        BOOST_CHECK_EQUAL(voters_dcs["dc-2"], 3);
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

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: Dead nodes are not selected as voters, and odd number is enforced (2 alive -> 1 voter).

    BOOST_CHECK_EQUAL(voters.size(), 1);
    BOOST_CHECK(!voters.contains(ids[1]));
}


BOOST_AUTO_TEST_CASE(dead_voters_kept_if_no_other_voters_are_available) {

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 5;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Mark some nodes as dead and refresh the registry.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = false}},
            {ids[1], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[2], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[3], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = false}},
            {ids[4], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = false}},
    };

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: Dead voters are retained in the voter set only if no other alive voters are available.
    //         Dead non-voters are not promoted to voters.

    BOOST_CHECK_EQUAL(voters.size(), 3);
    for (const auto id : voters) {
        const auto& node = nodes.at(id);
        BOOST_CHECK(node.is_voter);
    }
}


BOOST_AUTO_TEST_CASE(dead_voters_moved_to_available_alive_nodes) {

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 5;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Mark a voter node as dead (having other alive nodes) and refresh the registry.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[1], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[2], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[3], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = false}},
            {ids[4], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[5], {.datacenter = "dc", .rack = "rack", .is_voter = true, .is_alive = true}},
    };

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: Dead voters are replaced by available alive nodes, ensuring the voter set contains only alive nodes.

    BOOST_CHECK_EQUAL(voters.size(), max_voters);
    BOOST_CHECK(!voters.contains(ids[3]));
    BOOST_CHECK(voters.contains(ids[4]));
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

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: There is no duplicate rack across the voters (having 3 voters and 3 racks).

    BOOST_CHECK_EQUAL(voters.size(), max_voters);

    std::unordered_set<sstring> voters_racks;
    for (const auto id : voters) {
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
            {ids[0], {.datacenter = "dc-2", .rack = "rack-3", .is_voter = false, .is_alive = true}},
            {ids[1], {.datacenter = "dc-1", .rack = "rack-1", .is_voter = false, .is_alive = true}},
            {ids[2], {.datacenter = "dc-1", .rack = "rack-2", .is_voter = false, .is_alive = true}},
            {ids[3], {.datacenter = "dc-2", .rack = "rack-3", .is_voter = false, .is_alive = true}},
            {ids[4], {.datacenter = "dc-2", .rack = "rack-3", .is_voter = false, .is_alive = true}},
            {ids[5], {.datacenter = "dc-1", .rack = "rack-2", .is_voter = false, .is_alive = true}},
            {ids[6], {.datacenter = "dc-2", .rack = "rack-3", .is_voter = false, .is_alive = true}},
    };

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: The racks are preferred over the nodes (DC-1 has more racks so it should have more voters, despite DC-2 having more nodes).

    BOOST_CHECK_EQUAL(voters.size(), max_voters);

    std::unordered_map<sstring, size_t> voters_dcs;
    for (const auto id : voters) {
        const auto& node = nodes.at(id);
        ++voters_dcs[node.datacenter];
    }

    BOOST_CHECK_GT(voters_dcs["dc-1"], voters_dcs["dc-2"]);

    // Check that the DC-1 has voters in different racks.
    std::unordered_set<sstring> voters_racks;
    for (const auto id : voters | std::views::filter([&nodes](const auto id) {
             return nodes.at(id).datacenter == "dc-1";
         })) {
        const auto& node = nodes.at(id);
        BOOST_CHECK_MESSAGE(voters_racks.emplace(node.rack).second, fmt::format("Duplicate voter in the same rack: {}", node.rack));
    }
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
            {ids[1], {.datacenter = "dc-2", .rack = "rack-4", .is_voter = false, .is_alive = true}},
            {ids[2], {.datacenter = "dc-1", .rack = "rack-2", .is_voter = false, .is_alive = true}},
            {ids[3], {.datacenter = "dc-4", .rack = "rack-6", .is_voter = false, .is_alive = true}},
            {ids[4], {.datacenter = "dc-3", .rack = "rack-5", .is_voter = false, .is_alive = true}},
            {ids[5], {.datacenter = "dc-1", .rack = "rack-3", .is_voter = false, .is_alive = true}},
            {ids[6], {.datacenter = "dc-5", .rack = "rack-7", .is_voter = false, .is_alive = true}},
    };

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: The DCs are preferred over racks when distributing voters (each DC should have a voter).

    BOOST_CHECK_EQUAL(voters.size(), max_voters);

    std::unordered_set<sstring> voters_dcs;
    for (const auto id : voters) {
        const auto& node = nodes.at(id);
        BOOST_CHECK_MESSAGE(voters_dcs.emplace(node.datacenter).second, fmt::format("Duplicate voter in the same DC: {}", node.datacenter));
    }
}


BOOST_AUTO_TEST_CASE(existing_voters_are_retained_across_dcs) {

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 3;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Add the nodes (3 of them are voters).

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc-1", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[1], {.datacenter = "dc-2", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[2], {.datacenter = "dc-3", .rack = "rack", .is_voter = true, .is_alive = true}},
            {ids[3], {.datacenter = "dc-4", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[4], {.datacenter = "dc-5", .rack = "rack", .is_voter = true, .is_alive = true}},
    };

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: Existing voters are retained in the voter set.

    BOOST_CHECK_EQUAL(voters.size(), max_voters);

    for (const auto& [id, node] : nodes | std::views::filter([](const auto& node) {
             return node.second.is_voter;
         })) {
        BOOST_CHECK(voters.contains(id));
    }
}


BOOST_AUTO_TEST_CASE(existing_voters_are_kept_across_racks) {

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 3;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Add the nodes (3 of them are voters).

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc", .rack = "rack-1", .is_voter = true, .is_alive = true}},
            {ids[1], {.datacenter = "dc", .rack = "rack-2", .is_voter = false, .is_alive = true}},
            {ids[2], {.datacenter = "dc", .rack = "rack-3", .is_voter = true, .is_alive = true}},
            {ids[3], {.datacenter = "dc", .rack = "rack-4", .is_voter = false, .is_alive = true}},
            {ids[4], {.datacenter = "dc", .rack = "rack-5", .is_voter = true, .is_alive = true}},
    };

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: Existing voters are retained in the voter set.

    BOOST_CHECK_EQUAL(voters.size(), max_voters);

    for (const auto& [id, node] : nodes | std::views::filter([](const auto& node) {
             return node.second.is_voter;
         })) {
        BOOST_CHECK(voters.contains(id));
    }
}


BOOST_DATA_TEST_CASE(leader_is_retained_as_voter, boost::unit_test::data::make({0, 1, 2}), leader_node_idx) {

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 3;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Add a third DC to a 2 DC cluster.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            // The initial nodes (just 2 DCs)
            {ids[0], {.datacenter = "dc-1", .rack = "rack", .is_voter = true, .is_alive = true, .is_leader = leader_node_idx == 0}},
            {ids[1], {.datacenter = "dc-1", .rack = "rack", .is_voter = true, .is_alive = true, .is_leader = leader_node_idx == 1}},
            {ids[2], {.datacenter = "dc-2", .rack = "rack", .is_voter = true, .is_alive = true, .is_leader = leader_node_idx == 2}},
            {ids[3], {.datacenter = "dc-2", .rack = "rack", .is_voter = false, .is_alive = true}},
            // The new nodes (3rd DC)
            // - this will lead to 1 voter being removed from the DC-1
            {ids[4], {.datacenter = "dc-3", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[5], {.datacenter = "dc-3", .rack = "rack", .is_voter = false, .is_alive = true}},
    };

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: The current leader is retained as a voter, even after adding
    //         the third datacenter and redistributing voters.

    BOOST_CHECK_EQUAL(voters.size(), max_voters);
    BOOST_CHECK(voters.contains(ids[leader_node_idx]));
}


BOOST_DATA_TEST_CASE(leader_is_retained_as_voter_in_racks, boost::unit_test::data::make({0, 1, 3}), leader_node_idx) {

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 3;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Add a third DC to a 2 DC cluster.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            // The initial nodes (just 2 DCs)
            {ids[0], {.datacenter = "dc-1", .rack = "rack-1", .is_voter = true, .is_alive = true, .is_leader = leader_node_idx == 0}},
            {ids[1], {.datacenter = "dc-1", .rack = "rack-2", .is_voter = true, .is_alive = true, .is_leader = leader_node_idx == 1}},
            {ids[2], {.datacenter = "dc-2", .rack = "rack-3", .is_voter = false, .is_alive = true}},
            {ids[3], {.datacenter = "dc-2", .rack = "rack-4", .is_voter = true, .is_alive = true, .is_leader = leader_node_idx == 3}},
            // The new nodes (3rd DC)
            // - this will lead to 1 voter being removed from the DC-1
            {ids[4], {.datacenter = "dc-3", .rack = "rack-5", .is_voter = false, .is_alive = true}},
            {ids[5], {.datacenter = "dc-3", .rack = "rack-6", .is_voter = false, .is_alive = true}},
    };

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: The current leader is retained as a voter, even after adding
    //         the third datacenter and redistributing voters.

    BOOST_CHECK_EQUAL(voters.size(), max_voters);
    BOOST_CHECK(voters.contains(ids[leader_node_idx]));
}


BOOST_DATA_TEST_CASE(leader_is_retained_as_voter_in_two_dc_asymmetric_setup, boost::unit_test::data::make({0, 1}), leader_node_idx) {

    // Arrange: Set the voters limit and create the voter calculator.

    constexpr size_t max_voters = 3;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Remove nodes from a 2 DC cluster so that each DC has 1 node left.

    // - this will lead to 1 voter being removed from one DC
    // - the leader should be retained as a voter

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            // The result nodes (just one per DC)
            {ids[0], {.datacenter = "dc-1", .rack = "rack", .is_voter = true, .is_alive = true, .is_leader = leader_node_idx == 0}},
            {ids[1], {.datacenter = "dc-2", .rack = "rack", .is_voter = true, .is_alive = true, .is_leader = leader_node_idx == 1}},
            // no other nodes - all removed
    };

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: The current leader is retained as a voter.
    // Note: With 2 nodes, odd number enforcement reduces to 1 voter.

    BOOST_CHECK_EQUAL(voters.size(), 1);
    BOOST_CHECK(voters.contains(ids[leader_node_idx]));
}


BOOST_AUTO_TEST_CASE(enforces_odd_number_of_voters_for_single_dc) {

    // Arrange: Set an even voters limit and create the voter calculator.

    constexpr size_t max_voters = 5;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Add nodes to a single datacenter.

    const std::array ids = {
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[1], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[2], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[3], {.datacenter = "dc", .rack = "rack", .is_voter = false, .is_alive = true}},
    };

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: Enforces odd number of voters by reducing from 4 to 3.

    BOOST_CHECK_EQUAL(voters.size(), 3);
}


BOOST_AUTO_TEST_CASE(enforces_odd_number_of_voters_for_multiple_dc) {

    // Arrange: Set an even voters limit and create the voter calculator.

    constexpr size_t max_voters = 7;

    const service::group0_voter_calculator voter_calc{max_voters};

    // Act: Add nodes to multiple datacenters.

    const std::array ids = {raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(),
            raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id(), raft::server_id::create_random_id()};

    const service::group0_voter_calculator::nodes_list_t nodes = {
            {ids[0], {.datacenter = "dc-1", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[1], {.datacenter = "dc-1", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[2], {.datacenter = "dc-2", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[3], {.datacenter = "dc-2", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[4], {.datacenter = "dc-3", .rack = "rack", .is_voter = false, .is_alive = true}},
            {ids[5], {.datacenter = "dc-3", .rack = "rack", .is_voter = false, .is_alive = true}},
    };

    const auto& voters = voter_calc.distribute_voters(nodes);

    // Assert: Enforces odd number of voters by reducing from 6 to 5.

    BOOST_CHECK_EQUAL(voters.size(), 5);

    // Additional checks: each DC should have at least one voter.
    std::unordered_set<std::string> seen_dcs;
    for (const auto id : voters) {
        const auto& node = nodes.at(id);
        seen_dcs.insert(node.datacenter);
    }

    BOOST_CHECK_EQUAL(seen_dcs.size(), 3);
}

BOOST_AUTO_TEST_SUITE_END()
