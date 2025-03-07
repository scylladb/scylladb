/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "group0_voter_calculator.hh"

#include "raft/raft.hh"
#include "service/topology_state_machine.hh"

#include <seastar/core/coroutine.hh>


namespace gms {
class gossiper;
class feature_service;
} // namespace gms

namespace service {

class raft_group0;
struct replica_state;

// The voter handler is used to manage the voters in the group 0.
//
// The voter handler is responsible for keeping track of the voters in the group 0 and
// ensuring that the number of voters in each datacenter is balanced. It is a wrapper around the
// `group0_voter_calculator`, handling the actual modification of the voters and service-specicic code.
class group0_voter_handler {

    raft_group0& _group0;

    using topology_type = topology;

    const topology_type& _topology;
    const gms::gossiper& _gossiper;
    const gms::feature_service& _feature_service;

    group0_voter_calculator _calculator;

public:
    // The maximum number of voters is not limited (= all nodes are voters).
    constexpr static size_t MAX_VOTERS_UNLIMITED = std::numeric_limits<size_t>::max();

    // Create a new voter handler instance.
    //
    // The max_voters parameter is used to set the maximum number of voters in the group 0. The default value
    // is determined by the voter calculator implementation.
    group0_voter_handler(raft_group0& group0, const topology_type& topology, const gms::gossiper& gossiper, const gms::feature_service& feature_service,
            std::optional<size_t> max_voters = std::nullopt)
        : _group0(group0)
        , _topology(topology)
        , _gossiper(gossiper)
        , _feature_service(feature_service)
        , _calculator(max_voters) {
    }

    // Refresh the current voters state.
    //
    // Loads the current voters state of the configuration members and updates the voters if necessary,
    // maintaining the balance of voters in the group 0.
    //
    // This can lead to adding or removing voters from the group 0.
    future<> refresh(abort_source& as);

    // Inform the voter registry about a new node addition.
    //
    // Updates the voters if necessary, maintaining the balance of voters in the group 0.
    //
    // The new node might be switched to a voter or a non-voter depending on the current voter balance of the group 0.
    // It can also lead to switching some other voter(s) to non-voter(s) if the new node is a better candidate
    // for a voter (has more distance from the other voters than some existing voter).
    future<> insert_node(raft::server_id node, abort_source& as);

    // Informs the voter registry about new nodes addition.
    //
    // Updates the voters if necessary, maintaining the balance of voters in the group 0.
    //
    // The new nodes might be switched to voters or non-voters depending on the current voter balance of the group 0.
    // It can also lead to switching some other voter(s) to non-voter(s) if the new nodes are better candidates
    // for voters (have more distance from the other voters than some existing voters).
    future<> insert_nodes(const std::unordered_set<raft::server_id>& nodes, abort_source& as);

    // Inform the voter registry about a node removal.
    //
    // Updates the voters if necessary, maintaining the balance of voters in the group 0.
    //
    // The removed node will be switched to a non-voter if it was a voter. This can lead to switching some other
    // non-voter(s) to voter(s) if necessary to maintain the voters balance.
    future<> remove_node(raft::server_id node, abort_source& as);

    // Inform the voter registry about nodes removal.
    //
    // Updates the voters if necessary, maintaining the balance of voters in the group 0.
    //
    // The removed nodes will be switched to non-voters if they were voters. This can lead to switching some other
    // non-voter(s) to voter(s) if necessary to maintain the voters balance.
    future<> remove_nodes(const std::unordered_set<raft::server_id>& nodes, abort_source& as);

    // Inform the voter registry about a topology change (nodes addition or removal).
    //
    // Updates the voters if necessary, maintaining the balance of voters in the group 0.
    //
    // The nodes_added parameter contains the nodes that have been added and are eligible voters.
    // The nodes_removed parameter contains the nodes that were or are about to be removed, thus should be removed
    // from voters.
    future<> update_nodes(const std::unordered_set<raft::server_id>& nodes_added, const std::unordered_set<raft::server_id>& nodes_removed, abort_source& as);
};

inline future<> group0_voter_handler::refresh(abort_source& as) {
    co_await update_nodes({}, {}, as);
}

inline future<> group0_voter_handler::insert_node(raft::server_id node, abort_source& as) {
    co_await insert_nodes({node}, as);
}

inline future<> group0_voter_handler::insert_nodes(const std::unordered_set<raft::server_id>& nodes, abort_source& as) {
    co_await update_nodes(nodes, {}, as);
}

inline future<> group0_voter_handler::remove_node(raft::server_id node, abort_source& as) {
    co_await remove_nodes({node}, as);
}

inline future<> group0_voter_handler::remove_nodes(const std::unordered_set<raft::server_id>& nodes, abort_source& as) {
    co_await update_nodes({}, nodes, as);
}

} // namespace service
