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

// The voter handler manages voters in group 0.
//
// It is responsible for tracking voters in group 0 and ensuring that voters are evenly distributed
// across datacenters. The handler wraps the `group0_voter_calculator` and handles the actual
// modification of voters, as well as service-specific logic.
class group0_voter_handler {

    raft_group0& _group0;

    using topology_type = topology;

    const topology_type& _topology;
    const gms::gossiper& _gossiper;
    const gms::feature_service& _feature_service;

    semaphore _voter_lock = semaphore(1);

    group0_voter_calculator _calculator;

    // Updates the voter handler based on topology changes (node additions or removals).
    //
    // Parameters:
    // - `nodes_added`: A set of nodes that have been added and are eligible to become voters.
    // - `nodes_removed`: A set of nodes that have been removed or are about to be removed and their votership status
    //                    should be revoked (in case they are currently voters).
    //
    // This method updates the voter distribution to maintain balance in group 0. It ensures that the
    // voter set reflects the current topology while adhering to the balancing rules.
    future<> update_nodes(const std::unordered_set<raft::server_id>& nodes_added, const std::unordered_set<raft::server_id>& nodes_removed, abort_source& as);

public:
    // Constructs a new voter handler instance.
    //
    // The `voters_max` optional parameter specifies the maximum number of voters in the group 0.
    // If not provided, the default value is determined by the voter calculator.
    group0_voter_handler(raft_group0& group0, const topology_type& topology, const gms::gossiper& gossiper, const gms::feature_service& feature_service,
            std::optional<size_t> voters_max = std::nullopt)
        : _group0(group0)
        , _topology(topology)
        , _gossiper(gossiper)
        , _feature_service(feature_service)
        , _calculator(voters_max) {
    }

    // Refreshes the current voter state.
    //
    // This method reloads the current voter state from the configuration members and updates the voters
    // if necessary to maintain balance in group 0. It may result in adding or removing voters.
    future<> refresh(abort_source& as);

    // Notify the voter handler about a new node addition.
    //
    // This method updates the voter distribution to maintain balance in group 0. The new node may be
    // designated as a voter or non-voter based on the current voter balance. If the new node is a better
    // candidate (e.g., has more distance from other voters), it may replace an existing voter.
    future<> on_node_added(raft::server_id node, abort_source& as);

    // Notify the voter handler about a node removal.
    //
    // This method updates the voter distribution to maintain balance in group 0. If the removed node was a voter,
    // it will be switched to a non-voter. This may result in promoting other non-voters to voters to maintain balance.
    future<> on_node_removed(raft::server_id node, abort_source& as);
};

inline future<> group0_voter_handler::refresh(abort_source& as) {
    co_await update_nodes({}, {}, as);
}

inline future<> group0_voter_handler::on_node_added(raft::server_id node, abort_source& as) {
    co_await update_nodes({node}, {}, as);
}

inline future<> group0_voter_handler::on_node_removed(raft::server_id node, abort_source& as) {
    co_await update_nodes({}, {node}, as);
}

} // namespace service
