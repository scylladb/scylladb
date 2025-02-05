/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "raft/raft.hh"
#include "service/raft/raft_group0.hh"

#include <seastar/core/coroutine.hh>

namespace service {

struct replica_state;

// The interface to access the raft server information.
//
// The raft server information is used to retrieve the node information (DC, rack etc.) and the
// current state of the node (voter/non-voter).
class raft_server_info_accessor {

public:
    virtual ~raft_server_info_accessor() = default;

    // Get the current topology members.
    //
    // Returns a map of the raft server id to the replica state of the currently active ("normal") nodes.
    [[nodiscard]] virtual const std::unordered_map<raft::server_id, replica_state>& get_members() const = 0;

    // Get the replica state of the given raft server id.
    //
    // Returns the replica state of the given raft server id.
    //
    // Can throw an exception if the given raft server id is not found.
    [[nodiscard]] virtual const replica_state& find(raft::server_id id) const = 0;
};

// The interface to access the voter status of the raft server.
//
// The voter client is used to retrieve the current status of the node (voter/non-voter) and set the voter status
// of the raft server.
class raft_voter_client {

public:
    virtual ~raft_voter_client() = default;

    // Check if the given node is a voter.
    //
    // Returns true if the given node is a voter, false otherwise.
    //
    // Also expected to return false if the given raft server id is not found.
    [[nodiscard]] virtual bool is_voter(raft::server_id id) const = 0;

    // Set the voter status of the given nodes in the group 0.
    //
    // Expected to set the voter status of the given nodes.
    //
    // Can throw an exception if the operation fails (nodes not known, voter status cannot be set etc.).
    [[nodiscard]] virtual future<> set_voters_status(const std::unordered_set<raft::server_id>& nodes, can_vote can_vote, abort_source& as) = 0;
};

// The voter registry is used to manage the voters in the group 0.
//
// The voter registry is responsible for keeping track of the voters in the group 0 and
// ensuring that the number of voters in each datacenter is balanced. In particular, the voter
// registry ensures that no datacenter has a majority of voters (for the cases where this is
// possible, i.e. when the number of DC >= 3).
//
// For the case of 2 DCs, the voter registry ensures asymmetric distribution of voters. This means
// that we can survive the loss of the DC with the lower number of voters.
//
// The voter registry also maintains the most "distance" between voters in the group 0. The "distance"
// is determined by the node properties, like for example the datacenter or the rack. For example, if
// two nodes are in the same datacenter, they have less distance than two nodes in different datacenters.
// The purpose of managing the distance is to ensure the most even distribution of voters across the
// datacenters and racks.
class group0_voter_registry {

public:
    // The maximum number of voters is not limited (= all nodes are voters).
    constexpr static size_t MAX_VOTERS_UNLIMITED = std::numeric_limits<size_t>::max();

    // The maximum number of voters is adaptive, determined by the number of DCs in the cluster:
    // - if there are less than 5 DCs, there are max 5 voters;
    // - if there are more than 5 and less than 9 DCs, there number of voters is equal to the number of DCs;
    // - if there are more than 9 DCs, there are max 9 voters.
    constexpr static size_t MAX_VOTERS_ADAPTIVE = 0;

    using instance_ptr = std::unique_ptr<group0_voter_registry>;

    // Create a new voter registry.
    //
    // The voter registry is created with the given server info accessor and voter client and becomes the owner of them.
    //
    // The max_voters parameter is used to set the maximum number of voters in the group 0. The default value is to
    // allow an unlimited number of voters.
    static instance_ptr create(std::unique_ptr<const raft_server_info_accessor> server_info_accessor, std::unique_ptr<raft_voter_client> voter_client,
            std::optional<size_t> max_voters = std::nullopt);

    virtual ~group0_voter_registry() = default;

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

private:
    virtual future<> update_voters(
            const std::unordered_set<raft::server_id>& nodes_added, const std::unordered_set<raft::server_id>& nodes_removed, abort_source& as) = 0;
};

inline future<> group0_voter_registry::refresh(abort_source& as) {
    return update_voters({}, {}, as);
}

inline future<> group0_voter_registry::insert_node(raft::server_id node, abort_source& as) {
    return insert_nodes({node}, as);
}

inline future<> group0_voter_registry::insert_nodes(const std::unordered_set<raft::server_id>& nodes, abort_source& as) {
    return update_voters(nodes, {}, as);
}

inline future<> group0_voter_registry::remove_node(raft::server_id node, abort_source& as) {
    return remove_nodes({node}, as);
}

inline future<> group0_voter_registry::remove_nodes(const std::unordered_set<raft::server_id>& nodes, abort_source& as) {
    return update_voters({}, nodes, as);
}

} // namespace service
