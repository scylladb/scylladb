/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "raft/raft.hh"

namespace service {

// The voter calculator is responsible for distributing voters in group 0.
//
// It calculates the optimal distribution of voters across datacenters and racks, ensuring:
// 1. The number of voters in each datacenter is balanced.
// 2. No single datacenter or rack holds at least half of the total voters (when the number of DCs/racks >= 3).
//
// For scenarios with 2 datacenters (or 2 racks in a single datacenter), the calculator ensures an asymmetric
// distribution of voters. This allows the system to tolerate the loss of the datacenter or rack with fewer voters.
//
// Additionally, the calculator maximizes the "distance" between voters in group 0. The "distance" is based on
// node properties, such as datacenter or rack location. For example, nodes in different datacenters are considered
// farther apart than nodes in the same datacenter. This approach ensures an even distribution of voters across
// datacenters and racks.
class group0_voter_calculator {

    size_t _voters_max;

public:
    // Constructs a new voter calculator instance.
    //
    // The voter registry is initialized with the `voters_max` parameter that specifies the maximum number of voters
    // allowed in group 0. If not provided, the default behavior allows the default maximum number of voters.
    explicit group0_voter_calculator(std::optional<size_t> voters_max = std::nullopt);

    struct node_descriptor {
        sstring datacenter;
        sstring rack;
        bool is_voter;
        bool is_alive;
        bool is_leader = false;
    };

    using nodes_list_t = std::unordered_map<raft::server_id, node_descriptor>;

    using voters_set_t = std::unordered_set<raft::server_id>;

    // Calculates the optimal distribution of voters across datacenters and racks.
    //
    // Parameters:
    // - `nodes`: A map where each entry represents a node in group 0. The key is the node's
    //            unique server ID, and the value is a descriptor containing the node's
    //            datacenter, rack, voter status, and liveness information.
    //
    // Returns:
    // A set of server IDs representing the nodes selected as voters. The returned set ensures compliance
    // with the balancing and fault-tolerance rules for group 0.
    [[nodiscard]] voters_set_t distribute_voters(const nodes_list_t& nodes) const;
};

} // namespace service
