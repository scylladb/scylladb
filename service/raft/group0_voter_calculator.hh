/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "raft/raft.hh"

namespace service {

// The voter calculator is used to distribute the voters in the group 0.
//
// It computes the optimal distribution of voters across the datacenters and racks, ensuring that the
// number of voters in each datacenter is balanced and that no datacenter/rack has a majority of voters
// (for the cases where this is possible, i.e. when the number of DCs / racks >= 3).
//
// For the case of 2 DCs (or 32 racks in a single DC), the voter vcalculator ensures asymmetric distribution
// of voters. This means that we can survive the loss of the DC or rack with the lower number of voters.
//
// The computed distribution also maintains the most "distance" between voters in the group 0. The "distance"
// is determined by the node properties, like for example the datacenter or the rack. For example, if
// two nodes are in the same datacenter, they have less distance than two nodes in different datacenters.
// The purpose of managing the distance is to ensure the most even distribution of voters across the
// datacenters and racks.
class group0_voter_calculator {

    size_t _max_voters;

public:
    // Create a new voter calculator instance.
    //
    // The voter registry is created with the given server info accessor and voter client and becomes the owner of them.
    //
    // The max_voters parameter is used to set the maximum number of voters in the group 0. The default value is to
    // allow an unlimited number of voters.
    explicit group0_voter_calculator(std::optional<size_t> max_voters = std::nullopt);

    struct node_descriptor {
        sstring datacenter;
        sstring rack;
        bool is_voter;
        bool is_alive;
    };

    using nodes_list_t = std::unordered_map<raft::server_id, node_descriptor>;

    using voters_set_t = std::unordered_set<raft::server_id>;
    using voters_result_t = std::tuple<voters_set_t, voters_set_t>;

    // Calculate the optimal distribution of voters across the datacenters and racks.
    //
    // The nodes parameter contains the description of all nodes in the group 0.
    //
    // Returns a tuple of two sets of voters: the first set contains the voters to be added, the second set
    // contains the voters to be removed.
    [[nodiscard]] voters_result_t calculate_voters(const nodes_list_t& nodes) const;
};

} // namespace service
