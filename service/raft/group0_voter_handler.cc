/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "group0_voter_handler.hh"

#include <queue>

#include <seastar/util/log.hh>

#include "gms/feature_service.hh"
#include "gms/gossiper.hh"
#include "raft_group0.hh"
#include "tools/build_info.hh"

namespace service {

namespace {

seastar::logger rvlogger("group0_voter_handler");


using is_alive_t = bool_class<struct is_alive_tag>;
using is_voter_t = bool_class<struct is_voter_tag>;

bool operator<(const is_alive_t& lhs, const is_alive_t& rhs) {
    return static_cast<bool>(lhs) < static_cast<bool>(rhs);
}

bool operator<(const is_voter_t& lhs, const is_voter_t& rhs) {
    return static_cast<bool>(lhs) < static_cast<bool>(rhs);
}


// Represents information about a rack and provides functionality to manage voter selection.
//
// This class is responsible for storing information about nodes within a rack and selecting voters based
// on specific criteria.
// It maintains a map of nodes categorized by their status (alive/dead) and role (voter/non-voter) and provides
// methods to select the next voter and check if more candidates are available.
//
// The class also defines a priority comparator to determine the priority of racks based on the number of
// assigned voters, alive nodes, and dead voters.
class rack_info {

    using nodes_map_key_t = std::tuple<is_alive_t, is_voter_t>;
    using nodes_map_t = std::multimap<nodes_map_key_t, raft::server_id>;
    nodes_map_t _nodes;

    size_t _voters_count = 0;

    static nodes_map_t create_nodes_map(std::ranges::input_range auto&& nodes) {
        return nodes | std::views::transform([](const auto& node_entry) {
            const auto& [id, node] = node_entry;
            return std::make_pair(std::make_tuple(is_alive_t{node.is_alive}, is_voter_t{node.is_voter}), id);
        }) | std::ranges::to<nodes_map_t>();
    }

public:
    explicit rack_info(std::ranges::input_range auto&& nodes)
        : _nodes(create_nodes_map(nodes)) {
    }

    // Select the "best" next voter from the rack
    //
    // Returns the ID of the selected voter or `std::nullopt` if no more candidates are available.
    //
    // If a node is selected, it is removed from the list of candidates, and the number of voters assigned
    // to the rack is incremented.
    [[nodiscard]] std::optional<raft::server_id> select_next_voter() {
        const auto node = std::invoke([this]() {
            // Process alive nodes first, then dead nodes
            for (const auto is_alive : {is_alive_t::yes, is_alive_t::no}) {
                // Process voter nodes first, then non-voter nodes
                for (const auto is_voter : {is_voter_t::yes, is_voter_t::no}) {
                    const auto& [first, end] = _nodes.equal_range(std::make_tuple(is_alive, is_voter));
                    if (first != end) {
                        return first;
                    }
                }
            }

            return _nodes.end();
        });

        if (node == _nodes.end()) {
            return std::nullopt;
        }

        const auto voter_id = node->second;

        _nodes.erase(node);
        ++_voters_count;

        return voter_id;
    }

    // Check if there are more candidates available for voter selection
    //
    // Returns `true` if there are more candidates available, `false` otherwise.
    [[nodiscard]] bool has_more_candidates() const {
        return !_nodes.empty();
    }

    // The priority comparator for the rack_info
    friend bool operator<(const rack_info& rack1, const rack_info& rack2) {
        // First criteria: The number of already assigned voters (lower has more priority)
        if (rack1._voters_count != rack2._voters_count) {
            return rack1._voters_count > rack2._voters_count;
        }

        const auto& rack1_alive_nodes_boundary = rack1._nodes.lower_bound(std::make_tuple(is_alive_t::yes, is_voter_t::no));
        const auto& rack2_alive_nodes_boundary = rack2._nodes.lower_bound(std::make_tuple(is_alive_t::yes, is_voter_t::no));

        // Second criteria: The number of alive nodes (voters and non-voters) remaining (higher has more priority)

        const auto rack1_alive_nodes_remaining = std::distance(rack1_alive_nodes_boundary, rack1._nodes.end());
        const auto rack2_alive_nodes_remaining = std::distance(rack2_alive_nodes_boundary, rack2._nodes.end());
        if (rack1_alive_nodes_remaining != rack2_alive_nodes_remaining) {
            return rack1_alive_nodes_remaining < rack2_alive_nodes_remaining;
        }

        // Third criteria: The number of dead voters remaining (higher has more priority)
        //      We want to keep the dead voters in case we can't find enough alive voters.

        // Note that the nodes don't contain dead non-voters (we filter them out in `group0_voter_calculator::distribute_voters`),
        // so we can use the whole subrange from the beginning (and can avoid another boundary search).
        // We check for this condition in the non-release builds.
        if constexpr (!tools::build_info::is_release_build()) {
            SEASTAR_ASSERT(!rack1._nodes.contains(std::make_tuple(is_alive_t::no, is_voter_t::no)));
            SEASTAR_ASSERT(!rack2._nodes.contains(std::make_tuple(is_alive_t::no, is_voter_t::no)));
        }

        const auto rack1_dead_nodes_remaining = std::distance(rack1._nodes.begin(), rack1_alive_nodes_boundary);
        const auto rack2_dead_nodes_remaining = std::distance(rack2._nodes.begin(), rack2_alive_nodes_boundary);
        return rack1_dead_nodes_remaining < rack2_dead_nodes_remaining;
    }

};


// Represents information about a datacenter and provides functionality to manage voter selection.
//
// This class is responsible for storing information about nodes within a datacenter and selecting voters based
// on specific criteria.
// It maintains a map of nodes categorized by their status (alive/dead) and role (voter/non-voter) and provides
// methods to select the next voter and check if more candidates are available.
//
// The class also defines a priority comparator to determine the priority of datacenters based on the number of
// assigned voters, alive nodes, and dead voters.
class datacenter_info {

    size_t _nodes_remaining = 0;
    size_t _voters_count = 0;

    using racks_store_t = std::priority_queue<rack_info>;
    racks_store_t _racks;

    static racks_store_t create_racks_list(std::ranges::input_range auto&& nodes) {
        const auto nodes_by_rack = nodes | std::views::transform([](const auto& node_entry) {
            const auto& [id, node] = node_entry;
            return std::make_pair(std::string_view{node.rack}, std::make_pair(id, std::cref(node)));
        }) | std::ranges::to<std::unordered_multimap>();

        racks_store_t::container_type racks;

        for (const auto rack : nodes_by_rack | std::views::keys | std::ranges::to<std::set>()) {
            const auto [first, last] = nodes_by_rack.equal_range(rack);

            racks.emplace_back(std::ranges::subrange(first, last) | std::views::transform([](const auto& node_entry) {
                return node_entry.second;
            }));
        }

        return racks | std::ranges::to<racks_store_t>();
    }

public:
    explicit datacenter_info(std::ranges::sized_range auto&& nodes)
        : _nodes_remaining(nodes.size())
        , _racks(create_racks_list(nodes)) {
    }

    // Select the "best" next voter from the datacenter
    //
    // Returns the ID of the selected voter or `std::nullopt` if no more candidates are available.
    //
    // If a node is selected, it is removed from the list of candidates, and the number of voters assigned
    // to the datacenter is incremented.
    [[nodiscard]] std::optional<raft::server_id> select_next_voter() {
        while (!_racks.empty()) {

            // Select the datacenter with the highest priority (according to the comparator)
            auto rack = _racks.top();
            _racks.pop();

            const auto voter_id = rack.select_next_voter();

            if (!voter_id) {
                continue;
            }

            if (rack.has_more_candidates()) {
                _racks.push(rack);
            }

            SCYLLA_ASSERT(_nodes_remaining > 0);

            --_nodes_remaining;
            ++_voters_count;

            return voter_id;
        }

        // No more nodes to select
        return std::nullopt;
    }

    // Check if there are more candidates available for voter selection
    //
    // Returns `true` if there are more candidates available, `false` otherwise.
    //
    // The selection is limited by the maximum number of voters per datacenter.
    [[nodiscard]] bool has_more_candidates(size_t voters_max_per_dc) const {
        return _nodes_remaining > 0 && _voters_count < voters_max_per_dc;
    }

    // The priority comparator for the datacenter_info
    friend bool operator<(const datacenter_info& dc1, const datacenter_info& dc2) {
        // First criteria: The number of already assigned voters (lower has more priority)

        if (dc1._voters_count != dc2._voters_count) {
            return dc1._voters_count > dc2._voters_count;
        }

        // Second criteria: The number of racks (higher has more priority)
        return dc1._racks.size() < dc2._racks.size();
    }
};


class calculator_impl {

    using datacenters_store_t = std::priority_queue<datacenter_info>;

    size_t _largest_dc_size = 0;
    datacenters_store_t _datacenters;

    uint64_t _voters_max;
    uint64_t _voters_max_per_dc;

    static uint64_t calc_voters_max(
            uint64_t voters_max, const group0_voter_calculator::nodes_list_t& nodes, const datacenters_store_t& datacenters, size_t dc_largest_size) {
        auto num_voters = std::min(voters_max, nodes.size());
        // Any number of voters under 3 is allowed
        if (num_voters <= 3) {
            return num_voters;
        }

        // Odd number of voters is always allowed
        if (num_voters % 2 != 0) {
            return num_voters;
        }

        // With 2 DCs having an equal number of nodes, we want an asymmetric distribution
        // to survive the loss of one DC. Enforce an odd number of voters.
        if (datacenters.size() == 2 && dc_largest_size * 2 == nodes.size()) {
            return num_voters - 1;
        }

        // TODO(issue-23266): Enforce an odd number of voters in other cases as well
        //
        // Forcing an odd number of voters causes further tests to fail, so there will
        // be a separate follow-up. Note that the previous code allowed any number of voters,
        // so allowing even number of voters is not a regression.
        return num_voters;
    }

    static uint64_t calc_voters_max_per_dc(
            uint64_t voters_max, const group0_voter_calculator::nodes_list_t& nodes, const datacenters_store_t& datacenters, size_t dc_largest_size) {
        const auto datacenters_count = datacenters.size();

        // With 2 DCs (or fewer), we can't prevent one DC from taking half or more voters, so we allow more voters per DC.
        if (datacenters_count <= 2) {
            return voters_max;
        }

        // If the number of DCs is greater than 2, prevent any DC taking half or more of the voters.
        // The calculation is not trivial. For example, with 1 DC having 3 nodes and 2 DCs having 1 node each,
        // we can't simply take half of voters_max - 1, as that would still allow the first DC to
        // take 2 voters, thus having a majority.
        //
        // Therefore, we determine the DC with the maximal number of nodes and calculate the sum of the remaining
        // nodes across all the other DCs - with the largest DC being forced to have less voters than the sum of
        // all the other DC nodes.
        const auto nodes_count_exclude_largest = nodes.size() - dc_largest_size;

        return std::min(nodes_count_exclude_largest - 1, (voters_max - 1) / 2);
    }

    static datacenters_store_t create_datacenters_list(const group0_voter_calculator::nodes_list_t& nodes, size_t* largest_dc_size) {
        SCYLLA_ASSERT(largest_dc_size != nullptr);
        *largest_dc_size = 0;

        const auto& nodes_by_dc = std::invoke([&nodes]() {
            std::unordered_map<std::string_view, std::vector<std::pair<raft::server_id, const group0_voter_calculator::node_descriptor>>> nodes_by_dc;
            for (const auto& [id, node] : nodes) {
                nodes_by_dc[node.datacenter].emplace_back(id, node);
            }
            return nodes_by_dc;
        });

        const auto datacenters = nodes_by_dc | std::views::keys | std::ranges::views::transform([&nodes_by_dc, largest_dc_size](const auto& dc) {
            const auto& dc_nodes = nodes_by_dc.at(dc);
            *largest_dc_size = std::max<size_t>(*largest_dc_size, dc_nodes.size());
            return dc_nodes;
        });

        return datacenters | std::ranges::to<datacenters_store_t>();
    }

    // Select the next voter from the datacenters
    //
    // Returns the ID of the selected voter or `std::nullopt` if no more candidates are available.
    //
    // The method selects the datacenter with the highest priority (based on the comparator) and selects the next voter
    // from that datacenter.
    [[nodiscard]] std::optional<raft::server_id> select_next_voter() {
        while (!_datacenters.empty()) {

            // Select the datacenter with the highest priority (based on the comparator)
            auto dc = _datacenters.top();
            _datacenters.pop();

            const auto voter_id = dc.select_next_voter();

            if (!voter_id) {
                // no more available voter candidates in this DC
                continue;
            }

            // Put the DC back into the queue if it still has more voters to select
            if (dc.has_more_candidates(_voters_max_per_dc)) {
                _datacenters.push(dc);
            }

            return voter_id;
        }

        // No more nodes available for selection
        return std::nullopt;
    }

public:
    constexpr static size_t VOTERS_MAX_DEFAULT = 5;

    calculator_impl(uint64_t voters_max, const group0_voter_calculator::nodes_list_t& nodes)
        : _datacenters(create_datacenters_list(nodes, &_largest_dc_size))
        , _voters_max(calc_voters_max(voters_max, nodes, _datacenters, _largest_dc_size))
        , _voters_max_per_dc(calc_voters_max_per_dc(_voters_max, nodes, _datacenters, _largest_dc_size)) {

        rvlogger.debug("Max voters/per DC: {}/{}", _voters_max, _voters_max_per_dc);
    }

    [[nodiscard]] group0_voter_calculator::voters_set_t distribute_voters() {
        group0_voter_calculator::voters_set_t voters;
        voters.reserve(_voters_max);

        while (voters.size() < _voters_max) {
            const auto& voter = select_next_voter();
            if (!voter) {
                break;
            }
            voters.insert(*voter);
        }

        return voters;
    }
};


} // namespace


future<> group0_voter_handler::update_nodes(
        const std::unordered_set<raft::server_id>& nodes_added, const std::unordered_set<raft::server_id>& nodes_removed, abort_source& as) {

    // Make sure we are not interrupted while we are calculating and modifying the voters
    const auto lock = co_await get_units(_voter_lock, 1, as);

    if (!_feature_service.group0_limited_voters) {
        // The previous implementation only handled voter removals.
        // Thus, we only handle the removed nodes here if the feature is not active.
        co_return co_await _group0.modify_voters({}, nodes_removed, as);
    }

    // Load the current cluster members
    const auto& group0_config = _group0.group0_server().get_configuration();

    const auto& nodes_alive = _gossiper.get_live_members();
    const auto& nodes_dead = _gossiper.get_unreachable_members();

    group0_voter_calculator::nodes_list_t nodes;

    auto add_nodes = [this, &nodes, &group0_config](const std::set<locator::host_id>& nodes_set, bool is_alive) {
        for (const auto& host_id : nodes_set) {
            const raft::server_id id{host_id.uuid()};
            const auto& it = _topology.normal_nodes.find(id);
            if (it == _topology.normal_nodes.end()) {
                // This is expected, as the nodes present in the gossiper may not be present
                // in the topology yet or any more.
                rvlogger.debug("Node {} not found in the topology", id);
                continue;
            }
            const auto& rs = it->second;
            nodes.emplace(id, group0_voter_calculator::node_descriptor{
                                      .datacenter = rs.datacenter,
                                      .rack = rs.rack,
                                      .is_voter = group0_config.can_vote(id),
                                      .is_alive = is_alive,
                              });
        }
    };

    add_nodes(nodes_alive, true);
    add_nodes(nodes_dead, false);

    // Handle the added nodes
    for (const auto& id : nodes_added) {
        const auto itr = nodes.find(id);
        if (itr != nodes.end()) {
            // This is expected
            rvlogger.debug("Node {} to be added is already a member", id);
            continue;
        }
        const auto& it = _topology.normal_nodes.find(id);
        if (it == _topology.normal_nodes.end()) {
            rvlogger.warn("Node {} to be added not found in the topology - can't add as a voter candidate", id);
            continue;
        }
        const auto is_alive = _gossiper.is_alive(locator::host_id{id.uuid()});
        const auto& rs = it->second;
        const auto is_voter = group0_config.can_vote(id);
        nodes[id] = {
                .datacenter = rs.datacenter,
                .rack = rs.rack,
                .is_voter = is_voter,
                .is_alive = is_alive,
        };
    }

    std::unordered_set<raft::server_id> voters_add;
    std::unordered_set<raft::server_id> voters_del;

    // Force the removal of voter status in any case
    for (const auto& id : nodes_removed) {
        // force the removal of votership in any case
        voters_del.emplace(id);
        const auto itr = nodes.find(id);
        if (itr == nodes.end()) {
            rvlogger.warn("Node {} to be removed is not a member", id);
            continue;
        }
        // Remove the node from the list so it is not considered in further calculations
        nodes.erase(itr);
    }

    const auto& voters = _calculator.distribute_voters(nodes);

    // Calculate the voters diff against the current state
    for (const auto& [id, node] : nodes) {
        if (node.is_voter) {
            if (!voters.contains(id)) {
                voters_del.emplace(id);
            }
        } else {
            if (voters.contains(id)) {
                voters_add.emplace(id);
            }
        }
    }

    co_await _group0.modify_voters(voters_add, voters_del, as);
}

group0_voter_calculator::group0_voter_calculator(std::optional<size_t> voters_max)
    : _voters_max(voters_max.value_or(calculator_impl::VOTERS_MAX_DEFAULT)) {
    if (voters_max && *voters_max == 0) {
        throw std::invalid_argument("voters_max must be greater than zero");
    }
}

group0_voter_calculator::voters_set_t group0_voter_calculator::distribute_voters(const nodes_list_t& nodes) const {
    const auto& nodes_filtered = nodes | std::views::filter([](const auto& node_entry) {
        const auto& [id, node] = node_entry;
        rvlogger.debug("Node: {}, DC: {}, is_voter: {}, is_alive: {}", id, node.datacenter, node.is_voter, node.is_alive);
        if (node.is_alive || node.is_voter) {
            return true;
        }

        rvlogger.info("Node {} is not alive and non-voter, skipping", id);
        return false;
    }) | std::ranges::to<std::unordered_map>();

    calculator_impl impl(_voters_max, nodes_filtered);
    return impl.distribute_voters();
}

} // namespace service
