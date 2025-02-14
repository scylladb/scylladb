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

namespace service {

namespace {

seastar::logger rvlogger("group0_voter_handler");


struct flags {
    struct is_alive_tag {};
    struct is_voter_tag {};
};

using is_alive_t = bool_class<flags::is_alive_tag>;
using is_voter_t = bool_class<flags::is_voter_tag>;

bool operator<(const is_alive_t& lhs, const is_alive_t& rhs) {
    return static_cast<bool>(lhs) < static_cast<bool>(rhs);
}

bool operator<(const is_voter_t& lhs, const is_voter_t& rhs) {
    return static_cast<bool>(lhs) < static_cast<bool>(rhs);
}


class datacenter_info {

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
    explicit datacenter_info(std::ranges::input_range auto&& nodes)
        : _nodes(create_nodes_map(nodes)) {
    }

    [[nodiscard]] std::optional<raft::server_id> select_next_voter() {
        const auto node = std::invoke([this]() {
            // take the alive nodes first, then dead nodes
            for (const auto is_alive : {is_alive_t::yes, is_alive_t::no}) {
                // take the voter nodes first, then non-voter nodes
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

    [[nodiscard]] bool has_more_candidates(size_t voters_max_per_dc) const {
        return !_nodes.empty() && _voters_count < voters_max_per_dc;
    }

    // The priority comparator for the datacenter_info
    friend bool operator<(const datacenter_info& dc1, const datacenter_info& dc2) {
        // First criteria: The number of already assigned voters (lower has more priority)

        if (dc1._voters_count != dc2._voters_count) {
            return dc1._voters_count > dc2._voters_count;
        }

        const auto& dc1_alive_nodes_boundary = dc1._nodes.lower_bound(std::make_tuple(is_alive_t::yes, is_voter_t::no));
        const auto& dc2_alive_nodes_boundary = dc2._nodes.lower_bound(std::make_tuple(is_alive_t::yes, is_voter_t::no));

        // Second criteria: The number of alive nodes (voters and non-voters) remaining (higher has more priority)

        const auto dc1_alive_nodes_remaining = std::distance(dc1_alive_nodes_boundary, dc1._nodes.end());
        const auto dc2_alive_nodes_remaining = std::distance(dc2_alive_nodes_boundary, dc2._nodes.end());
        if (dc1_alive_nodes_remaining != dc2_alive_nodes_remaining) {
            return dc1_alive_nodes_remaining < dc2_alive_nodes_remaining;
        }

        // Third criteria: The number of dead voters remaining (higher has more priority)
        //      We want to keep the dead voters in case we can't find enough alive voters.

        // Note that the nodes don't contain dead non-voters (we filter them out in `group0_voter_calculator::distribute_voters`),
        // so we can use the whole subrange from the beginning (and can avoid another boundary search).
        // We check for this condition in the debug builds.
        assert(dc1._nodes.upper_bound(std::make_tuple(is_alive_t::no, is_voter_t::no)) == dc1._nodes.begin());
        assert(dc2._nodes.upper_bound(std::make_tuple(is_alive_t::no, is_voter_t::no)) == dc2._nodes.begin());

        const auto dc1_dead_nodes = std::distance(dc1._nodes.begin(), dc1_alive_nodes_boundary);
        const auto dc2_dead_nodes = std::distance(dc2._nodes.begin(), dc2_alive_nodes_boundary);
        return dc1_dead_nodes < dc2_dead_nodes;
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

        // With 2 DCs with equal number of nodes, we want an asymmetric distribution
        // to survive the loss of one DC - enforce odd number of voters
        if (datacenters.size() == 2 && dc_largest_size * 2 == nodes.size()) {
            return num_voters - 1;
        }

        // TODO(issue-23266): Enforce odd number of voters in other cases as well
        //
        // Forcing an odd number of voters causes further tests to fail, so there will
        // be a separate follow-up. Note that the previous code allowed any number of voters,
        // so allowing even number of voters is not a regression.
        return num_voters;
    }

    static uint64_t calc_voters_max_per_dc(
            uint64_t voters_max, const group0_voter_calculator::nodes_list_t& nodes, const datacenters_store_t& datacenters, size_t dc_largest_size) {
        const auto datacenters_count = datacenters.size();

        // with 2 DCs (or less), we can't prevent one DC from taking majority, so we allow more voters per DC
        if (datacenters_count <= 2) {
            return voters_max;
        }

        // if the number of DCs is greater than 2, prevent any DC taking majority of voters
        // the calculation is not trivial, as for example with 1 DC of 3 nodes and 2 DCs of 1 nodes each,
        // we can't simply take the half of the voters_max - 1, as that would still allow the first DC to
        // take 2 voters thus having majority
        //
        // therefore we determine the DC with the maximal number of nodes and calculater the sum of the remaining
        // nodes across all the other DCs - with the largest DC being forced to have less voters than the sum of
        // all the other DC nodes
        const auto nodes_count_exclude_largest = nodes.size() - dc_largest_size;

        return std::min(nodes_count_exclude_largest - 1, (voters_max - 1) / 2);
    }

    static datacenters_store_t create_datacenters_list(const group0_voter_calculator::nodes_list_t& nodes, size_t* largest_dc_size) {
        const auto nodes_by_dc = nodes | std::views::transform([](const auto& node_entry) {
            const auto& [id, node] = node_entry;
            return std::make_pair(std::string_view{node.datacenter}, std::make_pair(id, std::cref(node)));
        }) | std::ranges::to<std::unordered_multimap>();

        datacenters_store_t::container_type datacenters;

        for (const auto dc : nodes_by_dc | std::views::keys | std::ranges::to<std::set>()) {
            const auto [first, last] = nodes_by_dc.equal_range(dc);

            datacenters.emplace_back(std::ranges::subrange(first, last) | std::views::transform([](const auto& node_entry) {
                return node_entry.second;
            }));

            *largest_dc_size = std::max<size_t>(*largest_dc_size, std::distance(first, last));
        }

        return datacenters | std::ranges::to<datacenters_store_t>();
    }

    [[nodiscard]] std::optional<raft::server_id> select_next_voter() {
        while (!_datacenters.empty()) {

            // Select the datacenter with the highest priority (according to the comparator)
            auto dc = _datacenters.top();
            _datacenters.pop();

            const auto voter_id = dc.select_next_voter();

            if (!voter_id) {
                // no more available voter candidates in this DC
                continue;
            }

            // put the DC back to the queue if it still has more voters to select
            if (dc.has_more_candidates(_voters_max_per_dc)) {
                _datacenters.push(dc);
            }

            return voter_id;
        }

        // No more nodes to select
        return std::nullopt;
    }

public:
    constexpr static size_t VOTERS_MAX_DEFAULT = 5;

    calculator_impl(uint64_t voters_max, const group0_voter_calculator::nodes_list_t& nodes)
        : _datacenters(create_datacenters_list(nodes, &_largest_dc_size))
        , _voters_max(calc_voters_max(voters_max, nodes, _datacenters, _largest_dc_size))
        , _voters_max_per_dc(calc_voters_max_per_dc(_voters_max, nodes, _datacenters, _largest_dc_size)) {

        rvlogger.info("Max voters/per DC: {}/{}", _voters_max, _voters_max_per_dc);
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
    if (!_feature_service.group0_limited_voters) {
        // the previous implementation had just the voter removals part
        // (thus we only handle the removed nodes here if the feature is not active)
        co_return co_await _group0.modify_voters({}, nodes_removed, as);
    }

    // Load the current members
    const auto& group0_config = _group0.group0_server().get_configuration();

    const auto& nodes_alive = _gossiper.get_live_token_owners();
    const auto& nodes_dead = _gossiper.get_unreachable_nodes();

    group0_voter_calculator::nodes_list_t nodes;

    auto add_nodes = [this, &nodes, &group0_config](const std::set<locator::host_id>& nodes_set, bool is_alive) {
        for (const auto& host_id : nodes_set) {
            const raft::server_id id{host_id.uuid()};
            const auto* const node = _topology.find(id);
            if (!node) {
                rvlogger.warn("Node {} not found in the topology", id);
                continue;
            }
            const auto& rs = node->second;
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
            rvlogger.debug("Node {} to be added is already a member", id);
            continue;
        }
        const auto* const node = _topology.find(id);
        if (!node) {
            rvlogger.warn("Node {} not found in the topology", id);
            continue;
        }
        const auto is_alive = _gossiper.is_alive(locator::host_id{id.uuid()});
        const auto& rs = node->second;
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

    // Handle the removed nodes
    for (const auto& id : nodes_removed) {
        // force the removal of votership in any case
        voters_del.emplace(id);
        const auto itr = nodes.find(id);
        if (itr == nodes.end()) {
            rvlogger.warn("Node {} to be removed is not a member", id);
            continue;
        }
        // Remove the node from the list to not consider it in the further calculations
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
