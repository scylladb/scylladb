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
#include "utils/hash.hh"

namespace service {

namespace {

seastar::logger rvlogger("group0_voter_handler");


class group0_voter_calculator_impl {

    size_t _max_voters;

    using nodes_rec_t = std::unordered_multimap<bool, raft::server_id>;

    using nodes_key_t = std::tuple<sstring, sstring>;
    using nodes_map_t = std::unordered_map<nodes_key_t, nodes_rec_t, utils::tuple_hash>;

public:
    constexpr static size_t MAX_VOTERS_DEFAULT = 5;

    explicit group0_voter_calculator_impl(size_t max_voters)
        : _max_voters(max_voters) {
    }

    [[nodiscard]] group0_voter_calculator::voters_result_t calculate_voters(const group0_voter_calculator::nodes_list_t& nodes) const;

private:
    using voter_slots_key_t = std::tuple<sstring, sstring>;
    using voter_slots_t = std::unordered_map<voter_slots_key_t, size_t, utils::tuple_hash>;

    [[nodiscard]] voter_slots_t distribute_voter_slots(const group0_voter_calculator::nodes_list_t& nodes) const;
};

group0_voter_calculator::voters_result_t group0_voter_calculator_impl::calculate_voters(const group0_voter_calculator::nodes_list_t& nodes) const {
    std::unordered_set<raft::server_id> voters_add;
    std::unordered_set<raft::server_id> voters_del;

    rvlogger.debug("Calculating voters: {} nodes", nodes.size());

    const auto& nodes_filtered = nodes | std::views::filter([&voters_del](const auto& node_entry) {
        const auto& [id, node] = node_entry;
        rvlogger.debug("Node: {}, DC: {}, is_voter: {}, is_alive: {}", id, node.datacenter, node.is_voter, node.is_alive);
        if (node.is_alive) {
            return true;
        }

        rvlogger.info("Node {} is not alive, skipping", id);
        if (node.is_voter) {
            rvlogger.info("The dead node {} is a voter, marking for removal", id);
            voters_del.emplace(id);
        }
        return false;
    }) | std::ranges::to<std::unordered_map>();

    nodes_map_t nodes_map;

    for (const auto& [id, node] : nodes_filtered) {
        nodes_map[std::make_tuple(node.datacenter, node.rack)].emplace(node.is_voter, id);
    }

    // Distribute the available voter slots across the datacenters
    auto slots_left_per_dc = distribute_voter_slots(nodes_filtered);

    for (auto& [dc_rack, voters] : nodes_map) {
        auto slots_left_dc = slots_left_per_dc[dc_rack];

        // Process the (pre-existing) voters first
        {
            auto [itr, end] = voters.equal_range(true);
            while (slots_left_dc && itr != end) {
                --slots_left_dc;
                ++itr;
            }

            // Switch the remaining voters to non-voters
            const auto& removed_voters = std::ranges::subrange(itr, end) | std::ranges::views::values;
            voters_del.insert(removed_voters.begin(), removed_voters.end());

            const auto& removed_voters_as_set = removed_voters | std::ranges::views::transform([](const auto& node) {
                return std::make_pair(false, node);
            }) | std::ranges::to<std::unordered_multimap>();

            voters.insert(removed_voters_as_set.begin(), removed_voters_as_set.end());
        }

        // Process the non-voters
        {
            const auto [beg, end] = voters.equal_range(false);
            auto itr = beg;
            while (slots_left_dc && itr != end) {
                --slots_left_dc;
                ++itr;
            }

            // Switch the first non-voters to voters
            const auto& added_voters = std::ranges::subrange(beg, itr) | std::ranges::views::values;
            voters_add.insert(added_voters.begin(), added_voters.end());
        }
    }

    return std::make_tuple(std::move(voters_add), std::move(voters_del));
}

group0_voter_calculator_impl::voter_slots_t group0_voter_calculator_impl::distribute_voter_slots(const group0_voter_calculator::nodes_list_t& nodes) const {
    // Calculate the number of voter slots left for each DC
    voter_slots_t slots_left_per_dc_rack;

    std::unordered_map<sstring, size_t> slots_left_per_dc;

    struct dc_info_vals {
        size_t count_racks = 0;
        size_t count_nodes = 0;
        std::unordered_map<sstring, size_t> rack_node_count;
    };

    std::unordered_map<sstring, dc_info_vals> dc_info;

    for (const auto& [id, node] : nodes) {
        auto& dc_vals = dc_info[node.datacenter];
        slots_left_per_dc.emplace(node.datacenter, 0);
        dc_vals.count_nodes++;
        dc_vals.rack_node_count[node.rack]++;
        if (slots_left_per_dc_rack.emplace(std::make_tuple(node.datacenter, node.rack), 0).second) {
            dc_vals.count_racks++;
        }
    }

    auto compare_priority_dc = [&dc_info](const sstring& dc1, const sstring& dc2) {
        const auto& info_dc1 = dc_info.at(dc1);
        const auto& info_dc2 = dc_info.at(dc2);

        // primary ordering criteria: number of racks
        if (info_dc2.count_racks != info_dc1.count_racks) {
            return info_dc2.count_racks < info_dc1.count_racks;
        }

        // secondary ordering criteria: number of nodes in the DC
        return info_dc2.count_nodes < info_dc1.count_nodes;
    };

    std::priority_queue<sstring, std::vector<sstring>, decltype(compare_priority_dc)> dc_by_priority(
            compare_priority_dc, std::views::keys(slots_left_per_dc) | std::ranges::to<std::vector>());

    const auto dc_count = slots_left_per_dc.size();

    auto slots_left = std::min(_max_voters, nodes.size());

    const auto max_slots_per_dc = (dc_count > 2)
                                          // if the number of DCs is greater than 2, prevent any DC taking majority of voters
                                          ? (slots_left - 1) / 2
                                          // with 2 DCs (or less), we can't prevent one DC from taking majority, so we allow more voters per DC
                                          : slots_left;

    auto dc_count_left = dc_count;

    // Iterate from the DC with the smallest number of nodes to ensure the most even distribution of voters
    while (!dc_by_priority.empty()) {
        const auto& dc = dc_by_priority.top();

        auto& slots_left_dc = slots_left_per_dc[dc];

        auto slots_per_dc = std::max(slots_left / dc_count_left, 1UL);
        slots_per_dc = std::min(slots_per_dc, slots_left);

        // Slots for a dc are capped by the number of nodes in the dc
        slots_left_dc = std::min(slots_per_dc, dc_info[dc].count_nodes);
        if (slots_left_dc > max_slots_per_dc) {
            // If the DC has reached the majority limit, we can't add more
            rvlogger.debug("Max voters reached for DC: {}, slots: {}", dc, slots_left_dc);
            slots_left_dc = max_slots_per_dc;
        }
        slots_left -= slots_left_dc;

        --dc_count_left;
        dc_by_priority.pop();
    }

    if (slots_left) {
        // We might not be able to distribute all the slots if we have a DC(s) with more nodes that could become voters,
        // but we've reached the majority limit for that DC.

        rvlogger.debug("Did not distribute all available voter slots, left: {}", slots_left);
    }

    if (dc_count == 2) {
        // 2 DCs is a special case - we want to distribute the voters asymmetrically
        const auto dc_first = slots_left_per_dc.begin();
        const auto dc_second = std::next(dc_first);
        if (dc_first->second == dc_second->second) {
            // If the voters were distributed evenly, we want to make them asymmetric
            assert(dc_second->second > 0);
            dc_second->second--;
        }
    }

    // Now distribute the voters across the racks in each DC
    for (auto& [dc, slots_left_dc] : slots_left_per_dc) {
        const auto& info_dc = dc_info.at(dc);

        auto compare_priority_rack = [&info_dc](const sstring& rack1, const sstring& rack2) {
            return info_dc.rack_node_count.at(rack2) < info_dc.rack_node_count.at(rack1);
        };

        std::priority_queue<sstring, std::vector<sstring>, decltype(compare_priority_rack)> rack_by_priority(
                compare_priority_rack, std::views::keys(info_dc.rack_node_count) | std::ranges::to<std::vector>());

        auto rack_count_left = info_dc.rack_node_count.size();

        while (!rack_by_priority.empty()) {
            auto rack = rack_by_priority.top();
            rack_by_priority.pop();

            auto& slots_left_rack = slots_left_per_dc_rack[std::make_tuple(dc, rack)];

            auto slots_per_rack = std::max(slots_left_dc / rack_count_left, 1UL);
            slots_per_rack = std::min(slots_per_rack, slots_left_dc);

            // Slots for a DC/rack are capped by the number of nodes in the DC/rack
            slots_left_rack = std::min(slots_per_rack, info_dc.rack_node_count.at(rack));
            slots_left_dc -= slots_left_rack;

            --rack_count_left;
        }
    }

    return slots_left_per_dc_rack;
}

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
        if (!is_alive) {
            rvlogger.warn("Node {} is not alive, skipping", id);
            continue;
        }
        const auto& rs = node->second;
        const auto is_voter = group0_config.can_vote(id);
        nodes[id] = {
                .datacenter = rs.datacenter,
                .rack = rs.rack,
                .is_voter = is_voter,
                .is_alive = is_alive,
        };
    }

    // Handle the removed nodes
    for (const auto& id : nodes_removed) {
        const auto itr = nodes.find(id);
        if (itr != nodes.end()) {
            // Make sure the node is always marked a non-voter
            itr->second.is_alive = false;
            continue;
        }
        rvlogger.warn("Node {} to be removed is not a member", id);
        const auto* const node = _topology.find(id);
        if (!node) {
            rvlogger.warn("Node {} not found in the topology", id);
            continue;
        }
        const auto& rs = node->second;
        const auto is_voter = group0_config.can_vote(id);
        nodes[id] = {
                .datacenter = rs.datacenter,
                .rack = rs.rack,
                .is_voter = is_voter,
                // set is_alive to false to mark the node as removed
                .is_alive = false,
        };
    }

    const auto& [voters_add, voters_del] = _calculator.calculate_voters(nodes);

    co_await _group0.modify_voters(voters_add, voters_del, as);
}

group0_voter_calculator::group0_voter_calculator(std::optional<size_t> max_voters)
    : _max_voters(max_voters.value_or(group0_voter_calculator_impl::MAX_VOTERS_DEFAULT)) {
}

group0_voter_calculator::voters_result_t group0_voter_calculator::calculate_voters(const group0_voter_calculator::nodes_list_t& nodes) const {
    group0_voter_calculator_impl impl(_max_voters);
    return impl.calculate_voters(nodes);
}

} // namespace service
