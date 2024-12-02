/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "group0_voter_registry.hh"

#include <queue>

#include <seastar/util/log.hh>

#include "service/topology_state_machine.hh"

namespace service {

seastar::logger rvlogger("group0_voter_registry");

future<> group0_voter_registry::insert_nodes(const std::unordered_set<raft::server_id>& nodes, abort_source& as) {
    return update_voters(nodes, {}, as);
}

future<> group0_voter_registry::remove_nodes(const std::unordered_set<raft::server_id>& nodes, abort_source& as) {
    return update_voters({}, nodes, as);
}

future<> group0_voter_registry::update_voters(
        const std::unordered_set<raft::server_id>& nodes_added, const std::unordered_set<raft::server_id>& nodes_removed, abort_source& as) {
    std::unordered_set<raft::server_id> voters_add;
    std::unordered_set<raft::server_id> voters_del;
    voters_add.reserve(nodes_added.size());
    voters_del.reserve(nodes_removed.size());

    // Consider all the new nodes non-voters first
    for (const auto& node : nodes_added) {
        const auto& server_info = _server_info_accessor.find(node);
        auto& voters = _dc_voters[server_info.datacenter];
        const auto itr = std::find_if(voters.begin(), voters.end(), [node](const auto& voter) {
            return voter.second == node;
        });
        if (itr != voters.end()) {
            rvlogger.warn("Node {} to be added is already present in the raft voter registry", node);
            continue;
        }
        _dc_voters[server_info.datacenter].emplace(false, node);
        ++_nodes_count;
    }

    // Handle the removed nodes
    for (const auto& node : nodes_removed) {
        // Make sure the node is always marked a non-voter
        voters_del.emplace(node);

        const auto& server_info = _server_info_accessor.find(node);
        auto& voters = _dc_voters[server_info.datacenter];
        const auto itr = std::find_if(voters.begin(), voters.end(), [node](const auto& voter) {
            return voter.second == node;
        });
        if (itr == voters.end()) {
            rvlogger.warn("Node {} to be removed not found in the raft voter registry", node);
            continue;
        }
        voters.erase(itr);
        --_nodes_count;

        // Remove the DC if there are no nodes left
        if (voters.empty()) {
            _dc_voters.erase(server_info.datacenter);
        }
    }

    // Distribute the available voter slots across the datacenters
    auto slots_left_per_dc = distribute_slots();

    for (auto& [dc, voters] : _dc_voters) {
        auto slots_left_dc = slots_left_per_dc[dc];

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

            voters.erase(itr, end);
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

            const auto& added_voters_as_set = added_voters | std::ranges::views::transform([](const auto& node) {
                return std::make_pair(true, node);
            }) | std::ranges::to<std::unordered_multimap>();

            voters.erase(beg, itr);
            voters.insert(added_voters_as_set.begin(), added_voters_as_set.end());
        }
    }

    co_await _voter_client.set_voters_status(voters_add, can_vote::yes, as);
    co_await _voter_client.set_voters_status(voters_del, can_vote::no, as);
}

std::unordered_map<sstring, size_t> group0_voter_registry::distribute_slots() {
    // Calculate the number of voter slots left for each DC
    std::unordered_map<sstring, size_t> slots_left_per_dc;

    for (const auto& [dc, voters] : _dc_voters) {
        slots_left_per_dc.emplace(dc, 0);
    }

    const auto dc_count = _dc_voters.size();

    auto compare_priority_dc = [this](const sstring& dc1, const sstring& dc2) {
        return _dc_voters[dc2].size() < _dc_voters[dc1].size();
    };

    std::priority_queue<sstring, std::deque<sstring>, decltype(compare_priority_dc)> dc_by_priority(compare_priority_dc);

    for (const auto& [dc, _] : slots_left_per_dc) {
        dc_by_priority.push(dc);
    }

    auto slots_left = std::min(_max_voters, _nodes_count);

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

        auto slots_per_dc = slots_left / dc_count_left;

        // Slots for a dc are capped by the number of nodes in the dc
        slots_left_dc = std::min(slots_per_dc, _dc_voters[dc].size());
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

    return slots_left_per_dc;
}

} // namespace service
