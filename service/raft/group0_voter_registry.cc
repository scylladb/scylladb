/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "group0_voter_registry.hh"

#include <queue>

#include <seastar/util/log.hh>

#include "service/topology_state_machine.hh"
#include "utils/assert.hh"

namespace service {

namespace {

seastar::logger rvlogger("group0_voter_registry");

class group0_voter_registry_impl : public group0_voter_registry {

    std::unique_ptr<const raft_server_info_accessor> _server_info_accessor;
    std::unique_ptr<raft_voter_client> _voter_client;

    size_t _max_voters;

    using nodes_rec_t = std::unordered_multimap<bool, raft::server_id>;
    using nodes_map_t = std::unordered_map<sstring, nodes_rec_t>;

public:
    group0_voter_registry_impl(std::unique_ptr<const raft_server_info_accessor> server_info_accessor, std::unique_ptr<raft_voter_client> voter_client,
            std::optional<size_t> max_voters)
        : _server_info_accessor(std::move(server_info_accessor))
        , _voter_client(std::move(voter_client))
        , _max_voters(max_voters.value_or(std::numeric_limits<size_t>::max())) {
        SCYLLA_ASSERT(_server_info_accessor != nullptr);
        SCYLLA_ASSERT(_voter_client != nullptr);
    }

    future<> update_nodes(
            const std::unordered_set<raft::server_id>& nodes_added, const std::unordered_set<raft::server_id>& nodes_removed, abort_source& as) override;

private:
    using voter_slots_t = std::unordered_map<sstring, size_t>;

    [[nodiscard]] voter_slots_t distribute_voter_slots(nodes_map_t nodes) const;
};


future<> group0_voter_registry_impl::update_nodes(
        const std::unordered_set<raft::server_id>& nodes_added, const std::unordered_set<raft::server_id>& nodes_removed, abort_source& as) {
    std::unordered_set<raft::server_id> voters_add;
    std::unordered_set<raft::server_id> voters_del;
    voters_add.reserve(nodes_added.size());
    voters_del.reserve(nodes_removed.size());

    nodes_map_t nodes;

    // Load the current members
    const auto& members = _server_info_accessor->get_members();

    rvlogger.debug("Updating voters: {} members", members.size());

    for (const auto& [node, rs] : members) {
        const auto is_voter = _voter_client->is_voter(node);
        const auto is_alive = _server_info_accessor->is_alive(node);
        rvlogger.debug("Node: {}, DC: {}, is_voter: {}, is_alive: {}", node, rs.datacenter, is_voter, is_alive);
        if (!is_alive) {
            rvlogger.info("Node {} is not alive, skipping", node);
            if (is_voter) {
                rvlogger.info("The dead node {} is a voter, marking for removal", node);
                voters_del.emplace(node);
            }
            continue;
        }
        nodes[rs.datacenter].emplace(is_voter, node);
    }

    // Handle the added nodes
    for (const auto& node : nodes_added) {
        const auto& rs = _server_info_accessor->find(node);
        auto& voters = nodes[rs.datacenter];
        const auto itr = std::find_if(voters.begin(), voters.end(), [node](const auto& voter) {
            return voter.second == node;
        });
        if (itr != voters.end()) {
            rvlogger.debug("Node {} to be added is already present in the raft voter registry", node);
            continue;
        }
        const auto is_voter = _voter_client->is_voter(node);
        const auto is_alive = _server_info_accessor->is_alive(node);
        if (!is_alive) {
            rvlogger.warn("Node {} is not alive, skipping", node);
            continue;
        }
        nodes[rs.datacenter].emplace(is_voter, node);
    }

    // Handle the removed nodes
    for (const auto& node : nodes_removed) {
        // Make sure the node is always marked a non-voter
        voters_del.emplace(node);

        const auto& rs = _server_info_accessor->find(node);
        auto& voters = nodes[rs.datacenter];
        const auto itr = std::find_if(voters.begin(), voters.end(), [node](const auto& voter) {
            return voter.second == node;
        });
        if (itr == voters.end()) {
            rvlogger.debug("Node {} to be removed not found in the raft voter registry", node);
            continue;
        }
        voters.erase(itr);

        // Remove the DC if there are no nodes left
        if (voters.empty()) {
            nodes.erase(rs.datacenter);
        }
    }

    // Distribute the available voter slots across the datacenters
    auto slots_left_per_dc = distribute_voter_slots(nodes);

    for (auto& [dc, voters] : nodes) {
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

    co_await _voter_client->set_voters_status(voters_add, can_vote::yes, as);
    co_await _voter_client->set_voters_status(voters_del, can_vote::no, as);
}

group0_voter_registry_impl::voter_slots_t group0_voter_registry_impl::distribute_voter_slots(nodes_map_t nodes) const {
    // Calculate the number of voter slots left for each DC
    voter_slots_t slots_left_per_dc;

    size_t nodes_count = 0;

    for (const auto& [dc, voters] : nodes) {
        slots_left_per_dc.emplace(dc, 0);
        nodes_count += voters.size();
    }

    const auto dc_count = nodes.size();

    auto compare_priority_dc = [&nodes](const sstring& dc1, const sstring& dc2) {
        return nodes[dc2].size() < nodes[dc1].size();
    };

    std::priority_queue<sstring, std::deque<sstring>, decltype(compare_priority_dc)> dc_by_priority(compare_priority_dc);

    for (const auto& [dc, _] : slots_left_per_dc) {
        dc_by_priority.push(dc);
    }

    auto slots_left = std::min(_max_voters, nodes_count);

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
        slots_left_dc = std::min(slots_per_dc, nodes[dc].size());
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

} // namespace

group0_voter_registry::instance_ptr group0_voter_registry::create(std::unique_ptr<const raft_server_info_accessor> server_info_accessor,
        std::unique_ptr<raft_voter_client> voter_client, std::optional<size_t> max_voters) {
    return std::make_unique<group0_voter_registry_impl>(std::move(server_info_accessor), std::move(voter_client), max_voters);
}

} // namespace service
