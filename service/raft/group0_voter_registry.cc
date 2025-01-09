/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "group0_voter_registry.hh"

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/composite_key.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/key.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/tag.hpp>

#include <queue>

#include <seastar/util/log.hh>

#include "service/topology_state_machine.hh"
#include "utils/hash.hh"

namespace service {

namespace {

seastar::logger rvlogger("group0_voter_registry");

namespace bmi = boost::multi_index;

class group0_voter_registry_impl : public group0_voter_registry {

    constexpr static size_t ADAPTIVE_VOTERS_DC_LOW_CAP = 5;
    constexpr static size_t ADAPTIVE_VOTERS_DC_HIGH_CAP = 9;

    const raft_server_info_accessor& _server_info_accessor;
    raft_voter_client& _voter_client;

    size_t _max_voters;

    struct node_record {
        raft::server_id id;
        sstring dc;
        bool is_voter;
    };

    struct by_id {};
    struct by_dc {};

    boost::multi_index_container<node_record,
            bmi::indexed_by<
                    // Primary index by the node id.
                    bmi::hashed_unique<bmi::tag<by_id>, bmi::key<&node_record::id>, std::hash<raft::server_id>>,
                    // Secondary index by the datacenter and voter status.
                    bmi::ordered_non_unique<bmi::tag<by_dc>, bmi::composite_key<node_record, bmi::key<&node_record::dc>, bmi::key<&node_record::is_voter>>>>>
            _nodes;

public:
    group0_voter_registry_impl(const raft_server_info_accessor& server_info_accessor, raft_voter_client& voter_client, size_t max_voters)
        : _server_info_accessor(server_info_accessor)
        , _voter_client(voter_client)
        , _max_voters(max_voters) {
    }

    // Insert a node to the voter registry
    future<> insert_node(raft::server_id node, abort_source& as);

    // Insert a list of nodes to the voter registry
    future<> insert_nodes(const std::unordered_set<raft::server_id>& nodes, abort_source& as) override;

    // Remove a node from the voter registry
    future<> remove_node(raft::server_id node, abort_source& as);

    // Remove a list of nodes from the voter registry
    future<> remove_nodes(const std::unordered_set<raft::server_id>& nodes, abort_source& as) override;

private:
    static size_t get_max_slots(size_t max_voters, size_t dc_count);

    future<> update_voters(const std::unordered_set<raft::server_id>& nodes_added, const std::unordered_set<raft::server_id>& nodes_removed, abort_source& as);

    std::unordered_map<sstring, size_t> distribute_slots();
};


future<> group0_voter_registry_impl::insert_nodes(const std::unordered_set<raft::server_id>& nodes, abort_source& as) {
    return update_voters(nodes, {}, as);
}

future<> group0_voter_registry_impl::remove_nodes(const std::unordered_set<raft::server_id>& nodes, abort_source& as) {
    return update_voters({}, nodes, as);
}

size_t group0_voter_registry_impl::get_max_slots(size_t max_voters, size_t dc_count) {
    if (max_voters != group0_voter_registry::MAX_VOTERS_ADAPTIVE) {
        return max_voters;
    }

    // Adaptive voters cases

    if (dc_count < ADAPTIVE_VOTERS_DC_LOW_CAP) {
        return ADAPTIVE_VOTERS_DC_LOW_CAP;
    }

    if (dc_count > ADAPTIVE_VOTERS_DC_HIGH_CAP) {
        return ADAPTIVE_VOTERS_DC_HIGH_CAP;
    }

    return dc_count;
}

future<> group0_voter_registry_impl::update_voters(
        const std::unordered_set<raft::server_id>& nodes_added, const std::unordered_set<raft::server_id>& nodes_removed, abort_source& as) {
    std::unordered_set<raft::server_id> voters_add;
    std::unordered_set<raft::server_id> voters_del;
    voters_add.reserve(nodes_added.size());
    voters_del.reserve(nodes_removed.size());

    // Consider all the new nodes non-voters first
    for (const auto& node : nodes_added) {
        const auto itr = _nodes.find(node);
        if (itr != _nodes.end()) {
            rvlogger.warn("Node {} to be added is already present in the raft voter registry", node);
            continue;
        }
        const auto& server_info = _server_info_accessor.find(node);
        _nodes.emplace(node, server_info.datacenter, false);
    }

    // Handle the removed nodes
    for (const auto& node : nodes_removed) {
        // Make sure the node is always marked a non-voter
        voters_del.emplace(node);

        const auto itr = _nodes.find(node);
        if (itr == _nodes.end()) {
            rvlogger.warn("Node {} to be removed not found in the raft voter registry", node);
            continue;
        }
        _nodes.erase(itr);
    }

    // Distribute the available voter slots across the datacenters
    auto slots_left_per_dc = distribute_slots();

    auto& nodes_by_dc = _nodes.get<by_dc>();

    for (auto& [dc, slots_left_dc] : slots_left_per_dc) {
        rvlogger.debug("DC: {}, voter slots: {}", dc, slots_left_dc);

        // Process the (pre-existing) voters first
        {
            auto [itr, end] = nodes_by_dc.equal_range(std::make_tuple(dc, true));
            while (slots_left_dc && itr != end) {
                --slots_left_dc;
                ++itr;
            }

            // Switch the remaining voters to non-voters
            const auto& removed_voters = std::ranges::subrange(itr, end) | std::views::transform([](const auto& rec) {
                return rec.id;
            }) | std::ranges::to<std::unordered_set>();
            voters_del.insert(removed_voters.begin(), removed_voters.end());

            for (auto node : removed_voters) {
                _nodes.modify(_nodes.find(node), [](auto& rec) {
                    rvlogger.debug("Removing an existing voter: {}", rec.id);
                    rec.is_voter = false;
                });
            }
        }

        // Process the non-voters
        {
            const auto [beg, end] = nodes_by_dc.equal_range(std::make_tuple(dc, false));
            auto itr = beg;
            while (slots_left_dc && itr != end) {
                --slots_left_dc;
                ++itr;
            }

            // Switch the first non-voters to voters
            const auto& added_voters = std::ranges::subrange(beg, itr) | std::views::transform([](const auto& rec) {
                return rec.id;
            }) | std::ranges::to<std::unordered_set>();
            voters_add.insert(added_voters.begin(), added_voters.end());

            for (auto node : added_voters) {
                _nodes.modify(_nodes.find(node), [](auto& rec) {
                    rvlogger.debug("Adding a new voter: {}", rec.id);
                    rec.is_voter = true;
                });
            }
        }
    }

    co_await _voter_client.set_voters_status(voters_add, can_vote::yes, as);
    co_await _voter_client.set_voters_status(voters_del, can_vote::no, as);
}

std::unordered_map<sstring, size_t> group0_voter_registry_impl::distribute_slots() {
    // Calculate the number of voter slots left for each DC
    std::unordered_map<sstring, size_t> slots_left_per_dc;

    const auto& nodes_by_dc = _nodes.get<by_dc>();

    for (const auto& rec : nodes_by_dc) {
        slots_left_per_dc.emplace(rec.dc, 0);
    }

    auto compare_priority_dc = [&nodes_by_dc](const sstring& dc1, const sstring& dc2) {
        return nodes_by_dc.count(dc2) < nodes_by_dc.count(dc1);
    };

    std::priority_queue<sstring, std::deque<sstring>, decltype(compare_priority_dc)> dc_by_priority(compare_priority_dc);

    for (const auto& [dc, _] : slots_left_per_dc) {
        dc_by_priority.push(dc);
    }

    const auto dc_count = slots_left_per_dc.size();
    const size_t max_slots = get_max_slots(_max_voters, dc_count);
    auto slots_left = std::min(max_slots, _nodes.size());

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
        slots_left_dc = std::min(slots_per_dc, nodes_by_dc.count(dc));
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

std::unique_ptr<group0_voter_registry> group0_voter_registry::create(
        const raft_server_info_accessor& server_info_accessor, raft_voter_client& voter_client, size_t max_voters) {
    return std::make_unique<group0_voter_registry_impl>(server_info_accessor, voter_client, max_voters);
}

} // namespace service
