/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "group0_voter_registry.hh"

#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/uniqued.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/composite_key.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/key.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/tag.hpp>

#include <queue>

#include <seastar/util/log.hh>

#include "service/topology_state_machine.hh"
#include "utils/assert.hh"
#include "utils/hash.hh"

namespace service {

namespace {

seastar::logger rvlogger("group0_voter_registry");

namespace bmi = boost::multi_index;

class group0_voter_registry_impl : public group0_voter_registry {

    constexpr static size_t ADAPTIVE_VOTERS_DC_LOW_CAP = 5;
    constexpr static size_t ADAPTIVE_VOTERS_DC_HIGH_CAP = 9;

    std::unique_ptr<const raft_server_info_accessor> _server_info_accessor;
    std::unique_ptr<raft_voter_client> _voter_client;

    size_t _max_voters;

    struct node_record {
        raft::server_id id;
        sstring dc;
        sstring rack;
        bool is_voter;
    };

    struct by_id {};
    struct by_dc {};

    using nodes_map_t = boost::multi_index_container<node_record,
            bmi::indexed_by<
                    // Primary index by the node id.
                    bmi::hashed_unique<bmi::tag<by_id>, bmi::key<&node_record::id>, std::hash<raft::server_id>>,
                    // Secondary index by the datacenter and voter status.
                    bmi::ordered_non_unique<bmi::tag<by_dc>,
                            bmi::composite_key<node_record, bmi::key<&node_record::dc>, bmi::key<&node_record::rack>, bmi::key<&node_record::is_voter>>>>>;

public:
    group0_voter_registry_impl(std::unique_ptr<const raft_server_info_accessor> server_info_accessor, std::unique_ptr<raft_voter_client> voter_client,
            std::optional<size_t> max_voters)
        : _server_info_accessor(std::move(server_info_accessor))
        , _voter_client(std::move(voter_client))
        , _max_voters(max_voters.value_or(MAX_VOTERS_UNLIMITED)) {
        SCYLLA_ASSERT(_server_info_accessor != nullptr);
        SCYLLA_ASSERT(_voter_client != nullptr);
    }

private:
    static size_t get_max_slots(size_t max_voters, size_t dc_count);

    future<> update_voters(
            const std::unordered_set<raft::server_id>& nodes_added, const std::unordered_set<raft::server_id>& nodes_removed, abort_source& as) override;

    using voter_slots_key_t = std::tuple<sstring, sstring>;
    using voter_slots_t = std::unordered_map<voter_slots_key_t, size_t, utils::tuple_hash>;

    [[nodiscard]] voter_slots_t distribute_voter_slots(nodes_map_t nodes) const;
};


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

    nodes_map_t nodes;

    // Load the current members
    const auto& members = _server_info_accessor->get_members();

    rvlogger.debug("Updating voters: {} members", members.size());

    for (const auto& [node, rs] : members) {
        const auto is_voter = _voter_client->is_voter(node);
        rvlogger.debug("Node: {}, DC: {}, is_voter: {}", node, rs.datacenter, is_voter);
        nodes.emplace(node, rs.datacenter, rs.rack, is_voter);
    }

    // Handle the added nodes
    for (const auto& node : nodes_added) {
        const auto itr = nodes.find(node);
        if (itr != nodes.end()) {
            rvlogger.warn("Node {} to be added is already present in the raft voter registry", node);
            continue;
        }
        const auto& rs = _server_info_accessor->find(node);
        const auto is_voter = _voter_client->is_voter(node);
        nodes.emplace(node, rs.datacenter, rs.rack, is_voter);
    }

    // Handle the removed nodes
    for (const auto& node : nodes_removed) {
        // Make sure the node is always marked a non-voter
        voters_del.emplace(node);

        const auto itr = nodes.find(node);
        if (itr == nodes.end()) {
            rvlogger.warn("Node {} to be removed not found in the raft voter registry", node);
            continue;
        }
        nodes.erase(itr);
    }

    // Distribute the available voter slots across the datacenters
    auto slots_left_per_dc = distribute_voter_slots(nodes);

    auto& nodes_by_dc = nodes.get<by_dc>();

    for (auto& [dc_rack, slots_left_dc] : slots_left_per_dc) {
        rvlogger.debug("DC/rack: {}/{}, voter slots: {}", std::get<0>(dc_rack), std::get<1>(dc_rack), slots_left_dc);

        // Process the (pre-existing) voters first
        {
            auto [itr, end] = nodes_by_dc.equal_range(std::make_tuple(std::get<0>(dc_rack), std::get<1>(dc_rack), true));
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
                nodes.modify(nodes.find(node), [](auto& rec) {
                    rvlogger.debug("Removing an existing voter: {}", rec.id);
                    rec.is_voter = false;
                });
            }
        }

        // Process the non-voters
        {
            const auto [beg, end] = nodes_by_dc.equal_range(std::make_tuple(std::get<0>(dc_rack), std::get<1>(dc_rack), false));
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
                nodes.modify(nodes.find(node), [](auto& rec) {
                    rvlogger.debug("Adding a new voter: {}", rec.id);
                    rec.is_voter = true;
                });
            }
        }
    }

    co_await _voter_client->set_voters_status(voters_add, can_vote::yes, as);
    co_await _voter_client->set_voters_status(voters_del, can_vote::no, as);
}

group0_voter_registry_impl::voter_slots_t group0_voter_registry_impl::distribute_voter_slots(nodes_map_t nodes) const {
    // Calculate the number of voter slots left for each DC
    voter_slots_t slots_left_per_dc_rack;

    std::unordered_map<sstring, size_t> slots_left_per_dc;

    const auto& nodes_by_dc = nodes.get<by_dc>();

    struct dc_info_vals {
        size_t count_racks = 0;
        size_t count_nodes = 0;
        std::unordered_map<sstring, size_t> rack_node_count;
    };

    std::unordered_map<sstring, dc_info_vals> dc_info;

    for (const auto& dc : nodes_by_dc | boost::adaptors::transformed([](const auto& rec) {
             return rec.dc;
         }) | boost::adaptors::uniqued) {
        auto& dc_vals = dc_info[dc];
        const auto& dc_range = nodes_by_dc.equal_range(dc);
        slots_left_per_dc.emplace(dc, 0);
        dc_vals.count_nodes = std::distance(dc_range.first, dc_range.second);

        for (const auto rack : dc_range | boost::adaptors::transformed([](const auto& rec) {
                 return rec.rack;
             }) | boost::adaptors::uniqued) {
            const auto& dc_rack_key = std::make_tuple(dc, rack);
            slots_left_per_dc_rack.emplace(dc_rack_key, 0);
            dc_vals.count_racks++;
            dc_vals.rack_node_count[rack] = nodes_by_dc.count(dc_rack_key);
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

    std::priority_queue<sstring, std::deque<sstring>, decltype(compare_priority_dc)> dc_by_priority(compare_priority_dc);

    for (const auto& [dc, _] : slots_left_per_dc) {
        dc_by_priority.push(dc);
    }

    const auto dc_count = slots_left_per_dc.size();
    const size_t max_slots = get_max_slots(_max_voters, dc_count);
    auto slots_left = std::min(max_slots, nodes.size());

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

        std::priority_queue<sstring, std::deque<sstring>, decltype(compare_priority_rack)> rack_by_priority(compare_priority_rack);

        for (const auto& [rack, _] : info_dc.rack_node_count) {
            rack_by_priority.push(rack);
        }

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

group0_voter_registry::instance_ptr group0_voter_registry::create(std::unique_ptr<const raft_server_info_accessor> server_info_accessor,
        std::unique_ptr<raft_voter_client> voter_client, std::optional<size_t> max_voters) {
    return std::make_unique<group0_voter_registry_impl>(std::move(server_info_accessor), std::move(voter_client), max_voters);
}

} // namespace service
