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


// Represents the priority of nodes based on their status and role.
//
// The priority is used to determine the order in which nodes are selected as voters.
// Priority values can be combined as a single node might have multiple properties.
struct node_priority {

    using value_t = int;

    // Priority for alive nodes.
    //
    // This should be the largest priority value to prefer alive nodes over dead nodes.
    static constexpr value_t alive = 10;

    // Priority for nodes that are current voters.
    //
    // This should be smaller than the alive node priority to still prefer alive nodes over dead voters.
    static constexpr value_t voter = 1;

    // Priority for the current leader node.
    //
    // This should be smaller than the alive node priority to still prefer alive non-leader nodes.
    static constexpr value_t leader = 2;

    static constexpr value_t get_value(const group0_voter_calculator::node_descriptor& node) {
        value_t priority = 0;
        if (node.is_alive) {
            priority += node_priority::alive;
        }
        if (node.is_voter) {
            priority += node_priority::voter;
        }
        if (node.is_leader) {
            priority += node_priority::leader;
        }
        return priority;
    }
};


using node_ids_t = std::vector<raft::server_id>;


// Manages voter selection within a single rack.
//
// This class stores information about nodes within a rack and provides functionality
// to select voters based on priority criteria. It maintains a priority queue of nodes
// categorized by their status (alive/dead) and role (voter/non-voter) and provides
// methods to select the next voter and check if more candidates are available.
//
// The class also defines a priority comparator to determine the priority of racks
// based on the number of assigned voters, alive nodes, and dead voters.
class rack_info {

    size_t _alive_nodes_remaining = 0;
    size_t _existing_alive_voters_remaining = 0;
    size_t _existing_dead_voters_remaining = 0;

    bool _owns_alive_leader = false;

    using node_info_t = std::tuple<node_priority::value_t, raft::server_id>;

    struct node_info_priority_compare {
        bool operator()(const node_info_t& lhs, const node_info_t& rhs) const {
            // Compare by priority (higher priority has precedence)
            return std::get<0>(lhs) < std::get<0>(rhs);
        }
    };

    using nodes_store_t = std::priority_queue<node_info_t, std::vector<node_info_t>, node_info_priority_compare>;
    nodes_store_t _nodes;

    size_t _assigned_voters_count = 0;

    static nodes_store_t create_nodes_list(const group0_voter_calculator::nodes_list_t& all_nodes, const node_ids_t& rack_nodes, size_t& alive_nodes_remaining,
            size_t& existing_alive_voters_remaining, size_t& existing_dead_voters_remaining, bool& owns_alive_leader) {
        alive_nodes_remaining = 0;
        existing_alive_voters_remaining = 0;
        existing_dead_voters_remaining = 0;
        owns_alive_leader = false;

        return rack_nodes |
               std::views::transform([&all_nodes, &alive_nodes_remaining, &existing_alive_voters_remaining, &existing_dead_voters_remaining,
                                             &owns_alive_leader](const auto& id) {
                   const auto& node = all_nodes.at(id);
                   if (node.is_alive) {
                       ++alive_nodes_remaining;
                       if (node.is_leader) {
                           owns_alive_leader = true;
                       }
                   }
                   if (node.is_voter) {
                       if (node.is_alive) {
                           ++existing_alive_voters_remaining;
                       } else {
                           ++existing_dead_voters_remaining;
                       }
                   }
                   return std::make_tuple(node_priority::get_value(node), id);
               }) |
               std::ranges::to<nodes_store_t>();
    }

public:
    explicit rack_info(const group0_voter_calculator::nodes_list_t& all_nodes, const node_ids_t& rack_nodes)
        : _nodes(create_nodes_list(
                  all_nodes, rack_nodes, _alive_nodes_remaining, _existing_alive_voters_remaining, _existing_dead_voters_remaining, _owns_alive_leader)) {
    }

    // Selects the "best" next voter from the rack.
    //
    // Returns the ID of the selected voter or `std::nullopt` if no more candidates are available.
    //
    // If a node is selected, it is removed from the list of candidates, and the number of voters assigned
    // to the rack is incremented.
    [[nodiscard]] std::optional<raft::server_id> select_next_voter(const group0_voter_calculator::nodes_list_t& nodes_info) {
        if (_nodes.empty()) {
            return std::nullopt;
        }

        const auto [priority, voter_id] = _nodes.top();
        _nodes.pop();

        const auto& node = nodes_info.at(voter_id);

        if (node.is_alive) {
            SCYLLA_ASSERT(_alive_nodes_remaining > 0);
            --_alive_nodes_remaining;
            if (node.is_leader) {
                SCYLLA_ASSERT(_owns_alive_leader);
                _owns_alive_leader = false;
            }
        }
        if (node.is_voter) {
            if (node.is_alive) {
                SCYLLA_ASSERT(_existing_alive_voters_remaining > 0);
                --_existing_alive_voters_remaining;
            } else {
                SCYLLA_ASSERT(_existing_dead_voters_remaining > 0);
                --_existing_dead_voters_remaining;
            }
        }

        ++_assigned_voters_count;

        return voter_id;
    }

    // Checks if there are more candidates available for voter selection.
    //
    // Returns `true` if there are more candidates available, `false` otherwise.
    [[nodiscard]] bool has_more_candidates() const {
        return !_nodes.empty();
    }

    // Priority comparator for rack_info objects.
    // Used to determine which rack should be selected next for voter assignment.
    struct priority_compare {
        bool operator()(const rack_info& lhs, const rack_info& rhs) const {
            if (lhs._assigned_voters_count != rhs._assigned_voters_count) {
                return lhs._assigned_voters_count > rhs._assigned_voters_count;
            }
            // We don't consider dead leaders here (only alive leaders), as a dead leader is about to lose
            // its leadership and be replaced by a new leader.
            if (lhs._owns_alive_leader != rhs._owns_alive_leader) {
                return rhs._owns_alive_leader;
            }
            if (lhs._existing_alive_voters_remaining != rhs._existing_alive_voters_remaining) {
                return lhs._existing_alive_voters_remaining < rhs._existing_alive_voters_remaining;
            }
            if (lhs._alive_nodes_remaining != rhs._alive_nodes_remaining) {
                return lhs._alive_nodes_remaining < rhs._alive_nodes_remaining;
            }
            // Retain dead voters as fallback if we can't find enough alive voters.
            return lhs._existing_dead_voters_remaining < rhs._existing_dead_voters_remaining;
        }
    };
};


// Manages voter selection within a single datacenter.
//
// This class stores information about nodes within a datacenter organized by racks
// and provides functionality to select voters based on priority criteria. It maintains
// a priority queue of racks and provides methods to select the next voter and check
// if more candidates are available.
//
// The class also defines a priority comparator to determine the priority of datacenters based on the number of
// assigned voters, alive nodes, and dead voters.
class datacenter_info {

    size_t _nodes_remaining = 0;
    size_t _assigned_voters_count = 0;

    size_t _existing_alive_voters_remaining = 0;

    bool _owns_alive_leader = false;

    using racks_store_t = std::priority_queue<rack_info, std::vector<rack_info>, rack_info::priority_compare>;
    racks_store_t _racks;

    static racks_store_t create_racks_list(const group0_voter_calculator::nodes_list_t& all_nodes, const node_ids_t& dc_nodes,
            size_t& existing_alive_voters_remaining, bool& owns_alive_leader) {
        existing_alive_voters_remaining = 0;
        owns_alive_leader = false;

        std::unordered_map<std::string_view, node_ids_t> nodes_by_rack;
        for (const auto& id : dc_nodes) {
            const auto& node = all_nodes.at(id);
            nodes_by_rack[node.rack].emplace_back(id);
            if (node.is_alive) {
                if (node.is_voter) {
                    ++existing_alive_voters_remaining;
                }
                if (node.is_leader) {
                    owns_alive_leader = true;
                }
            }
        }

        return nodes_by_rack | std::views::values | std::views::transform([&all_nodes](const auto& rack_nodes) {
            return rack_info(all_nodes, rack_nodes);
        }) | std::ranges::to<racks_store_t>();
    }

public:
    explicit datacenter_info(const group0_voter_calculator::nodes_list_t& all_nodes, const node_ids_t& dc_nodes)
        : _nodes_remaining(dc_nodes.size())
        , _racks(create_racks_list(all_nodes, dc_nodes, _existing_alive_voters_remaining, _owns_alive_leader)) {
    }

    // Selects the "best" next voter from the datacenter.
    //
    // Returns the ID of the selected voter or `std::nullopt` if no more candidates are available.
    //
    // If a node is selected, it is removed from the list of candidates, and the number of voters assigned
    // to the datacenter is incremented.
    [[nodiscard]] std::optional<raft::server_id> select_next_voter(const group0_voter_calculator::nodes_list_t& nodes_info) {
        while (!_racks.empty()) {
            auto rack = _racks.top();
            _racks.pop();

            const auto voter_id = rack.select_next_voter(nodes_info);

            if (!voter_id) {
                continue;
            }

            if (rack.has_more_candidates()) {
                _racks.push(rack);
            }

            const auto& node = nodes_info.at(voter_id.value());

            if (node.is_alive) {
                if (node.is_voter) {
                    SCYLLA_ASSERT(_existing_alive_voters_remaining > 0);
                    --_existing_alive_voters_remaining;
                }
                if (node.is_leader) {
                    SCYLLA_ASSERT(_owns_alive_leader);
                    _owns_alive_leader = false;
                }
            }

            SCYLLA_ASSERT(_nodes_remaining > 0);

            --_nodes_remaining;
            ++_assigned_voters_count;

            return voter_id;
        }

        // No more nodes to select
        return std::nullopt;
    }

    // Checks if there are more candidates available for voter selection.
    //
    // Returns `true` if there are more candidates available, `false` otherwise.
    //
    // The selection is limited by the maximum number of voters per datacenter.
    [[nodiscard]] bool has_more_candidates(size_t voters_max_per_dc) const {
        return _nodes_remaining > 0 && _assigned_voters_count < voters_max_per_dc;
    }

    // Priority comparator for datacenter_info objects.
    // Used to determine which datacenter should be selected next for voter assignment.
    struct priority_compare {
        bool operator()(const datacenter_info& lhs, const datacenter_info& rhs) const {
            if (lhs._assigned_voters_count != rhs._assigned_voters_count) {
                return lhs._assigned_voters_count > rhs._assigned_voters_count;
            }
            // We don't consider dead leaders here (only alive leaders), as a dead leader is about to lose
            // its leadership and be replaced by a new leader
            if (lhs._owns_alive_leader != rhs._owns_alive_leader) {
                return rhs._owns_alive_leader;
            }
            if (lhs._existing_alive_voters_remaining != rhs._existing_alive_voters_remaining) {
                return lhs._existing_alive_voters_remaining < rhs._existing_alive_voters_remaining;
            }
            return lhs._racks.size() < rhs._racks.size();
        }
    };
};


class calculator_impl {

    const group0_voter_calculator::nodes_list_t& _nodes_info;

    using datacenters_store_t = std::priority_queue<datacenter_info, std::vector<datacenter_info>, datacenter_info::priority_compare>;

    size_t _largest_dc_size = 0;
    datacenters_store_t _datacenters;

    uint64_t _voters_max;
    uint64_t _voters_max_per_dc;

    static uint64_t calc_voters_max(
            uint64_t voters_max, const group0_voter_calculator::nodes_list_t& nodes, const datacenters_store_t& datacenters, size_t dc_largest_size) {
        auto num_voters = std::min(voters_max, nodes.size());

        // Return unchanged if 0 nodes or already odd
        if (num_voters == 0 || num_voters % 2 != 0) {
            return num_voters;
        }

        // Convert even count to odd by reducing by 1
        return num_voters - 1;
    }

    static uint64_t calc_voters_max_per_dc(
            uint64_t voters_max, const group0_voter_calculator::nodes_list_t& nodes, const datacenters_store_t& datacenters, size_t dc_largest_size) {
        const auto datacenters_count = datacenters.size();

        // With 2 DCs (or fewer), we can't prevent one DC from taking half or more voters, so we allow more voters per DC.
        if (datacenters_count <= 2) {
            return voters_max;
        }

        // If the number of DCs is greater than 2, prevent any DC from taking half or more of the voters.
        // The calculation is not trivial. For example, with 1 DC having 3 nodes and 2 DCs having 1 node each,
        // we can't simply take half of voters_max - 1, as that would still allow the first DC to
        // take 2 voters, thus having a majority.
        //
        // Therefore, we determine the DC with the maximal number of nodes and calculate the sum of the remaining
        // nodes across all the other DCs - with the largest DC being forced to have fewer voters than the sum of
        // all the other DC nodes.
        const auto nodes_count_exclude_largest = nodes.size() - dc_largest_size;

        return std::min(nodes_count_exclude_largest - 1, (voters_max - 1) / 2);
    }

    static datacenters_store_t create_datacenters_list(const group0_voter_calculator::nodes_list_t& nodes, size_t& largest_dc_size) {
        largest_dc_size = 0;

        std::unordered_map<std::string_view, node_ids_t> nodes_by_dc;
        for (const auto& [id, node] : nodes) {
            nodes_by_dc[node.datacenter].emplace_back(id);
        }

        if (!nodes_by_dc.empty()) {
            largest_dc_size = std::ranges::max_element(nodes_by_dc, [](const auto& dc1, const auto& dc2) {
                return dc1.second.size() < dc2.second.size();
            })->second.size();
        }

        return nodes_by_dc | std::views::values | std::views::transform([&nodes](const auto& dc_nodes) {
            return datacenter_info(nodes, dc_nodes);
        }) | std::ranges::to<datacenters_store_t>();
    }

    // Selects the next voter from the datacenters.
    //
    // Returns the ID of the selected voter or `std::nullopt` if no more candidates are available.
    //
    // The method selects the datacenter with the highest priority (based on the comparator) and selects the next voter
    // from that datacenter.
    [[nodiscard]] std::optional<raft::server_id> select_next_voter(const group0_voter_calculator::nodes_list_t& nodes_info) {
        while (!_datacenters.empty()) {
            auto dc = _datacenters.top();
            _datacenters.pop();

            const auto voter_id = dc.select_next_voter(nodes_info);

            if (!voter_id) {
                // No more available voter candidates in this DC
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
        : _nodes_info(nodes)
        , _datacenters(create_datacenters_list(nodes, _largest_dc_size))
        , _voters_max(calc_voters_max(voters_max, nodes, _datacenters, _largest_dc_size))
        , _voters_max_per_dc(calc_voters_max_per_dc(_voters_max, nodes, _datacenters, _largest_dc_size)) {

        rvlogger.debug("Max voters/per DC: {}/{}", _voters_max, _voters_max_per_dc);
    }

    [[nodiscard]] group0_voter_calculator::voters_set_t distribute_voters() {
        group0_voter_calculator::voters_set_t voters;
        voters.reserve(_voters_max);

        while (voters.size() < _voters_max) {
            const auto& voter = select_next_voter(_nodes_info);
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

    // Ensure we are not interrupted while calculating and modifying voters
    const auto lock = co_await get_units(_voter_lock, 1, as);

    if (!_feature_service.group0_limited_voters) {
        // The previous implementation only handled voter removals.
        // Thus, we only handle the removed nodes here if the feature is not active.
        co_return co_await _group0.modify_voters({}, nodes_removed, as);
    }

    auto& raft_server = _group0.group0_server();

    const auto& leader_id = raft_server.current_leader();

    // Load the current cluster members
    const auto& group0_config = raft_server.get_configuration();

    const auto& nodes_alive = _gossiper.get_live_members();
    const auto& nodes_dead = _gossiper.get_unreachable_members();

    group0_voter_calculator::nodes_list_t nodes;

    auto get_replica_state = [this](const raft::server_id& id, bool allow_transitioning) -> const replica_state* {
        if (const auto it = _topology.normal_nodes.find(id); it != _topology.normal_nodes.end()) {
            return &it->second;
        }
        rvlogger.debug("Node {} is not present in the normal nodes", id);
        if (allow_transitioning) {
            if (const auto it = _topology.transition_nodes.find(id); it != _topology.transition_nodes.end()) {
                return &it->second;
            }
            rvlogger.debug("Node {} is not present in the transitioning nodes", id);
        }
        return nullptr;
    };

    // Helper for adding a single node to the nodes list
    auto add_node = [&nodes, &group0_config, &leader_id](const raft::server_id& id, const replica_state& rs, bool is_alive) {
        const auto is_voter = group0_config.can_vote(id);
        const auto is_leader = (id == leader_id);
        nodes.emplace(id, group0_voter_calculator::node_descriptor{
                                  .datacenter = rs.datacenter,
                                  .rack = rs.rack,
                                  .is_voter = is_voter,
                                  .is_alive = is_alive,
                                  .is_leader = is_leader,
                          });
    };

    // Helper function to add nodes from a gossiper node set
    auto add_nodes_from_list = [&add_node, &get_replica_state, &group0_config](const std::set<locator::host_id>& nodes_set, bool is_alive) {
        for (const auto& host_id : nodes_set) {
            const raft::server_id id{host_id.uuid()};
            // We only allow transitioning nodes to be added if they are already voters.
            const bool allow_transitioning = group0_config.can_vote(id);
            const auto* node = get_replica_state(id, allow_transitioning);
            if (!node) {
                continue;
            }
            add_node(id, *node, is_alive);
        }
    };

    add_nodes_from_list(nodes_alive, true);
    add_nodes_from_list(nodes_dead, false);

    // Process newly added nodes
    for (const auto& id : nodes_added) {
        const auto itr = nodes.find(id);
        if (itr != nodes.end()) {
            // This is expected - node already processed from gossiper
            rvlogger.debug("Node {} to be added is already a member", id);
            continue;
        }
        // When adding a node, it may not be present in normal nodes yet, but might be joining
        // and be in the transitioning state.
        const auto* node = get_replica_state(id, true);
        if (!node) {
            rvlogger.warn("Node {} was not found in the topology, can't add as a voter candidate", id);
            continue;
        }
        add_node(id, *node, _gossiper.is_alive(locator::host_id{id.uuid()}));
    }

    std::unordered_set<raft::server_id> voters_add;
    std::unordered_set<raft::server_id> voters_del;

    // Process removed nodes
    for (const auto& id : nodes_removed) {
        // Force the removal of votership in any case
        voters_del.emplace(id);
        const auto itr = nodes.find(id);
        if (itr == nodes.end()) {
            rvlogger.warn("Node {} to be removed is not a member", id);
            continue;
        }
        // Remove the node from the list so it is not considered in further calculations
        nodes.erase(itr);
    }

    // Calculate the optimal voter distribution
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
    // Filter nodes to include only alive nodes or existing voters
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
