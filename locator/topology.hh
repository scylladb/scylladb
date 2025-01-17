/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include <boost/functional/hash.hpp>
#include <cstddef>
#include <functional>
#include <unordered_set>
#include <unordered_map>
#include <iostream>
#include <random>

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/bool_class.hh>

#include "locator/types.hh"
#include "inet_address_vectors.hh"

using namespace seastar;

struct sort_by_proximity_topology;

namespace locator {
class topology;
}

namespace locator {

class node;
using node_holder = std::unique_ptr<node>;

using shard_id = seastar::shard_id;

class node {
public:
    using this_node = bool_class<struct this_node_tag>;
    using idx_type = int;

    enum class state {
        none = 0,
        bootstrapping,  // (joining)
        replacing,      // (joining)
        normal,
        being_decommissioned, // (leaving)
        being_removed,        // (leaving)
        being_replaced,       // (leaving)
        left        // after decommissioned, removed, replaced
    };

private:
    const locator::topology* _topology;
    locator::host_id _host_id;
    endpoint_dc_rack _dc_rack;
    state _state;
    shard_id _shard_count = 0;
    bool _excluded = false;

    // Is this node the `localhost` instance
    this_node _is_this_node;
    idx_type _idx = -1;

public:
    node(const locator::topology* topology,
         locator::host_id id,
         endpoint_dc_rack dc_rack,
         state state,
         shard_id shard_count = 0,
         this_node is_this_node = this_node::no,
         idx_type idx = -1);

    node(const node&) = delete;
    node(node&&) = delete;

    bool operator==(const node& other) const noexcept {
        return this == &other;
    }

    const locator::topology* topology() const noexcept {
        return _topology;
    }

    const locator::host_id& host_id() const noexcept {
        return _host_id;
    }

    const endpoint_dc_rack& dc_rack() const noexcept {
        return _dc_rack;
    }

    // Is this "localhost"?
    this_node is_this_node() const noexcept { return _is_this_node; }

    // idx < 0 means "unassigned"
    idx_type idx() const noexcept { return _idx; }

    state get_state() const noexcept { return _state; }

    bool is_joining() const noexcept {
        switch (_state) {
        case state::bootstrapping:
        case state::replacing:
            return true;
        default:
            return false;
        }
    }

    bool is_normal() const noexcept {
        return _state == state::normal;
    }

    // Excluded nodes are still part of topology, possibly present in replica sets, but
    // are not going to be up again and can be ignored in barriers.
    bool is_excluded() const {
        return _excluded;
    }

    void set_excluded(bool excluded) {
        _excluded = excluded;
    }

    bool is_leaving() const noexcept {
        switch (_state) {
        case state::being_decommissioned:
        case state::being_removed:
        case state::being_replaced:
            return true;
        default:
            return false;
        }
    }

    bool is_member() const noexcept {
        return is_normal() || is_leaving();
    }

    bool left() const noexcept {
        return _state == state::left;
    }

    bool is_none() const noexcept {
        return _state == state::none;
    }

    shard_id get_shard_count() const noexcept { return _shard_count; }

    static std::string to_string(state);

private:
    static node_holder make(const locator::topology* topology,
                            locator::host_id id,
                            endpoint_dc_rack dc_rack,
                            state state,
                            shard_id shard_count = 0,
                            node::this_node is_this_node = this_node::no,
                            idx_type idx = -1);
    node_holder clone() const;

    void set_topology(const locator::topology* topology) noexcept { _topology = topology; }
    void set_idx(idx_type idx) noexcept { _idx = idx; }
    void set_state(state state) noexcept { _state = state; }
    void set_shard_count(shard_id shard_count) noexcept { _shard_count = shard_count; }

    friend class topology;
};

class topology {
public:
    struct config {
        inet_address this_endpoint;
        inet_address this_cql_address;   // corresponds to broadcast_rpc_address
        host_id this_host_id;
        endpoint_dc_rack local_dc_rack;
        bool disable_proximity_sorting = false;

        bool operator==(const config&) const = default;
    };
    struct shallow_copy{};
    topology(shallow_copy, config cfg);
    topology(config cfg);
    topology(topology&&) noexcept;

    topology& operator=(topology&&) noexcept;

    future<topology> clone_gently() const;
    future<> clear_gently() noexcept;

public:
    const config& get_config() const noexcept { return _cfg; }

    void set_host_id_cfg(host_id this_host_id);

    const node* this_node() const noexcept {
        return _this_node;
    }

    // Adds a node with given host_id, endpoint, and DC/rack.
    const node& add_node(host_id id, const endpoint_dc_rack& dr, node::state state,
                         shard_id shard_count = 0);

    // Optionally updates node's current host_id, endpoint, or DC/rack.
    // Note: the host_id may be updated from null to non-null after a new node gets a new, random host_id,
    // or a peer node host_id may be updated when the node is replaced with another node using the same ip address.
    void update_node(node& node,
                            std::optional<host_id> opt_id,
                            std::optional<endpoint_dc_rack> opt_dr,
                            std::optional<node::state> opt_st,
                            std::optional<shard_id> opt_shard_count = std::nullopt);

    // Removes a node using its host_id
    // Returns true iff the node was found and removed.
    bool remove_node(host_id id);

    // Looks up a node by its host_id.
    // Returns a pointer to the node if found, or nullptr otherwise.
    const node* find_node(host_id id) const noexcept;
    node* find_node(host_id id) noexcept;

    const node& get_node(host_id id) const {
        auto n = find_node(id);
        if (!n) {
            throw_with_backtrace<std::runtime_error>(format("Node {} not found in topology", id));
        }
        return *n;
    };

    // Finds a node by its index
    // Returns a pointer to the node if found, or nullptr otherwise.
    const node* find_node(node::idx_type idx) const noexcept;

    // Returns true if a node with given host_id is found
    bool has_node(host_id id) const noexcept;

    /**
     * Stores current DC/rack assignment for ep
     *
     * Adds or updates a node with given endpoint
     */
    const node& add_or_update_endpoint(host_id id, std::optional<endpoint_dc_rack> opt_dr = std::nullopt,
                                       std::optional<node::state> opt_st = std::nullopt,
                                       std::optional<shard_id> shard_count = std::nullopt);

    bool remove_endpoint(locator::host_id ep);

    const std::unordered_map<sstring,
                           std::unordered_set<host_id>>&
    get_datacenter_endpoints() const {
        return _dc_endpoints;
    }

    std::unordered_map<sstring, std::unordered_set<host_id>>
    get_datacenter_host_ids() const;

    const std::unordered_map<sstring,
                            std::unordered_set<std::reference_wrapper<const node>>>&
    get_datacenter_nodes() const {
        return _dc_nodes;
    }

    const std::unordered_map<sstring, std::unordered_map<sstring, std::unordered_set<std::reference_wrapper<const node>>>>&
    get_datacenter_rack_nodes() const noexcept {
        return _dc_rack_nodes;
    }

    const std::unordered_map<sstring,
                       std::unordered_map<sstring,
                                          std::unordered_set<host_id>>>&
    get_datacenter_racks() const {
        return _dc_racks;
    }

    const std::unordered_set<sstring>& get_datacenters() const noexcept {
        return _datacenters;
    }

    // Get dc/rack location of this node
    const endpoint_dc_rack& get_location() const noexcept {
        return _this_node ? _this_node->dc_rack() : _cfg.local_dc_rack;
    }
    // Get dc/rack location of a node identified by host_id
    // The specified node must exist.
    const endpoint_dc_rack& get_location(host_id id) const {
        return find_node(id)->dc_rack();
    }

    // Get datacenter of this node
    const sstring& get_datacenter() const noexcept {
        return get_location().dc;
    }
    // Get datacenter of a node identified by host_id
    // The specified node must exist.
    const sstring& get_datacenter(host_id id) const {
        return get_location(id).dc;
    }

    // Get rack of this node
    const sstring& get_rack() const noexcept {
        return get_location().rack;
    }
    // Get rack of a node identified by host_id
    // The specified node must exist.
    const sstring& get_rack(host_id id) const {
        return get_location(id).rack;
    }

    auto get_local_dc_filter() const noexcept {
        return [ this, local_dc = get_datacenter() ] (auto ep) {
            return get_datacenter(ep) == local_dc;
        };
    };

    template <std::ranges::range Range>
    inline size_t count_local_endpoints(const Range& endpoints) const {
        return std::count_if(endpoints.begin(), endpoints.end(), get_local_dc_filter());
    }

    bool can_sort_by_proximity() const noexcept {
        return _sort_by_proximity;
    }

    /**
     * This method will sort the addresses list by proximity to the given host_id,
     * if `can_sort_by_proximity()`.
     */
    void sort_by_proximity(locator::host_id address, host_id_vector_replica_set& addresses) const;

    /**
     * Unconditionally sort the addresses list by proximity to the given host_id,
     * assuming `can_sort_by_proximity`.
     */
    void do_sort_by_proximity(locator::host_id address, host_id_vector_replica_set& addresses) const;

    /**
     * Calculates topology-distance between two endpoints.
     *
     * The closest nodes to a given node are:
     * 1. The node itself
     * 2. Nodes in the same RACK
     * 3. Nodes in the same DC
     */
    static int distance(const locator::host_id& address, const endpoint_dc_rack& loc, const locator::host_id& address1, const endpoint_dc_rack& loc1) noexcept;

    // Executes a function for each node in a state other than "none" and "left".
    void for_each_node(std::function<void(const node&)> func) const;

    // Returns pointers to all nodes in a state other than "none" and "left".
    std::unordered_set<std::reference_wrapper<const node>> get_nodes() const;

    // Returns ids of all nodes in a state other than "none" and "left".
    std::unordered_set<locator::host_id> get_all_host_ids() const;

    host_id my_host_id() const noexcept {
        return _cfg.this_host_id;
    }

    inet_address my_address() const noexcept {
        return _cfg.this_endpoint;
    }

    inet_address my_cql_address() const noexcept {
        return _cfg.this_cql_address;
    }

    bool is_me(const locator::host_id& id) const noexcept {
        return id == my_host_id();
    }

private:
    using random_engine_type = std::mt19937_64;

    bool is_configured_this_node(const node&) const;
    const node& add_node(node_holder node);
    void remove_node(const node& node);

    static std::string debug_format(const node*);

    void index_node(const node& node);
    void unindex_node(const node& node);
    node_holder pop_node(const node& node);

    static node* make_mutable(const node* nptr) {
        return const_cast<node*>(nptr);
    }

    void seed_random_engine(random_engine_type::result_type);

    unsigned _shard;
    config _cfg;
    const node* _this_node = nullptr;
    std::vector<node_holder> _nodes;
    std::unordered_map<host_id, std::reference_wrapper<const node>> _nodes_by_host_id;

    std::unordered_map<sstring, std::unordered_set<std::reference_wrapper<const node>>> _dc_nodes;
    std::unordered_map<sstring, std::unordered_map<sstring, std::unordered_set<std::reference_wrapper<const node>>>> _dc_rack_nodes;

    /** multi-map: DC -> endpoints in that DC */
    std::unordered_map<sstring,
                       std::unordered_set<host_id>>
        _dc_endpoints;

    /** map: DC -> (multi-map: rack -> endpoints in that rack) */
    std::unordered_map<sstring,
                       std::unordered_map<sstring,
                                          std::unordered_set<host_id>>>
        _dc_racks;

    bool _sort_by_proximity = true;

    // pre-calculated
    std::unordered_set<sstring> _datacenters;

    void calculate_datacenters();

    mutable random_engine_type _random_engine;

    friend class token_metadata_impl;
    friend struct ::sort_by_proximity_topology;
public:
    void test_compare_endpoints(const locator::host_id& address, const locator::host_id& a1, const locator::host_id& a2) const;
    void test_sort_by_proximity(const locator::host_id& address, const host_id_vector_replica_set& nodes) const;
};

} // namespace locator

namespace std {

template<>
struct hash<std::reference_wrapper<const locator::node>> {
    std::size_t operator()(const std::reference_wrapper<const locator::node>& ref) const {
        return std::hash<const locator::node*>()(&ref.get());
    }
};

template<>
struct equal_to<std::reference_wrapper<const locator::node>> {
    bool operator()(const std::reference_wrapper<const locator::node>& lhs,
                    const std::reference_wrapper<const locator::node>& rhs) const {
        return lhs.get() == rhs.get();
    }
};

std::ostream& operator<<(std::ostream& out, const locator::node& node);
std::ostream& operator<<(std::ostream& out, const locator::node::state& state);

} // namespace std

// Accepts :v format option for verbose printing
template <>
struct fmt::formatter<locator::node> : fmt::formatter<string_view> {
    bool verbose = false;
    constexpr auto parse(fmt::format_parse_context& ctx) {
        auto it = ctx.begin(), end = ctx.end();
        if (it != end) {
            if (*it == 'v') {
                verbose = true;
                it++;
            }
            if (it != end && *it != '}') {
                throw fmt::format_error("invalid format specifier");
            }
        }
        return it;
    }
    template <typename FormatContext>
    auto format(const locator::node& node, FormatContext& ctx) const {
        if (!verbose) {
            return fmt::format_to(ctx.out(), "{}", node.host_id());
        } else {
            return fmt::format_to(ctx.out(), " idx={} host_id={} dc={} rack={} state={} shards={} this_node={}",
                    node.idx(),
                    node.host_id(),
                    node.dc_rack().dc,
                    node.dc_rack().rack,
                    locator::node::to_string(node.get_state()),
                    node.get_shard_count(),
                    bool(node.is_this_node()));
        }
    }
};

template <>
struct fmt::formatter<locator::node::state> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const locator::node::state& state, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", locator::node::to_string(state));
    }
};
