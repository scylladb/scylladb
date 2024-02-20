/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <boost/intrusive/link_mode.hpp>
#include <unordered_set>
#include <unordered_map>
#include <compare>
#include <iostream>

#include <boost/intrusive/list.hpp>

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/bool_class.hh>

#include "locator/types.hh"
#include "inet_address_vectors.hh"
#include "seastar/core/sharded.hh"

using namespace seastar;

namespace locator {
class topology;
}

namespace std {
std::ostream& operator<<(std::ostream& out, const locator::topology&);
}

namespace locator {

class node;
using node_holder = std::unique_ptr<node>;

using shard_id = seastar::shard_id;

struct rack {
    sstring name;

    explicit rack(sstring_view rack_name) : name(rack_name) {}
};

struct datacenter {
    using rack_ptr = std::unique_ptr<rack>;
    using racks_map_t = std::unordered_map<sstring_view, rack_ptr>;

    sstring name;
    racks_map_t racks;

    explicit datacenter(sstring_view dc_name) : name(dc_name) {}
};

struct location {
    const datacenter* dc = nullptr;
    const rack* rack = nullptr;

    endpoint_dc_rack get_dc_rack() const;
};

// A global service that manages datacenters and racks by name
class topology_registry {
public:
    using datacenter_ptr = std::unique_ptr<datacenter>;
    using datacenters_map_t = std::unordered_map<sstring_view, datacenter_ptr>;

private:
    datacenters_map_t _datacenters;

public:
    const datacenters_map_t& datacenters() const noexcept {
        return _datacenters;
    }

    const datacenter* find_datacenter(sstring_view name) const noexcept;
    location find_location(sstring_view dc_name, sstring_view rack_name) const noexcept;

    // Datacenters and racks can only be created on shard 0
    // They are kept forever, even when empty.
    location find_or_create_location(sstring_view dc_name, sstring_view rack_name);
};

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
    inet_address _endpoint;
    location _location;
    state _state;
    shard_id _shard_count = 0;
    bool _excluded = false;

    // Is this node the `localhost` instance
    this_node _is_this_node;
    idx_type _idx = -1;

    using list_hook_t = boost::intrusive::list_member_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>;
    list_hook_t _dc_hook;
    list_hook_t _rack_hook;

public:
    node(const locator::topology* topology,
         locator::host_id id,
         inet_address endpoint,
         state state,
         shard_id shard_count = 0,
         this_node is_this_node = this_node::no,
         idx_type idx = -1);

    node(const node&) = delete;
    node(node&&) = delete;

    const locator::topology* topology() const noexcept {
        return _topology;
    }

    const locator::host_id& host_id() const noexcept {
        return _host_id;
    }

    const inet_address& endpoint() const noexcept {
        return _endpoint;
    }

    const locator::location& location() const noexcept {
        return _location;
    }

    endpoint_dc_rack dc_rack() const {
        return _location.get_dc_rack();
    }

    const datacenter* dc() const noexcept {
        return _location.dc;
    }

    const rack* rack() const noexcept {
        return _location.rack;
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

    bool left() const noexcept {
        return _state == state::left;
    }

    shard_id get_shard_count() const noexcept { return _shard_count; }

    static std::string to_string(state);

private:
    static node_holder make(const locator::topology* topology,
                            locator::host_id id,
                            inet_address endpoint,
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
    topology(topology_registry& topology_registry, config cfg);
    topology(topology&&) noexcept;

    topology& operator=(topology&&) noexcept;

    // Note: must be called on shard 0
    future<topology> clone_gently() const;
    future<> clear_gently() noexcept;

public:
    struct datacenter_inventory {
        using dc_nodes_list_t = boost::intrusive::list<node,
            boost::intrusive::member_hook<node, node::list_hook_t, &node::_dc_hook>,
            boost::intrusive::constant_time_size<false>>;
        dc_nodes_list_t nodes;
        size_t node_count;

        using rack_nodes_list_t = boost::intrusive::list<node,
            boost::intrusive::member_hook<node, node::list_hook_t, &node::_rack_hook>,
            boost::intrusive::constant_time_size<false>>;
        std::unordered_map<const rack*, rack_nodes_list_t> racks;
    };
    using inventory_map = std::unordered_map<const datacenter*, datacenter_inventory>;

    topology_registry& get_topology_registry() const noexcept {
        return _topology_registry;
    }

    const config& get_config() const noexcept { return _cfg; }

    void set_host_id_cfg(host_id this_host_id) {
        _cfg.this_host_id = this_host_id;
    }

    const node* this_node() const noexcept {
        return _this_node;
    }

    // Adds a node with given host_id, endpoint, and DC/rack.
    //
    // Note: must be called on shard 0
    const node* add_node(host_id id, const inet_address& ep, const endpoint_dc_rack& dr, node::state state,
                         shard_id shard_count = 0);

    // Optionally updates node's current host_id, endpoint, or DC/rack.
    // Note: the host_id may be updated from null to non-null after a new node gets a new, random host_id,
    // or a peer node host_id may be updated when the node is replaced with another node using the same ip address.
    //
    // Note: must be called on shard 0
    const node* update_node(node* node,
                            std::optional<host_id> opt_id,
                            std::optional<inet_address> opt_ep,
                            std::optional<endpoint_dc_rack> opt_dr,
                            std::optional<node::state> opt_st,
                            std::optional<shard_id> opt_shard_count = std::nullopt);

    // Removes a node using its host_id
    // Returns true iff the node was found and removed.
    //
    // Note: must be called on shard 0
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

    // Looks up a node by its inet_address.
    // Returns a pointer to the node if found, or nullptr otherwise.
    const node* find_node(const inet_address& ep) const noexcept;

    // Finds a node by its index
    // Returns a pointer to the node if found, or nullptr otherwise.
    const node* find_node(node::idx_type idx) const noexcept;

    // Returns true if a node with given host_id is found
    bool has_node(host_id id) const noexcept;
    bool has_node(inet_address id) const noexcept;

    /**
     * Stores current DC/rack assignment for ep
     *
     * Adds or updates a node with given endpoint
     *
     * Note: must be called on shard 0
     */
    const node* add_or_update_endpoint(host_id id, std::optional<inet_address> opt_ep,
                                       std::optional<endpoint_dc_rack> opt_dr = std::nullopt,
                                       std::optional<node::state> opt_st = std::nullopt,
                                       std::optional<shard_id> shard_count = std::nullopt);

    // Note: must be called on shard 0
    bool remove_endpoint(locator::host_id ep);

    /**
     * Returns true iff contains given endpoint.
     */
    bool has_endpoint(inet_address) const;

    // Return the total number of nodes.
    size_t node_count() const noexcept {
        return _nodes.size();
    }

    // Return the number of nodes in the datacenter.
    // datacenter must exist in topology.
    size_t node_count(const datacenter* dc) const {
        return _inventory_map.at(dc).node_count;
    }

    std::unordered_map<sstring,
                           std::unordered_set<inet_address>>
    get_datacenter_endpoints() const;

    std::unordered_map<sstring,
                            std::unordered_set<const node*>>
    get_datacenter_nodes() const;

    std::unordered_map<sstring,
                       std::unordered_map<sstring,
                                          std::unordered_set<inet_address>>>
    get_datacenter_racks() const;

    std::unordered_set<sstring> get_datacenter_names() const noexcept;

    const datacenter* find_datacenter(sstring_view name) const noexcept {
        if (const auto* dc = _topology_registry.find_datacenter(name); _inventory_map.contains(dc)) {
            return dc;
        }
        return nullptr;
    }

    const inventory_map& get_inventory() const noexcept {
        return _inventory_map;
    }

    // Get dc/rack location of this node
    location get_location() const {
        return _this_node ? _this_node->location() :
                _topology_registry.find_or_create_location(_cfg.local_dc_rack.dc, _cfg.local_dc_rack.rack);
    }
    // Get dc/rack location of a node identified by host_id
    // The specified node must exist.
    location get_location(host_id id) const {
        return find_node(id)->location();
    }
    // Get dc/rack location of a node identified by endpoint
    // The specified node must exist.
    location get_location(const inet_address& ep) const;

    // Get datacenter of this node
    const datacenter* get_datacenter() const noexcept {
        return get_location().dc;
    }
    // Get datacenter of a node identified by host_id
    // The specified node must exist.
    const datacenter* get_datacenter(host_id id) const {
        return get_location(id).dc;
    }
    // Get datacenter of a node identified by endpoint
    // The specified node must exist.
    const datacenter* get_datacenter(inet_address ep) const {
        return get_location(ep).dc;
    }

    // Get rack of this node
    const rack* get_rack() const noexcept {
        return get_location().rack;
    }
    // Get rack of a node identified by host_id
    // The specified node must exist.
    const rack* get_rack(host_id id) const {
        return get_location(id).rack;
    }
    // Get rack of a node identified by endpoint
    // The specified node must exist.
    const rack* get_rack(inet_address ep) const {
        return get_location(ep).rack;
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

    /**
     * This method will sort the <tt>List</tt> by proximity to the given
     * address.
     */
    void sort_by_proximity(inet_address address, inet_address_vector_replica_set& addresses) const;

    void for_each_node(std::function<void(const node*)> func) const;

    // Call func for every node in the datacenter
    void for_each_node(const datacenter*, std::function<void(const node*)> func) const;

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

    bool is_me(const inet_address& addr) const noexcept {
        return addr == my_address();
    }

private:
    bool is_configured_this_node(const node&) const;
    const node* add_node(node_holder node, const endpoint_dc_rack& dc_rack);
    void remove_node(const node* node);

    static std::string debug_format(const node*);

    void index_node(node* node, const endpoint_dc_rack& dr);
    void unindex_node(node* node);
    node_holder pop_node(node* node);

    static node* make_mutable(const node* nptr) {
        return const_cast<node*>(nptr);
    }

    /**
     * compares two endpoints in relation to the target endpoint, returning as
     * Comparator.compare would
     *
     * The closest nodes to a given node are:
     * 1. The node itself
     * 2. Nodes in the same RACK as the reference node
     * 3. Nodes in the same DC as the reference node
     */
    std::weak_ordering compare_endpoints(const inet_address& address, const inet_address& a1, const inet_address& a2) const;

    unsigned _shard;
    topology_registry& _topology_registry;
    config _cfg;
    const node* _this_node = nullptr;
    std::vector<node_holder> _nodes;
    std::unordered_map<host_id, const node*> _nodes_by_host_id;
    std::unordered_map<inet_address, const node*> _nodes_by_endpoint;

    inventory_map _inventory_map;

    bool _sort_by_proximity = true;

    const std::unordered_map<inet_address, const node*>& get_nodes_by_endpoint() const noexcept {
        return _nodes_by_endpoint;
    };

    friend class token_metadata_impl;
public:
    void test_compare_endpoints(const inet_address& address, const inet_address& a1, const inet_address& a2) const;

    friend std::ostream& std::operator<<(std::ostream& out, const topology&);
};

} // namespace locator

namespace std {

std::ostream& operator<<(std::ostream& out, const locator::node& node);
std::ostream& operator<<(std::ostream& out, const locator::node::state& state);

} // namespace std

// Accepts :v format option for verbose printing
template <>
struct fmt::formatter<locator::node> : fmt::formatter<std::string_view> {
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
            return fmt::format_to(ctx.out(), "{}/{}", node.host_id(), node.endpoint());
        } else {
            return fmt::format_to(ctx.out(), "idx={} host_id={} endpoint={} dc={} rack={} state={} shards={} this_node={}",
                    node.idx(),
                    node.host_id(),
                    node.endpoint(),
                    node.location().dc ? node.location().dc->name : "",
                    node.location().rack ? node.location().rack->name : "",
                    locator::node::to_string(node.get_state()),
                    node.get_shard_count(),
                    bool(node.is_this_node()));
        }
    }
};

template <>
struct fmt::formatter<locator::node::state> : fmt::formatter<std::string_view> {
    template <typename FormatContext>
    auto format(const locator::node::state& state, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", locator::node::to_string(state));
    }
};
