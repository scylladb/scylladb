/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/util/lazy.hh>
#include <utility>

#include <boost/range/adaptors.hpp>

#include "log.hh"
#include "locator/topology.hh"
#include "locator/production_snitch_base.hh"
#include "locator/types.hh"
#include "utils/stall_free.hh"

struct node_printer {
    const locator::node* v;
    node_printer(const locator::node* n) noexcept : v(n) {}
};

template <>
struct fmt::formatter<node_printer> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const node_printer& np, fmt::format_context& ctx) const {
        const locator::node* node = np.v;
        auto out = fmt::format_to(ctx.out(), "node={}", fmt::ptr(node));
        if (node) {
            out = fmt::format_to(out, " {:v}", *node);
        }
        return out;
    }
};

static auto lazy_backtrace() {
    return seastar::value_of([] { return current_backtrace(); });
}

namespace locator {

static logging::logger tlogger("topology");

thread_local const endpoint_dc_rack endpoint_dc_rack::default_location = {
    .dc = locator::production_snitch_base::default_dc,
    .rack = locator::production_snitch_base::default_rack,
};

node::node(const locator::topology* topology, locator::host_id id, inet_address endpoint, state state, shard_id shard_count, this_node is_this_node, node::idx_type idx)
    : _topology(topology)
    , _host_id(id)
    , _endpoint(endpoint)
    , _state(state)
    , _shard_count(std::move(shard_count))
    , _is_this_node(is_this_node)
    , _idx(idx)
{}

node_holder node::make(const locator::topology* topology, locator::host_id id, inet_address endpoint, state state, shard_id shard_count, node::this_node is_this_node, node::idx_type idx) {
    return std::make_unique<node>(topology, std::move(id), std::move(endpoint), std::move(state), shard_count, is_this_node, idx);
}

node_holder node::clone() const {
    return make(nullptr, host_id(), endpoint(), get_state(), get_shard_count(), is_this_node());
}

std::string node::to_string(node::state s) {
    switch (s) {
    case state::none:           return "none";
    case state::bootstrapping:  return "bootstrapping";
    case state::replacing:      return "replacing";
    case state::normal:         return "normal";
    case state::being_decommissioned: return "being_decommissioned";
    case state::being_removed:        return "being_removed";
    case state::being_replaced:       return "being_replaced";
    case state::left:           return "left";
    }
    __builtin_unreachable();
}

future<> topology::clear_gently() noexcept {
    _this_node = nullptr;
    _inventory_map.clear();
    _nodes_by_endpoint.clear();
    _nodes_by_host_id.clear();
    co_await utils::clear_gently(_nodes);
}

topology::topology(topology_registry& topology_registry, config cfg)
        : _shard(this_shard_id())
        , _topology_registry(topology_registry)
        , _cfg(cfg)
        , _sort_by_proximity(!cfg.disable_proximity_sorting)
{
    tlogger.trace("topology[{}]: constructing using config: endpoint={} dc={} rack={}", fmt::ptr(this),
            cfg.this_endpoint, cfg.local_dc_rack.dc, cfg.local_dc_rack.rack);
}

topology::topology(topology&& o) noexcept
    : _shard(o._shard)
    , _topology_registry(o._topology_registry)
    , _cfg(std::move(o._cfg))
    , _this_node(std::exchange(o._this_node, nullptr))
    , _nodes(std::move(o._nodes))
    , _nodes_by_host_id(std::move(o._nodes_by_host_id))
    , _nodes_by_endpoint(std::move(o._nodes_by_endpoint))
    , _inventory_map(std::move(o._inventory_map))
    , _sort_by_proximity(o._sort_by_proximity)
{
    assert(_shard == this_shard_id());
    tlogger.trace("topology[{}]: move from [{}]", fmt::ptr(this), fmt::ptr(&o));

    for (auto& n : _nodes) {
        if (n) {
            n->set_topology(this);
        }
    }
}

topology& topology::operator=(topology&& o) noexcept {
    if (this != &o) {
        this->~topology();
        new (this) topology(std::move(o));
    }
    return *this;
}

future<topology> topology::clone_gently() const {
    topology ret(_topology_registry, _cfg);
    tlogger.debug("topology[{}]: clone_gently to {} from shard {}", fmt::ptr(this), fmt::ptr(&ret), _shard);
    for (const auto& nptr : _nodes) {
        if (nptr) {
            ret.add_node(nptr->clone(), nptr->dc_rack());
        }
        co_await coroutine::maybe_yield();
    }
    ret._sort_by_proximity = _sort_by_proximity;
    co_return ret;
}

const node* topology::add_node(host_id id, const inet_address& ep, const endpoint_dc_rack& dr, node::state state, shard_id shard_count) {
    assert(this_shard_id() == 0);
    return add_node(node::make(this, id, ep, state, shard_count), dr);
}

bool topology::is_configured_this_node(const node& n) const {
    if (_cfg.this_host_id && n.host_id()) { // Selection by host_id
        return _cfg.this_host_id == n.host_id();
    }
    if (_cfg.this_endpoint != inet_address()) { // Selection by endpoint
        return _cfg.this_endpoint == n.endpoint();
    }
    return false; // No selection;
}

const node* topology::add_node(node_holder nptr, const endpoint_dc_rack& dr) {
    node* node = nptr.get();

    if (nptr->topology() != this) {
        if (nptr->topology()) {
            on_fatal_internal_error(tlogger, format("topology[{}]: {} belongs to different topology={}", fmt::ptr(this), node_printer(node), fmt::ptr(node->topology())));
        }
        nptr->set_topology(this);
    }

    if (node->idx() > 0) {
        on_internal_error(tlogger, format("topology[{}]: {}: has assigned idx", fmt::ptr(this), node_printer(nptr.get())));
    }

    // Note that _nodes contains also the this_node()
    nptr->set_idx(_nodes.size());
    _nodes.emplace_back(std::move(nptr));

    try {
        if (is_configured_this_node(*node)) {
            if (_this_node) {
                on_internal_error(tlogger, format("topology[{}]: {}: local node already mapped to {}", fmt::ptr(this), node_printer(node), node_printer(this_node())));
            }
            locator::node& n = *_nodes.back();
            n._is_this_node = node::this_node::yes;
        }

        tlogger.debug("topology[{}]: add_node: {}, at {}", fmt::ptr(this), node_printer(nptr.get()), lazy_backtrace());

        index_node(node, dr);
    } catch (...) {
        pop_node(make_mutable(node));
        throw;
    }
    return node;
}

const node* topology::update_node(node* node, std::optional<host_id> opt_id, std::optional<inet_address> opt_ep, std::optional<endpoint_dc_rack> opt_dr, std::optional<node::state> opt_st, std::optional<shard_id> opt_shard_count) {
    assert(this_shard_id() == 0);
    tlogger.debug("topology[{}]: update_node: {}: to: host_id={} endpoint={} dc={} rack={} state={}, at {}", fmt::ptr(this), node_printer(node),
        opt_id ? format("{}", *opt_id) : "unchanged",
        opt_ep ? format("{}", *opt_ep) : "unchanged",
        opt_dr ? format("{}", opt_dr->dc) : "unchanged",
        opt_dr ? format("{}", opt_dr->rack) : "unchanged",
        opt_st ? format("{}", *opt_st) : "unchanged",
        opt_shard_count ? format("{}", *opt_shard_count) : "unchanged",
        lazy_backtrace());

    bool changed = false;
    if (opt_id) {
        if (*opt_id != node->host_id()) {
            if (!*opt_id) {
                on_internal_error(tlogger, format("Updating node host_id to null is disallowed: {}: new host_id={}", node_printer(node), *opt_id));
            }
            if (node->is_this_node() && node->host_id()) {
                on_internal_error(tlogger, format("This node host_id is already set: {}: new host_id={}", node_printer(node), *opt_id));
            }
            if (_nodes_by_host_id.contains(*opt_id)) {
                on_internal_error(tlogger, format("Cannot update node host_id: {}: new host_id already exists: {}", node_printer(node), node_printer(_nodes_by_host_id[*opt_id])));
            }
            changed = true;
        } else {
            opt_id.reset();
        }
    }
    if (opt_ep) {
        if (*opt_ep != node->endpoint()) {
            if (*opt_ep == inet_address{}) {
                on_internal_error(tlogger, format("Updating node endpoint to null is disallowed: {}: new endpoint={}", node_printer(node), *opt_ep));
            }
            changed = true;
        } else {
            opt_ep.reset();
        }
    }
    if (opt_dr) {
        if (opt_dr->dc.empty() || opt_dr->dc == production_snitch_base::default_dc) {
            opt_dr->dc = node->dc_rack().dc;
        }
        if (opt_dr->rack.empty() || opt_dr->rack == production_snitch_base::default_rack) {
            opt_dr->rack = node->dc_rack().rack;
        }
        if (*opt_dr != node->dc_rack()) {
            changed = true;
        }
    }
    if (opt_st) {
        changed |= node->get_state() != *opt_st;
    }
    if (opt_shard_count) {
        changed |= node->get_shard_count() != *opt_shard_count;
    }

    if (!changed) {
        return node;
    }

    // Populate opt_dr even if it is set for update.
    // It is used for reindexing the node below.
    if (!opt_dr) {
        opt_dr = node->dc_rack();
    }

    unindex_node(node);
    // The following block must not throw
    try {
        auto mutable_node = make_mutable(node);
        if (opt_id) {
            mutable_node->_host_id = *opt_id;
        }
        if (opt_ep) {
            mutable_node->_endpoint = *opt_ep;
        }
        if (opt_st) {
            mutable_node->set_state(*opt_st);
        }
        if (opt_shard_count) {
            mutable_node->set_shard_count(*opt_shard_count);
        }
    } catch (...) {
        std::terminate();
    }
    index_node(node, *opt_dr);
    return node;
}

bool topology::remove_node(host_id id) {
    auto node = find_node(id);
    tlogger.debug("topology[{}]: remove_node: host_id={}: {}", fmt::ptr(this), id, node_printer(node));
    if (node) {
        remove_node(node);
        return true;
    }
    return false;
}

void topology::remove_node(const node* node) {
    assert(this_shard_id() == 0);
    pop_node(make_mutable(node));
}

void topology::index_node(node* node, const endpoint_dc_rack& dr) {
    tlogger.trace("topology[{}]: index_node: {}, at {}", fmt::ptr(this), node_printer(node), lazy_backtrace());

    if (node->idx() < 0) {
        on_internal_error(tlogger, format("topology[{}]: {}: must already have a valid idx", fmt::ptr(this), node_printer(node)));
    }

    // FIXME: for now we allow adding nodes with null host_id, for the following cases:
    // 1. This node might be added with no host_id on pristine nodes.
    // 2. Other nodes may be introduced via gossip with their endpoint only first
    //    and their host_id is updated later on.
    if (node->host_id()) {
        auto [nit, inserted_host_id] = _nodes_by_host_id.emplace(node->host_id(), node);
        if (!inserted_host_id) {
            on_internal_error(tlogger, format("topology[{}]: {}: node already exists", fmt::ptr(this), node_printer(node)));
        }
    }
    if (node->endpoint() != inet_address{}) {
        auto eit = _nodes_by_endpoint.find(node->endpoint());
        if (eit != _nodes_by_endpoint.end()) {
            if (eit->second->get_state() == node::state::replacing && node->get_state() == node::state::being_replaced) {
                // replace-with-same-ip, map ip to the old node
                _nodes_by_endpoint.erase(node->endpoint());
            } else if (eit->second->get_state() == node::state::being_replaced && node->get_state() == node::state::replacing) {
                // replace-with-same-ip, map ip to the old node, do nothing if it's already the case
            } else if (eit->second->is_leaving() || eit->second->left()) {
                _nodes_by_endpoint.erase(node->endpoint());
            } else if (!node->is_leaving() && !node->left()) {
                if (node->host_id()) {
                    _nodes_by_host_id.erase(node->host_id());
                }
                on_internal_error(tlogger, format("topology[{}]: {}: node endpoint already mapped to {}", fmt::ptr(this), node_printer(node), node_printer(eit->second)));
            }
        }
        if (node->get_state() != node::state::left) {
            _nodes_by_endpoint.try_emplace(node->endpoint(), node);
        }
    }

    endpoint_dc_rack loc = dr;
    if (dr.dc.empty() || dr.dc == production_snitch_base::default_dc) {
        loc.dc = _cfg.local_dc_rack.dc;
        if (dr.rack.empty() || dr.rack == production_snitch_base::default_rack) {
            loc.rack = _cfg.local_dc_rack.rack;
        }
    }
    node->_location = _topology_registry.find_or_create_location(loc.dc, loc.rack);

    auto& i = _inventory_map[node->dc()];
    i.nodes.push_back(*node);
    ++i.node_count;
    i.racks[node->rack()].push_back(*node);

    if (node->is_this_node()) {
        _this_node = node;
    }
}

void topology::unindex_node(node* node) {
    tlogger.trace("topology[{}]: unindex_node: {}, at {}", fmt::ptr(this), node_printer(node), lazy_backtrace());

    // Erase empty datacenters / racks
    if (auto dc_it = _inventory_map.find(node->dc()); dc_it != _inventory_map.end()) {
        auto& i = dc_it->second;
        node->_dc_hook.unlink();
        node->_rack_hook.unlink();
        --i.node_count;
        if (i.nodes.empty()) {
            assert(i.node_count == 0);
            _inventory_map.erase(dc_it);
        } else if (auto rack_it = i.racks.find(node->rack()); rack_it != i.racks.end()) {
            if (rack_it->second.empty()) {
                i.racks.erase(rack_it);
            }
        }
    }
    auto host_it = _nodes_by_host_id.find(node->host_id());
    if (host_it != _nodes_by_host_id.end() && host_it->second == node) {
        _nodes_by_host_id.erase(host_it);
    }
    auto ep_it = _nodes_by_endpoint.find(node->endpoint());
    if (ep_it != _nodes_by_endpoint.end() && ep_it->second == node) {
        _nodes_by_endpoint.erase(ep_it);
    }
    if (_this_node == node) {
        _this_node = nullptr;
    }
}

node_holder topology::pop_node(node* node) {
    tlogger.trace("topology[{}]: pop_node: {}, at {}", fmt::ptr(this), node_printer(node), lazy_backtrace());

    unindex_node(node);

    auto nh = std::exchange(_nodes[node->idx()], {});

    // shrink _nodes if the last node is popped
    // like when failing to index a newly added node
    if (std::cmp_equal(node->idx(), _nodes.size() - 1)) {
        _nodes.resize(node->idx());
    }

    return nh;
}

// Finds a node by its host_id
// Returns nullptr if not found
const node* topology::find_node(host_id id) const noexcept {
    auto it = _nodes_by_host_id.find(id);
    if (it != _nodes_by_host_id.end()) {
        return it->second;
    }
    return nullptr;
}

// Finds a node by its host_id
// Returns nullptr if not found
node* topology::find_node(host_id id) noexcept {
    return make_mutable(const_cast<const topology*>(this)->find_node(id));
}

// Finds a node by its endpoint
// Returns nullptr if not found
const node* topology::find_node(const inet_address& ep) const noexcept {
    auto it = _nodes_by_endpoint.find(ep);
    if (it != _nodes_by_endpoint.end()) {
        return it->second;
    }
    return nullptr;
}

// Finds a node by its index
// Returns nullptr if not found
const node* topology::find_node(node::idx_type idx) const noexcept {
    if (std::cmp_greater_equal(idx, _nodes.size())) {
        return nullptr;
    }
    return _nodes.at(idx).get();
}

const node* topology::add_or_update_endpoint(host_id id, std::optional<inet_address> opt_ep, std::optional<endpoint_dc_rack> opt_dr, std::optional<node::state> opt_st, std::optional<shard_id> shard_count)
{
    tlogger.trace("topology[{}]: add_or_update_endpoint: host_id={} ep={} dc={} rack={} state={} shards={}, at {}", fmt::ptr(this),
        id, opt_ep, opt_dr.value_or(endpoint_dc_rack{}).dc, opt_dr.value_or(endpoint_dc_rack{}).rack, opt_st.value_or(node::state::none), shard_count,
        lazy_backtrace());

    const auto* n = find_node(id);
    if (n) {
        return update_node(make_mutable(n), std::nullopt, opt_ep, std::move(opt_dr), std::move(opt_st), std::move(shard_count));
    } else if (opt_ep && (n = find_node(*opt_ep))) {
        return update_node(make_mutable(n), id, std::nullopt, std::move(opt_dr), std::move(opt_st), std::move(shard_count));
    }

    return add_node(id,
                    opt_ep.value_or(inet_address{}),
                    opt_dr.value_or(endpoint_dc_rack::default_location),
                    opt_st.value_or(node::state::normal),
                    shard_count.value_or(0));
}

bool topology::remove_endpoint(locator::host_id host_id)
{
    auto node = find_node(host_id);
    tlogger.debug("topology[{}]: remove_endpoint: host_id={}: {}", fmt::ptr(this), host_id, node_printer(node));
    if (node) {
        remove_node(node);
        return true;
    }
    return false;
}

bool topology::has_node(host_id id) const noexcept {
    auto node = find_node(id);
    tlogger.trace("topology[{}]: has_node: host_id={}: {}", fmt::ptr(this), id, node_printer(node));
    return bool(node);
}

bool topology::has_node(inet_address ep) const noexcept {
    auto node = find_node(ep);
    tlogger.trace("topology[{}]: has_node: endpoint={}: node={}", fmt::ptr(this), ep, node_printer(node));
    return bool(node);
}

bool topology::has_endpoint(inet_address ep) const
{
    return has_node(ep);
}

void topology::for_each_node(const datacenter* dc, std::function<void(const node*)> func) const {
    for (const auto& node : _inventory_map.at(dc).nodes) {
        func(&node);
    }
}

std::unordered_map<sstring, std::unordered_set<inet_address>> topology::get_datacenter_endpoints() const {
    std::unordered_map<sstring, std::unordered_set<inet_address>> ret;
    ret.reserve(_inventory_map.size());
    for (const auto& [dc, dc_inventory] : _inventory_map) {
        ret.emplace(dc->name, boost::copy_range<std::unordered_set<inet_address>>(dc_inventory.nodes | boost::adaptors::transformed([] (const node& node) {
            return node.endpoint();
        })));
    }
    return ret;
}

std::unordered_map<sstring, std::unordered_set<const node*>> topology::get_datacenter_nodes() const {
    std::unordered_map<sstring, std::unordered_set<const node*>> ret;
    ret.reserve(_inventory_map.size());
    for (const auto& [dc, dc_inventory] : _inventory_map) {
        ret.emplace(dc->name, boost::copy_range<std::unordered_set<const node*>>(dc_inventory.nodes | boost::adaptors::transformed([] (const node& node) {
            return &node;
        })));
    }
    return ret;
}

std::unordered_map<sstring, std::unordered_map<sstring, std::unordered_set<inet_address>>> topology::get_datacenter_racks() const {
    std::unordered_map<sstring, std::unordered_map<sstring, std::unordered_set<inet_address>>> ret;
    for (const auto& [dc, dc_inventory] : _inventory_map) {
        std::unordered_map<sstring, std::unordered_set<inet_address>> racks;
        for (const auto& node : dc_inventory.nodes) {
            racks[node.rack()->name].insert(node.endpoint());
        }
        ret.emplace(dc->name, std::move(racks));
    }
    return ret;
}

std::unordered_set<sstring> topology::get_datacenter_names() const noexcept {
    return boost::copy_range<std::unordered_set<sstring>>(_inventory_map | boost::adaptors::map_keys | boost::adaptors::transformed([] (const datacenter* dc) {
        return dc->name;
    }));
}

endpoint_dc_rack topology::get_location(const inet_address& ep) const {
    if (auto node = find_node(ep)) {
        return node->dc_rack();
    }
    // We should do the following check after lookup in nodes.
    // In tests, there may be no config for local node, so fall back to get_location()
    // only if no mapping is found. Otherwise, get_location() will return empty location
    // from config or random node, neither of which is correct.
    if (ep == _cfg.this_endpoint) {
        return get_location();
    }
    // FIXME -- this shouldn't happen. After topology is stable and is
    // correctly populated with endpoints, this should be replaced with
    // on_internal_error()
    tlogger.warn("Requested location for node {} not in topology. backtrace {}", ep, lazy_backtrace());
    return endpoint_dc_rack::default_location;
}

void topology::sort_by_proximity(inet_address address, inet_address_vector_replica_set& addresses) const {
    if (_sort_by_proximity) {
        std::sort(addresses.begin(), addresses.end(), [this, &address](inet_address& a1, inet_address& a2) {
            return compare_endpoints(address, a1, a2) < 0;
        });
    }
}

std::weak_ordering topology::compare_endpoints(const inet_address& address, const inet_address& a1, const inet_address& a2) const {
    const auto loc = get_location(address);
    const auto loc1 = get_location(a1);
    const auto loc2 = get_location(a2);

    // The farthest nodes from a given node are:
    // 1. Nodes in other DCs then the reference node
    // 2. Nodes in the other RACKs in the same DC as the reference node
    // 3. Other nodes in the same DC/RACk as the reference node
    int same_dc1 = loc1.dc == loc.dc;
    int same_rack1 = same_dc1 & (loc1.rack == loc.rack);
    int same_node1 = a1 == address;
    int d1 = ((same_dc1 << 2) | (same_rack1 << 1) | same_node1) ^ 7;

    int same_dc2 = loc2.dc == loc.dc;
    int same_rack2 = same_dc2 & (loc2.rack == loc.rack);
    int same_node2 = a2 == address;
    int d2 = ((same_dc2 << 2) | (same_rack2 << 1) | same_node2) ^ 7;

    return d1 <=> d2;
}

void topology::for_each_node(std::function<void(const node*)> func) const {
    for (const auto& np : _nodes) {
        if (np) {
            func(np.get());
        }
    }
}

const datacenter* topology_registry::find_datacenter(sstring_view name) const noexcept {
    if (auto it = _datacenters.find(name); it != _datacenters.end()) {
        return it->second.get();
    }
    return nullptr;
}

location topology_registry::find_location(sstring_view dc_name, sstring_view rack_name) const noexcept {
    if (const auto* dc = find_datacenter(dc_name)) {
        if (auto rack_it = dc->racks.find(rack_name); rack_it != dc->racks.end()) {
            return location{ .dc = dc, .rack = rack_it->second.get() };
        }
    }
    return location{};
}

location topology_registry::find_or_create_location(sstring_view dc_name, sstring_view rack_name) {
    auto dc_it = _datacenters.find(dc_name);
    if (dc_it == _datacenters.end()) {
        if (this_shard_id() != 0) {
            on_internal_error(tlogger, "datacenters and racks can be created only on shard 0");
        }
        auto dc_ptr = std::make_unique<locator::datacenter>(dc_name);
        dc_it = _datacenters.emplace(dc_ptr->name, std::move(dc_ptr)).first;
    }
    auto* dc = dc_it->second.get();

    auto rack_it = dc->racks.find(rack_name);
    if (rack_it == dc->racks.end()) {
        if (this_shard_id() != 0) {
            on_internal_error(tlogger, "datacenters and racks can be created only on shard 0");
        }
        auto rack_ptr = std::make_unique<rack>(rack_name);
        rack_it = const_cast<datacenter*>(dc)->racks.emplace(rack_ptr->name, std::move(rack_ptr)).first;
    }
    auto* rack = rack_it->second.get();

    return {dc, rack};
}

endpoint_dc_rack location::get_dc_rack() const {
    return endpoint_dc_rack{
        .dc = dc ? dc->name : endpoint_dc_rack::default_location.dc,
        .rack = rack ? rack->name : endpoint_dc_rack::default_location.rack
    };
}

} // namespace locator

namespace std {

std::ostream& operator<<(std::ostream& out, const locator::topology& t) {
    out << "{this_endpoint: " << t._cfg.this_endpoint
        << ", dc: " << t._cfg.local_dc_rack.dc
        << ", rack: " << t._cfg.local_dc_rack.rack
        << ", nodes:\n";
    for (auto&& node : t._nodes) {
        out << "  " << fmt::format("{}", node_printer(node.get())) << "\n";
    }
    return out << "}";
}

std::ostream& operator<<(std::ostream& out, const locator::node& node) {
    fmt::print(out, "{}", node);
    return out;
}

std::ostream& operator<<(std::ostream& out, const locator::node::state& state) {
    fmt::print(out, "{}", state);
    return out;
}

} // namespace std
