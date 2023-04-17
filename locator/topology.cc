/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/core/on_internal_error.hh>

#include "log.hh"
#include "locator/topology.hh"
#include "locator/production_snitch_base.hh"
#include "utils/stall_free.hh"
#include "utils/fb_utilities.hh"

namespace locator {

static logging::logger tlogger("topology");

thread_local const endpoint_dc_rack endpoint_dc_rack::default_location = {
    .dc = locator::production_snitch_base::default_dc,
    .rack = locator::production_snitch_base::default_rack,
};

node::node(const locator::topology* topology, locator::host_id id, inet_address endpoint, endpoint_dc_rack dc_rack, state state, this_node is_this_node, node::idx_type idx)
    : _topology(topology)
    , _host_id(id)
    , _endpoint(endpoint)
    , _dc_rack(std::move(dc_rack))
    , _state(state)
    , _is_this_node(is_this_node)
    , _idx(idx)
{}

node_holder node::make(const locator::topology* topology, locator::host_id id, inet_address endpoint, endpoint_dc_rack dc_rack, state state, node::this_node is_this_node, node::idx_type idx) {
    return std::make_unique<node>(topology, std::move(id), std::move(endpoint), std::move(dc_rack), std::move(state), is_this_node, idx);
}

node_holder node::clone() const {
    return make(nullptr, host_id(), endpoint(), dc_rack(), get_state(), is_this_node());
}

std::string node::to_string(node::state s) {
    switch (s) {
    case state::none:       return "none";
    case state::joining:    return "joining";
    case state::normal:     return "normal";
    case state::leaving:    return "leaving";
    case state::left:       return "left";
    }
    __builtin_unreachable();
}

future<> topology::clear_gently() noexcept {
    co_await utils::clear_gently(_dc_endpoints);
    co_await utils::clear_gently(_dc_racks);
    _datacenters.clear();
    _dc_rack_nodes.clear();
    _dc_nodes.clear();
    _nodes_by_endpoint.clear();
    _nodes_by_host_id.clear();
    co_await utils::clear_gently(_nodes);
}

topology::topology() noexcept
        : _shard(this_shard_id())
{
    tlogger.trace("topology[{}]: default-constructed", fmt::ptr(this));
}

topology::topology(config cfg)
        : _shard(this_shard_id())
        , _sort_by_proximity(!cfg.disable_proximity_sorting)
{
    tlogger.trace("topology[{}]: constructing using config: host_id={} endpoint={} dc={} rack={}", fmt::ptr(this),
            cfg.this_host_id, cfg.this_endpoint, cfg.local_dc_rack.dc, cfg.local_dc_rack.rack);
    if (cfg.this_host_id || cfg.this_endpoint != inet_address{}) {
        add_node(node::make(this, cfg.this_host_id, cfg.this_endpoint, cfg.local_dc_rack, node::state::joining, node::this_node::yes));
    }
}

topology::topology(topology&& o) noexcept
    : _shard(o._shard)
    , _nodes(std::move(o._nodes))
    , _nodes_by_host_id(std::move(o._nodes_by_host_id))
    , _nodes_by_endpoint(std::move(o._nodes_by_endpoint))
    , _dc_nodes(std::move(o._dc_nodes))
    , _dc_rack_nodes(std::move(o._dc_rack_nodes))
    , _dc_endpoints(std::move(o._dc_endpoints))
    , _dc_racks(std::move(o._dc_racks))
    , _sort_by_proximity(o._sort_by_proximity)
    , _datacenters(std::move(o._datacenters))
{
    assert(_shard == this_shard_id());
    tlogger.trace("topology[{}]: move from [{}]", fmt::ptr(this), fmt::ptr(&o));

    for (auto& n : _nodes) {
        if (n) {
            n->set_topology(this);
        }
    }
}

future<topology> topology::clone_gently() const {
    topology ret;
    tlogger.debug("topology[{}]: clone_gently to {} from shard {}", fmt::ptr(this), fmt::ptr(&ret), _shard);
    for (const auto& nptr : _nodes) {
        if (nptr) {
            ret.add_node(nptr->clone());
        }
        co_await coroutine::maybe_yield();
    }
    ret._sort_by_proximity = _sort_by_proximity;
    co_return ret;
}

std::string topology::debug_format(const node* node) {
    if (!node) {
        return format("node={}", fmt::ptr(node));
    }
    return format("node={} idx={} host_id={} endpoint={} dc={} rack={} state={} this_node={}", fmt::ptr(node),
            node->idx(), node->host_id(), node->endpoint(), node->dc_rack().dc, node->dc_rack().rack, node::to_string(node->get_state()), bool(node->is_this_node()));
}

const node* topology::add_node(host_id id, const inet_address& ep, const endpoint_dc_rack& dr, node::state state) {
    if (dr.dc.empty() || dr.rack.empty()) {
        on_internal_error(tlogger, "Node must have valid dc and rack");
    }
    // OK to add a different node with the same ip address.
    if (utils::fb_utilities::is_me(ep) && this_node() && this_node()->host_id() == id) {
        on_internal_error(tlogger, format("topology[{}]: local node already set: host_id={} endpoint={} dc={} rack={}: currently mapped to {}", fmt::ptr(this),
                id, ep, dr.dc, dr.rack, debug_format(this_node())));
    }
    return add_node(node::make(this, id, ep, dr, state));
}

const node* topology::add_node(node_holder nptr) {
    const node* node = nptr.get();

    if (nptr->topology() != this) {
        if (nptr->topology()) {
            on_fatal_internal_error(tlogger, format("topology[{}]: {} belongs to different topology={}", fmt::ptr(this), debug_format(node), fmt::ptr(node->topology())));
        }
        nptr->set_topology(this);
    }

    if (node->is_this_node() && this_node()) {
        on_internal_error(tlogger, format("topology[{}]: {}: local node already mapped to {}", fmt::ptr(this), debug_format(node), debug_format(this_node())));
    }

    if (node->idx() > 0) {
        on_internal_error(tlogger, format("topology[{}]: {}: has assigned idx", fmt::ptr(this), debug_format(node)));
    }

    // Note that _nodes contains also the this_node()
    nptr->set_idx(_nodes.size());
    _nodes.emplace_back(std::move(nptr));

    tlogger.debug("topology[{}]: add_node: {}, at {}", fmt::ptr(this), debug_format(node), current_backtrace());

    try {
        index_node(node);
    } catch (...) {
        pop_node(make_mutable(node));
        throw;
    }
    return node;
}

const node* topology::update_node(node* node, std::optional<host_id> opt_id, std::optional<inet_address> opt_ep, std::optional<endpoint_dc_rack> opt_dr, std::optional<node::state> opt_st) {
    tlogger.debug("topology[{}]: update_node: {}: to: host_id={} endpoint={} dc={} rack={} state={}, at {}", fmt::ptr(this), debug_format(node),
            opt_id ? format("{}", *opt_id) : "unchanged",
            opt_ep ? format("{}", *opt_ep) : "unchanged",
            opt_dr ? format("{}", opt_dr->dc) : "unchanged",
            opt_dr ? format("{}", opt_dr->rack) : "unchanged",
            opt_st ? format("{}", *opt_st) : "unchanged",
            current_backtrace());

    bool changed = false;
    if (opt_id) {
        if (*opt_id != node->host_id()) {
            if (!*opt_id) {
                on_internal_error(tlogger, format("Updating node host_id to null is disallowed: {}: new host_id={}", debug_format(node), *opt_id));
            }
            if (node->is_this_node() && node->host_id()) {
                on_internal_error(tlogger, format("This node host_id is alredy set: {}: new host_id={}", debug_format(node), *opt_id));
            }
            if (_nodes_by_host_id.contains(*opt_id)) {
                on_internal_error(tlogger, format("Cannot update node host_id: {}: new host_id already exists: {}", debug_format(node), debug_format(_nodes_by_host_id[*opt_id])));
            }
            changed = true;
        } else {
            opt_id.reset();
        }
    }
    if (opt_ep) {
        if (*opt_ep != node->endpoint()) {
            if (*opt_ep == inet_address{}) {
                on_internal_error(tlogger, format("Updating node endpoint to null is disallowed: {}: new endpoint={}", debug_format(node), *opt_ep));
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
        } else {
            opt_dr.reset();
        }
    }
    if (opt_st) {
        changed |= node->get_state() != *opt_st;
    }

    if (!changed) {
        return node;
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
        if (opt_dr) {
            mutable_node->_dc_rack = std::move(*opt_dr);
        }
        if (opt_st) {
            mutable_node->set_state(*opt_st);
        }
    } catch (...) {
        std::terminate();
    }
    index_node(node);
    return node;
}

bool topology::remove_node(host_id id) {
    auto node = find_node(id);
    tlogger.debug("topology[{}]: remove_node: host_id={}: {}", fmt::ptr(this), id, debug_format(node));
    if (node) {
        remove_node(node);
        return true;
    }
    return false;
}

void topology::remove_node(const node* node) {
    // never delete this node
    if (node->is_this_node()) {
        unindex_node(node);
    } else {
        pop_node(node);
    }
}

void topology::index_node(const node* node) {
    tlogger.trace("topology[{}]: index_node: {}, at {}", fmt::ptr(this), debug_format(node), current_backtrace());

    if (node->idx() < 0) {
        on_internal_error(tlogger, format("topology[{}]: {}: must already have a valid idx", fmt::ptr(this), debug_format(node)));
    }

    // FIXME: for now we allow adding nodes with null host_id, for the following cases:
    // 1. This node might be added with no host_id on pristine nodes.
    // 2. Other nodes may be introduced via gossip with their endpoint only first
    //    and their host_id is updated later on.
    if (node->host_id()) {
        auto [nit, inserted_host_id] = _nodes_by_host_id.emplace(node->host_id(), node);
        if (!inserted_host_id) {
            on_internal_error(tlogger, format("topology[{}]: {}: node already exists", fmt::ptr(this), debug_format(node)));
        }
    }
    if (node->endpoint() != inet_address{}) {
        auto eit = _nodes_by_endpoint.find(node->endpoint());
        if (eit != _nodes_by_endpoint.end()) {
            if (eit->second->get_state() == node::state::leaving || eit->second->get_state() == node::state::left) {
                _nodes_by_endpoint.erase(node->endpoint());
            } else if (node->get_state() != node::state::leaving && node->get_state() != node::state::left) {
                if (node->host_id()) {
                    _nodes_by_host_id.erase(node->host_id());
                }
                on_internal_error(tlogger, format("topology[{}]: {}: node endpoint already mapped to {}", fmt::ptr(this), debug_format(node), debug_format(eit->second)));
            }
        }
        if (node->get_state() != node::state::left) {
            _nodes_by_endpoint.try_emplace(node->endpoint(), node);
        }
    }

    const auto& dc = node->dc_rack().dc;
    const auto& rack = node->dc_rack().rack;
    const auto& endpoint = node->endpoint();
    _dc_nodes[dc].emplace(node);
    _dc_rack_nodes[dc][rack].emplace(node);
    _dc_endpoints[dc].insert(endpoint);
    _dc_racks[dc][rack].insert(endpoint);
    _datacenters.insert(dc);
}

void topology::unindex_node(const node* node) {
    tlogger.trace("topology[{}]: unindex_node: {}, at {}", fmt::ptr(this), debug_format(node), current_backtrace());

    const auto& dc = node->dc_rack().dc;
    const auto& rack = node->dc_rack().rack;
    if (_dc_nodes.contains(dc)) {
        bool found = _dc_nodes.at(dc).erase(node);
        if (found) {
            if (auto dit = _dc_endpoints.find(dc); dit != _dc_endpoints.end()) {
                const auto& ep = node->endpoint();
                auto& eps = dit->second;
                eps.erase(ep);
                if (eps.empty()) {
                    _dc_rack_nodes.erase(dc);
                    _dc_racks.erase(dc);
                    _dc_endpoints.erase(dit);
                } else {
                    _dc_rack_nodes[dc][rack].erase(node);
                    auto& racks = _dc_racks[dc];
                    if (auto rit = racks.find(rack); rit != racks.end()) {
                        eps = rit->second;
                        eps.erase(ep);
                        if (eps.empty()) {
                            racks.erase(rit);
                        }
                    }
                }
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
}

node_holder topology::pop_node(const node* node) {
    tlogger.trace("topology[{}]: pop_node: {}, at {}", fmt::ptr(this), debug_format(node), current_backtrace());

    unindex_node(node);

    // this node?
    if (node->idx() == 0) {
        on_internal_error(tlogger, format("topology[{}]: {}: cannot pop this_node", fmt::ptr(this), debug_format(node)));
    }

    auto nh = std::exchange(_nodes[node->idx()], {});

    // shrink _nodes if the last node is popped
    // like when failing to index a newly added node
    if (node->idx() == _nodes.size() - 1) {
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
    if (idx >= _nodes.size()) {
        return nullptr;
    }
    return _nodes.at(idx).get();
}

const node* topology::add_or_update_endpoint(inet_address ep, std::optional<host_id> opt_id, std::optional<endpoint_dc_rack> opt_dr, std::optional<node::state> opt_st)
{
    tlogger.trace("topology[{}]: add_or_update_endpoint: ep={} host_id={} dc={} rack={} state={}, at {}", fmt::ptr(this),
            ep, opt_id.value_or(host_id::create_null_id()), opt_dr.value_or(endpoint_dc_rack{}).dc, opt_dr.value_or(endpoint_dc_rack{}).rack, opt_st.value_or(node::state::none),
            current_backtrace());
    auto n = find_node(ep);
    if (n) {
        return update_node(make_mutable(n), opt_id, std::nullopt, std::move(opt_dr), std::move(opt_st));
    } else if (opt_id && (n = find_node(*opt_id))) {
        return update_node(make_mutable(n), std::nullopt, ep, std::move(opt_dr), std::move(opt_st));
    } else {
        return add_node(opt_id.value_or(host_id::create_null_id()), ep, opt_dr.value_or(endpoint_dc_rack::default_location), opt_st.value_or(node::state::normal));
    }
}

bool topology::remove_endpoint(inet_address ep)
{
    auto node = find_node(ep);
    tlogger.debug("topology[{}]: remove_endpoint: endpoint={}: {}", fmt::ptr(this), ep, debug_format(node));
    if (node) {
        remove_node(node);
        return true;
    }
    return false;
}

bool topology::has_node(host_id id) const noexcept {
    auto node = find_node(id);
    tlogger.trace("topology[{}]: has_node: host_id={}: {}", fmt::ptr(this), id, debug_format(node));
    return bool(node);
}

bool topology::has_node(inet_address ep) const noexcept {
    auto node = find_node(ep);
    tlogger.trace("topology[{}]: has_node: endpoint={}: node={}", fmt::ptr(this), ep, debug_format(node));
    return bool(node);
}

bool topology::has_endpoint(inet_address ep) const
{
    return has_node(ep);
}

const endpoint_dc_rack& topology::get_location(const inet_address& ep) const {
    if (ep == utils::fb_utilities::get_broadcast_address()) {
        return get_location();
    }
    if (auto node = find_node(ep)) {
        return node->dc_rack();
    }
    // FIXME -- this shouldn't happen. After topology is stable and is
    // correctly populated with endpoints, this should be replaced with
    // on_internal_error()
    tlogger.warn("Requested location for node {} not in topology. backtrace {}", ep, current_backtrace());
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
    const auto& loc = get_location(address);
    const auto& loc1 = get_location(a1);
    const auto& loc2 = get_location(a2);

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

} // namespace locator

namespace std {

std::ostream& operator<<(std::ostream& out, const locator::node& node) {
    fmt::print(out, "{}", node);
    return out;
}

std::ostream& operator<<(std::ostream& out, const locator::node::state& state) {
    fmt::print(out, "{}", state);
    return out;
}

} // namespace std
