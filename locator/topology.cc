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

node::node(::locator::host_id id, inet_address endpoint, endpoint_dc_rack dc_rack, local is_local)
    : _host_id(id)
    , _endpoint(endpoint)
    , _dc_rack(std::move(dc_rack))
    , _is_local(is_local)
{}

future<> topology::clear_gently() noexcept {
    co_await utils::clear_gently(_dc_endpoints);
    co_await utils::clear_gently(_dc_racks);
    _datacenters.clear();
    _dc_rack_nodes.clear();
    _dc_nodes.clear();
    _nodes_by_endpoint.clear();
    _nodes_by_host_id.clear();
    _local_node = {};
    co_await utils::clear_gently(_all_nodes);
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
            cfg.local_host_id, cfg.local_endpoint, cfg.local_dc_rack.dc, cfg.local_dc_rack.rack);
    if (cfg.local_host_id || cfg.local_endpoint != inet_address{}) {
        add_node(make_lw_shared<node>(cfg.local_host_id, cfg.local_endpoint, cfg.local_dc_rack, node::local::yes));
    }
}

topology::topology(topology&& o) noexcept
    : _shard(o._shard)
    , _all_nodes(std::move(o._all_nodes))
    , _local_node(std::move(o._local_node))
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
}

future<topology> topology::clone_gently() const {
    topology ret;
    if (this_shard_id() == _shard) {
        tlogger.debug("topology[{}]: clone_gently to {} on same shard", fmt::ptr(this), fmt::ptr(&ret));
        ret._all_nodes = _all_nodes;
        co_await coroutine::maybe_yield();
        ret._local_node = _local_node;
        ret._nodes_by_host_id = _nodes_by_host_id;
        ret._nodes_by_endpoint = _nodes_by_endpoint;
        ret._dc_nodes = _dc_nodes;
        ret._dc_rack_nodes = _dc_rack_nodes;
        ret._dc_endpoints = _dc_endpoints;
        ret._dc_racks = _dc_racks;
        ret._datacenters = _datacenters;
    } else {
        tlogger.debug("topology[{}]: clone_gently to {} from shard {}", fmt::ptr(this), fmt::ptr(&ret), _shard);
        for (const auto& n : _all_nodes) {
            ret.add_node(make_lw_shared<node>(*n));
            co_await coroutine::maybe_yield();
        }
        // local node may be detached for _all_nodes
        // if it was decommissioned.
        if (!ret._local_node && _local_node) {
            ret.add_node(make_lw_shared<node>(*_local_node));
        }
    }
    co_await coroutine::maybe_yield();
    ret._sort_by_proximity = _sort_by_proximity;
    co_return ret;
}

node_ptr topology::add_node(host_id id, const inet_address& ep, const endpoint_dc_rack& dr) {
    if (dr.dc.empty() || dr.rack.empty()) {
        on_internal_error(tlogger, "Node must have valid dc and rack");
    }
    auto is_local = node::local(ep == utils::fb_utilities::get_broadcast_address());
    if (is_local && _local_node) {
        if (_local_node->host_id() == id) {
            on_internal_error_noexcept(tlogger, format("Local node already set: host_id={} endpoint={} dc={} rack={}: currently mapped to host_id={} endpoint={}",
                    id, ep, dr.dc, dr.rack,
                    _local_node->host_id(), _local_node->endpoint()));
            return _local_node;
        }
        // Replacing node with the same ip address
        is_local = node::local::no;
    }
    return add_node(make_lw_shared<node>(id, ep, dr, is_local));
}

node_ptr topology::add_node(mutable_node_ptr node) {
    tlogger.debug("topology[{}]: add_node: node={} host_id={} endpoint={} dc={} rack={} local={}, at {}", fmt::ptr(this), fmt::ptr(node.get()),
            node->host_id(), node->endpoint(), node->dc_rack().dc, node->dc_rack().rack, node->is_local(), current_backtrace());
    if (node->is_local() && _local_node && _local_node != node) {
        on_internal_error(tlogger, format("Local node already set: host_id={} endpoint={} dc={} rack={}: currently mapped to host_id={} endpoint={}",
                node->host_id(), node->endpoint(), node->dc_rack().dc, node->dc_rack().rack,
                _local_node->host_id(), _local_node->endpoint()));
    }
    if (_all_nodes.contains(node)) {
        return node;
    }
    try {
        // FIXME: for now we allow adding nodes with null host_id
        if (node->host_id()) {
            auto [nit, inserted_host_id] = _nodes_by_host_id.emplace(node->host_id(), node.get());
            if (!inserted_host_id) {
                on_internal_error(tlogger, format("Node already exists: host_id={} endpoint={} dc={} rack={}",
                        node->host_id(), node->endpoint(), node->dc_rack().dc, node->dc_rack().rack));
            }
        }
        if (node->endpoint() != inet_address{}) {
            auto [eit, inserted_endpoint] = _nodes_by_endpoint.emplace(node->endpoint(), node.get());
            if (!inserted_endpoint) {
                if (node->host_id()) {
                    _nodes_by_host_id.erase(node->host_id());
                }
                on_internal_error(tlogger, format("Node endpoint already mapped: host_id={} endpoint={} dc={} rack={}: currently mapped to host_id={}",
                        node->host_id(), node->endpoint(), node->dc_rack().dc, node->dc_rack().rack,
                        eit->second->host_id()));
            }
        }

        const auto& dc = node->dc_rack().dc;
        const auto& rack = node->dc_rack().rack;
        const auto& endpoint = node->endpoint();
        _dc_nodes[dc].emplace(node.get());
        _dc_rack_nodes[dc][rack].emplace(node.get());
        _dc_endpoints[dc].insert(endpoint);
        _dc_racks[dc][rack].insert(endpoint);
        _datacenters.insert(dc);

        if (node->is_local()) {
            _local_node = node;
        }
        _all_nodes.emplace(node);
    } catch (...) {
        do_remove_node(node);
        throw;
    }
    return node;
}

topology::mutable_node_ptr topology::make_mutable(const node_ptr& nptr) {
    return const_cast<class node*>(nptr.get())->shared_from_this();
}

node_ptr topology::update_node(node_ptr node, std::optional<host_id> opt_id, std::optional<inet_address> opt_ep, std::optional<endpoint_dc_rack> opt_dr) {
    tlogger.debug("topology[{}]: update_node: node={} host_id={} endpoint={} dc={} rack={}, at {}", fmt::ptr(this), fmt::ptr(node.get()),
            opt_id.value_or(host_id::create_null_id()), opt_ep.value_or(inet_address{}), opt_dr.value_or(endpoint_dc_rack{}).dc, opt_dr.value_or(endpoint_dc_rack{}).rack,
            current_backtrace());
    bool changed = false;
    if (opt_id) {
        if (*opt_id != node->host_id()) {
            // FIXME: allow updating host_id for replace node.
            // if (node->host_id()) {
            //    on_internal_error(tlogger, format("Updating non-null node host_id is disallowed: host_id={} endpoint={}: new host_id={}",
            //            node->host_id(), node->endpoint(), *opt_id));
            // }
            if (!*opt_id) {
                on_internal_error(tlogger, format("Updating node host_id to null is disallowed: host_id={} endpoint={}: new host_id={}",
                        node->host_id(), node->endpoint(), *opt_id));
            }
            if (_nodes_by_host_id.contains(*opt_id)) {
                on_internal_error(tlogger, format("Cannot update node host_id: new host_id={} already exists: endpoint={}: ",
                        *opt_id, node->endpoint()));
            }
            changed = true;
        } else {
            opt_id.reset();
        }
    }
    if (opt_ep) {
        if (*opt_ep != node->endpoint()) {
            if (*opt_ep == inet_address{}) {
                on_internal_error(tlogger, format("Updating node endpoint to null is disallowed: host_id={} endpoint={}: new endpoint={}",
                        node->host_id(), node->endpoint(), *opt_ep));
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

    if (!changed) {
        return node;
    }

    auto mutable_node = make_mutable(node);
    do_remove_node(mutable_node);
    if (opt_id) {
        mutable_node->_host_id = *opt_id;
    }
    if (opt_ep) {
        mutable_node->_endpoint = *opt_ep;
    }
    if (opt_dr) {
        mutable_node->_dc_rack = std::move(*opt_dr);
    }
    return add_node(mutable_node);
}

void topology::remove_node(host_id id, must_exist require_exist) {
    if (id == _local_node->host_id()) {
        on_internal_error(tlogger, format("Cannot remove the local node: host_id={} endpoint={}",
                _local_node->host_id(), _local_node->endpoint()));
    }
    tlogger.trace("topology[{}]: remove_node: host_id={}", fmt::ptr(this), id);
    auto it = _nodes_by_host_id.find(id);
    if (it != _nodes_by_host_id.end()) {
        do_remove_node(make_mutable(it->second->shared_from_this()));
    } else if (require_exist) {
        on_internal_error(tlogger, format("Node not found: host_id={}", id));
    }
}

void topology::remove_node(node_ptr node) {
    if (node) {
        do_remove_node(make_mutable(node));
    }
}

void topology::do_remove_node(mutable_node_ptr node) {
    tlogger.debug("remove_node: node={} host_id={} endpoint={}, at {}", fmt::ptr(node.get()), node->host_id(), node->endpoint(), current_backtrace());
 
    const auto& dc = node->dc_rack().dc;
    const auto& rack = node->dc_rack().rack;
    if (auto dit = _dc_endpoints.find(dc); dit != _dc_endpoints.end()) {
        const auto& ep = node->endpoint();
        auto& eps = dit->second;
        eps.erase(ep);
        if (eps.empty()) {
            _dc_racks.erase(dc);
            _dc_endpoints.erase(dit);
        } else {
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
    if (auto dit = _dc_nodes.find(dc); dit != _dc_nodes.end()) {
        auto& nodes = dit->second;
        nodes.erase(node.get());
        if (nodes.empty()) {
            _dc_rack_nodes.erase(dc);
            _datacenters.erase(dc);
            _dc_nodes.erase(dit);
        } else {
            auto& racks = _dc_rack_nodes[dc];
            if (auto rit = racks.find(rack); rit != racks.end()) {
                nodes = rit->second;
                nodes.erase(node.get());
                if (nodes.empty()) {
                    racks.erase(rit);
                }
            }
        }
    }
    _nodes_by_host_id.erase(node->host_id());
    _nodes_by_endpoint.erase(node->endpoint());
    _all_nodes.erase(node);
}

// Finds a node by its host_id
// Returns nullptr if not found
node_ptr topology::find_node(host_id id, must_exist must_exist) const noexcept {
    auto it = _nodes_by_host_id.find(id);
    if (it != _nodes_by_host_id.end()) {
        return it->second->shared_from_this();
    }
    if (must_exist) {
        on_internal_error(tlogger, format("Could not find node: host_id={}", id));
    }
    return nullptr;
}

// Finds a node by its endpoint
// Returns nullptr if not found
node_ptr topology::find_node(const inet_address& ep, must_exist must_exist) const noexcept {
    auto it = _nodes_by_endpoint.find(ep);
    if (it != _nodes_by_endpoint.end()) {
        return it->second->shared_from_this();
    }
    if (must_exist) {
        on_internal_error(tlogger, format("Could not find node: endpoint={}", ep));
    }
    return nullptr;
}

node_ptr topology::update_endpoint(inet_address ep, std::optional<host_id> opt_id, std::optional<endpoint_dc_rack> opt_dr)
{
    tlogger.trace("topology[{}]: update_endpoint: ep={} host_id={} dc={} rack={}, at {}", fmt::ptr(this),
            ep, opt_id.value_or(host_id::create_null_id()), opt_dr.value_or(endpoint_dc_rack{}).dc, opt_dr.value_or(endpoint_dc_rack{}).rack,
            current_backtrace());
    node_ptr n = find_node(ep);
    if (n) {
        return update_node(make_mutable(n), opt_id, std::nullopt, std::move(opt_dr));
    } else if (opt_id && (n = find_node(*opt_id))) {
        return update_node(make_mutable(n), std::nullopt, ep, std::move(opt_dr));
    } else {
        return add_node(opt_id.value_or(host_id::create_null_id()), ep, opt_dr.value_or(endpoint_dc_rack::default_location));
    }
}

void topology::remove_endpoint(inet_address ep)
{
    tlogger.trace("topology[{}]: remove_endpoint: endpoint={}", fmt::ptr(this), ep);
    remove_node(find_node(ep));
}

bool topology::has_node(host_id id) const noexcept {
    auto node = find_node(id);
    tlogger.trace("topology[{}]: has_node: host_id={}: node={}", fmt::ptr(this), id, fmt::ptr(node.get()));
    return bool(node);
}

bool topology::has_node(inet_address ep) const noexcept {
    auto node = find_node(ep);
    tlogger.trace("topology[{}]: has_node: endpoint={}: node={}", fmt::ptr(this), ep, fmt::ptr(node.get()));
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
    if (auto node = find_node(ep, must_exist::no)) {
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

int topology::compare_endpoints(const inet_address& address, const inet_address& a1, const inet_address& a2) const {
    //
    // if one of the Nodes IS the Node we are comparing to and the other one
    // IS NOT - then return the appropriate result.
    //
    if (address == a1 && address != a2) {
        return -1;
    }

    if (address == a2 && address != a1) {
        return 1;
    }

    // ...otherwise perform the similar check in regard to Data Center
    sstring address_datacenter = get_datacenter(address);
    sstring a1_datacenter = get_datacenter(a1);
    sstring a2_datacenter = get_datacenter(a2);

    if (address_datacenter == a1_datacenter &&
        address_datacenter != a2_datacenter) {
        return -1;
    } else if (address_datacenter == a2_datacenter &&
               address_datacenter != a1_datacenter) {
        return 1;
    } else if (address_datacenter == a2_datacenter &&
               address_datacenter == a1_datacenter) {
        //
        // ...otherwise (in case Nodes belong to the same Data Center) check
        // the racks they belong to.
        //
        sstring address_rack = get_rack(address);
        sstring a1_rack = get_rack(a1);
        sstring a2_rack = get_rack(a2);

        if (address_rack == a1_rack && address_rack != a2_rack) {
            return -1;
        }

        if (address_rack == a2_rack && address_rack != a1_rack) {
            return 1;
        }
    }
    //
    // We don't differentiate between Nodes if all Nodes belong to different
    // Data Centers, thus make them equal.
    //
    return 0;
}

} // namespace locator

namespace std {

std::ostream& operator<<(std::ostream& out, const locator::node_ptr& node) {
    return out << node->host_id() << '/' << node->endpoint();
}

} // namespace std
