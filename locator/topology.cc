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

future<> topology::clear_gently() noexcept {
    co_await utils::clear_gently(_dc_endpoints);
    co_await utils::clear_gently(_dc_racks);
    co_await utils::clear_gently(_current_locations);
    _datacenters.clear();
    co_return;
}

topology::topology(config cfg)
        : _sort_by_proximity(!cfg.disable_proximity_sorting)
{
    update_endpoint(utils::fb_utilities::get_broadcast_address(), cfg.local_dc_rack);
}

future<topology> topology::clone_gently() const {
    topology ret;
    ret._dc_endpoints.reserve(_dc_endpoints.size());
    for (const auto& p : _dc_endpoints) {
        ret._dc_endpoints.emplace(p);
    }
    co_await coroutine::maybe_yield();
    ret._dc_racks.reserve(_dc_racks.size());
    for (const auto& [dc, rack_endpoints] : _dc_racks) {
        ret._dc_racks[dc].reserve(rack_endpoints.size());
        for (const auto& p : rack_endpoints) {
            ret._dc_racks[dc].emplace(p);
        }
    }
    co_await coroutine::maybe_yield();
    ret._current_locations.reserve(_current_locations.size());
    for (const auto& p : _current_locations) {
        ret._current_locations.emplace(p);
    }
    co_await coroutine::maybe_yield();
    ret._datacenters = _datacenters;
    ret._sort_by_proximity = _sort_by_proximity;
    co_return ret;
}

void topology::update_endpoint(const inet_address& ep, endpoint_dc_rack dr)
{
    auto current = _current_locations.find(ep);

    if (current != _current_locations.end()) {
        if (current->second.dc == dr.dc && current->second.rack == dr.rack) {
            return;
        }
        remove_endpoint(ep);
    }

    tlogger.debug("update_endpoint: {} {}/{}", ep, dr.dc, dr.rack);
    _dc_endpoints[dr.dc].insert(ep);
    _dc_racks[dr.dc][dr.rack].insert(ep);
    _datacenters.insert(dr.dc);
    _current_locations[ep] = std::move(dr);
}

void topology::remove_endpoint(inet_address ep)
{
    auto cur_dc_rack = _current_locations.find(ep);

    if (cur_dc_rack == _current_locations.end()) {
        return;
    }

    const auto& dc = cur_dc_rack->second.dc;
    const auto& rack = cur_dc_rack->second.rack;
    tlogger.debug("remove_endpoint: {} {}/{}", ep, dc, rack);
    if (auto dit = _dc_endpoints.find(dc); dit != _dc_endpoints.end()) {
        auto& eps = dit->second;
        eps.erase(ep);
        if (eps.empty()) {
            _dc_endpoints.erase(dit);
            _datacenters.erase(dc);
            _dc_racks.erase(dc);
        } else {
            auto& racks = _dc_racks[dc];
            if (auto rit = racks.find(rack); rit != racks.end()) {
                auto& rack_eps = rit->second;
                rack_eps.erase(ep);
                if (rack_eps.empty()) {
                    racks.erase(rit);
                }
            }
        }
    }

    // Keep the local endpoint around
    // Just unlist it from _dc_endpoints and _dc_racks
    // This is needed after it is decommissioned
    if (ep != utils::fb_utilities::get_broadcast_address()) {
        _current_locations.erase(cur_dc_rack);
    }
}

bool topology::has_endpoint(inet_address ep) const
{
    return _current_locations.contains(ep);
}

const endpoint_dc_rack& topology::get_location(const inet_address& ep) const {
    if (_current_locations.contains(ep)) {
        return _current_locations.at(ep);
    }

    // FIXME -- this shouldn't happen. After topology is stable and is
    // correctly populated with endpoints, this should be replaced with
    // on_internal_error()
    static thread_local endpoint_dc_rack default_location = {
        .dc = locator::production_snitch_base::default_dc,
        .rack = locator::production_snitch_base::default_rack,
    };

    tlogger.warn("Requested location for node {} not in topology. backtrace {}", ep, current_backtrace());
    return default_location;
}

// FIXME -- both methods below should rather return data from the
// get_location() result, but to make it work two things are to be fixed:
// - topology should be aware of internal-ip conversions
// - topology should be pre-populated with data loaded from system ks

sstring topology::get_rack() const {
    return get_rack(utils::fb_utilities::get_broadcast_address());
}

sstring topology::get_rack(inet_address ep) const {
    return get_location(ep).rack;
}

sstring topology::get_datacenter() const {
    return get_datacenter(utils::fb_utilities::get_broadcast_address());
}

sstring topology::get_datacenter(inet_address ep) const {
    return get_location(ep).dc;
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

} // namespace locator
