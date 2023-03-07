/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <unordered_set>
#include <unordered_map>
#include <compare>

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/bool_class.hh>

#include "locator/types.hh"
#include "inet_address_vectors.hh"

using namespace seastar;

namespace locator {

class topology {
public:
    struct config {
        endpoint_dc_rack local_dc_rack;
        bool disable_proximity_sorting = false;
    };
    topology(config cfg);
    topology(topology&&) = default;

    topology& operator=(topology&&) = default;

    future<topology> clone_gently() const;
    future<> clear_gently() noexcept;

    /**
     * Stores current DC/rack assignment for ep
     */
    void update_endpoint(const inet_address& ep, endpoint_dc_rack dr);

    /**
     * Removes current DC/rack assignment for ep
     */
    void remove_endpoint(inet_address ep);

    /**
     * Returns true iff contains given endpoint.
     */
    bool has_endpoint(inet_address) const;

    const std::unordered_map<sstring,
                           std::unordered_set<inet_address>>&
    get_datacenter_endpoints() const {
        return _dc_endpoints;
    }

    const std::unordered_map<sstring,
                       std::unordered_map<sstring,
                                          std::unordered_set<inet_address>>>&
    get_datacenter_racks() const {
        return _dc_racks;
    }

    const std::unordered_set<sstring>& get_datacenters() const noexcept {
        return _datacenters;
    }

    const endpoint_dc_rack& get_location(const inet_address& ep) const;
    sstring get_rack() const;
    sstring get_rack(inet_address ep) const;
    sstring get_datacenter() const;
    sstring get_datacenter(inet_address ep) const;

    auto get_local_dc_filter() const noexcept {
        return [ this, local_dc = get_datacenter() ] (inet_address ep) {
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

private:
    // default constructor for cloning purposes
    topology() = default;

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

    /** multi-map: DC -> endpoints in that DC */
    std::unordered_map<sstring,
                       std::unordered_set<inet_address>>
        _dc_endpoints;

    /** map: DC -> (multi-map: rack -> endpoints in that rack) */
    std::unordered_map<sstring,
                       std::unordered_map<sstring,
                                          std::unordered_set<inet_address>>>
        _dc_racks;

    /** reverse-lookup map: endpoint -> current known dc/rack assignment */
    std::unordered_map<inet_address, endpoint_dc_rack> _current_locations;

    bool _sort_by_proximity = true;

    // pre-calculated
    std::unordered_set<sstring> _datacenters;

    void calculate_datacenters();

public:
    void test_compare_endpoints(const inet_address& address, const inet_address& a1, const inet_address& a2) const;
};

} // namespace locator
