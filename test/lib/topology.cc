/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "gms/inet_address.hh"

#include "test/lib/topology.hh"
#include "test/lib/random_utils.hh"

using namespace locator;

namespace tests {

void generate_topology(locator::topology& topo, const std::unordered_map<sstring, size_t>& datacenters, const host_id_vector_replica_set& nodes) {
    auto& e1 = seastar::testing::local_random_engine;

    std::unordered_map<sstring, size_t> racks_per_dc;
    std::vector<std::reference_wrapper<const sstring>> dcs;

    dcs.reserve(datacenters.size() * 4);

    using udist = std::uniform_int_distribution<size_t>;

    auto out = std::back_inserter(dcs);

    for (auto& p : datacenters) {
        auto& dc = p.first;
        auto rf = p.second;
        auto rc = udist(0, rf * 3 - 1)(e1) + 1;
        racks_per_dc.emplace(dc, rc);
        out = std::fill_n(out, rf, std::cref(dc));
    }

    unsigned i = 0;
    for (auto& node : nodes) {
        const sstring& dc = dcs[udist(0, dcs.size() - 1)(e1)];
        auto rc = racks_per_dc.at(dc);
        auto r = udist(0, rc)(e1);
        topo.add_or_update_endpoint(node, inet_address((127u << 24) | ++i), endpoint_dc_rack{dc, to_sstring(r)}, locator::node::state::normal);
    }
}

} // namespace tests
