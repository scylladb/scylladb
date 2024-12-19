/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <unordered_map>
#include <vector>

#include <seastar/core/sstring.hh>

#include "locator/topology.hh"
#include "inet_address_vectors.hh"

namespace tests {

void generate_topology(locator::topology& topo, const std::unordered_map<sstring, size_t>& datacenters, const host_id_vector_replica_set& nodes);

} // namespace tests
