/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <optional>
#include <seastar/util/bool_class.hh>
#include "inet_address_vectors.hh"
#include "utils/small_vector.hh"

namespace locator {

using with_datacenters = seastar::bool_class<struct with_datacenters_tag>;

// Replication factor of one token's replica set, as the coordinator sizes consistency levels.
// Tablets count the tablet's replicas, which differ from the schema during an RF change.
// Vnodes use the schema replication factor.
struct token_replication_factor {
    struct datacenter_counts {
        // In the local datacenter, whether the schema lists it or not.
        size_t local = 0;
        // Indexed like network_topology_strategy::get_datacenters(). Empty for other strategies.
        utils::small_vector<size_t, 4> per_dc;
    };
    size_t total = 0;
    // Engaged when asked for with with_datacenters::yes.
    std::optional<datacenter_counts> dcs;
};

struct replicas_with_rf {
    host_id_vector_replica_set replicas;
    token_replication_factor rf;
};

}
