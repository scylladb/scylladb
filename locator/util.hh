/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
 
#pragma once

#include "dht/token_range_endpoints.hh"
#include "dht/i_partitioner_fwd.hh"
#include "inet_address_vectors.hh"
#include "locator/abstract_replication_strategy.hh"

namespace replica {
    class database;
}

namespace gms {
    class gossiper;
}

namespace locator {
    future<std::vector<dht::token_range_endpoints>> describe_ring(const replica::database& db, const gms::gossiper& gossiper, const sstring& keyspace, bool include_only_local_dc = false);
    future<std::unordered_map<dht::token_range, host_id_vector_replica_set>> get_range_to_address_map(
        locator::effective_replication_map_ptr erm, const std::vector<token>& sorted_tokens);
}