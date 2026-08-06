/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include "db/consistency_level_type.hh"
#include "db/operation_type.hh"
#include "db/read_repair_decision.hh"
#include "dht/token.hh"
#include "inet_address_vectors.hh"
#include "utils/log.hh"
#include "replica/database_fwd.hh"

namespace gms {
class gossiper;
};

namespace locator {
class effective_replication_map;
}

namespace db {

extern logging::logger cl_logger;

// The functions below count the tablet's read replica set for operation_type::read and its
// write replica set for operation_type::write. The two differ while a tablet migrates.
size_t quorum_for(const locator::effective_replication_map& erm, dht::token token, operation_type op);

size_t local_quorum_for(const locator::effective_replication_map& erm, const sstring& dc, dht::token token, operation_type op);

size_t block_for_local_serial(const locator::effective_replication_map& erm, dht::token token, operation_type op);

size_t block_for_each_quorum(const locator::effective_replication_map& erm, dht::token token, operation_type op);

// EACH_QUORUM quota of one datacenter, from the tablet's replicas there. A datacenter the
// schema does not list gets no quota even if a tablet holds a replica there, so the quotas
// add up to block_for_each_quorum(). Requires NetworkTopologyStrategy.
size_t each_quorum_block_for_dc(const locator::effective_replication_map& erm, const sstring& dc, dht::token token, operation_type op);

size_t block_for(const locator::effective_replication_map& erm, consistency_level cl, dht::token token, operation_type op);

bool is_datacenter_local(consistency_level l);

host_id_vector_replica_set
filter_for_query(consistency_level cl,
                 const locator::effective_replication_map& erm,
                 host_id_vector_replica_set live_endpoints,
                 const host_id_vector_replica_set& preferred_endpoints,
                 read_repair_decision read_repair,
                 const gms::gossiper& g,
                 std::optional<locator::host_id>* extra,
                 replica::column_family* cf,
                 dht::token token);

struct dc_node_count {
    size_t live = 0;
    size_t pending = 0;
};

bool
is_sufficient_live_nodes(consistency_level cl,
                         const locator::effective_replication_map& erm,
                         const host_id_vector_replica_set& live_endpoints,
                         dht::token token);

void assure_sufficient_live_nodes(
        consistency_level cl,
        const locator::effective_replication_map& erm,
        const host_id_vector_replica_set& live_endpoints,
        dht::token token,
        operation_type op,
        const host_id_vector_topology_change& pending_endpoints = host_id_vector_topology_change());
}
