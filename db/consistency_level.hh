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
#include "db/read_repair_decision.hh"
#include "inet_address_vectors.hh"
#include "locator/token_replication_factor.hh"
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

// Whether the consistency level needs the per-datacenter counts of token_replication_factor:
// LOCAL_ONE, LOCAL_QUORUM, LOCAL_SERIAL and EACH_QUORUM.
locator::with_datacenters needs_datacenter_counts(consistency_level cl);

// The functions below size a request from the replication factor of the replica set it was
// sent to: the tablet's read set for reads, its write set for writes. The two differ in the
// write_both_read_new stage of a migration.
size_t quorum_for(const locator::token_replication_factor& rf);

size_t local_quorum_for(const locator::token_replication_factor& rf);

size_t block_for_each_quorum(const locator::effective_replication_map& erm, const locator::token_replication_factor& rf);

// EACH_QUORUM quota of one datacenter, from the tablet's replicas there. A datacenter the schema
// does not list, or lists at RF 0, gets no quota even if a tablet holds a replica there, so the
// quotas add up to block_for_each_quorum(). Requires NetworkTopologyStrategy.
size_t each_quorum_block_for_dc(const locator::effective_replication_map& erm, const locator::token_replication_factor& rf, const sstring& dc);

size_t block_for(const locator::effective_replication_map& erm, consistency_level cl, const locator::token_replication_factor& rf);

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
                 const locator::token_replication_factor& rf);

struct dc_node_count {
    size_t live = 0;
    size_t pending = 0;
};

bool
is_sufficient_live_nodes(consistency_level cl,
                         const locator::effective_replication_map& erm,
                         const host_id_vector_replica_set& live_endpoints,
                         const locator::token_replication_factor& rf);

void assure_sufficient_live_nodes(
        consistency_level cl,
        const locator::effective_replication_map& erm,
        const host_id_vector_replica_set& live_endpoints,
        const locator::token_replication_factor& rf,
        const host_id_vector_topology_change& pending_endpoints = host_id_vector_topology_change());
}
