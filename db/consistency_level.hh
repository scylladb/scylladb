/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "db/consistency_level_type.hh"
#include "db/read_repair_decision.hh"
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

size_t quorum_for(const locator::effective_replication_map& erm);

size_t local_quorum_for(const locator::effective_replication_map& erm, const sstring& dc);

size_t block_for_local_serial(const locator::effective_replication_map& erm);

size_t block_for_each_quorum(const locator::effective_replication_map& erm);

size_t block_for(const locator::effective_replication_map& erm, consistency_level cl);

bool is_datacenter_local(consistency_level l);

host_id_vector_replica_set
filter_for_query(consistency_level cl,
                 const locator::effective_replication_map& erm,
                 host_id_vector_replica_set live_endpoints,
                 const host_id_vector_replica_set& preferred_endpoints,
                 read_repair_decision read_repair,
                 const gms::gossiper& g,
                 std::optional<locator::host_id>* extra,
                 replica::column_family* cf);

struct dc_node_count {
    size_t live = 0;
    size_t pending = 0;
};

bool
is_sufficient_live_nodes(consistency_level cl,
                         const locator::effective_replication_map& erm,
                         const host_id_vector_replica_set& live_endpoints);

void assure_sufficient_live_nodes(
        consistency_level cl,
        const locator::effective_replication_map& erm,
        const host_id_vector_replica_set& live_endpoints,
        const host_id_vector_topology_change& pending_endpoints = host_id_vector_topology_change());
}
