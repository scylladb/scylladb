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
#include "dht/token.hh"
#include "gms/inet_address.hh"
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

size_t quorum_for(const locator::effective_replication_map& erm, dht::token token_id);

size_t local_quorum_for(const locator::effective_replication_map& erm, const sstring& dc, dht::token token_id);

size_t block_for_local_serial(const locator::effective_replication_map& erm, dht::token token_id);

size_t block_for_each_quorum(const locator::effective_replication_map& erm, dht::token token_id);

size_t block_for(const locator::effective_replication_map& erm, consistency_level cl, dht::token token_id);

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
                 dht::token token_id);

struct dc_node_count {
    size_t live = 0;
    size_t pending = 0;
};

bool
is_sufficient_live_nodes(consistency_level cl,
                         const locator::effective_replication_map& erm,
                         const host_id_vector_replica_set& live_endpoints,
                         dht::token token_id);

template<typename Range, typename PendingRange = std::array<gms::inet_address, 0>>
void assure_sufficient_live_nodes(
        consistency_level cl,
        const locator::effective_replication_map& erm,
        const Range& live_endpoints,
        dht::token token_id,
        const PendingRange& pending_endpoints = std::array<gms::inet_address, 0>());

extern template void assure_sufficient_live_nodes(consistency_level, const locator::effective_replication_map&, const inet_address_vector_replica_set&, dht::token token_id, const std::array<gms::inet_address, 0>&);
extern template void assure_sufficient_live_nodes(db::consistency_level, const locator::effective_replication_map&, const inet_address_vector_replica_set&, dht::token token_id, const utils::small_vector<gms::inet_address, 1ul>&);
extern template void assure_sufficient_live_nodes(db::consistency_level, const locator::effective_replication_map&, const host_id_vector_replica_set&, dht::token token_id, const host_id_vector_topology_change&);

}
