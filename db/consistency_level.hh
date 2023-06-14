/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "db/consistency_level_type.hh"
#include "db/read_repair_decision.hh"
#include "exceptions/exceptions.hh"
#include "gms/inet_address.hh"
#include "inet_address_vectors.hh"
#include "log.hh"
#include "replica/database_fwd.hh"

#include <iosfwd>
#include <vector>
#include <unordered_set>

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

inet_address_vector_replica_set
filter_for_query(consistency_level cl,
                 const locator::effective_replication_map& erm,
                 inet_address_vector_replica_set live_endpoints,
                 const inet_address_vector_replica_set& preferred_endpoints,
                 read_repair_decision read_repair,
                 const gms::gossiper& g,
                 std::optional<gms::inet_address>* extra,
                 replica::column_family* cf);

struct dc_node_count {
    size_t live = 0;
    size_t pending = 0;
};

bool
is_sufficient_live_nodes(consistency_level cl,
                         const locator::effective_replication_map& erm,
                         const inet_address_vector_replica_set& live_endpoints);

template<typename Range, typename PendingRange = std::array<gms::inet_address, 0>>
void assure_sufficient_live_nodes(
        consistency_level cl,
        const locator::effective_replication_map& erm,
        const Range& live_endpoints,
        const PendingRange& pending_endpoints = std::array<gms::inet_address, 0>());

extern template void assure_sufficient_live_nodes(consistency_level, const locator::effective_replication_map&, const inet_address_vector_replica_set&, const std::array<gms::inet_address, 0>&);
extern template void assure_sufficient_live_nodes(db::consistency_level, const locator::effective_replication_map&, const inet_address_vector_replica_set&, const utils::small_vector<gms::inet_address, 1ul>&);

}
