/*
 * Copyright (C) 2023-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/shared_ptr.hh>
#include <seastar/util/bool_class.hh>

#include "dht/i_partitioner_fwd.hh"

namespace compaction {

class compaction_group_view;
class strategy_control;
struct compaction_state;

using owned_ranges_ptr = seastar::lw_shared_ptr<const dht::token_range_vector>;

// Set for a cleanup driven by a topology change, i.e. one started by sstable_vnodes_cleanup_fiber
// for the whole cluster (including in response to a cluster-wide cleanup_all REST request),
// as opposed to a cleanup requested node-locally via keyspace_cleanup or cleanup_all?global=false.
using is_topology_cleanup = seastar::bool_class<struct is_topology_cleanup_tag>;

} // namespace compaction
