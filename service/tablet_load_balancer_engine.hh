/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <array>
#include <cstdint>
#include <vector>

#include <seastar/core/future.hh>
#include <seastar/util/noncopyable_function.hh>

#include "locator/tablets.hh"
#include "schema/schema_fwd.hh"
#include "utils/small_vector.hh"

// The "ranked" and "pressure" tablet balancing engines.
//
// They replace the inter-node and intra-node balancing phases of
// load_balancer::make_plan() when no node is being drained. See
// tablet_load_balancer_engine.cc for the objective.
//
// The engine does not know about token metadata. The caller describes the nodes in
// scope (a DC, or a rack of a DC), their shards, the per-table tablet counts and the
// movable candidates, and gets back a list of moves which it turns into migrations.

namespace service::sblb {

enum class comparator {
    ranked,   // the three measures compared lexicographically
    pressure, // a weighted sum of the three measures with a per-migration threshold
};

struct engine_config {
    comparator cmp = comparator::ranked;

    // Shared by both comparators.
    double u_high = 0.90;      // disk safety threshold of measure 0 (ranked)
    double critical = 0.98;    // hard filter: no destination node may end above it
    double disk_high = 0.05;   // node-disk trigger: start (the tolerance)
    // Node-disk trigger: stop, and the band of measure 2, as a fraction of disk_high.
    double disk_low_ratio = 0.4;
    double inter_factor = 4.0; // an inter-node byte costs this many intra-node bytes in the tie-break
    size_t k_dst = 4;          // shortlist length of the per-table generators
    size_t k_disk = 6;         // shortlist length of the pair and totals generators
    double tot_weight = 0.2;   // a tablet of total excess counts this much of a table-cell tablet

    // Pressure comparator.
    double gamma = 0.02;
    std::array<double, 3> weights = {1.0, 1.0, 1.0};
    double u_safe = 0.85;
    double kappa = 25.0;
    double u_knee = 0.97;
    double node_unit = 0.02;

    // Streaming limits per shard, as in load_balancer::can_accept_load().
    size_t max_read_streaming_load = 2;
    size_t max_write_streaming_load = 2;
};

// State which survives between plan-making rounds, one per scope (DC or DC/rack).
// Owned by the tablet allocator, reset on leadership loss.
struct persistent_state {
    bool disk_trigger_active = false; // the node-disk Schmitt trigger
};

// A movable unit on one shard: a tablet replica, or the co-located replicas of two
// sibling tablets which must move together.
struct candidate {
    uint32_t handle;  // opaque to the engine, returned in planned_move
    uint64_t size;    // bytes moved
    uint32_t count;   // tablets in it (1 or 2)
    utils::small_vector<locator::global_tablet_id, 2> tablets;
};

struct table_candidates {
    table_id table;
    // The table's tablets may move to another node, but not to another shard of their
    // node (strongly consistent tables).
    bool no_intranode_moves = false;
    std::vector<candidate> candidates;
};

struct shard_input {
    uint64_t used = 0;
    uint64_t tablet_count = 0;
    std::vector<std::pair<table_id, uint64_t>> tablet_count_per_table;
    std::vector<table_candidates> candidates;
    size_t streaming_read_load = 0;
    size_t streaming_write_load = 0;
};

struct node_input {
    locator::host_id id;
    uint64_t capacity = 0;
    uint64_t used = 0;
    std::vector<shard_input> shards; // indexed by shard id
};

struct planned_move {
    uint32_t handle;
    locator::tablet_replica src;
    locator::tablet_replica dst;
};

// Returns true if moving the candidate with the given handle from src to dst does not
// violate the replication constraints (no two replicas of a tablet on one node, no
// rack overload). Only called for moves between different nodes.
using constraint_check = noncopyable_function<bool(uint32_t handle, locator::tablet_replica src, locator::tablet_replica dst)>;

// Plans at most one batch of moves (the largest shard count of a node in scope).
future<std::vector<planned_move>> make_plan(const engine_config& cfg, persistent_state& state,
        std::vector<node_input> nodes, constraint_check constraints);

} // namespace service::sblb
