/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <optional>
#include <vector>
#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include "gms/inet_address.hh"
#include "repair/repair.hh"
#include "repair/hash.hh"
#include "utils/UUID.hh"
#include "dht/i_partitioner.hh"
#include "database.hh"

class repair_meta;

class row_level_repair {
    repair_info& _ri;
    sstring _cf_name;
    utils::UUID _table_id;
    dht::token_range _range;
    std::vector<gms::inet_address> _all_live_peer_nodes;
    column_family& _cf;

    // Repair master and followers will propose a sync boundary. Each of them
    // read N bytes of rows from disk, the row with largest
    // `position_in_partition` value is the proposed sync boundary of that
    // node. The repair master uses `get_sync_boundary` rpc call to
    // get all the proposed sync boundary and stores in in
    // `_sync_boundaries`. The `get_sync_boundary` rpc call also
    // returns the combined hashes and the total size for the rows which are
    // in the `_row_buf`. `_row_buf` buffers the rows read from sstable. It
    // contains rows at most of `_max_row_buf_size` bytes.
    // If all the peers return the same `_sync_boundaries` and
    // `_combined_hashes`, we think the rows are synced.
    // If not, we proceed to the next step.
    std::vector<repair_sync_boundary> _sync_boundaries;
    std::vector<repair_hash> _combined_hashes;

    // `common_sync_boundary` is the boundary all the peers agrees on
    std::optional<repair_sync_boundary> _common_sync_boundary = {};

    // `_skipped_sync_boundary` is used in case we find the range is synced
    // only with the `get_sync_boundary` rpc call. We use it to make
    // sure the remote peers update the `_current_sync_boundary` and
    // `_last_sync_boundary` correctly.
    std::optional<repair_sync_boundary> _skipped_sync_boundary = {};

    // If the total size of the `_row_buf` on either of the nodes is zero,
    // we set this flag, which is an indication that rows are not synced.
    bool _zero_rows = false;

    // Sum of estimated_partitions on all peers
    uint64_t _estimated_partitions = 0;

    // A flag indicates any error during the repair
    bool _failed = false;

    // Seed for the repair row hashing. If we ever had a hash conflict for a row
    // and we are not using stable hash, there is chance we will fix the row in
    // the next repair.
    uint64_t _seed;

public:
    row_level_repair(repair_info& ri,
                     sstring cf_name,
                     utils::UUID table_id,
                     dht::token_range range,
                     std::vector<gms::inet_address> all_live_peer_nodes);

private:
    enum class op_status {
        next_round,
        next_step,
        all_done,
    };

    size_t get_max_row_buf_size(row_level_diff_detect_algorithm algo);

    // Step A: Negotiate sync boundary to use
    op_status negotiate_sync_boundary(repair_meta& master);

    // Step B: Get missing rows from peer nodes so that local node contains all the rows
    op_status get_missing_rows_from_follower_nodes(repair_meta& master);

    // Step C: Send missing rows to the peer nodes
    void send_missing_rows_to_follower_nodes(repair_meta& master);

public:
    future<> run();
};

extern const std::vector<row_level_diff_detect_algorithm>& suportted_diff_detect_algorithms();
