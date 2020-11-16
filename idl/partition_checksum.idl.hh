/*
 * Copyright 2016 ScyllaDB
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

enum class repair_checksum : uint8_t {
    legacy = 0,
    streamed = 1,
};

class partition_checksum {
  std::array<uint8_t, 32> digest();
};

class repair_hash {
    uint64_t hash;
};

enum class bound_weight : int8_t {
    before_all_prefixed = -1,
    equal = 0,
    after_all_prefixed = 1,
};

enum class partition_region : uint8_t {
    partition_start,
    static_row,
    clustered,
    partition_end,
};

class position_in_partition {
    partition_region get_type();
    bound_weight get_bound_weight();
    std::optional<clustering_key_prefix> get_clustering_key_prefix();
};

struct partition_key_and_mutation_fragments {
    partition_key get_key();
    std::list<frozen_mutation_fragment> get_mutation_fragments();
};

class repair_sync_boundary {
    dht::decorated_key pk;
    position_in_partition position;
};

struct get_sync_boundary_response {
    std::optional<repair_sync_boundary> boundary;
    repair_hash row_buf_combined_csum;
    uint64_t row_buf_size;
    uint64_t new_rows_size;
    uint64_t new_rows_nr;
};

enum class row_level_diff_detect_algorithm : uint8_t {
    send_full_set,
    send_full_set_rpc_stream,
};

enum class repair_stream_cmd : uint8_t {
    error,
    hash_data,
    row_data,
    end_of_current_hash_set,
    needs_all_rows,
    end_of_current_rows,
    get_full_row_hashes,
    put_rows_done,
};

struct repair_hash_with_cmd {
    repair_stream_cmd cmd;
    repair_hash hash;
};

struct repair_row_on_wire_with_cmd {
    repair_stream_cmd cmd;
    partition_key_and_mutation_fragments row;
};

enum class repair_row_level_start_status: uint8_t {
    ok,
    no_such_column_family,
};

struct repair_row_level_start_response {
    repair_row_level_start_status status;
};

enum class node_ops_cmd : uint32_t {
     removenode_prepare,
     removenode_heartbeat,
     removenode_sync_data,
     removenode_abort,
     removenode_done,
};

struct node_ops_cmd_request {
    node_ops_cmd cmd;
    utils::UUID ops_uuid;
    std::list<gms::inet_address> ignore_nodes;
    std::list<gms::inet_address> leaving_nodes;
};

struct node_ops_cmd_response {
    bool ok;
};
