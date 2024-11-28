/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "dht/decorated_key.hh"
#include "gms/inet_address_serializer.hh"
#include "node_ops/id.hh"

#include "idl/keys.idl.hh"
#include "idl/position_in_partition.idl.hh"
#include "idl/uuid.idl.hh"
#include "idl/frozen_mutation.idl.hh"
#include "idl/token.idl.hh"

class repair_hash {
    uint64_t hash;
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

struct repair_update_system_table_request {
    tasks::task_id repair_uuid;
    table_id table_uuid;
    sstring keyspace_name;
    sstring table_name;
    dht::token_range range;
    gc_clock::time_point repair_time;
};

struct repair_update_system_table_response {
};

struct repair_flush_hints_batchlog_request {
    tasks::task_id repair_uuid;
    std::list<gms::inet_address> target_nodes;
    std::chrono::seconds hints_timeout;
    std::chrono::seconds batchlog_timeout;
};

struct repair_flush_hints_batchlog_response {
    gc_clock::time_point flush_time [[version 6.2]];
};

verb [[with_client_info]] repair_update_system_table (repair_update_system_table_request req [[ref]]) -> repair_update_system_table_response;
verb [[with_client_info]] repair_flush_hints_batchlog (repair_flush_hints_batchlog_request req [[ref]]) -> repair_flush_hints_batchlog_response;
