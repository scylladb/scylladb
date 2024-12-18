/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
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
    std::list<gms::inet_address> unused;
    std::chrono::seconds hints_timeout;
    std::chrono::seconds batchlog_timeout;
};

struct repair_flush_hints_batchlog_response {
    gc_clock::time_point flush_time [[version 6.2]];
};

verb [[with_client_info]] repair_update_system_table (repair_update_system_table_request req [[ref]]) -> repair_update_system_table_response;
verb [[with_client_info]] repair_flush_hints_batchlog (repair_flush_hints_batchlog_request req [[ref]]) -> repair_flush_hints_batchlog_response;
verb [[with_client_info]] repair_get_full_row_hashes (uint32_t repair_meta_id, shard_id dst_shard_id [[version 5.2]]) -> repair_hash_set;
verb [[with_client_info]] repair_get_combined_row_hash (uint32_t repair_meta_id, std::optional<repair_sync_boundary> common_sync_boundary, shard_id dst_shard_id [[version 5.2]]) -> get_combined_row_hash_response;
verb [[with_client_info]] repair_get_sync_boundary (uint32_t repair_meta_id, std::optional<repair_sync_boundary> skipped_sync_boundary, shard_id dst_shard_id [[version 5.2]]) -> get_sync_boundary_response;
verb [[with_client_info]] repair_get_row_diff (uint32_t repair_meta_id, repair_hash_set set_diff, bool needs_all_rows, shard_id dst_shard_id [[version 5.2]]) -> repair_rows_on_wire;
verb [[with_client_info]] repair_put_row_diff (uint32_t repair_meta_id, repair_rows_on_wire row_diff, shard_id dst_shard_id [[version 5.2]]);
verb [[with_client_info]] repair_row_level_start (uint32_t repair_meta_id, sstring keyspace_name, sstring cf_name, dht::token_range range, row_level_diff_detect_algorithm algo, uint64_t max_row_buf_size, uint64_t seed, unsigned remote_shard, unsigned remote_shard_count, unsigned remote_ignore_msb, sstring remote_partitioner_name, table_schema_version schema_version, streaming::stream_reason reason [[version 4.1.0]], gc_clock::time_point compaction_time [[version 5.2]], shard_id dst_shard_id [[version 5.2]]) -> repair_row_level_start_response [[version 4.2.0]];
verb [[with_client_info]] repair_row_level_stop (uint32_t repair_meta_id, sstring keyspace_name, sstring cf_name, dht::token_range range, shard_id dst_shard_id [[version 5.2]]);
verb [[with_client_info]] repair_get_estimated_partitions (uint32_t repair_meta_id, shard_id dst_shard_id [[version 5.2]]) -> uint64_t;
verb [[with_client_info]] repair_set_estimated_partitions (uint32_t repair_meta_id, uint64_t estimated_partitions, shard_id dst_shard_id [[version 5.2]]);
verb [[with_client_info]] repair_get_diff_algorithms () -> std::vector<row_level_diff_detect_algorithm>;
