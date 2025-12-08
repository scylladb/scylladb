/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "message/messaging_service.hh"
#include "idl/consistency_level.idl.hh"
#include "idl/paging_state.idl.hh"
#include "idl/storage_service.idl.hh"
#include "idl/uuid.idl.hh"


namespace service {

struct forward_cql_execute_request {
    bytes prepared_id;
    std::optional<sstring> query_string; // Only used if prepared_id is not found
    // query_options fields
    db::consistency_level consistency;
    std::vector<bytes_opt> values;
    utils::small_vector<bool, 16> unset;
    std::optional<std::vector<sstring>> names;
    bool skip_metadata;
    std::optional<service::pager::paging_state> paging_state;
    std::optional<int32_t> page_size;
    std::optional<db::consistency_level> serial_consistency;
    api::timestamp_type ts;
    // query_state fields
    std::optional<sstring> keyspace;
    std::optional<tracing::trace_info> trace_info;
    // CQL protocol version for response serialization
    uint8_t cql_version;
};

enum class forward_cql_status : uint8_t {
    ok = 0,
    prepared_not_found = 1,
    stale_topology = 2,
    not_a_leader = 3,
};

struct forward_cql_execute_response {
    service::forward_cql_status status;
    // Raw CQL wire-format response body (includes result, warnings, custom_payload)
    bytes response_body;
    uint8_t response_flags;
};

// Request to query a raft group member for the leader
struct query_tablet_leader_request {
    utils::UUID table_id;
    int64_t token;  // dht::token serialized as int64
};

struct query_tablet_leader_response {
    std::optional<utils::UUID> leader_host_id;
};

verb [[with_client_info, with_timeout]] forward_cql_execute (service::forward_cql_execute_request req [[ref]], int64_t topology_version) -> service::forward_cql_execute_response;

verb [[with_client_info, with_timeout]] query_tablet_leader (service::query_tablet_leader_request req) -> service::query_tablet_leader_response;

}
