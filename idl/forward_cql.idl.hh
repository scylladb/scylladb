/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "message/messaging_service.hh"
#include "idl/consistency_level.idl.hh"
#include "idl/paging_state.idl.hh"
#include "idl/storage_service.idl.hh"
#include "idl/client_state.idl.hh"

namespace cql3 {

struct dialect {
    bool duplicate_bind_variable_names_refer_to_same_variable;
};

}

namespace cql_transport {

struct forward_cql_execute_request {
    // Raw CQL wire-format request body (EXECUTE or QUERY frame body)
    bytes request_buffer;
    // The opcode of the request (EXECUTE or QUERY)
    uint8_t opcode;
    // Protocol version and dialect for parsing
    uint8_t cql_version;
    cql3::dialect dialect;
    // Forwarded client state (keyspace, user, timeouts, extensions)
    service::forwarded_client_state client_state;
    std::optional<tracing::trace_info> trace_info;
    // Cached PK function call values (e.g. now(), uuid()) that must stay
    // consistent across forwarding hops.
    std::unordered_map<uint8_t, bytes_opt> cached_fn_calls;
};

enum class forward_cql_status : uint8_t {
    success = 0,
    error = 1,
    prepared_not_found = 2,
    redirect = 3,
};

struct forward_cql_execute_response {
    cql_transport::forward_cql_status status;
    // Raw CQL wire-format response body (includes result, warnings, custom_payload)
    bytes response_body;
    uint8_t response_flags;
    // If status == prepared_not_found, we know the prepared_id of the statement,
    // so to avoid parsing the request again, we return it back.
    bytes prepared_id;
    // If status == redirect, we already found the leader to redirect to while
    // trying to execute the request, so we include it here.
    locator::host_id target_host;
    unsigned target_shard;
    // Optional timeout for read_failure_exception_with_timeout - the coordinator
    // should call sleep_until_timeout_passes() before returning the response
    std::optional<seastar::lowres_clock::time_point> timeout;
};

// Request to prepare a CQL statement on a remote node
struct forward_cql_prepare_request {
    sstring query_string;
    service::forwarded_client_state client_state;
    std::optional<tracing::trace_info> trace_info;
    cql3::dialect dialect;
};

verb [[with_client_info, with_timeout]] forward_cql_execute (unsigned shard, cql_transport::forward_cql_execute_request req [[ref]]) -> cql_transport::forward_cql_execute_response;

// Prepare a CQL statement on a remote node
verb [[with_client_info, with_timeout]] forward_cql_prepare (cql_transport::forward_cql_prepare_request req [[ref]]) -> bytes;

}
