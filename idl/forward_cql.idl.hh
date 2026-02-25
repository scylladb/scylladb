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
    // Stream ID used in the serialized response
    uint16_t stream;
    // client_state fields
    std::optional<sstring> keyspace;
    std::optional<tracing::trace_info> trace_info;
    // For rate limit error response serialization
    bool has_rate_limit_extension;
    // Cached PK function call values (e.g. now(), uuid()) that must stay
    // consistent across forwarding hops.
    std::unordered_map<uint8_t, bytes_opt> cached_fn_calls;
};

enum class forward_cql_status : uint8_t {
    finished = 0,
    prepared_not_found = 1,
    redirect = 2,
};

// Error information returned from forwarded CQL execution.
// Used by the coordinator to log error messages and update statistics.
struct forwarded_error_info {
    // Exception code for statistics tracking (corresponds to exceptions::exception_code)
    int32_t exception_code;
    // Log message for logging on the forwarding node
    sstring log_message;
    // Optional timeout for read_failure_exception_with_timeout - the coordinator
    // should call sleep_until_timeout_passes() before returning the response
    std::optional<seastar::lowres_clock::time_point> timeout;
};

struct forward_cql_execute_response {
    cql_transport::forward_cql_status status;
    // Raw CQL wire-format response body (includes result, warnings, custom_payload)
    bytes response_body;
    uint8_t response_flags;
    // If status == redirect, we already found the leader to redirect to while
    // trying to execute the request, so we include it here.
    locator::host_id target_host;
    unsigned target_shard;
    // Error information for logging and stats tracking on the forwarding node
    std::optional<cql_transport::forwarded_error_info> error_info;
};

// Request to prepare a CQL statement on a remote node
struct forward_cql_prepare_request {
    // The query string to prepare
    sstring query_string;
    // Keyspace for the query
    std::optional<sstring> keyspace;
    // Dialect flag
    cql3::dialect dialect;
};

verb [[with_client_info, with_timeout]] forward_cql_execute (unsigned shard, cql_transport::forward_cql_execute_request req [[ref]]) -> cql_transport::forward_cql_execute_response;

// Prepare a CQL statement on a remote node
verb [[with_client_info, with_timeout]] forward_cql_prepare (cql_transport::forward_cql_prepare_request req [[ref]]) -> bytes;

}
