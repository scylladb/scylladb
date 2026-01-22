/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/rpc/rpc.hh>

#include "cql3/cql_statement.hh"
#include "cql3/prepared_statements_cache.hh"
#include "cql3/query_options.hh"
#include "cql3/statements/prepared_statement.hh"
#include "db/consistency_level_type.hh"
#include "tracing/tracing.hh"
#include "transport/response.hh"
#include "transport/server.hh"

namespace netw {
class messaging_service;
}

namespace cql3 {
class query_processor;
}

namespace service {

class storage_proxy;

struct forward_cql_execute_request {
    bytes prepared_id;
    sstring query_string; // Only non-empty if prepared_id is not found
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
    // For response serialization
    uint16_t stream;
    uint8_t cql_version;
    std::optional<bytes> request_metadata_id;
    std::optional<bytes> response_metadata_id;
    // For rate limit error response serialization
    bool has_rate_limit_extension;
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
    forward_cql_status status;
    // Raw CQL wire-format response body (includes result, warnings, custom_payload)
    bytes response_body;
    uint8_t response_flags;
    // If status == redirect, we already found the leader to redirect to while
    // trying to execute the request, so we include it here.
    locator::host_id target_host;
    unsigned target_shard;
    // Error information for logging and stats tracking on the forwarding node
    std::optional<forwarded_error_info> error_info;
};

// Result of forward_cql execution, containing both the response and error information
// for logging and statistics tracking on the coordinator node.
struct forward_cql_result {
    foreign_ptr<std::unique_ptr<cql_transport::response>> response;
    // Error information set when the response is an error response
    std::optional<forwarded_error_info> error_info;
};

// Service for forwarding CQL statement execution to replicas
class forward_cql_service : public seastar::peering_sharded_service<forward_cql_service> {
    netw::messaging_service& _ms;
    cql3::query_processor& _qp;

public:
    forward_cql_service(netw::messaging_service& ms, cql3::query_processor& qp);
    ~forward_cql_service();

    future<> stop();

    // Execute a prepared statement, forwarding to leader if necessary.
    // If this node is the leader, executes locally without RPC.
    future<forward_cql_result>
    forward_cql(
        ::shared_ptr<cql3::cql_statement> stmt,
        cql3::cql_prepared_id_type prepared_id,
        service::query_state& query_state,
        const cql3::query_options& options,
        uint16_t stream,
        cql_protocol_version_type version,
        cql_transport::cql_metadata_id_wrapper metadata_id);

private:
    // Register RPC handlers
    void register_handlers();

    cql3::query_options make_query_options(const forward_cql_execute_request& req) const;

    future<forward_cql_execute_response> handle_forward_execute(const rpc::client_info& cinfo, rpc::opt_time_point timeout, unsigned shard, forward_cql_execute_request req);
    future<forward_cql_execute_response> handle_forward_execute_without_checking_exceptions(query_state& qs, rpc::opt_time_point timeout, unsigned shard, forward_cql_execute_request& req);

    forward_cql_execute_request make_forward_cql_request(
        ::shared_ptr<cql3::cql_statement> stmt,
        const cql3::cql_prepared_id_type& prepared_id,
        service::query_state& qs,
        const cql3::query_options& options,
        uint16_t stream,
        uint8_t cql_version,
        cql_transport::cql_metadata_id_wrapper metadata_id) const;

    // Performs the work of forward_cql, but may return an exceptional future for
    // the caller to handle.
    future<forward_cql_result>
    forward_cql_without_checking_exceptions(
        ::shared_ptr<cql3::cql_statement> stmt,
        cql3::cql_prepared_id_type prepared_id,
        service::query_state& query_state,
        const cql3::query_options& options,
        uint16_t stream,
        cql_protocol_version_type version,
        cql_transport::cql_metadata_id_wrapper metadata_id);

    // Execute a statement on a specific shard (for shard bouncing). Query string might be in the request, but it may also come from the cached prepared statement.
    future<cql_transport::cql_server::process_fn_return_type>
    execute_on_shard(
        unsigned target_shard,
        service::client_state& cs,
        const tracing::trace_state_ptr& trace_state,
        forward_cql_execute_request req,
        cql3::computed_function_values cached_fn_calls,
        const sstring& query_string);
};

} // namespace service
