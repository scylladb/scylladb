/*
 * Copyright (C) 2025-present ScyllaDB
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
#include "dht/token.hh"
#include "locator/host_id.hh"
#include "service/strong_consistency/groups_manager.hh"
#include "service/topology_state_machine.hh"
#include "tracing/tracing.hh"
#include "transport/messages/result_message_base.hh"
#include "transport/response.hh"
#include "transport/server.hh"

namespace netw {
class messaging_service;
}

namespace cql3 {
class query_processor;
namespace statements {
class modification_statement;
class select_statement;
}
}

namespace service {

class storage_proxy;

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
    forward_cql_status status;
    bytes response_body;
    uint8_t response_flags;
};

// Request to query a raft group member for the current group leader
struct query_tablet_leader_request {
    utils::UUID table_id;
    int64_t token;
};

struct query_tablet_leader_response {
    std::optional<utils::UUID> leader_host_id;
};

// Service for forwarding CQL statement execution to replicas
class forward_cql_service : public seastar::peering_sharded_service<forward_cql_service> {
    netw::messaging_service& _ms;
    cql3::query_processor& _qp;
    storage_proxy& _proxy;
    seastar::sharded<strong_consistency::groups_manager>& _groups_manager;

public:
    forward_cql_service(netw::messaging_service& ms, cql3::query_processor& qp, storage_proxy& proxy,
                        seastar::sharded<strong_consistency::groups_manager>& groups_manager);
    ~forward_cql_service();

    future<> stop();

    // Execute a prepared statement, forwarding to leader if necessary.
    // If this node is the leader, executes locally without RPC.
    // Returns the serialized CQL response.
    future<std::unique_ptr<cql_transport::response>>
    forward_cql(
        ::shared_ptr<cql3::cql_statement> stmt,
        const cql3::cql_prepared_id_type& prepared_id,
        service::query_state& query_state,
        const cql3::query_options& options,
        uint16_t stream,
        cql_protocol_version_type version);

private:
    // Register RPC handlers
    void register_handlers();

    service::query_state make_query_state(service::client_state& cs, const std::optional<tracing::trace_info>& trace_info, locator::host_id src_host_id) const;

    cql3::query_options make_query_options(const forward_cql_execute_request& req) const;

    bool is_stale_topology(const replica::column_family& tab, topology::version_t req_topology_version) const;

    future<forward_cql_execute_response> handle_forward_execute(const rpc::client_info& cinfo, rpc::opt_time_point timeout, const forward_cql_execute_request& req, topology::version_t topology_version);

    future<query_tablet_leader_response> handle_query_tablet_leader(const rpc::client_info& cinfo, rpc::opt_time_point timeout, const query_tablet_leader_request& req);

    dht::token get_token(const cql3::cql_statement& stmt, const cql3::query_options& options) const;

    // Select the replica to forward to. Uses local Raft server if we're a replica,
    // otherwise queries a replica via RPC to find the leader.
    // Returns std::nullopt if leader cannot be determined (caller should retry with fresh topology).
    future<std::optional<locator::host_id>> select_replica(table_id id, dht::token token, locator::effective_replication_map_ptr erm) const;

    // Check if we're the leader for this statement and execute locally if so.
    // Returns the response if executed locally, nullopt if we need to forward.
    future<std::optional<std::unique_ptr<cql_transport::response>>>
    try_execute_as_leader(
        ::shared_ptr<cql3::cql_statement> stmt,
        const cql3::cql_prepared_id_type& prepared_id,
        service::query_state& qs,
        const cql3::query_options& options,
        uint16_t stream,
        cql_protocol_version_type version);

    forward_cql_execute_request make_forward_cql_request(
        ::shared_ptr<cql3::cql_statement> stmt,
        const cql3::cql_prepared_id_type& prepared_id,
        service::query_state& qs,
        const cql3::query_options& options,
        uint8_t cql_version) const;

    const replica::column_family& get_column_family(const cql3::cql_statement& stmt) const;

    // Execute a statement on a specific shard (for shard bouncing)
    future<std::unique_ptr<cql_transport::response>>
    execute_on_shard(
        unsigned target_shard,
        service::client_state& cs,
        const tracing::trace_state_ptr& trace_state,
        const forward_cql_execute_request& req,
        cql3::computed_function_values cached_fn_calls);
};

} // namespace service
