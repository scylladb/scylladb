/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <variant>

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/rpc/rpc.hh>

#include "cql3/cql_statement.hh"
#include "cql3/query_options.hh"
#include "cql3/result_set.hh"
#include "cql3/statements/forwarding_statement.hh"
#include "db/consistency_level_type.hh"
#include "dht/token.hh"
#include "exceptions/coordinator_result.hh"
#include "locator/host_id.hh"
#include "service/topology_state_machine.hh"

namespace cql_transport {
namespace messages {
class result_message;
}
}

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
};

struct forward_cql_execute_response {
    bool prepared_not_found;
    bool stale_topology;
    // result message fields
    std::variant<std::monostate, cql3::result_set, exceptions::coordinator_exception_container> result;
    std::vector<sstring> warnings;
    std::optional<std::unordered_map<sstring, bytes>> custom_payload;
};

// Service for forwarding CQL statement execution to replicas
class forward_cql_service : public seastar::peering_sharded_service<forward_cql_service> {
    netw::messaging_service& _ms;
    cql3::query_processor& _qp;
    storage_proxy& _proxy;

public:
    forward_cql_service(netw::messaging_service& ms, cql3::query_processor& qp, storage_proxy& proxy);
    ~forward_cql_service();

    future<> stop();

    // Execute a prepared statement on a random replica
    // If the replica doesn't have the prepared statement, it will retry with the query text
    future<::shared_ptr<cql_transport::messages::result_message>>
    forward_cql(const cql3::statements::forwarding_statement& stmt, service::query_state& query_state, const cql3::query_options& options);

private:
    // Register RPC handlers
    void register_handlers();

    // Handler for forward_cql_execute verb
    future<forward_cql_execute_response>
    handle_forward_execute(const rpc::client_info& cinfo, rpc::opt_time_point timeout, const forward_cql_execute_request& req);

    service::query_state make_query_state(service::client_state& cs, const std::optional<tracing::trace_info>& trace_info, locator::host_id src_host_id) const;

    cql3::query_options make_query_options(const forward_cql_execute_request& req) const;

    bool is_stale_topology(const cql3::statements::forwarding_statement& stmt, topology::version_t req_topology_version) const;

    forward_cql_execute_response make_forward_cql_response(const ::shared_ptr<cql_transport::messages::result_message>& result_msg) const;

    future<forward_cql_execute_response> handle_forward_execute(const rpc::client_info& cinfo, rpc::opt_time_point timeout, const forward_cql_execute_request& req, topology::version_t topology_version);

    dht::token get_token(const cql3::statements::forwarding_statement& stmt, const cql3::query_options& options) const;

    locator::host_id select_replica(const cql3::statements::forwarding_statement& stmt, const cql3::query_options& options, const locator::effective_replication_map& erm) const;

    forward_cql_execute_request make_forward_cql_request(
        const cql3::statements::forwarding_statement& stmt,
        service::query_state& qs,
        const cql3::query_options& options,
        const cql3::query_processor& qp) const;

    ::shared_ptr<cql_transport::messages::result_message> make_result_message_from_response(forward_cql_execute_response& response) const;

    const replica::column_family& get_column_family_from_statement(const cql3::statements::forwarding_statement& stmt) const;

};

} // namespace service
