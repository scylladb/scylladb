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
#include "idl/result.idl.hh"
#include "idl/cql3_result.idl.hh"
#include "idl/coordinator_exception.idl.hh"


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
};

struct forward_cql_execute_response {
    bool prepared_not_found;
    bool stale_topology;
    // result message fields
    std::optional<cql3::result_set_serialized> result;
    std::optional<exceptions::coordinator_exception_serialized> ex;
    std::vector<sstring> warnings;
    std::optional<std::unordered_map<sstring, bytes>> custom_payload;
};

verb [[with_client_info, with_timeout]] forward_cql_execute (service::forward_cql_execute_request req [[ref]], int64_t topology_version) -> service::forward_cql_execute_response;

}