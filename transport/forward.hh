/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "bytes_ostream.hh"
#include "cql3/dialect.hh"
#include "service/client_state.hh"
#include "tracing/tracing.hh"
#include "cql3/query_options.hh"

namespace cql_transport {

struct forward_cql_execute_request {
    bytes_ostream request_buffer;
    uint8_t opcode;
    uint8_t cql_version;
    cql3::dialect dialect;
    service::forwarded_client_state client_state;
    std::optional<tracing::trace_info> trace_info;
    cql3::computed_function_values cached_fn_calls;
};

enum class forward_cql_status : uint8_t {
    success = 0,
    error = 1,
    prepared_not_found = 2,
    redirect = 3,
};

struct forward_cql_execute_response {
    forward_cql_status status;
    bytes_ostream response_body;
    uint8_t response_flags;
    bytes prepared_id;
    locator::host_id target_host;
    unsigned target_shard;
    std::optional<seastar::lowres_clock::time_point> timeout;
};

struct forward_cql_prepare_request {
    sstring query_string;
    service::forwarded_client_state client_state;
    std::optional<tracing::trace_info> trace_info;
    cql3::dialect dialect;
};

}
