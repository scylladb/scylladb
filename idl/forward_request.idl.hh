/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "dht/i_partitioner.hh"
#include "parallel_aggregations.hh"

#include "idl/read_command.idl.hh"
#include "idl/consistency_level.idl.hh"

namespace db {
namespace functions {
class function_name {
    sstring keyspace;
    sstring name;
};
}
}

namespace parallel_aggregations {

enum class reduction_type : uint8_t {
    count,
    aggregate
};

struct aggregation_info {
    db::functions::function_name name;
    std::vector<sstring> column_names;
};

struct reductions_info {
    // Used by selector_factries to prepare reductions information
    std::vector<parallel_aggregations::reduction_type> types;
    std::vector<parallel_aggregations::aggregation_info> infos;
};

template<typename TimePoint>
struct parametrized_forward_request {
    std::vector<parallel_aggregations::reduction_type> reduction_types;

    query::read_command cmd;
    dht::partition_range_vector pr;

    db::consistency_level cl;
    TimePoint timeout;
    std::optional<std::vector<parallel_aggregations::aggregation_info>> aggregation_infos [[version 5.1]];
};

struct forward_result {
    std::vector<bytes_opt> query_results;
};

verb forward_request(parallel_aggregations::parametrized_forward_request<lowres_clock::time_point>, std::optional<tracing::trace_info>) -> parallel_aggregations::forward_result;

}
