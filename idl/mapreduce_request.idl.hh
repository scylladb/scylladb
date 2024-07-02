/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "dht/i_partitioner_fwd.hh"

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
namespace query {
struct mapreduce_request {
    struct aggregation_info {
        db::functions::function_name name;
        std::vector<sstring> column_names;
    };
    enum class reduction_type : uint8_t {
        count,
        aggregate
    };

    std::vector<query::mapreduce_request::reduction_type> reduction_types;

    query::read_command cmd;
    dht::partition_range_vector pr;

    db::consistency_level cl;
    lowres_system_clock::time_point timeout;

    std::optional<std::vector<query::mapreduce_request::aggregation_info>> aggregation_infos [[version 5.1]];
};

struct mapreduce_result {
    std::vector<bytes_opt> query_results;
};

verb mapreduce_request(query::mapreduce_request req [[ref]], std::optional<tracing::trace_info> trace_info [[ref]]) -> query::mapreduce_result;
}
