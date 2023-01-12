
/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <optional>
#include <ostream>
#include <vector>

#include "bytes.hh"
#include "db/consistency_level_type.hh"
#include "db/functions/aggregate_function.hh"
#include "db/functions/function.hh"
#include "db/functions/function_name.hh"
#include "dht/i_partitioner.hh"
#include "query-request.hh"

namespace parallel_aggregations {

enum class reduction_type {
    count,
    aggregate
};

struct aggregation_info {
    db::functions::function_name name;
    std::vector<sstring> column_names;
};

struct reductions_info {
    // Used by selector_factries to prepare reductions information
    std::vector<reduction_type> types;
    std::vector<aggregation_info> infos;
};

struct forward_request {
    std::vector<reduction_type> reduction_types;

    query::read_command cmd;
    dht::partition_range_vector pr;

    db::consistency_level cl;
    lowres_clock::time_point timeout;
    std::optional<std::vector<aggregation_info>> aggregation_infos;
};

struct forward_result {
    // vector storing query result for each selected column
    std::vector<bytes_opt> query_results;

    struct printer {
        const std::vector<::shared_ptr<db::functions::aggregate_function>> functions;
        const forward_result& res;
    };
};

std::ostream& operator<<(std::ostream& out, const reduction_type& r);
std::ostream& operator<<(std::ostream& out, const aggregation_info& a);
std::ostream& operator<<(std::ostream& out, const forward_request& r);
std::ostream& operator<<(std::ostream& out, const forward_result::printer&);

} // namespace parallel_aggregations
