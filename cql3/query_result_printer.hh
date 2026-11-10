/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <ostream>

#include <vector>

#include <seastar/core/shared_ptr.hh>

#include "seastarx.hh"
#include "utils/managed_bytes.hh"

namespace rjson {
class streaming_writer;
}

namespace cql3 {

class column_specification;
class result;

void print_query_results_text(std::ostream& os, const result& result);
// Prints the result rows as a JSON array of objects, each object being a row,
// with the column names as keys. Values are formatted the same as with SELECT JSON.
void print_query_results_json(std::ostream& os, const result& result);
// Writes a single row of a query result as a JSON object, in the format of
// print_query_results_json(), for callers which compose a larger document.
void print_query_result_row_json(rjson::streaming_writer& writer, const std::vector<lw_shared_ptr<column_specification>>& columns,
        const std::vector<managed_bytes_opt>& row);

} // namespace cql3
