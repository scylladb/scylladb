/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <ostream>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>

#include "seastarx.hh"

namespace cql3 {

class result;

// expand=false: all rows are printed in a single table, each result row is one line in the table.
// expand=true: each row is printed in a separate table, each result row is a separate table.
// Use the latter when the former results in very wide tables that are hard to read.
future<> print_query_results_text(std::ostream& os, const result& result, bool expand = false);
future<> print_query_results_json(std::ostream& os, const result& result);

future<> print_query_results_text(output_stream<char>& os, const result& result, bool expand = false);
future<> print_query_results_json(output_stream<char>& os, const result& result);

} // namespace cql3
