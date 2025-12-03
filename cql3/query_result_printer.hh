/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <ostream>
#include <seastar/core/future.hh>

#include "seastarx.hh"

namespace cql3 {

class result;

future<> print_query_results_text(std::ostream& os, const result& result);
future<> print_query_results_json(std::ostream& os, const result& result);

} // namespace cql3
