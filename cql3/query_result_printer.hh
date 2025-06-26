/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <ostream>

namespace cql3 {

class result;

void print_query_results_text(std::ostream& os, const result& result);
void print_query_results_json(std::ostream& os, const result& result);

} // namespace cql3
