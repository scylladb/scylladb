/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/functions/function.hh"
#include "cql3/functions/function_name.hh"

namespace cql3 {
namespace functions {

static const function_name ANN_FUNCTION_NAME = function_name::native_function("ann");

shared_ptr<function> make_bm25_function();

/// Creates the planning-only ANN ordering function: ann(vector<float, n>, vector<float, n>) -> float.
/// The argument types are inferred from the call site (indexed column type and query vector)
/// and must both be float vectors of the same dimension.
/// Never executed: an ANN ordering is resolved at prepare time and served by
/// vector_indexed_table_select_statement.
shared_ptr<function> make_ann_function(const std::vector<data_type>& arg_types);

} // namespace functions
} // namespace cql3
