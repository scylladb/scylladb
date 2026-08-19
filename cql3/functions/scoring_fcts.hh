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

static const function_name BM25_FUNCTION_NAME = function_name::native_function("bm25");
static const function_name BM25_HIGHLIGHT_FUNCTION_NAME = function_name::native_function("bm25_highlight");
static const function_name ANN_FUNCTION_NAME = function_name::native_function("ann");

shared_ptr<function> make_bm25_function();

/// Creates the full-text search highlighting function, which reports a fragment of the searched
/// column's text with the matched terms marked.  Nullable: a row the index finds no fragment in
/// reports none.
shared_ptr<function> make_bm25_highlight_function();

/// Creates the ANN ordering function. The argument types are not fixed: they are inferred
/// from the call site and must be float vectors of the same dimension.
shared_ptr<function> make_ann_function(const std::vector<data_type>& arg_types);

} // namespace functions
} // namespace cql3
