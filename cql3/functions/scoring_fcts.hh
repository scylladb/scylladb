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

shared_ptr<function> make_bm25_function();

} // namespace functions
} // namespace cql3
