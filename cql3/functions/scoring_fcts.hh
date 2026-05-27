/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/functions/function.hh"

namespace cql3 {
namespace functions {

shared_ptr<function> make_bm25_function();

} // namespace functions
} // namespace cql3
