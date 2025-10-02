/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "cql3/assignment_testable.hh"
#include "cql3/functions/function_name.hh"

namespace cql3 {
namespace functions {

std::vector<data_type> retrieve_vector_arg_types(const function_name& name, const std::vector<shared_ptr<assignment_testable>>& provided_args);

} // namespace functions
} // namespace cql3
