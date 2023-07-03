// Copyright (C) 2023-present ScyllaDB
// SPDX-License-Identifier: AGPL-3.0-or-later

#pragma once

#include "aggregate_function.hh"
#include "function_name.hh"

/// Factory methods for aggregate functions.
namespace cql3::functions::aggregate_fcts {

/// A aggregate function that accepts a single input; the aggregation result
/// is the first value seen (if the first value is NULL then that's the result too)
shared_ptr<aggregate_function> make_first_function(data_type io_type);

function_name first_function_name();

}
