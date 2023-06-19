/*
 * Copyright (C) 2014-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "aggregate_function.hh"

namespace cql3 {
namespace functions {

/// Factory methods for aggregate functions.
namespace aggregate_fcts {

static const sstring COUNT_ROWS_FUNCTION_NAME = "countRows";

/// The function used to count the number of rows of a result set. This function is called when COUNT(*) or COUNT(1)
/// is specified.
shared_ptr<aggregate_function>
make_count_rows_function();

/// The same as `make_max_function()' but with type provided in runtime.
shared_ptr<aggregate_function>
make_max_function(data_type io_type);

/// The same as `make_min_function()' but with type provided in runtime.
shared_ptr<aggregate_function>
make_min_function(data_type io_type);

/// count(col) function for the specified type
shared_ptr<aggregate_function> make_count_function(data_type input_type);

}
}
}
