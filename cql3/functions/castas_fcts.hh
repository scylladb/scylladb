/*
 * Modified by ScyllaDB
 *
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "cql3/functions/function.hh"
#include <seastar/core/format.hh>

namespace cql3 {
namespace functions {

/*
 * Support for CAST(. AS .) functions.
 */

using castas_fctn = data_value(*)(data_value);

castas_fctn get_castas_fctn(data_type to_type, data_type from_type);
::shared_ptr<function> get_castas_fctn_as_cql3_function(data_type to_type, data_type from_type);

}
}
