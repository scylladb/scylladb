/*
 * Modified by ScyllaDB
 *
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <tuple>
#include <unordered_map>

#include "cql3/functions/function.hh"
#include "cql3/functions/abstract_function.hh"
#include "exceptions/exceptions.hh"
#include <seastar/core/print.hh>
#include "cql3/cql3_type.hh"
#include "cql3/selection/selector.hh"

namespace cql3 {
namespace functions {

/*
 * Support for CAST(. AS .) functions.
 */

using castas_fctn = data_value(*)(data_value);

castas_fctn get_castas_fctn(data_type to_type, data_type from_type);
::shared_ptr<function> get_castas_fctn_as_cql3_function(data_type to_type, data_type from_type);

class castas_functions {
public:
    static shared_ptr<function> get(data_type to_type, const std::vector<shared_ptr<cql3::selection::selector>>& provided_args);
};

}
}
