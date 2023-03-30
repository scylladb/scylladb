// Copyright (C) 2023-present ScyllaDB
// SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)

#pragma once

#include "scalar_function.hh"
#include "function_name.hh"
#include <optional>

namespace db::functions {

struct stateless_aggregate_function final {
    function_name name;
    std::optional<sstring> column_name_override; // if unset, column name is synthesized from name and argument names

    data_type state_type;
    data_type result_type;
    std::vector<data_type> argument_types;

    bytes_opt initial_state;

    // aggregates another input
    // signature: (state_type, argument_types...) -> state_type
    shared_ptr<scalar_function> aggregation_function;

    // converts the state type to a result
    // signature: (state_type) -> result_type
    shared_ptr<scalar_function> state_to_result_function;

    // optional: reduces states computed in parallel
    // signature: (state_type, state_type) -> state_type
    shared_ptr<scalar_function> state_reduction_function;
};

}
