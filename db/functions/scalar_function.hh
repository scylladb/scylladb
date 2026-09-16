/*
 * Copyright (C) 2014-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include "bytes.hh"
#include "function.hh"
#include "utils/managed_bytes.hh"
#include <ranges>
#include <span>
#include <vector>

namespace db::functions {

class scalar_function : public virtual function {
public:
    /**
     * Applies this function to the specified parameter.
     *
     * @param parameters the input parameters
     * @return the result of applying this function to the parameter
     * @throws InvalidRequestException if this function cannot not be applied to the parameter
     */
    virtual managed_bytes_opt execute(std::span<const managed_bytes_opt> parameters) = 0;
};

/**
 * Linearizes the parameters of a scalar function.
 *
 * Used by the implementations that haven't been converted to fragmented
 * buffers yet; converting one means dropping its call to this function.
 */
inline
std::vector<bytes_opt>
linearize_parameters(std::span<const managed_bytes_opt> parameters) {
    return parameters
            | std::views::transform([] (const managed_bytes_opt& parameter) { return to_bytes_opt(parameter); })
            | std::ranges::to<std::vector>();
}


}
