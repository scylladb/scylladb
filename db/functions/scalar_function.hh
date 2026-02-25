/*
 * Copyright (C) 2014-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "bytes_fwd.hh"
#include "function.hh"
#include <span>

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
    virtual bytes_opt execute(std::span<const bytes_opt> parameters) = 0;
};


}
