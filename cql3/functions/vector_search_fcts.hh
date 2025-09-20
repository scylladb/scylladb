/*
 * Modified by ScyllaDB
 *
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "native_scalar_function.hh"

namespace cql3 {
namespace functions {

class vector_similarity_fct: public native_scalar_function {
public:
    vector_similarity_fct()
            : native_scalar_function("vector_similarity",
                    float_type, {}) {
    }

    virtual bool is_pure() const override {
        return false;
    }

    bytes_opt execute(std::span<const bytes_opt> parameters) override {
        SCYLLA_ASSERT(parameters.size() == 1);

        // The first parameter is a distance provided by the ANN query.
        // We just return it as is.
        return parameters[0];
    }
};

}
}
