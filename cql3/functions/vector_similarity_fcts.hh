/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "native_scalar_function.hh"
#include "cql3/assignment_testable.hh"
#include "cql3/functions/function_name.hh"
#include <span>

namespace cql3 {
namespace functions {

static const function_name SIMILARITY_COSINE_FUNCTION_NAME = function_name::native_function("similarity_cosine");
static const function_name SIMILARITY_EUCLIDEAN_FUNCTION_NAME = function_name::native_function("similarity_euclidean");
static const function_name SIMILARITY_DOT_PRODUCT_FUNCTION_NAME = function_name::native_function("similarity_dot_product");

using similarity_function_t = float (*)(std::span<const double>, std::span<const double>);
extern thread_local const std::unordered_map<function_name, similarity_function_t> SIMILARITY_FUNCTIONS;

std::vector<data_type> retrieve_vector_arg_types(const function_name& name, const std::vector<shared_ptr<assignment_testable>>& provided_args);

class vector_similarity_fct : public native_scalar_function {
public:
    vector_similarity_fct(const sstring& name, const std::vector<data_type>& arg_types)
        : native_scalar_function(name, float_type, arg_types) {
    }

    virtual bytes_opt execute(std::span<const bytes_opt> parameters) override;
};

namespace detail {

// Extract double vector directly from serialized bytes, bypassing data_value overhead.
// Supports both vector<float, N> and vector<double, N> element types.
// This is an internal API exposed for testing purposes.
// Wire format: N elements as big-endian values (4 bytes per float, 8 bytes per double).
std::vector<double> extract_double_vector(const bytes_opt& param, size_t dimension, data_type element_type);

} // namespace detail

} // namespace functions
} // namespace cql3
