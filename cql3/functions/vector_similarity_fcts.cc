/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "vector_similarity_fcts.hh"
#include "types/types.hh"
#include "types/vector.hh"
#include "exceptions/exceptions.hh"

namespace cql3 {
namespace functions {
namespace {

// You should only use this function if you need to preserve the original vectors and cannot normalize
// them in advance.
float compute_cosine_similarity(const std::vector<data_value>& v1, const std::vector<data_value>& v2) {
    double dot_product = 0.0;
    double squared_norm_a = 0.0;
    double squared_norm_b = 0.0;

    for (size_t i = 0; i < v1.size(); ++i) {
        double a = value_cast<float>(v1[i]);
        double b = value_cast<float>(v2[i]);

        dot_product += a * b;
        squared_norm_a += a * a;
        squared_norm_b += b * b;
    }

    if (squared_norm_a == 0 || squared_norm_b == 0) {
        throw exceptions::invalid_request_exception("Function system.similarity_cosine doesn't support all-zero vectors");
    }

    // The cosine similarity is in the range [-1, 1].
    // It's mapped to [0, 1] for consistency with other similarity functions.
    return (1 + (dot_product / (std::sqrt(squared_norm_a) * std::sqrt(squared_norm_b)))) / 2;
}

float compute_euclidean_similarity(const std::vector<data_value>& v1, const std::vector<data_value>& v2) {
    double sum = 0.0;

    for (size_t i = 0; i < v1.size(); ++i) {
        double a = value_cast<float>(v1[i]);
        double b = value_cast<float>(v2[i]);

        double diff = a - b;
        sum += diff * diff;
    }

    // The Euclidean distance is mapped to a similarity score in the range (0, 1].
    return (1 / (1 + sum));
}

// Assumes that both vectors are L2-normalized.
// This similarity is intended as an optimized way to perform cosine similarity calculation.
float compute_dot_product_similarity(const std::vector<data_value>& v1, const std::vector<data_value>& v2) {
    double dot_product = 0.0;

    for (size_t i = 0; i < v1.size(); ++i) {
        double a = value_cast<float>(v1[i]);
        double b = value_cast<float>(v2[i]);
        dot_product += a * b;
    }

    // The dot product is in the range [-1, 1] for L2-normalized vectors.
    // It's mapped to [0, 1] for consistency with other similarity functions.
    return ((1 + dot_product) / 2);
}

} // namespace

std::vector<data_type> retrieve_vector_arg_types(const function_name& name, const std::vector<shared_ptr<assignment_testable>>& provided_args) {
    if (provided_args.size() != 2) {
        throw exceptions::invalid_request_exception(fmt::format("Invalid number of arguments for function {}(vector<float, n>, vector<float, n>)", name));
    }

    auto [first_result, first_dim_opt] = provided_args[0]->test_assignment_any_size_float_vector();
    auto [second_result, second_dim_opt] = provided_args[1]->test_assignment_any_size_float_vector();

    if (!is_assignable(first_result)) {
        throw exceptions::invalid_request_exception(
                fmt::format("Function {} requires a float vector argument, but found {}", name, provided_args[0]->assignment_testable_source_context()));
    }
    if (!is_assignable(second_result)) {
        throw exceptions::invalid_request_exception(
                fmt::format("Function {} requires a float vector argument, but found {}", name, provided_args[1]->assignment_testable_source_context()));
    }

    if (!first_dim_opt && !second_dim_opt) {
        throw exceptions::invalid_request_exception(fmt::format("Cannot infer type of argument {} for function {}(vector<float, n>, vector<float, n>)",
                provided_args[0]->assignment_testable_source_context(), name));
    }
    if (first_dim_opt && second_dim_opt) {
        if (*first_dim_opt != *second_dim_opt) {
            throw exceptions::invalid_request_exception(fmt::format(
                    "All arguments must have the same vector dimensions, but found vector<float, {}> and vector<float, {}>", *first_dim_opt, *second_dim_opt));
        }
    }

    size_t dimension = first_dim_opt ? *first_dim_opt : *second_dim_opt;
    auto type = vector_type_impl::get_instance(float_type, dimension);
    return {type, type};
}

} // namespace functions
} // namespace cql3
