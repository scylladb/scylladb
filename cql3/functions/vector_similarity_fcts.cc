/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "vector_similarity_fcts.hh"
#include "cql3/column_identifier.hh"
#include "types/list.hh"
#include "types/types.hh"
#include "types/vector.hh"

namespace cql3 {
namespace functions {

std::vector<data_type> vector_similarity_fct::provide_arg_types(
        const function_name& name, const std::vector<shared_ptr<assignment_testable>>& provided_args, const data_dictionary::database& db) {
    if (provided_args.size() != 2) {
        throw exceptions::invalid_request_exception(fmt::format("Invalid number of arguments for function {}(vector<float, n>, vector<float, n>)", name));
    }

    auto first_arg_type_opt = provided_args[0]->assignment_testable_type_opt();
    auto second_arg_type_opt = provided_args[1]->assignment_testable_type_opt();

    auto validate_vector_type = [&](const data_type& type, const shared_ptr<assignment_testable>& arg) {
        if (!type->is_vector()) {
            throw exceptions::invalid_request_exception(fmt::format("Function {} requires float vector arguments, but found {} of type {}", name,
                    arg->assignment_testable_source_context(), type->cql3_type_name()));
        }
        auto elem_type = dynamic_cast<const vector_type_impl&>(*type).get_elements_type();
        if (elem_type != float_type) {
            throw exceptions::invalid_request_exception(fmt::format("Function {} requires float vector arguments, but found {} of type {}", name,
                    arg->assignment_testable_source_context(), type->cql3_type_name()));
        }

        if (!is_assignable(arg->test_assignment(db, {}, {}, column_specification({}, {}, ::make_shared<column_identifier>("<arg>", true), type)))) {
            throw exceptions::invalid_request_exception(fmt::format("Function {} requires arguments to be assignable to {}, but found {}", name,
                    type->cql3_type_name(), arg->assignment_testable_source_context()));
        }
    };

    if (first_arg_type_opt) {
        auto type = *first_arg_type_opt;
        validate_vector_type(type, provided_args[0]);
        validate_vector_type(type, provided_args[1]);
        return {type, type};
    }

    if (second_arg_type_opt) {
        auto type = *second_arg_type_opt;
        validate_vector_type(type, provided_args[0]);
        validate_vector_type(type, provided_args[1]);
        return {type, type};
    }

    // If neither type is known use a list type to indicate unknown dimension.
    // The dimension compatibility will be checked at execution time.
    auto type = list_type_impl::get_instance(float_type, false);
    return {type, type};
}

float similarity_cosine_fct::compute_similarity(const std::vector<data_value>& v1, const std::vector<data_value>& v2) {
    float dot_product = 0.0f;
    float norm_a = 0.0f;
    float norm_b = 0.0f;

    for (size_t i = 0; i < v1.size(); ++i) {
        float a = value_cast<float>(v1[i]);
        float b = value_cast<float>(v2[i]);

        dot_product += a * b;
        norm_a += a * a;
        norm_b += b * b;
    }

    float result_if_zero[2][2];
    result_if_zero[0][0] = 1 - dot_product / (std::sqrt(norm_a) * std::sqrt(norm_b));
    result_if_zero[0][1] = result_if_zero[1][0] = 1;
    result_if_zero[1][1] = 0;
    return result_if_zero[norm_a == 0][norm_b == 0];
}

float similarity_euclidean_fct::compute_similarity(const std::vector<data_value>& v1, const std::vector<data_value>& v2) {
    float sum = 0.0f;

    for (size_t i = 0; i < v1.size(); ++i) {
        float a = value_cast<float>(v1[i]);
        float b = value_cast<float>(v2[i]);

        float diff = a - b;
        sum += diff * diff;
    }

    return sum;
}

float similarity_dot_product_fct::compute_similarity(const std::vector<data_value>& v1, const std::vector<data_value>& v2) {
    float dot_product = 0.0f;

    for (size_t i = 0; i < v1.size(); ++i) {
        float a = value_cast<float>(v1[i]);
        float b = value_cast<float>(v2[i]);

        dot_product += a * b;
    }

    return dot_product;
}

bytes_opt vector_similarity_fct::execute(std::span<const bytes_opt> parameters) {
    if (std::any_of(parameters.begin(), parameters.end(), [](const auto& param) {
            return !param;
        })) {
        throw exceptions::invalid_request_exception("Vector similarity functions cannot be executed with null arguments");
    }

    const auto& types = arg_types();
    data_value v1 = types[0]->deserialize(*parameters[0]);
    data_value v2 = types[1]->deserialize(*parameters[1]);

    auto get_vector_elements = [](const data_value& dv) -> const std::vector<data_value>& {
        return value_cast<std::vector<data_value>>(dv);
    };

    const auto& v1_elements = get_vector_elements(v1);
    const auto& v2_elements = get_vector_elements(v2);

    if (v1_elements.size() != v2_elements.size()) {
        throw exceptions::invalid_request_exception(
                fmt::format("Vector similarity functions require both vectors to have the same dimension, but found vector<float, {}> and vector<float, {}>",
                        v1_elements.size(), v2_elements.size()));
    }

    float result = compute_similarity(v1_elements, v2_elements);
    return float_type->decompose(result);
}

bytes_opt similarity_cosine_fct::execute(std::span<const bytes_opt> parameters) {
    return vector_similarity_fct::execute(parameters);
}

bytes_opt similarity_euclidean_fct::execute(std::span<const bytes_opt> parameters) {
    return vector_similarity_fct::execute(parameters);
}

bytes_opt similarity_dot_product_fct::execute(std::span<const bytes_opt> parameters) {
    return vector_similarity_fct::execute(parameters);
}

} // namespace functions
} // namespace cql3
