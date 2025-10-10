/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "vector_search_fcts.hh"
#include "cql3/query_options.hh"
#include "types/vector.hh"

namespace cql3 {
namespace functions {
namespace {

static size_t find_matching_key_index(const expr::evaluation_inputs& inputs, const std::vector<cql3::statements::primary_key>& keys) {
    for (size_t i = 0; i < keys.size(); ++i) {
        auto primary_key = std::move(keys[i]);
        auto partition_key = primary_key.partition.key().explode();
        auto clustering_key = primary_key.clustering.explode();

        // Check if partition key matches
        if (partition_key.size() == inputs.partition_key.size() && std::equal(partition_key.begin(), partition_key.end(), inputs.partition_key.begin())) {
            if (clustering_key.size() == inputs.clustering_key.size() &&
                    std::equal(clustering_key.begin(), clustering_key.end(), inputs.clustering_key.begin())) {
                return i;
            }
        }
    }
    return keys.size(); // Not found
}

} // namespace

std::vector<data_type> vector_similarity_fct::provide_arg_types(
        const function_name& name, const std::vector<shared_ptr<assignment_testable>>& provided_args, const data_dictionary::database& db) {
    if (provided_args.size() != 2) {
        throw exceptions::invalid_request_exception(fmt::format("Invalid number of arguments for function {}(vector<float, n>, vector<float, n>)", name));
    }

    auto vector_column_type_opt = provided_args[0]->assignment_testable_type_opt();
    if (!vector_column_type_opt) {
        throw exceptions::invalid_request_exception("Vector similarity functions are only valid when the first argument type is known");
    }

    // The first argument must be a vector<float, N> column.
    auto vector_column_type = *vector_column_type_opt;
    if (!vector_column_type->is_vector()) {
        throw exceptions::invalid_request_exception(fmt::format("Function {} requires a float vector argument, but found argument {} of type {}", name,
                provided_args[0], vector_column_type->cql3_type_name()));
    }
    auto elem_type = dynamic_cast<const vector_type_impl&>(*vector_column_type).get_elements_type();
    if (elem_type != float_type) {
        throw exceptions::invalid_request_exception(fmt::format("Function {} requires a float vector argument, but found argument {} of type {}", name,
                provided_args[0], vector_column_type->cql3_type_name()));
    }

    // The second argument is a vector literal that should match the given vector column's type.
    if (!is_assignable(provided_args[1]->test_assignment(db, {}, {},
                                           column_specification({}, {}, ::make_shared<column_identifier>("<vector_literal>", true), vector_column_type)))) {

        throw exceptions::invalid_request_exception(fmt::format("Function {} requires a float vector argument, but found argument {}", name,
                provided_args[1]));
    }

    return {vector_column_type, vector_column_type};
}

bytes_opt vector_similarity_fct::execute(std::span<const bytes_opt> parameters, const expr::evaluation_inputs& inputs) {
    auto options = inputs.options->get_specific_options();
    auto [keys, distances] = options.ann_results;

    // It is not possible that the result here is empty when using ANN queries.
    // If the ANN returns no result, we won't call this function at all as it's called once per every row selected.
    // The rows are selected according to the ANN result - the same we use here.
    // No ANN results = no rows selected = no execute() function calls.
    if (keys.empty()) {
        throw exceptions::invalid_request_exception("Vector similarity functions can only be used with ANN queries");
    }

    size_t index = find_matching_key_index(inputs, keys);
    if (index == keys.size()) {
        throw std::runtime_error("No matching distance found for given primary key");
    }

    return float_type->decompose(distances[index]);
}

bytes_opt similarity_cosine_fct::execute(std::span<const bytes_opt> parameters, const expr::evaluation_inputs& inputs) {
    if (std::any_of(parameters.begin(), parameters.end(), [](const auto& param) {
            return !param;
        })) {
        return std::nullopt;
    }

    return vector_similarity_fct::execute(parameters, inputs);
}

bytes_opt similarity_euclidean_fct::execute(std::span<const bytes_opt> parameters, const expr::evaluation_inputs& inputs) {
    if (std::any_of(parameters.begin(), parameters.end(), [](const auto& param) {
            return !param;
        })) {
        return std::nullopt;
    }

    return vector_similarity_fct::execute(parameters, inputs);
}

bytes_opt similarity_dot_product_fct::execute(std::span<const bytes_opt> parameters, const expr::evaluation_inputs& inputs) {
    if (std::any_of(parameters.begin(), parameters.end(), [](const auto& param) {
            return !param;
        })) {
        return std::nullopt;
    }

    return vector_similarity_fct::execute(parameters, inputs);
}

} // namespace functions
} // namespace cql3
