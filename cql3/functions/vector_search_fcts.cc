/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "vector_search_fcts.hh"
#include "dht/i_partitioner.hh"
#include "cql3/column_identifier.hh"
#include "types/vector.hh"
#include "index/vector_index.hh"

namespace cql3 {
namespace functions {

float vector_similarity_fct::find_matching_distance() {
    vector_search::primary_key row_primary_key =
            vector_search::primary_key{dht::decorated_key{dht::get_token(*_schema, *_row_partition_key), *_row_partition_key}, *_row_clustering_key};

    auto it = _ann_results.find(row_primary_key);
    if (it == _ann_results.end()) {
        throw std::runtime_error("No matching distance found for given primary key");
    }
    return it->second;
}

void vector_similarity_fct::validate_similarity_function() {
    auto vector_index_options = secondary_index::vector_index::get_index_options(*_schema, _target);
    if (vector_index_options.empty()) {
        throw exceptions::invalid_request_exception(fmt::format("No vector index found on column {}", _target));
    }
    auto similarity_function_it = vector_index_options.find("similarity_function");
    sstring similarity_function = similarity_function_it != vector_index_options.end() ? similarity_function_it->second : "cosine";
    std::transform(similarity_function.begin(), similarity_function.end(), similarity_function.begin(), ::tolower);

    // The `_name.name` returns the function name, e.g., "similarity_cosine", but the similarity function option
    // in the vector index options is just "cosine", "euclidean", or "dot_product".
    // Therefore, we check if the function name contains the metric name.
    if (!_name.name.contains(similarity_function)) {
        throw exceptions::invalid_request_exception(fmt::format("Vector index on column {} was not built with {} metric", _target, _name.name));
    }
}

std::vector<data_type> vector_similarity_fct::provide_arg_types(
        const function_name& name, const std::vector<shared_ptr<assignment_testable>>& provided_args, const data_dictionary::database& db) {
    if (provided_args.size() != 2) {
        throw exceptions::invalid_request_exception(fmt::format("Invalid number of arguments for function {}(vector<float, n>, vector<float, n>)", name));
    }

    auto vector_column_type_opt = provided_args[0]->assignment_testable_type_opt();
    if (!vector_column_type_opt) {
        throw exceptions::invalid_request_exception("Vector similarity functions are only valid when the vector column is known");
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
    if (!is_assignable(provided_args[1]->test_assignment(
                db, {}, {}, column_specification({}, {}, ::make_shared<column_identifier>("<vector_literal>", true), vector_column_type)))) {

        throw exceptions::invalid_request_exception(fmt::format("Function {} requires a float vector argument, but found argument {}", name, provided_args[1]));
    }

    return {vector_column_type, vector_column_type};
}

bytes_opt vector_similarity_fct::execute(std::span<const bytes_opt> parameters) {
    if (std::any_of(parameters.begin(), parameters.end(), [](const auto& param) {
            return !param;
        })) {
        throw exceptions::invalid_request_exception("Vector similarity functions cannot be executed with null arguments");
    }

    // It is not possible that the result here is empty when using ANN queries.
    // If the ANN returns no result, we won't call this function at all as it's called once per every row selected.
    // The rows are selected according to the ANN result - the same we use here.
    // No ANN results = no rows selected = no execute() function calls.
    if (_ann_results.empty()) {
        throw exceptions::invalid_request_exception("Vector similarity functions can only be used with ANN queries");
    }

    float distance = find_matching_distance();

    return float_type->decompose(distance);
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
