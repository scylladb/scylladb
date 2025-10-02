/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "vector_search_fcts.hh"
#include "cql3/column_identifier.hh"
#include "types/vector.hh"

namespace cql3 {
namespace functions {

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

    return std::nullopt; // Unimplemented
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
