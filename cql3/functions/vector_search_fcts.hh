/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "cql3/assignment_testable.hh"
#include "native_scalar_function.hh"

namespace cql3 {
namespace functions {

class vector_similarity_fct : public native_scalar_function {
public:
    vector_similarity_fct(const std::vector<data_type>& arg_types, sstring name)
        : native_scalar_function(name, float_type, arg_types) {
    }

    virtual bool is_pure() const override {
        return false;
    }

    virtual bytes_opt execute(std::span<const bytes_opt> parameters) override;

    static std::vector<data_type> provide_arg_types(
            const function_name& name, const std::vector<shared_ptr<assignment_testable>>& provided_args, const data_dictionary::database& db);
};

class similarity_cosine_fct : public vector_similarity_fct {
public:
    similarity_cosine_fct(const std::vector<data_type>& arg_types)
        : vector_similarity_fct(arg_types, "similarity_cosine") {
    }

    virtual bytes_opt execute(std::span<const bytes_opt> parameters) override;
};


class similarity_euclidean_fct : public vector_similarity_fct {
public:
    similarity_euclidean_fct(const std::vector<data_type>& arg_types)
        : vector_similarity_fct( arg_types, "similarity_euclidean") {
    }

    virtual bytes_opt execute(std::span<const bytes_opt> parameters) override;
};

class similarity_dot_product_fct : public vector_similarity_fct {
public:
    similarity_dot_product_fct(const std::vector<data_type>& arg_types)
        : vector_similarity_fct(arg_types, "similarity_dot_product") {
    }

    virtual bytes_opt execute(std::span<const bytes_opt> parameters) override;
};

} // namespace functions
} // namespace cql3
