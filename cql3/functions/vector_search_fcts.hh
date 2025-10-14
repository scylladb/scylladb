/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "native_scalar_function.hh"

namespace cql3 {
namespace functions {

class vector_similarity_fct : public native_scalar_function {
public:
    vector_similarity_fct(const sstring& name)
        : native_scalar_function(name, float_type, {}) {
    }

    virtual bool is_pure() const override {
        return false;
    }

    virtual bytes_opt execute(std::span<const bytes_opt> parameters) override;
};

class similarity_cosine_fct : public vector_similarity_fct {
public:
    similarity_cosine_fct()
        : vector_similarity_fct("similarity_cosine") {
    }

    virtual bytes_opt execute(std::span<const bytes_opt> parameters) override;
};


class similarity_euclidean_fct : public vector_similarity_fct {
public:
    similarity_euclidean_fct()
        : vector_similarity_fct("similarity_euclidean") {
    }

    virtual bytes_opt execute(std::span<const bytes_opt> parameters) override;
};

class similarity_dot_product_fct : public vector_similarity_fct {
public:
    similarity_dot_product_fct()
        : vector_similarity_fct("similarity_dot_product") {
    }

    virtual bytes_opt execute(std::span<const bytes_opt> parameters) override;
};

} // namespace functions
} // namespace cql3
