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

class vector_similarity_fct: public native_scalar_function {
private:
    schema_ptr _schema;
public:
    vector_similarity_fct(schema_ptr s, const sstring& name)
        : native_scalar_function(name,
            float_type,
            ([](const schema_ptr& s) {
                auto columns = s->primary_key_columns();
                std::vector<data_type> types;
                for (const auto& col : columns) {
                    types.push_back(col.type);
                }
                return types;
            })(s))
            , _schema(s) {
    }

    virtual bool is_pure() const override {
        return false;
    }

    virtual bytes_opt execute(std::span<const bytes_opt> parameters, const expr::evaluation_inputs& inputs) override {
        return std::nullopt; // Unimplemented
    }
};

class similarity_cosine_fct: public vector_similarity_fct {
public:
    similarity_cosine_fct(schema_ptr s)
        : vector_similarity_fct(s, "similarity_cosine") {
    }

    virtual bytes_opt execute(std::span<const bytes_opt> parameters, const expr::evaluation_inputs& inputs) override {
        return std::nullopt; // Unimplemented
    }
};


class similarity_euclidean_fct: public vector_similarity_fct {
public:
    similarity_euclidean_fct(schema_ptr s)
        : vector_similarity_fct(s, "similarity_euclidean") {
    }

    virtual bytes_opt execute(std::span<const bytes_opt> parameters, const expr::evaluation_inputs& inputs) override {
        return std::nullopt; // Unimplemented
    }
};

class similarity_dot_product_fct: public vector_similarity_fct {
public:
    similarity_dot_product_fct(schema_ptr s)
        : vector_similarity_fct(s, "similarity_dot_product") {
    }

    virtual bytes_opt execute(std::span<const bytes_opt> parameters, const expr::evaluation_inputs& inputs) override {
        return std::nullopt; // Unimplemented
    }
};

}
}
