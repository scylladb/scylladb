/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "cql3/assignment_testable.hh"
#include "cql3/statements/raw/select_statement.hh"
#include "native_scalar_function.hh"
#include "vector_search/vector_store_client.hh"

namespace cql3 {
namespace functions {

using ann_results = vector_search::vector_store_client::ann_results;

class vector_similarity_fct : public native_scalar_function {
private:
    schema_ptr _schema;
    sstring _target;
    std::optional<statements::raw::select_statement::prepared_ann_ordering_type> _ann_ordering;
    std::unordered_map<vector_search::primary_key, float, vector_search::primary_key::hashing, vector_search::primary_key::equality> _ann_results;
    std::optional<partition_key> _row_partition_key;
    std::optional<clustering_key_prefix> _row_clustering_key;

    void validate_target();
    void validate_vector(const std::span<const bytes_opt> parameters);
    void validate_similarity_function();
    float find_matching_distance();

public:
    vector_similarity_fct(schema_ptr s, sstring target, const std::vector<data_type>& arg_types, sstring name)
        : native_scalar_function(name, float_type, arg_types)
        , _schema(s)
        , _target(std::move(target))
        , _ann_results(8, vector_search::primary_key::hashing(*s), vector_search::primary_key::equality(*s)) {
        validate_similarity_function();
    }

    virtual bool is_pure() const override {
        return false;
    }

    void set_primary_key(const partition_key& partition_key, const clustering_key_prefix& clustering_key) {
        _row_partition_key = partition_key;
        _row_clustering_key = clustering_key;
    }

    void set_ann_ordering(const std::optional<statements::raw::select_statement::prepared_ann_ordering_type>& ann_ordering) {
        // The ordering should be set only once
        if (!_ann_ordering.has_value()) {
            _ann_ordering = ann_ordering;
        }
    }

    void set_results(const ann_results& ann_results) {
        // The results should be set only once
        if (_ann_results.empty()) {
            for (const auto& [pk, distance] : ann_results) {
                _ann_results.emplace(pk, distance);
            }
        }
    }

    virtual bytes_opt execute(std::span<const bytes_opt> parameters) override;

    static std::vector<data_type> provide_arg_types(
            const function_name& name, const std::vector<shared_ptr<assignment_testable>>& provided_args, const data_dictionary::database& db);
};

class similarity_cosine_fct : public vector_similarity_fct {
public:
    similarity_cosine_fct(schema_ptr s, sstring target, const std::vector<data_type>& arg_types)
        : vector_similarity_fct(s, target, arg_types, "similarity_cosine") {
    }

    virtual bytes_opt execute(std::span<const bytes_opt> parameters) override;
};


class similarity_euclidean_fct : public vector_similarity_fct {
public:
    similarity_euclidean_fct(schema_ptr s, sstring target, const std::vector<data_type>& arg_types)
        : vector_similarity_fct(s, target, arg_types, "similarity_euclidean") {
    }

    virtual bytes_opt execute(std::span<const bytes_opt> parameters) override;
};

class similarity_dot_product_fct : public vector_similarity_fct {
public:
    similarity_dot_product_fct(schema_ptr s, sstring target, const std::vector<data_type>& arg_types)
        : vector_similarity_fct(s, target, arg_types, "similarity_dot_product") {
    }

    virtual bytes_opt execute(std::span<const bytes_opt> parameters) override;
};

} // namespace functions
} // namespace cql3
