/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "schema/schema.hh"

#include "data_dictionary/data_dictionary.hh"
#include "cql3/statements/index_target.hh"
#include "index/external_index.hh"

#include <vector>

namespace secondary_index {

class vector_index: public external_index {
public:
    static constexpr std::string_view INDEX_TYPE_NAME = "vector";
    static constexpr std::string_view SEARCH_TYPE_NAME = "Vector Search";
    std::string_view index_type_name() const override { return INDEX_TYPE_NAME; }

    vector_index() = default;
    ~vector_index() override = default;
    std::optional<cql3::description> describe(const index_metadata& im, const schema& base_schema) const override;
    void validate(const schema &schema, const cql3::statements::index_specific_prop_defs &properties,
            const std::vector<::shared_ptr<cql3::statements::index_target>> &targets, const gms::feature_service& fs,
        const data_dictionary::database& db) const override;
    static bool has_index(const schema& s) { return has_index_impl<vector_index>(s); }
    static bool has_vector_index_on_column(const schema& s, const sstring& target_name);
    static bool is_vector_index_on_column(const index_metadata& im, const sstring& target_name);
    static void check_cdc_options(const schema& s) {
        check_cdc_options_impl<vector_index>(s);
    }

    static sstring serialize_targets(const std::vector<::shared_ptr<cql3::statements::index_target>>& targets);
    static sstring get_target_column(const sstring& targets);

    static bool is_rescoring_enabled(const index_options_map& properties);
    static float get_oversampling(const index_options_map& properties);
    static sstring get_cql_similarity_function_name(const index_options_map& properties);
private:
    void check_cdc_not_explicitly_disabled(const schema& schema) const;
    void check_target(const schema& schema, const std::vector<::shared_ptr<cql3::statements::index_target>>& targets) const;
    void check_index_options(const cql3::statements::index_specific_prop_defs& properties) const;
};

std::unique_ptr<secondary_index::custom_index> vector_index_factory();
}
