/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "schema/schema.hh"

#include "data_dictionary/data_dictionary.hh"
#include "cql3/statements/index_target.hh"
#include "index/secondary_index_manager.hh"

#include <vector>

namespace secondary_index {

class vector_index: public custom_index {
public:
    // The minimal TTL for the CDC used by Vector Search.
    // Required to ensure that the data is not deleted until the vector index is fully built.
    static constexpr int VS_TTL_SECONDS = 86400; // 24 hours

    static inline const sstring NOT_SUFFICIENT_CDC_OPTIONS_TO_ENABLE_VECTOR_SEARCH =
        format("To enable Vector Search on this table, the CDC log must meet the minimal requirements of Vector Search.\n"
            "CDC's TTL must be at least {} seconds (24 hours), and the CDC's delta mode must be set to 'full' to enable Vector Search.\n"
            "Check documentation on how to setup TTL - https://docs.scylladb.com/manual/branch-2025.2/features/cdc/cdc-intro.html#cdc-parameters",
            VS_TTL_SECONDS);

    static inline const sstring NOT_SUFFICIENT_CDC_OPTIONS_FOR_VECTOR_SEARCH =
        format("Vector Search is enabled on this table.\n"
            "The CDC log must meet the minimal requirements of Vector Search.\n"
            "This means that the CDC's TTL must be at least {} seconds (24 hours),"
            "and the CDC's delta mode must be set to 'full'.\n",
            VS_TTL_SECONDS);

    vector_index() = default;
    ~vector_index() override = default;
    std::optional<cql3::description> describe(const index_metadata& im, const schema& base_schema) const override;
    bool view_should_exist() const override;
    void validate(const schema &schema, cql3::statements::index_prop_defs &properties, const std::vector<::shared_ptr<cql3::statements::index_target>> &targets, const gms::feature_service& fs) override;
    bool is_vector_index() const override;
    static bool has_vector_index(const schema& s);
    static void check_cdc_options(const schema& schema, bool index_validation = false);
private:
    void check_target(const schema& schema, const std::vector<::shared_ptr<cql3::statements::index_target>>& targets);
    void check_index_options(cql3::statements::index_prop_defs& properties);
};

std::unique_ptr<secondary_index::custom_index> vector_index_factory();
}