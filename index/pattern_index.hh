/*
 * Copyright 2026-present ScyllaDB
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

/// Serves `LIKE` on a text column from the external index.
class pattern_index : public external_index {
public:
    static constexpr std::string_view INDEX_TYPE_NAME = "pattern";
    static constexpr std::string_view SEARCH_TYPE_NAME = "Pattern Search";

    std::string_view index_type_name() const override {
        return INDEX_TYPE_NAME;
    }

    pattern_index() = default;
    ~pattern_index() override = default;
    std::optional<cql3::description> describe(const index_metadata& im, const schema& base_schema) const override;
    void validate(const schema& schema, const cql3::statements::index_specific_prop_defs& properties,
            const std::vector<::shared_ptr<cql3::statements::index_target>>& targets, const gms::feature_service& fs,
            const data_dictionary::database& db) const override;
    static bool has_index(const schema& s) {
        return has_index_impl<pattern_index>(s);
    }
    static bool has_index_on_column(const schema& s, const sstring& column);
    static void check_cdc_options(const schema& s) {
        check_cdc_options_impl<pattern_index>(s);
    }

private:
    void check_target(const schema& schema, const std::vector<::shared_ptr<cql3::statements::index_target>>& targets) const;
    void check_index_options(const cql3::statements::index_specific_prop_defs& properties) const;
};

std::unique_ptr<secondary_index::custom_index> pattern_index_factory();

} // namespace secondary_index
