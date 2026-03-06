/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/selection/selection.hh"
#include "vector_search/vector_store_client.hh"

class schema;

namespace cql3::statements {

/// Injects external search result scores into result rows.
/// Matches each base-table row against the ranked result list by PK/CK
/// and fills the corresponding external_value slot with the score.
class external_score_provider : public cql3::selection::external_values_provider {
    const vector_search::vector_store_client::primary_keys& _results;
    mutable size_t _current_index;
    const size_t _external_value_index;
    const schema& _schema;

public:
    external_score_provider(const vector_search::vector_store_client::primary_keys& results, size_t external_value_index, const schema& schema);

    bool try_fill(std::vector<cql3::raw_value>& external_values, std::span<const bytes> partition_key, std::span<const bytes> clustering_key,
            const query::result_row_view& static_row, const query::result_row_view* row) const override;
};

} // namespace cql3::statements
