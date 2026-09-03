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
/// and fills the corresponding temporary slot with the score.
///
/// The cursor only moves forward: base-table results are merged in external
/// search primary-key order, so a row can only ever match at or after the
/// current position. Entries it steps over are keys the index still knows about
/// but that are no longer in the base table.
///
/// A provider instance is therefore single-use and tied to one response - it
/// cannot be rewound or replayed, which is worth keeping in mind when paging
/// arrives.
class external_score_provider : public cql3::selection::external_values_provider {
    const vector_search::vector_store_client::primary_keys& _results;
    mutable size_t _next_result;    // cursor into _results: which entry to match next
    const size_t _score_slot;       // temporary slot the score is written to
    const schema& _schema;

public:
    external_score_provider(const vector_search::vector_store_client::primary_keys& results, size_t score_slot, const schema& schema);

    bool try_fill(std::vector<cql3::raw_value>& temporaries, std::span<const bytes> partition_key, std::span<const bytes> clustering_key,
            const query::result_row_view& static_row, const query::result_row_view* row) const override;
};

} // namespace cql3::statements
