/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/selection/selection.hh"
#include "cql3/values.hh"
#include "vector_search/vector_store_client.hh"

#include <optional>
#include <span>
#include <vector>

class schema;

namespace query {
class result;
class partition_slice;
}

namespace cql3::statements::external_search {

struct joined_row {
    /// Index of the external result naming this row, or nothing when no result does or the rows
    /// were not matched.
    std::optional<size_t> external_result;
};

/// Walks the rows just read from the base table, one joined_row per row, matching each to the
/// external result that names it when `external_results` is given. The walk visits exactly the
/// rows the result set is built from, in the same order; `slice` must be the slice
/// `table_results` were read with.
///
/// `table_results` must hold the rows in the order of `external_results` - query_base_table()
/// reads them that way - so matching walks both forward at once, comparing primary keys, and a
/// result stepped over is one whose row the base table no longer has. With results [k1, k2, k3]
/// and rows [k1, k3] the joined rows are [{external_result = 0}, {external_result = 2}].
///
/// Matching compares primary keys, so `slice` must include the key columns; it asserts if they are
/// missing. A null `external_results` skips matching, leaving every row unnamed.
std::vector<joined_row> join_table_results(const query::result& table_results, const query::partition_slice& slice, const schema& schema,
        const vector_search::vector_store_client::primary_keys* external_results);

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
class values_provider : public cql3::selection::external_values_provider {
    const vector_search::vector_store_client::primary_keys& _results;
    mutable size_t _next_result;    // cursor into _results: which entry to match next
    const size_t _score_slot;       // temporary slot the score is written to
    const schema& _schema;

public:
    values_provider(const vector_search::vector_store_client::primary_keys& results, size_t score_slot, const schema& schema);

    bool try_fill(std::vector<cql3::raw_value>& temporaries, std::span<const bytes> partition_key, std::span<const bytes> clustering_key,
            const query::result_row_view& static_row, const query::result_row_view* row) const override;
};

} // namespace cql3::statements::external_search
