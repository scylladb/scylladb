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

/// Walks the rows just read from the base table, one joined_row per row, matching each row to the
/// external result that names it when `external_results` is given. The rows are walked with
/// result_set_builder::visitor, the same visitor the result set is later built with, so the joined
/// rows are exactly the rows of the result set, in order; `slice` must be the slice `table_results`
/// were read with.
///
/// The rows were read in the order of `external_results`, so matching walks both lists forward at
/// once, comparing primary keys. An external result skipped on the way is one whose row is no
/// longer in the base table. For example, with results [k1, k2, k3] and rows [k1, k3], row k1 is
/// matched to result 0 and row k3 to result 2; result 1 is skipped.
///
/// Matching compares primary keys, so it only works if `slice` includes the key columns. Pass
/// `external_results` as nullptr to skip matching when the selection does not read the key columns.
std::vector<joined_row> join_table_results(const query::result& table_results, const query::partition_slice& slice, const schema& schema,
        const selection::selection& selection, const vector_search::vector_store_client::primary_keys* external_results);

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
class external_search_provider : public cql3::selection::external_values_provider {
    const vector_search::vector_store_client::primary_keys& _results;
    mutable size_t _next_result;    // cursor into _results: which entry to match next
    const size_t _score_slot;       // temporary slot the score is written to
    const schema& _schema;

public:
    external_search_provider(const vector_search::vector_store_client::primary_keys& results, size_t score_slot, const schema& schema);

    bool try_fill(std::vector<cql3::raw_value>& temporaries, std::span<const bytes> partition_key, std::span<const bytes> clustering_key,
            const query::result_row_view& static_row, const query::result_row_view* row) const override;
};

} // namespace cql3::statements::external_search
