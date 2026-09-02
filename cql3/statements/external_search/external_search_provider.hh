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
    /// True if the row is left out of the result set. Once set, nothing clears it: for example,
    /// a row dropped because it has no score stays dropped when the highlights are computed.
    bool dropped = false;
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

/// Marks dropped every row that has no similarity to report: one no external result names, or one
/// scored with something that is not a number.
void drop_unscored_rows(std::span<joined_row> rows, const vector_search::vector_store_client::primary_keys& external_results);

/// The similarity of each joined row, as the values of one temporary: null for a row that has none
/// to report (see drop_unscored_rows()) or is already dropped.
std::vector<cql3::raw_value> similarities_of(
        std::span<const joined_row> rows, const vector_search::vector_store_client::primary_keys& external_results);

/// One temporary and the value every row is given under it, in the order the rows are emitted.
struct external_values {
    size_t temporary_index;
    std::vector<cql3::raw_value> values;
};

/// The values an external search injects into the rows of its result set, handed out in the order
/// the rows are offered. Single-use: it cannot be rewound, which will matter when paging arrives.
class external_search_provider final : public cql3::selection::external_values_provider {
    std::vector<external_values> _values;
    std::vector<bool> _dropped;
    mutable size_t _next_row = 0;

public:
    /// Copies each row's `dropped` flag; `rows` is not retained.
    external_search_provider(std::vector<external_values> values, std::span<const joined_row> rows);

    bool try_fill(std::vector<cql3::raw_value>& temporaries) const override;
};

} // namespace cql3::statements::external_search
