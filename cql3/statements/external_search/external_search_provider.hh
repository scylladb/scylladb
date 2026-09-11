/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/selection/selection.hh"
#include "cql3/statements/external_search/external_function.hh"
#include "cql3/values.hh"
#include "utils/managed_bytes.hh"
#include "vector_search/hybrid_search.hh"

#include <optional>
#include <span>
#include <vector>

class schema;
class column_definition;

namespace query {
class result;
class partition_slice;
}

namespace cql3::statements::external_search {

struct joined_row {
    /// Index of the candidate naming this row, or nothing where no candidate does or the rows were
    /// not matched.
    std::optional<size_t> candidate;
    /// True if the row is left out of the result set; see drop_unscored_rows().
    bool dropped = false;
    /// The values of the columns the join was asked to read out of the row, in the order asked.
    std::vector<managed_bytes_opt> columns;
};

/// Walks the rows just read from the base table, one joined_row per row, reading `columns` out of
/// every row and, when `candidates` is given, matching each row to the candidate that names it. The
/// rows are walked with result_set_builder::visitor, the same visitor the result set is later built
/// with, so the joined rows are exactly the rows of the result set, in order; `slice` must be the
/// slice `table_results` were read with.
///
/// The candidates are indexed by primary key and every row is looked up among them. A key a search
/// returned but the base table no longer has is simply not found.
///
/// Matching compares primary keys, so it only works if `slice` includes the key columns. Pass no
/// candidates to skip matching when the selection does not read the key columns.
std::vector<joined_row> join_table_results(const query::result& table_results, const query::partition_slice& slice, const schema& schema,
        const selection::selection& selection, const std::vector<vector_search::hybrid_candidate>* candidates,
        std::span<const column_definition* const> columns);

/// Marks dropped every row no search has a score for: one no candidate names, or one whose candidate
/// no search hit. A row some search scores is kept, with nulls for the others. Given no candidates,
/// nothing is dropped.
void drop_unscored_rows(std::span<joined_row> rows, const std::vector<vector_search::hybrid_candidate>* candidates);

/// The score one search gave each joined row, as the values of one temporary: null for a row that
/// search has no hit for (see drop_unscored_rows()).
std::vector<cql3::raw_value> scores_of(
        std::span<const joined_row> rows, size_t search, const std::vector<vector_search::hybrid_candidate>& candidates);

/// The rank one search gave each joined row, as the values of one temporary: the position of the
/// row's key in that search's answer, counted from 1. Null for the same rows scores_of() gives null.
std::vector<cql3::raw_value> ranks_of(
        std::span<const joined_row> rows, size_t search, const std::vector<vector_search::hybrid_candidate>& candidates);

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
