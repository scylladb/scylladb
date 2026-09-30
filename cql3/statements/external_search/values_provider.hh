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
#include "vector_search/vector_store_client.hh"

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
    /// Index of the external result naming this row, or nothing when no result does or the rows
    /// were not matched.
    std::optional<size_t> external_result;
    /// True if the row is left out of the result set; see join_table_results().
    bool dropped = false;
    /// The values of the columns the join was asked to read out of the row, in the order asked.
    std::vector<managed_bytes_opt> columns;
};

/// Walks the rows just read from the base table, one joined_row per row, reading `columns` out of
/// every row and, when `external_results` is given, matching each to the external result that
/// names it. The walk visits exactly the
/// rows the result set is built from, in the same order; `slice` must be the slice
/// `table_results` were read with.
///
/// `table_results` must hold the rows in the order of `external_results` - query_base_table()
/// reads them that way - so matching walks both forward at once, comparing primary keys, and a
/// result stepped over is one whose row the base table no longer has. With results [k1, k2, k3]
/// and rows [k1, k3] the joined rows are [{external_result = 0}, {external_result = 2}].
///
/// A row the matching leaves with no similarity to report - one no result names, or one whose
/// result scored it with something that is not a finite number - is marked dropped as it is built.
///
/// Matching compares primary keys, so `slice` must include the key columns; it asserts if they are
/// missing. A null `external_results` skips matching, leaving every row unnamed and none dropped.
///
/// Each column of `columns` is read from where the row keeps it: a key column from the key, any
/// other from the cells `slice` asked for, which must therefore include it; it asserts if it does
/// not, a column nobody asked for having nothing to read. A row with no value
/// for a column - a regular column of a partition holding nothing but a static row, say - gets an
/// absent value. Nothing is deserialized.
std::vector<joined_row> join_table_results(const query::result& table_results, const query::partition_slice& slice, const schema& schema,
        const vector_search::vector_store_client::primary_keys* external_results, std::span<const column_definition* const> columns);

/// The similarity of each joined row, as the values of one temporary: null for a row that has none
/// to report (see join_table_results()).
std::vector<cql3::raw_value> similarities_of(
        std::span<const joined_row> rows, const vector_search::vector_store_client::primary_keys& external_results);

/// The rank of each joined row, as the values of one temporary: the position of the row's external
/// result in the response, counted from 1. Null for the same rows similarities_of() gives null.
std::vector<cql3::raw_value> ranks_of(
        std::span<const joined_row> rows, const vector_search::vector_store_client::primary_keys& external_results);

/// One temporary and the value every row is given under it, in the order the rows are emitted.
struct external_values {
    size_t temporary_index;
    std::vector<cql3::raw_value> values;
};

/// The values of one search's temporaries, filled from the joined rows: the similarity of each row
/// under `temporaries.score` and its rank under `temporaries.rank`, each only if allocated.
std::vector<external_values> search_values_of(const search_temporaries& temporaries, std::span<const joined_row> rows,
        const vector_search::vector_store_client::primary_keys& external_results);

/// Hands each row of a search's result set the values already read off the index's response, in
/// the order the rows are offered. Single-use: it cannot be rewound.
class values_provider final : public cql3::selection::external_values_provider {
    std::vector<external_values> _values;
    std::vector<bool> _dropped;
    mutable size_t _next_row = 0;

public:
    /// Copies each row's `dropped` flag; `rows` is not retained.
    values_provider(std::vector<external_values> values, std::span<const joined_row> rows);

    bool try_fill(std::vector<cql3::raw_value>& temporaries) const override;
};

} // namespace cql3::statements::external_search
