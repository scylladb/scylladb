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

namespace cql3::statements {

/// One row of a base-table read, as an external search sees it.
struct joined_row {
    /// Where in the external results the answer naming this row sits, or nothing when none does.
    std::optional<size_t> answer;
};

/// Joins an external search's answers to the rows just read from the base table.
///
/// Answers are matched by primary key with a cursor that only moves forward, the results being
/// merged in the index's primary-key order: an answer stepped over names a key the index still knows
/// about but that is no longer in the base table. Only the keys of `external_results` are read.
///
/// `table_results` must be read with `slice`, the slice they were read with: what this produces is
/// handed back out by position, and only that slice reproduces the walk the result set is built by.
/// The keys are in the read because external_search::fetch_primary_key_columns() asked for them.
std::vector<joined_row> join_table_results(const query::result& table_results, const query::partition_slice& slice, const schema& schema,
        const vector_search::vector_store_client::primary_keys& external_results);

/// The relevance each joined row was given, as the values of one temporary.
///
/// A row the index no longer names, or one it scored with something that is not a number, has no
/// relevance to report and is marked in `dropped`; a row already marked there is passed over and
/// given no value. `dropped` must have one entry per row.
std::vector<cql3::raw_value> similarities_of(std::span<const joined_row> rows,
        const vector_search::vector_store_client::primary_keys& external_results, std::vector<bool>& dropped);

/// One temporary and the value every row is given under it, in the order the rows are emitted.
struct external_values {
    size_t temporary_index;
    std::vector<cql3::raw_value> values;
};

/// The values an external search injects into the rows of its result set, computed before the result
/// set is built: it hands out each row's values in the order the rows are offered to it, and says
/// which rows to leave out.
///
/// Single-use: it cannot be rewound, which is worth keeping in mind when paging arrives.
class external_search_provider final : public cql3::selection::external_values_provider {
    std::vector<external_values> _values;
    std::vector<bool> _dropped;
    mutable size_t _next_row = 0;

public:
    external_search_provider(std::vector<external_values> values, std::vector<bool> dropped);

    bool try_fill(std::vector<cql3::raw_value>& temporaries, std::span<const bytes> partition_key, std::span<const bytes> clustering_key,
            const query::result_row_view& static_row, const query::result_row_view* row) const override;
};

} // namespace cql3::statements
