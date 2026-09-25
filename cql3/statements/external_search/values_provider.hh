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
#include "vector_search/hybrid_search.hh"

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
    /// Index of the candidate naming this row, or nothing where no candidate does or the rows were
    /// not matched.
    std::optional<size_t> candidate;
    /// True if the row is left out of the result set; see join_table_results().
    bool dropped = false;
};

/// Walks the rows just read from the base table, in the order the result set is built from them,
/// matching each row to the candidate that names it when `candidates` is given. `slice` must be the
/// slice `table_results` were read with.
///
/// `table_results` must hold the rows in the order of `candidates`, as query_base_table() reads
/// them, so matching walks both forward at once comparing primary keys; a candidate stepped over is
/// one whose row the base table no longer has. A row no search has a score for is marked dropped.
///
/// Matching needs the key columns in `slice`, which is asserted. A null `candidates` skips
/// matching, leaving every row unnamed and none dropped. Nothing is deserialized.
std::vector<joined_row> join_table_results(const query::result& table_results, const query::partition_slice& slice, const schema& schema,
        const std::vector<vector_search::search_candidate>* candidates);

/// The score one search gave each joined row, null where it has no hit for the row.
std::vector<cql3::raw_value> scores_of(
        std::span<const joined_row> rows, size_t search, const std::vector<vector_search::search_candidate>& candidates);

/// The rank one search gave each joined row, counted from 1, null where it has no hit for the row.
std::vector<cql3::raw_value> ranks_of(
        std::span<const joined_row> rows, size_t search, const std::vector<vector_search::search_candidate>& candidates);

/// One temporary and the value every row is given under it, in the order the rows are emitted.
struct external_values {
    size_t temporary_index;
    std::vector<cql3::raw_value> values;
};

/// The values of one search's temporaries, filled from the joined rows: the score of each row under
/// `temporaries.score` and its rank under `temporaries.rank`, each only if allocated.
std::vector<external_values> search_values_of(const search_temporaries& temporaries, std::span<const joined_row> rows, size_t search,
        const std::vector<vector_search::search_candidate>& candidates);

/// Hands each row its values, in the order the rows are offered. Single-use: it cannot be rewound.
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
