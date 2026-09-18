/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/statements/external_search/fulltext_indexed_table_select_statement.hh"

namespace cql3::statements {

/// Resolves BM25 ordering metadata from the query's prepared ORDER BY call.
/// Returns std::nullopt if the call is not a native bm25() call, i.e. this is not an FTS query.
std::optional<bm25_ordering_info> get_bm25_ordering_info(
        data_dictionary::database db,
        schema_ptr schema,
        const expr::function_call& fc);

/// Replaces every BM25(), BM25_SCORE() and BM25_RANK() call in the SELECT clause, nested
/// occurrences included, with a read of the temporary holding that value, allocating the temporary
/// on the first occurrence of each. Rejects an occurrence with no BM25 ordering and WHERE clause to
/// agree with, or one that disagrees with them on the column or the search term; a disagreement
/// only execution can settle is recorded in ordering_info for it to check.
void prepare_bm25_selectors(std::vector<selection::prepared_selector>& prepared_selectors, std::optional<bm25_ordering_info>& ordering_info,
        expr::temporary_allocator& temporaries_allocator, prepare_context& ctx);

} // namespace cql3::statements
