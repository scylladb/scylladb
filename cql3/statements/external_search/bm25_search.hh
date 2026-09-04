/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/expr/expression.hh"
#include "cql3/statements/external_search/external_search_provider.hh"
#include "index/secondary_index.hh"
#include "schema/schema_fwd.hh"
#include "vector_search/vector_store_client.hh"

#include <seastar/core/future.hh>

#include <optional>
#include <span>

/// The parts of running a full-text search that are specific to it: how the query value is read,
/// how the index is asked, what a relation on it may say, and how excerpts are fetched. The
/// statement running the search decides when to ask and what to do with the rows.
namespace cql3::statements::bm25_search {

/// The search term an evaluated query value holds.
sstring query_term(const cql3::raw_value& value);

/// Asks the index for the rows matching the term. A failed request fails the query.
seastar::future<vector_search::vector_store_client::primary_keys> ask(vector_search::vector_store_client& client,
        const sstring& keyspace, const sstring& index_name, schema_ptr schema, const sstring& term, uint64_t wanted,
        seastar::abort_source& as);

/// Checks the one relation a full-text search takes, WHERE BM25(column, term) > 0, against the
/// search the rows are ranked by. Returns the WHERE term when a bind marker leaves the comparison
/// with the ORDER BY term to execution.
std::optional<expr::expression> validate_restriction(const expr::binary_operator& binop, const secondary_index::index& index,
        const expr::expression& search_term);

/// Asks the index for a highlighted excerpt of every row's text, and returns the excerpts as the
/// values of the highlight temporary: one per joined row, in the order the rows are emitted.
///
/// The text of each row is `row.columns[column]`. All texts are sent in one request, and the reply
/// is an array of the same length. A row with no text is sent as an empty string, so that the
/// positions still line up. A row the index found no excerpt in gets a null and is kept. If the
/// request fails, the query fails.
seastar::future<std::vector<cql3::raw_value>> highlights_of(vector_search::vector_store_client& client, const schema& schema,
        const secondary_index::index& index, const sstring& search_term, std::span<const external_search::joined_row> rows, size_t column,
        seastar::abort_source& as);

} // namespace cql3::statements::bm25_search
