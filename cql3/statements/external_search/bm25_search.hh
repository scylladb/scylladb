/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/expr/expression.hh"
#include "cql3/statements/external_search/values_provider.hh"
#include "data_dictionary/data_dictionary.hh"
#include "index/secondary_index.hh"
#include "schema/schema.hh"
#include "vector_search/vector_store_client.hh"

#include <seastar/core/future.hh>

#include <optional>
#include <span>

/// The parts of running a full-text search that are specific to it: which index serves it, how the
/// query value is read, which relation on it is accepted, and how excerpts are fetched. The
/// statement running the search decides when to ask and what to do with the rows.
namespace cql3::statements::bm25_search {

/// The full-text index that serves searches on `column`.
secondary_index::index index_for(data_dictionary::database db, const schema_ptr& schema, const column_definition& column);

/// The search term an evaluated query value holds.
sstring query_term(const cql3::raw_value& value);

/// Checks the form of the one relation a full-text search takes, WHERE BM25(column, term) > 0. The
/// plan compares the term with the search's.
void validate_restriction(const expr::binary_operator& binop);

/// Asks the index for a highlighted excerpt of every row's text, and returns the excerpts as the
/// values of the highlight temporary: one per joined row, in the order the rows are emitted.
///
/// The text of each row is `row.columns[column]`. The texts are sent in one request, and the reply
/// is an array of the same length: reply[i] is the excerpt of the i-th row sent. A row is not sent
/// if it is dropped, its excerpt being thrown away with the row, or if it has no text, there being
/// nothing to find an excerpt in; either gets a null. A row the search did not return has no text,
/// since the join leaves it out (see join_table_results()). A row the index found no excerpt in
/// gets a null and is kept. If the request fails, the query fails.
seastar::future<std::vector<cql3::raw_value>> highlights_of(vector_search::vector_store_client& client, const schema& schema,
        const secondary_index::index& index, const sstring& search_term, std::span<const external_search::joined_row> rows, size_t column,
        seastar::abort_source& as);

} // namespace cql3::statements::bm25_search
