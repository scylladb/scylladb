/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/expr/expression.hh"
#include "cql3/statements/external_search/values_provider.hh"
#include "index/secondary_index.hh"
#include "schema/schema.hh"
#include "vector_search/vector_store_client.hh"

#include <optional>

/// The parts of running a full-text search that are specific to it: how the query value is read and
/// which relation on it is accepted. The statement running the search decides when to ask and what
/// to do with the rows.
namespace cql3::statements::bm25_search {

/// The search term an evaluated query value holds.
sstring query_term(const cql3::raw_value& value);

/// Checks the one relation a full-text search takes, WHERE BM25(column, term) > 0. Returns the
/// WHERE term when a bind marker leaves its comparison with the ORDER BY term to execution.
std::optional<expr::expression> validate_restriction(const expr::binary_operator& binop, const expr::expression& search_term);

} // namespace cql3::statements::bm25_search
