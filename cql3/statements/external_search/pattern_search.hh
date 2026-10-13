/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/statements/external_search/external_search_plan.hh"
#include "data_dictionary/data_dictionary.hh"
#include "index/secondary_index.hh"
#include "schema/schema.hh"

#include <optional>

namespace cql3::restrictions {
class select_restrictions;
}

/// The parts of running a pattern search that are specific to it. Unlike ANN and BM25, a pattern
/// search:
/// - is introduced by WHERE, a LIKE on a pattern-indexed column, rather than by ORDER BY,
/// - is never combined with another search.
namespace cql3::statements::pattern_search {

/// The pattern index on a column restricted by LIKE in the unprepared WHERE clause, if any.
std::optional<secondary_index::index> index_for(data_dictionary::database db, const schema_ptr& schema,
        const expr::expression& where_clause);

/// The search to run on the index. Rejects a query that:
/// - restricts anything but a single LIKE, or
/// - has ORDER BY, since the rows come back unordered.
search_source source_for(secondary_index::index index, const schema& schema, const restrictions::select_restrictions& restrictions,
        bool has_ordering);

/// The pattern an evaluated query value holds.
sstring query_pattern(const cql3::raw_value& value);

} // namespace cql3::statements::pattern_search
