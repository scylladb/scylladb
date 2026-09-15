/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "external_index_select_statement.hh"
#include "cql3/statements/external_search/external_function.hh"
#include "cql3/expr/temporary_allocator.hh"

#include <optional>
#include <string_view>

namespace cql3::statements {

/// A search term written in a SELECT call that prepare could not prove equal to the ORDER BY term,
/// because a bind marker is involved. Execution compares the bound values; `function_name` is the
/// function the call was written with, for the error message.
struct deferred_select_term {
    expr::expression term;
    sstring function_name;
};

struct bm25_ordering_info {
    secondary_index::index index;
    expr::expression search_term;
    // Temporaries holding the score and the rank; see external_search::search_temporaries. BM25()
    // is replaced with a tuple of the two, so it has no temporary of its own.
    external_search::search_temporaries temporaries;
    // The SELECT occurrences' search terms that only execution can compare, a bind marker standing
    // where at least one of the two values will be.
    std::vector<deferred_select_term> deferred_select_terms;
    // The WHERE clause's term, likewise.
    std::optional<expr::expression> deferred_where_term;
};

class fulltext_indexed_table_select_statement : public external_index_select_statement {
    bm25_ordering_info _bm25_ordering_info;

public:
    static constexpr size_t max_fts_query_limit = 1000;
    static ::shared_ptr<cql3::statements::select_statement> prepare(data_dictionary::database db,
            schema_ptr schema,
            uint32_t bound_terms,
            lw_shared_ptr<const parameters> parameters,
            ::shared_ptr<selection::selection> selection,
            ::shared_ptr<const restrictions::statement_restrictions> restrictions,
            ::shared_ptr<std::vector<size_t>> group_by_cell_indices,
            bool is_reversed,
            ordering_comparator_type ordering_comparator,
            std::optional<expr::expression> limit,
            std::optional<expr::expression> per_partition_limit,
            cql_stats& stats,
            std::optional<bm25_ordering_info> ordering_info,
            std::unique_ptr<cql3::attributes> attrs);

    fulltext_indexed_table_select_statement(schema_ptr schema, uint32_t bound_terms, lw_shared_ptr<const parameters> parameters,
            ::shared_ptr<selection::selection> selection, ::shared_ptr<const restrictions::statement_restrictions> restrictions,
            ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed, ordering_comparator_type ordering_comparator,
            std::optional<expr::expression> limit, std::optional<expr::expression> per_partition_limit, cql_stats& stats, bm25_ordering_info ordering_info,
            std::unique_ptr<cql3::attributes> attrs);

private:
    std::string_view index_search_type_name() const override {
        return "Full-Text Search";
    }

    future<::shared_ptr<cql_transport::messages::result_message>> execute_search(
            query_processor& qp, service::query_state& state, const query_options& options, uint64_t limit) const override;
};

} // namespace cql3::statements
