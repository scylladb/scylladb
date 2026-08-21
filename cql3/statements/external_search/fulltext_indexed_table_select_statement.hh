/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "external_index_select_statement.hh"
#include "cql3/expr/temporary_allocator.hh"

#include <optional>

namespace cql3::statements {

struct bm25_ordering_info {
    secondary_index::index index;
    expr::expression search_term;
    // Temporary slot the score is delivered in, allocated on the first bm25()
    // occurrence in SELECT and filled per row by external_score_provider.
    std::optional<size_t> temporary_index;
    // The SELECT occurrences' search terms that only execution can compare, a bind marker standing
    // where at least one of the two values will be.
    std::vector<expr::expression> deferred_select_terms;
    // The WHERE clause's term, likewise.
    std::optional<expr::expression> deferred_where_term;
};

/// Resolves BM25 ordering metadata from the query's prepared ORDER BY call.
/// Returns std::nullopt if the call is not a native bm25() call, i.e. this is not an FTS query.
std::optional<bm25_ordering_info> get_bm25_ordering_info(
        data_dictionary::database db,
        schema_ptr schema,
        const expr::function_call& fc);

/// Lowers every bm25() call in the SELECT clause, nested occurrences included, to the slot the
/// row's score is delivered in, allocating it on the first one.  Rejects an occurrence with no
/// BM25 ordering and WHERE clause to agree with, or one that disagrees with them on the column or
/// the search term; a disagreement only execution can settle is recorded in ordering_info for it
/// to check.
void prepare_bm25_selectors(std::vector<selection::prepared_selector>& prepared_selectors, std::optional<bm25_ordering_info>& ordering_info,
        expr::temporary_allocator& temporaries_allocator, prepare_context& ctx);

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
