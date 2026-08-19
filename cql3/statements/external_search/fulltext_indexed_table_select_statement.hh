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

/// A SELECT occurrence's search term that prepare could not tell apart from the ORDER BY term,
/// together with the name of the function it was written in, so that execution can name that
/// function when the two turn out to differ.
struct deferred_select_term {
    expr::expression term;
    std::string_view function_name;
};

/// Which of a full-text search's values a SELECT occurrence names.  Both are read from the one
/// search the rows are ranked by and validated by the one set of rules; they differ only in where
/// the value comes from and in what its slot holds.
enum class bm25_value {
    /// The relevance score, which the index reports along with the row's key.
    score,
    /// A fragment of the row's own text with the matched terms marked, which only exists once the
    /// rows have been read and a second request has asked the index for it.
    fragment,
};

/// The name the given value is called by in CQL, for matching a call and for naming it in a
/// rejection message.
std::string_view function_name_of(bm25_value value);

struct bm25_ordering_info {
    secondary_index::index index;
    expr::expression search_term;
    // Temporary slot the score is delivered in, allocated on the first bm25()
    // occurrence in SELECT and filled per row by external_score_provider.
    std::optional<size_t> score_temporary_index;
    // Temporary slot the fragment is delivered in, allocated on the first bm25_highlight()
    // occurrence in SELECT and filled per row from the second request's answer.
    std::optional<size_t> highlight_temporary_index;
    // The column the fragment is generated from, recorded when a fragment is selected: its text has
    // to be read from every row and sent to the index.  Always the column the rows are ranked by.
    const column_definition* highlighted_column = nullptr;
    // The search terms of the SELECT occurrences, and of the WHERE one, that prepare could not tell
    // apart from the ORDER BY term - a bind marker stands where at least one of the two values will
    // be. Checked once they have values; left empty where prepare has already settled it.
    std::vector<deferred_select_term> deferred_select_terms;
    std::optional<expr::expression> deferred_where_term;
};

/// Resolves BM25 ordering metadata from the query's prepared ORDER BY call.
/// Returns std::nullopt if the call is not a native bm25() call, i.e. this is not an FTS query.
std::optional<bm25_ordering_info> get_bm25_ordering_info(
        data_dictionary::database db,
        schema_ptr schema,
        const expr::function_call& fc);

/// Lowers every call to a full-text search's values in the SELECT clause - bm25() and
/// bm25_highlight(), nested occurrences included - to the slot that value is delivered in,
/// allocating it on the first occurrence of each, and rejects an occurrence that does not agree with
/// the WHERE and ORDER BY clauses on the column and the search term, or that has no such clauses to
/// agree with.  A term whose agreement only execution can settle is recorded in ordering_info for it
/// to check.  Returns whether any slot was allocated, i.e. whether the clause named either value.
bool prepare_bm25_selectors(std::vector<selection::prepared_selector>& prepared_selectors, std::optional<bm25_ordering_info>& ordering_info,
        expr::temporary_allocator& temporaries_allocator);

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
