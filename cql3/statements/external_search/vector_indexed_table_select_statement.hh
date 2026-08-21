/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/statements/external_search/external_index_select_statement.hh"
#include "cql3/statements/external_search/filter.hh"
#include "cql3/expr/temporary_allocator.hh"

#include <optional>

namespace cql3::statements {

/// ANN ordering metadata resolved during prepare.
struct ann_ordering_info {
    secondary_index::index index;
    raw::select_statement::prepared_ann_ordering_type prepared_ann_ordering;
    bool is_rescoring_enabled;
    /// Temporary slot the Vector Store's own score is delivered in, allocated on the first ann()
    /// occurrence in SELECT and filled per row by external_search_provider.
    std::optional<size_t> temporary_index;
    /// The query vectors of the SELECT occurrences that prepare could not tell apart from the
    /// ordering's - a bind marker stands where at least one of the two values will be.  Compared once
    /// they have values; left empty where prepare has already settled it.
    std::vector<expr::expression> deferred_select_vectors;
};

/// Resolves ANN ordering metadata from the query's prepared ORDER BY call.
/// Returns std::nullopt if the call is not a native ann() call, i.e. this is not an ANN query.
std::optional<ann_ordering_info> get_ann_ordering_info(
        data_dictionary::database db,
        schema_ptr schema,
        const expr::function_call& fc);

/// Lowers every ANN() call in the SELECT clause, nested occurrences included, to the expression that
/// yields the score of the row it is reported for: the slot the score the Vector Store reported is
/// delivered in, or, when the index rescores, the similarity the coordinator recomputes - which is
/// what it ranks the rows by, so reporting anything else would disagree with the order.  Rejects an
/// occurrence that does not agree with the ordering on the column and the query vector, or that has
/// no ANN ordering to agree with.  A query vector whose agreement only execution can settle is
/// recorded in ordering_info for it to check.
///
/// Returns whether the SELECT clause had any such call, which the caller cannot see afterwards: what
/// replaced it says nothing about the score it stands for.
bool prepare_ann_selectors(std::vector<selection::prepared_selector>& prepared_selectors,
        std::optional<ann_ordering_info>& ordering_info, expr::temporary_allocator& temporaries_allocator,
        data_dictionary::database db, const schema_ptr& schema);

/// The order the rows come back in when the index rescores.  The Vector Store returned them ordered
/// by the score it reported, which is not the requested order then: the coordinator recomputes the
/// similarity from the stored vectors and has to sort by that.
///
/// Sorting reads a column of the result row, so this appends one trailing selector holding the
/// recomputed similarity - the column the returned comparator sorts by, and the one the caller has
/// to hide from the client.
select_statement::ordering_comparator_type rescored_similarity_ordering(
        std::vector<selection::prepared_selector>& prepared_selectors,
        const ann_ordering_info& ann_ordering_info,
        data_dictionary::database db,
        schema_ptr schema);

class vector_indexed_table_select_statement : public external_index_select_statement {
    ann_ordering_info _ann_ordering_info;
    external_search::prepared_filter _prepared_filter;

public:
    static constexpr size_t max_ann_query_limit = 1000;

    static ::shared_ptr<cql3::statements::select_statement> prepare(data_dictionary::database db, schema_ptr schema, uint32_t bound_terms,
            lw_shared_ptr<const parameters> parameters, ::shared_ptr<selection::selection> selection,
            ::shared_ptr<const restrictions::statement_restrictions> restrictions, ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed,
            ordering_comparator_type ordering_comparator, std::optional<expr::expression> limit,
            std::optional<expr::expression> per_partition_limit, cql_stats& stats, ann_ordering_info ordering_info, std::unique_ptr<cql3::attributes> attrs);

    vector_indexed_table_select_statement(schema_ptr schema, uint32_t bound_terms, lw_shared_ptr<const parameters> parameters,
            ::shared_ptr<selection::selection> selection, ::shared_ptr<const restrictions::statement_restrictions> restrictions,
            ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed, ordering_comparator_type ordering_comparator,
            std::optional<expr::expression> limit, std::optional<expr::expression> per_partition_limit,
            cql_stats& stats, ann_ordering_info ordering_info, external_search::prepared_filter prepared_filter, std::unique_ptr<cql3::attributes> attrs);

private:
    std::string_view index_search_type_name() const override {
        return "Vector Search";
    }

    future<::shared_ptr<cql_transport::messages::result_message>> execute_search(
            query_processor& qp, service::query_state& state, const query_options& options, uint64_t limit) const override;
};

} // namespace cql3::statements
