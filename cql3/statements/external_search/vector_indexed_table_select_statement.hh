/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/statements/external_search/external_index_select_statement.hh"
#include "cql3/statements/external_search/filter.hh"

#include <optional>
namespace cql3::functions { class vector_similarity_fct; }

namespace cql3::statements {

/// ANN ordering metadata resolved during prepare.
struct ann_ordering_info {
    secondary_index::index _index;
    raw::select_statement::prepared_ann_ordering_type _prepared_ann_ordering;
    bool is_rescoring_enabled;
};

/// Resolves ANN ordering metadata from the query's ORDER BY clause.
/// Returns std::nullopt if the query is not an ANN query.
std::optional<ann_ordering_info> get_ann_ordering_info(
        data_dictionary::database db,
        schema_ptr schema,
        lw_shared_ptr<const raw::select_statement::parameters> parameters,
        prepare_context& ctx);

/// Adds a similarity function call to prepared_selectors based on the ANN index.
/// Returns the index of the appended selector within prepared_selectors.
uint32_t add_similarity_function_to_selectors(
        std::vector<selection::prepared_selector>& prepared_selectors,
        const ann_ordering_info& ann_ordering_info,
        data_dictionary::database db,
        schema_ptr schema);

/// Builds an ordering comparator that sorts by descending similarity score.
select_statement::ordering_comparator_type get_similarity_ordering_comparator(
        std::vector<selection::prepared_selector>& prepared_selectors,
        uint32_t similarity_column_index);

class vector_indexed_table_select_statement : public external_index_select_statement {
public:
    /// Aggregates all rescoring-related state resolved at prepare time.
    struct rescoring_config {
        /// Non-null when rescoring is enabled.
        seastar::shared_ptr<cql3::functions::vector_similarity_fct> function;
        /// Storage class of the indexed column.
        column_kind indexed_col_kind = column_kind::regular_column;
        /// Unified index into the relevant data source for the indexed column:
        ///  - partition_key / clustering_key: component_index() into the exploded key span.
        ///  - static_column / regular_column: offset among same-kind columns in selection order
        ///    (PK/CK columns do not consume result_row_view iterator slots).
        size_t index = 0;

        static rescoring_config make(const secondary_index::index& index,
                                     const column_definition* indexed_column,
                                     const cql3::selection::selection& sel);

        bool is_enabled() const { return bool(function); }

        std::unique_ptr<cql3::selection::temporaries_provider>
        make_similarity_provider(
                const cql3::query_options& options,
                const cql3::expr::expression& ann_vector_expr,
                size_t similarity_temporary_index) const;
    };

private:
    prepared_ann_ordering_type _prepared_ann_ordering;
    external_search::prepared_filter _prepared_filter;

public:
    static constexpr size_t max_ann_query_limit = 1000;

    static ::shared_ptr<cql3::statements::select_statement> prepare(data_dictionary::database db, schema_ptr schema, uint32_t bound_terms,
            lw_shared_ptr<const parameters> parameters, ::shared_ptr<selection::selection> selection,
            ::shared_ptr<const restrictions::statement_restrictions> restrictions, ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed,
            ordering_comparator_type ordering_comparator, prepared_ann_ordering_type prepared_ann_ordering, std::optional<expr::expression> limit,
            std::optional<expr::expression> per_partition_limit, cql_stats& stats, const secondary_index::index& index, std::unique_ptr<cql3::attributes> attrs);

    vector_indexed_table_select_statement(schema_ptr schema, uint32_t bound_terms, lw_shared_ptr<const parameters> parameters,
            ::shared_ptr<selection::selection> selection, ::shared_ptr<const restrictions::statement_restrictions> restrictions,
            ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed, ordering_comparator_type ordering_comparator,
            prepared_ann_ordering_type prepared_ann_ordering, std::optional<expr::expression> limit, std::optional<expr::expression> per_partition_limit,
            cql_stats& stats, const secondary_index::index& index, external_search::prepared_filter prepared_filter, std::unique_ptr<cql3::attributes> attrs);

private:
    std::string_view index_search_type_name() const override {
        return "Vector Search";
    }

    future<::shared_ptr<cql_transport::messages::result_message>> execute_search(
            query_processor& qp, service::query_state& state, const query_options& options, uint64_t limit) const override;
};

} // namespace cql3::statements
