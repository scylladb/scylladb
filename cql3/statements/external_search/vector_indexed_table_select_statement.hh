/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/statements/select_statement.hh"
#include "vector_search/vector_store_client.hh"
#include "vector_search/filter.hh"

#include <optional>

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

class vector_indexed_table_select_statement : public select_statement {
    secondary_index::index _index;
    prepared_ann_ordering_type _prepared_ann_ordering;
    vector_search::prepared_filter _prepared_filter;
    mutable gc_clock::time_point _query_start_time_point;

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
            cql_stats& stats, const secondary_index::index& index, vector_search::prepared_filter prepared_filter, std::unique_ptr<cql3::attributes> attrs);

private:
    future<::shared_ptr<cql_transport::messages::result_message>> do_execute(
            query_processor& qp, service::query_state& state, const query_options& options) const override;

    void update_stats() const;

    lw_shared_ptr<query::read_command> prepare_command_for_base_query(query_processor& qp, service::query_state& state, const query_options& options, uint64_t fetch_limit) const;

    std::vector<float> get_ann_ordering_vector(const query_options& options) const;

    future<::shared_ptr<cql_transport::messages::result_message>> query_base_table(query_processor& qp, service::query_state& state,
            const query_options& options, const std::vector<vector_search::primary_key>& pkeys, lowres_clock::time_point timeout) const;

    future<coordinator_result<foreign_ptr<lw_shared_ptr<query::result>>>> query_base_table(query_processor& qp, service::query_state& state,
            const query_options& options, lw_shared_ptr<query::read_command> command, lowres_clock::time_point timeout,
            const std::vector<vector_search::primary_key>& pkeys) const;

    future<coordinator_result<foreign_ptr<lw_shared_ptr<query::result>>>> query_base_table(query_processor& qp, service::query_state& state,
            const query_options& options, lw_shared_ptr<query::read_command> command, lowres_clock::time_point timeout,
            std::vector<dht::partition_range> partition_ranges) const;
};

} // namespace cql3::statements
