/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

 #pragma once

 #include "select_statement.hh"

namespace cql3 {
namespace statements {

class view_indexed_table_select_statement : public select_statement {
    secondary_index::index _index;
    expr::expression _used_index_restrictions;
    schema_ptr _view_schema;
    noncopyable_function<dht::partition_range_vector(const query_options&)> _get_partition_ranges_for_posting_list;
    noncopyable_function<query::partition_slice(const query_options&)> _get_partition_slice_for_posting_list;
public:
    static constexpr size_t max_base_table_query_concurrency = 4096;

    static ::shared_ptr<cql3::statements::select_statement> prepare(data_dictionary::database db,
                                                                    schema_ptr schema,
                                                                    uint32_t bound_terms,
                                                                    lw_shared_ptr<const parameters> parameters,
                                                                    ::shared_ptr<selection::selection> selection,
                                                                    ::shared_ptr<restrictions::statement_restrictions> restrictions,
                                                                    ::shared_ptr<std::vector<size_t>> group_by_cell_indices,
                                                                    bool is_reversed,
                                                                    ordering_comparator_type ordering_comparator,
                                                                    std::optional<expr::expression> limit,
                                                                    std::optional<expr::expression> per_partition_limit,
                                                                    cql_stats &stats,
                                                                    std::unique_ptr<cql3::attributes> attrs);

    view_indexed_table_select_statement(schema_ptr schema,
                                   uint32_t bound_terms,
                                   lw_shared_ptr<const parameters> parameters,
                                   ::shared_ptr<selection::selection> selection,
                                   ::shared_ptr<const restrictions::statement_restrictions> restrictions,
                                   ::shared_ptr<std::vector<size_t>> group_by_cell_indices,
                                   bool is_reversed,
                                   ordering_comparator_type ordering_comparator,
                                   std::optional<expr::expression> limit,
                                   std::optional<expr::expression> per_partition_limit,
                                   cql_stats &stats,
                                   const secondary_index::index& index,
                                   expr::expression used_index_restrictions,
                                   schema_ptr view_schema,
                                   std::unique_ptr<cql3::attributes> attrs);

private:
    virtual future<::shared_ptr<cql_transport::messages::result_message>> do_execute(query_processor& qp,
            service::query_state& state, const query_options& options) const override;
            
    future<::shared_ptr<cql_transport::messages::result_message>> actually_do_execute(query_processor& qp,
            service::query_state& state, const query_options& options) const;

    lw_shared_ptr<const service::pager::paging_state> generate_view_paging_state_from_base_query_results(lw_shared_ptr<const service::pager::paging_state> paging_state,
            const foreign_ptr<lw_shared_ptr<query::result>>& results, service::query_state& state, const query_options& options) const;

    future<coordinator_result<std::tuple<dht::partition_range_vector, lw_shared_ptr<const service::pager::paging_state>>>> find_index_partition_ranges(query_processor& qp,
                                                                    service::query_state& state,
                                                                    const query_options& options) const;

    future<coordinator_result<std::tuple<std::vector<primary_key>, lw_shared_ptr<const service::pager::paging_state>>>> find_index_clustering_rows(query_processor& qp,
                                                                service::query_state& state,
                                                                const query_options& options) const;

    future<shared_ptr<cql_transport::messages::result_message>>
    process_base_query_results(
            foreign_ptr<lw_shared_ptr<query::result>> results,
            lw_shared_ptr<query::read_command> cmd,
            service::query_state& state,
            const query_options& options,
            gc_clock::time_point now,
            lw_shared_ptr<const service::pager::paging_state> paging_state) const;

    lw_shared_ptr<query::read_command>
    prepare_command_for_base_query(query_processor& qp, const query_options& options, service::query_state& state, gc_clock::time_point now,
            bool use_paging) const;

    future<coordinator_result<std::tuple<foreign_ptr<lw_shared_ptr<query::result>>, lw_shared_ptr<query::read_command>>>>
    do_execute_base_query(
            query_processor& qp,
            dht::partition_range_vector&& partition_ranges,
            service::query_state& state,
            const query_options& options,
            gc_clock::time_point now,
            lw_shared_ptr<const service::pager::paging_state> paging_state) const;
    future<shared_ptr<cql_transport::messages::result_message>>
    execute_base_query(
            query_processor& qp,
            dht::partition_range_vector&& partition_ranges,
            service::query_state& state,
            const query_options& options,
            gc_clock::time_point now,
            lw_shared_ptr<const service::pager::paging_state> paging_state) const;

    // Function for fetching the selected columns from a list of clustering rows.
    // It is currently used only in our Secondary Index implementation - ordinary
    // CQL SELECT statements do not have the syntax to request a list of rows.
    // FIXME: The current implementation is very inefficient - it requests each
    // row separately (and, incrementally, in parallel). Even multiple rows from a single
    // partition are requested separately. This last case can be easily improved,
    // but to implement the general case (multiple rows from multiple partitions)
    // efficiently, we will need more support from other layers.
    // Keys are ordered in token order (see #3423)
    future<coordinator_result<std::tuple<foreign_ptr<lw_shared_ptr<query::result>>, lw_shared_ptr<query::read_command>>>>
    do_execute_base_query(
            query_processor& qp,
            std::vector<primary_key>&& primary_keys,
            service::query_state& state,
            const query_options& options,
            gc_clock::time_point now,
            lw_shared_ptr<const service::pager::paging_state> paging_state) const;
    future<shared_ptr<cql_transport::messages::result_message>>
    execute_base_query(
            query_processor& qp,
            std::vector<primary_key>&& primary_keys,
            service::query_state& state,
            const query_options& options,
            gc_clock::time_point now,
            lw_shared_ptr<const service::pager::paging_state> paging_state) const;

    virtual void update_stats_rows_read(int64_t rows_read) const override {
        _stats.rows_read += rows_read;
        _stats.secondary_index_rows_read += rows_read;
    }

    future<coordinator_result<::shared_ptr<cql_transport::messages::result_message::rows>>> read_posting_list(
            query_processor& qp,
            const query_options& options,
            uint64_t limit,
            service::query_state& state,
            gc_clock::time_point now,
            db::timeout_clock::time_point timeout,
            bool include_base_clustering_key) const;

    dht::partition_range_vector get_partition_ranges_for_local_index_posting_list(const query_options& options) const;
    dht::partition_range_vector get_partition_ranges_for_global_index_posting_list(const query_options& options) const;

    query::partition_slice get_partition_slice_for_local_index_posting_list(const query_options& options) const;
    query::partition_slice get_partition_slice_for_global_index_posting_list(const query_options& options) const;

    bytes compute_idx_token(const partition_key& key) const;
};

class vector_indexed_table_select_statement : public select_statement {
    secondary_index::index _index;
    prepared_ann_ordering_type _prepared_ann_ordering;
    mutable gc_clock::time_point _query_start_time_point;

public:
    static constexpr size_t max_ann_query_limit = 1000;

    static ::shared_ptr<cql3::statements::select_statement> prepare(data_dictionary::database db, schema_ptr schema, uint32_t bound_terms,
            lw_shared_ptr<const parameters> parameters, ::shared_ptr<selection::selection> selection,
            ::shared_ptr<restrictions::statement_restrictions> restrictions, ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed,
            ordering_comparator_type ordering_comparator, prepared_ann_ordering_type prepared_ann_ordering, std::optional<expr::expression> limit,
            std::optional<expr::expression> per_partition_limit, cql_stats& stats, std::unique_ptr<cql3::attributes> attrs);

    vector_indexed_table_select_statement(schema_ptr schema, uint32_t bound_terms, lw_shared_ptr<const parameters> parameters,
            ::shared_ptr<selection::selection> selection, ::shared_ptr<const restrictions::statement_restrictions> restrictions,
            ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed, ordering_comparator_type ordering_comparator,
            prepared_ann_ordering_type prepared_ann_ordering, std::optional<expr::expression> limit, std::optional<expr::expression> per_partition_limit,
            cql_stats& stats, const secondary_index::index& index, std::unique_ptr<cql3::attributes> attrs);

private:
    future<::shared_ptr<cql_transport::messages::result_message>> do_execute(
            query_processor& qp, service::query_state& state, const query_options& options) const override;

    void update_stats() const;

    lw_shared_ptr<query::read_command> prepare_command_for_base_query(query_processor& qp, service::query_state& state, const query_options& options) const;

    std::vector<float> get_ann_ordering_vector(const query_options& options) const;

    future<::shared_ptr<cql_transport::messages::result_message>> query_base_table(
            query_processor& qp, service::query_state& state, const query_options& options, const std::vector<primary_key>& pkeys) const;
};

}
}
