/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/statements/raw/select_statement.hh"
#include "cql3/expr/unset.hh"
#include "cql3/cql_statement.hh"
#include "cql3/stats.hh"
#include <seastar/core/shared_ptr.hh>
#include "transport/messages/result_message.hh"
#include "index/secondary_index_manager.hh"
#include "exceptions/exceptions.hh"
#include "exceptions/coordinator_result.hh"

namespace locator {
    class node;
} // namespace locator

namespace service {
    class client_state;
    class storage_proxy;
    class storage_proxy_coordinator_query_options;
    class storage_proxy_coordinator_query_result;
} // namespace service

namespace cql3 {

class query_processor;

namespace selection {
    class selection;
} // namespace selection

namespace restrictions {
    class restrictions;
    class statement_restrictions;
} // namespace restrictions

namespace statements {

/**
 * Encapsulates a completely parsed SELECT query, including the target
 * column family, expression, result count, and ordering clause.
 *
 */
class select_statement : public cql_statement {
public:
    template<typename T>
    using coordinator_result = exceptions::coordinator_result<T>;
    using parameters = raw::select_statement::parameters;
    using ordering_comparator_type = raw::select_statement::ordering_comparator_type;
    static constexpr int DEFAULT_COUNT_PAGE_SIZE = 10000;
protected:
    static thread_local const lw_shared_ptr<const parameters> _default_parameters;
    schema_ptr _schema;
    uint32_t _bound_terms;
    lw_shared_ptr<const parameters> _parameters;
    ::shared_ptr<selection::selection> _selection;
    const ::shared_ptr<const restrictions::statement_restrictions> _restrictions;
    const bool _restrictions_need_filtering;
    ::shared_ptr<std::vector<size_t>> _group_by_cell_indices; ///< Indices in result row of cells holding GROUP BY values.
    bool _is_reversed;
    expr::unset_bind_variable_guard _limit_unset_guard;
    std::optional<expr::expression> _limit;
    expr::unset_bind_variable_guard _per_partition_limit_unset_guard;
    std::optional<expr::expression> _per_partition_limit;

    template<typename T>
    using compare_fn = raw::select_statement::compare_fn<T>;

    using result_row_type = raw::select_statement::result_row_type;

    /**
     * The comparator used to orders results when multiple keys are selected (using IN).
     */
    ordering_comparator_type _ordering_comparator;

    query::partition_slice::option_set _opts;
    cql_stats& _stats;
    const ks_selector _ks_sel;
    bool _range_scan = false;
    bool _range_scan_no_bypass_cache = false;
    std::unique_ptr<cql3::attributes> _attrs;
private:
    future<shared_ptr<cql_transport::messages::result_message>> process_results_complex(foreign_ptr<lw_shared_ptr<query::result>> results,
        lw_shared_ptr<query::read_command> cmd, const query_options& options, gc_clock::time_point now) const;
protected :
    virtual future<::shared_ptr<cql_transport::messages::result_message>> do_execute(query_processor& qp,
        service::query_state& state, const query_options& options) const;
    friend class select_statement_executor;
public:
    select_statement(schema_ptr schema,
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
            std::unique_ptr<cql3::attributes> attrs);

    virtual ::shared_ptr<const cql3::metadata> get_result_metadata() const override;
    virtual uint32_t get_bound_terms() const override;
    virtual future<> check_access(query_processor& qp, const service::client_state& state) const override;
    virtual bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const override;

    virtual future<::shared_ptr<cql_transport::messages::result_message>> execute(query_processor& qp,
        service::query_state& state, const query_options& options) const override;

    virtual future<::shared_ptr<cql_transport::messages::result_message>>
        execute_without_checking_exception_message(query_processor& qp, service::query_state& qs, const query_options& options) const override;

    future<::shared_ptr<cql_transport::messages::result_message>> execute_non_aggregate_unpaged(query_processor& qp,
        lw_shared_ptr<query::read_command> cmd, dht::partition_range_vector&& partition_ranges, service::query_state& state,
         const query_options& options, gc_clock::time_point now) const;

    future<::shared_ptr<cql_transport::messages::result_message>> execute_without_checking_exception_message_non_aggregate_unpaged(query_processor& qp,
        lw_shared_ptr<query::read_command> cmd, dht::partition_range_vector&& partition_ranges, service::query_state& state,
         const query_options& options, gc_clock::time_point now) const;

    future<::shared_ptr<cql_transport::messages::result_message>> execute_without_checking_exception_message_aggregate_or_paged(query_processor& qp,
        lw_shared_ptr<query::read_command> cmd, dht::partition_range_vector&& partition_ranges, service::query_state& state,
         const query_options& options, gc_clock::time_point now, int32_t page_size, bool aggregate, bool nonpaged_filtering) const;


    struct primary_key {
        dht::decorated_key partition;
        clustering_key_prefix clustering;
    };

    future<shared_ptr<cql_transport::messages::result_message>> process_results(foreign_ptr<lw_shared_ptr<query::result>> results,
        lw_shared_ptr<query::read_command> cmd, const query_options& options, gc_clock::time_point now) const;

    const sstring& keyspace() const;

    const sstring& column_family() const;

    query::partition_slice make_partition_slice(const query_options& options) const;

    const ::shared_ptr<const restrictions::statement_restrictions> get_restrictions() const;

    bool has_group_by() const { return _group_by_cell_indices && !_group_by_cell_indices->empty(); }

    db::timeout_clock::duration get_timeout(const service::client_state& state, const query_options& options) const;

protected:
    uint64_t do_get_limit(const query_options& options, const std::optional<expr::expression>& limit, const expr::unset_bind_variable_guard& unset_guard, uint64_t default_limit) const;
    uint64_t get_limit(const query_options& options) const {
        return do_get_limit(options, _limit, _limit_unset_guard, query::max_rows);
    }
    uint64_t get_per_partition_limit(const query_options& options) const {
        return do_get_limit(options, _per_partition_limit, _per_partition_limit_unset_guard, query::partition_max_rows);
    }
    bool needs_post_query_ordering() const;
    virtual void update_stats_rows_read(int64_t rows_read) const {
        _stats.rows_read += rows_read;
    }
};

class primary_key_select_statement : public select_statement {
public:
    primary_key_select_statement(schema_ptr schema,
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
                     std::unique_ptr<cql3::attributes> attrs);
};

class indexed_table_select_statement : public select_statement {
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

    indexed_table_select_statement(schema_ptr schema,
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

class mutation_fragments_select_statement : public select_statement {
    schema_ptr _underlying_schema;
public:
    mutation_fragments_select_statement(
            schema_ptr output_schema,
            schema_ptr underlying_schema,
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
            std::unique_ptr<cql3::attributes> attrs);

    // This statement has a schema that is different from that of the underlying table.
    static schema_ptr generate_output_schema(schema_ptr underlying_schema);

private:
    future<exceptions::coordinator_result<service::storage_proxy_coordinator_query_result>>
    do_query(
            const locator::node* this_node,
            service::storage_proxy& sp,
            schema_ptr schema,
            lw_shared_ptr<query::read_command> cmd,
            dht::partition_range_vector partition_ranges,
            db::consistency_level cl,
            service::storage_proxy_coordinator_query_options optional_params) const;

    virtual future<::shared_ptr<cql_transport::messages::result_message>> do_execute(query_processor& qp,
            service::query_state& state, const query_options& options) const override;
};

}

}
