/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/statements/select_statement.hh"
#include "vector_search/vector_store_client.hh"

namespace cql3::statements {

/// Base class for SELECT statements that query an external index node
/// and then fetch base-table rows by primary key, preserving the index node's
/// result ordering.
///
/// Subclasses implement `execute_search()` to call the external index service
/// and return the result rows ordered by relevance.
class external_index_select_statement : public select_statement {
protected:
    secondary_index::index _index;
    mutable gc_clock::time_point _query_start_time_point;

public:
    external_index_select_statement(schema_ptr schema, uint32_t bound_terms,
            lw_shared_ptr<const parameters> parameters,
            ::shared_ptr<selection::selection> selection,
            ::shared_ptr<const restrictions::statement_restrictions> restrictions,
            ::shared_ptr<std::vector<size_t>> group_by_cell_indices,
            bool is_reversed,
            ordering_comparator_type ordering_comparator,
            std::optional<expr::expression> limit,
            std::optional<expr::expression> per_partition_limit,
            cql_stats& stats,
            const secondary_index::index& index,
            std::unique_ptr<cql3::attributes> attrs);

protected:
    lw_shared_ptr<query::read_command> prepare_command_for_base_query(query_processor& qp, service::query_state& state, const query_options& options, uint64_t fetch_limit) const;

    future<::shared_ptr<cql_transport::messages::result_message>> query_base_table(query_processor& qp, service::query_state& state,
            const query_options& options, const std::vector<vector_search::primary_key>& pkeys, lowres_clock::time_point timeout) const;

    future<coordinator_result<foreign_ptr<lw_shared_ptr<query::result>>>> query_base_table(query_processor& qp, service::query_state& state,
            const query_options& options, lw_shared_ptr<query::read_command> command, lowres_clock::time_point timeout,
            const std::vector<vector_search::primary_key>& pkeys) const;

    future<coordinator_result<foreign_ptr<lw_shared_ptr<query::result>>>> query_base_table(query_processor& qp, service::query_state& state,
            const query_options& options, lw_shared_ptr<query::read_command> command, lowres_clock::time_point timeout,
            std::vector<dht::partition_range> partition_ranges) const;

    virtual future<::shared_ptr<cql_transport::messages::result_message>> execute_search(
            query_processor& qp, service::query_state& state, const query_options& options, uint64_t limit) const = 0;

    void update_stats_rows_read(int64_t rows_read) const override {
        _stats.rows_read += rows_read;
        _stats.secondary_index_rows_read += rows_read;
    }

    bool needs_post_filtering() const override {
        return false; // All filtering is done by the index query, so no post-filtering is allowed.
    }

private:
    virtual std::string_view index_search_type_name() const = 0;

    future<::shared_ptr<cql_transport::messages::result_message>> do_execute(
            query_processor& qp, service::query_state& state, const query_options& options) const final;

    void update_stats() const;
    void setup_execute(service::query_state& state, const query_options& options) const;
    void maybe_add_paging_warning(const ::shared_ptr<cql_transport::messages::result_message>& result, const query_options& options, uint64_t limit) const;
};

} // namespace cql3::statements
