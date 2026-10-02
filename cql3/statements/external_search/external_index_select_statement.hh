/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/statements/external_search/external_search_plan.hh"
#include "cql3/statements/external_search/filter.hh"
#include "cql3/statements/select_statement.hh"
#include "vector_search/hybrid_search.hh"

#include <vector>

namespace cql3::statements {

/// What select_statement::prepare() passes on to the statement, bundled so that the call site reads
/// the same for one search or several.
struct external_statement_args {
    schema_ptr schema;
    uint32_t bound_terms;
    lw_shared_ptr<const raw::select_statement::parameters> parameters;
    ::shared_ptr<selection::selection> selection;
    ::shared_ptr<const restrictions::select_restrictions> restrictions;
    ::shared_ptr<std::vector<size_t>> group_by_cell_indices;
    bool is_reversed;
    select_statement::ordering_comparator_type ordering_comparator;
    std::optional<expr::expression> limit;
    std::optional<expr::expression> per_partition_limit;
    cql_stats& stats;
    std::unique_ptr<cql3::attributes> attrs;
};

/// A SELECT served by one or more external searches (ANN, BM25): asks each index for ranked keys,
/// reads those rows from the base table, fills in the values each search reported for every row,
/// and emits the rows in order. What differs by family lives in ann_search and bm25_search.
class external_index_select_statement : public select_statement {
    std::vector<search_source> _sources;
    /// The restrictions a vector search prefilters by. Empty for any other query.
    external_search::prepared_filter _prepared_filter;
    mutable gc_clock::time_point _query_start_time_point;

public:
    /// The largest LIMIT an external-search query may ask for.
    static constexpr uint64_t max_query_limit = 1000;

    static ::shared_ptr<select_statement> prepare(std::vector<search_source> sources, external_statement_args args);

    external_index_select_statement(std::vector<search_source> sources, external_search::prepared_filter prepared_filter,
            external_statement_args args);

private:
    /// Base-table rows read for the keys the searches returned, together with the command they were
    /// read with. The command's slice is needed to walk the rows.
    struct base_table_read {
        foreign_ptr<lw_shared_ptr<query::result>> rows;
        lw_shared_ptr<query::read_command> command;
    };

    future<::shared_ptr<cql_transport::messages::result_message>> do_execute(
            query_processor& qp, service::query_state& state, const query_options& options) const override;

    future<::shared_ptr<cql_transport::messages::result_message>> execute_search(
            query_processor& qp, service::query_state& state, const query_options& options, uint64_t limit) const;

    /// Reads a row for every candidate, preserving their order.
    future<coordinator_result<base_table_read>> query_base_table(query_processor& qp, service::query_state& state,
            const query_options& options, lowres_clock::time_point timeout,
            std::span<const vector_search::search_candidate> candidates) const;

    /// Turns rows already read into the result set the client is sent, injecting the values
    /// `provider` supplies per row. Separate from the read so that the rows can be looked at first.
    future<::shared_ptr<cql_transport::messages::result_message>> emit_result_set(
            coordinator_result<base_table_read> table_results, const query_options& options,
            const cql3::selection::external_values_provider* provider) const;

    lw_shared_ptr<query::read_command> prepare_command_for_base_query(
            query_processor& qp, service::query_state& state, const query_options& options, uint64_t fetch_limit) const;

    future<coordinator_result<foreign_ptr<lw_shared_ptr<query::result>>>> query_partition_ranges(query_processor& qp, service::query_state& state,
            const query_options& options, lw_shared_ptr<query::read_command> command, lowres_clock::time_point timeout,
            std::vector<dht::partition_range> partition_ranges) const;

    void update_stats() const;
    void setup_execute(service::query_state& state, const query_options& options) const;
    void maybe_add_paging_warning(const ::shared_ptr<cql_transport::messages::result_message>& result, const query_options& options, uint64_t limit) const;

    void update_stats_rows_read(int64_t rows_read) const override {
        _stats.rows_read += rows_read;
        _stats.secondary_index_rows_read += rows_read;
    }

    bool needs_post_filtering() const override {
        return false; // All filtering is done by the index query, so no post-filtering is allowed.
    }
};

} // namespace cql3::statements
