/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/statements/select_statement.hh"
#include "vector_search/hybrid_search.hh"
#include "cql3/expr/expression.hh"
#include "cql3/expr/temporary_allocator.hh"
#include "cql3/statements/external_search/external_function.hh"
#include "cql3/statements/external_search/filter.hh"
#include "data_dictionary/data_dictionary.hh"
#include "index/secondary_index.hh"
#include "schema/schema_fwd.hh"
#include "utils/rjson.hh"
#include "vector_search/vector_store_client.hh"
#include "cql3/expr/expression.hh"
#include "cql3/expr/temporary_allocator.hh"
#include "cql3/statements/external_search/external_function.hh"
#include "cql3/statements/external_search/values_provider.hh"
#include "index/secondary_index.hh"
#include "schema/schema.hh"
#include "vector_search/vector_store_client.hh"

#include <seastar/core/future.hh>
#include <optional>
#include <seastar/core/future.hh>
#include <optional>
#include <span>

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
            ::shared_ptr<const restrictions::select_restrictions> restrictions,
            ::shared_ptr<std::vector<size_t>> group_by_cell_indices,
            bool is_reversed,
            ordering_comparator_type ordering_comparator,
            std::optional<expr::expression> limit,
            std::optional<expr::expression> per_partition_limit,
            cql_stats& stats,
            const secondary_index::index& index,
            std::unique_ptr<cql3::attributes> attrs);

protected:
    /// Base-table rows read for the keys an external index returned, together with the command they
    /// were read with. The command's slice is needed to walk the rows.
    struct base_table_read {
        foreign_ptr<lw_shared_ptr<query::result>> rows;
        lw_shared_ptr<query::read_command> command;
    };

    /// Reads a row for every candidate, preserving their order.
    future<coordinator_result<base_table_read>> query_base_table(query_processor& qp, service::query_state& state,
            const query_options& options, lowres_clock::time_point timeout,
            std::span<const vector_search::search_candidate> candidates) const;

    /// Turns rows already read into the result set the client is sent, injecting the values
    /// `provider` supplies per row. Separate from the read so that a search can look at its rows first.
    future<::shared_ptr<cql_transport::messages::result_message>> emit_result_set(
            coordinator_result<base_table_read> table_results, const query_options& options,
            const cql3::selection::external_values_provider* provider) const;

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
    lw_shared_ptr<query::read_command> prepare_command_for_base_query(
            query_processor& qp, service::query_state& state, const query_options& options, uint64_t fetch_limit) const;

    future<coordinator_result<foreign_ptr<lw_shared_ptr<query::result>>>> query_partition_ranges(query_processor& qp, service::query_state& state,
            const query_options& options, lw_shared_ptr<query::read_command> command, lowres_clock::time_point timeout,
            std::vector<dht::partition_range> partition_ranges) const;

    virtual std::string_view index_search_type_name() const = 0;

    future<::shared_ptr<cql_transport::messages::result_message>> do_execute(
            query_processor& qp, service::query_state& state, const query_options& options) const final;

    void update_stats() const;
    void setup_execute(service::query_state& state, const query_options& options) const;
    void maybe_add_paging_warning(const ::shared_ptr<cql_transport::messages::result_message>& result, const query_options& options, uint64_t limit) const;
};


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
            ::shared_ptr<const restrictions::select_restrictions> restrictions,
            ::shared_ptr<std::vector<size_t>> group_by_cell_indices,
            bool is_reversed,
            ordering_comparator_type ordering_comparator,
            std::optional<expr::expression> limit,
            std::optional<expr::expression> per_partition_limit,
            cql_stats& stats,
            std::optional<bm25_ordering_info> ordering_info,
            std::unique_ptr<cql3::attributes> attrs);

    fulltext_indexed_table_select_statement(schema_ptr schema, uint32_t bound_terms, lw_shared_ptr<const parameters> parameters,
            ::shared_ptr<selection::selection> selection, ::shared_ptr<const restrictions::select_restrictions> restrictions,
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


/// A query vector written in a SELECT call that prepare could not prove equal to the ORDER BY
/// vector, because a bind marker is involved. Execution compares the bound values;
/// `function_name` is the function the call was written with, for the error message.
struct deferred_select_vector {
    expr::expression vector;
    sstring function_name;
};

/// ANN ordering metadata resolved during prepare.
struct ann_ordering_info {
    secondary_index::index index;
    raw::select_statement::prepared_ann_ordering_type prepared_ann_ordering;
    bool is_rescoring_enabled;
    /// Temporaries holding the Vector Store's score and rank; see
    /// external_search::search_temporaries. ANN() is replaced with a tuple of the two, so it has no
    /// temporary of its own. A rescoring index allocates neither: it recomputes the score locally
    /// and reports no rank.
    external_search::search_temporaries temporaries;
    /// The SELECT occurrences' query vectors that only execution can compare, a bind marker
    /// standing where at least one of the two values will be.
    std::vector<deferred_select_vector> deferred_select_vectors;
};

class vector_indexed_table_select_statement : public external_index_select_statement {
    ann_ordering_info _ann_ordering_info;
    external_search::prepared_filter _prepared_filter;

public:
    static constexpr size_t max_ann_query_limit = 1000;

    static ::shared_ptr<cql3::statements::select_statement> prepare(data_dictionary::database db, schema_ptr schema, uint32_t bound_terms,
            lw_shared_ptr<const parameters> parameters, ::shared_ptr<selection::selection> selection,
            ::shared_ptr<const restrictions::select_restrictions> restrictions, ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed,
            ordering_comparator_type ordering_comparator, std::optional<expr::expression> limit,
            std::optional<expr::expression> per_partition_limit, cql_stats& stats, ann_ordering_info ordering_info, std::unique_ptr<cql3::attributes> attrs);

    vector_indexed_table_select_statement(schema_ptr schema, uint32_t bound_terms, lw_shared_ptr<const parameters> parameters,
            ::shared_ptr<selection::selection> selection, ::shared_ptr<const restrictions::select_restrictions> restrictions,
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
