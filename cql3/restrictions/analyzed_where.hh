/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "index_table_query_restrictions.hh"
#include "single_table_query_restrictions.hh"
#include "statement_restrictions.hh"

namespace cql3 {
namespace restrictions {

// This class analyzes the WHERE clause and generates a query plan.
class analyzed_where_clause {
    // The query is a single_table_query if possible without filtering, otherwise it might be an indexed query.
    std::optional<std::variant<single_table_query_restrictions, index_table_query_restrictions>> query_restrictions;

    // For now we also keep the old implementation using statement_restrictions.
    // Its functionality will be gradually replaced by single_table_query_restrictions and
    // index_table_query_restrictions
    ::shared_ptr<cql3::restrictions::statement_restrictions> restrictions;

public:
    analyzed_where_clause(data_dictionary::database db,
                          schema_ptr schema,
                          statements::statement_type type,
                          const expr::expression& where_clause,
                          prepare_context& ctx,
                          bool selects_only_static_columns,
                          bool for_view = false,
                          bool allow_filtering = false);

    // Creates analyzed_where_clause for an empty WHERE.
    analyzed_where_clause(schema_ptr schema, bool allow_filtering);

    // # Partition key

    // Access expression containing all parittion key restrictions.
    const expr::expression& get_partition_key_restrictions() const;

    // Get a map which keeps single column restrictions for each partition key column.
    const expr::single_column_restrictions_map& get_single_column_partition_key_restrictions() const;

    // Check if there are no partition key restrictions.
    bool partition_key_restrictions_is_empty() const;

    // Check that partition key restrictions contain only binary operaters with = operation..
    bool partition_key_restrictions_is_all_eq() const;

    // Check if every partition key column has some kind of restriction.
    bool has_partition_key_unrestricted_components() const;

    // Are there restrictions on partition key token.
    bool has_token_restrictions() const;

    // Checks if the query requests a range of partition keys (token range, no restrictions, etc).
    // Multiple values because of IN don't count as a key range.
    bool is_key_range() const;

    // Checks if there is an IN restriction in partition key restrictions.
    bool key_is_in_relation() const;

    // Checks if filtering has to involve partition key restrictions.
    bool pk_restrictions_need_filtering() const;

    // Calculates partition key ranges to use in a query.
    dht::partition_range_vector get_partition_key_ranges(const query_options& options) const;

    // # Clustering key

    // Access expression containing all clustering key restrictions.
    const expr::expression& get_clustering_columns_restrictions() const;

    // Get a map which keeps single column restrictions for each clustering key column.
    const expr::single_column_restrictions_map& get_single_column_clustering_key_restrictions() const;

    // Checks if the WHERE clause has any restrictions on clustering key columns.
    bool has_clustering_columns_restriction() const;

    // Check if every clustering key column has some kind of restriction.
    bool has_unrestricted_clustering_columns() const;

    // Checks if filtering has to involve clustering key restrictions.
    bool ck_restrictions_need_filtering() const;

    // Calculates clustering key ranges to use in a query.
    std::vector<query::clustering_range> get_clustering_bounds(const query_options& options) const;

    // # Nonprimary columns

    // Get a map which keeps single column restrictions for each non-primary key column.
    const expr::single_column_restrictions_map& get_non_pk_restriction() const;

    // Checks if the WHERE clause has any restrictions on non-primary columns.
    bool has_non_primary_key_restriction() const;

    // # Indexes

    // Should this query use an index.
    bool uses_secondary_indexing() const;

    // Which index should the query use.
    std::pair<std::optional<secondary_index::index>, expr::expression> find_idx(
        const secondary_index::secondary_index_manager& sim) const;

    // Prepare data for local index query.
    void prepare_indexed_local(const schema& idx_tbl_schema) const;

    // Prepare data for global index query;
    void prepare_indexed_global(const schema& idx_tbl_schema) const;

    // Calculates clustering ranges for querying a global-index table. Before using this function
    // data has to be prepared using prepare_indexed_global().
    std::vector<query::clustering_range> get_global_index_clustering_ranges(const query_options& options,
                                                                            const schema& idx_tbl_schema) const;

    // Calculates clustering ranges for querying a global-index table for queries with token restrictions present.
    // Before using this function data has to be prepared using prepare_indexed_global().
    std::vector<query::clustering_range> get_global_index_token_clustering_ranges(const query_options& options,
                                                                                  const schema& idx_tbl_schema) const;

    // Calculates clustering ranges for querying a local-index table. Before using this function
    // data has to be prepared using prepare_indexed_local().
    std::vector<query::clustering_range> get_local_index_clustering_ranges(const query_options& options,
                                                                           const schema& idx_tbl_schema) const;

    // # Other

    // Does the WHERE clause contain restriction that involve this column.
    bool is_restricted(const column_definition* cdef) const;

    // Is there an EQ restriciton on this column, e.g. my_col = 4.
    bool has_eq_restriction_on_column(const column_definition& column) const;

    // Does this query need filtering, or are the bounds from get_partition_key_ranges
    // and get_clustering_bounds enough to check all conditions.
    bool need_filtering() const;

    // Builds a possibly empty collection of column definitions that will be used for filtering
    std::vector<const column_definition*> get_column_defs_for_filtering(data_dictionary::database db) const;
};
}  // namespace restrictions
}  // namespace cql3
