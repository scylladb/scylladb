/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "analyzed_where.hh"
#include "cql3/expr/expression.hh"
#include "cql3/restrictions/single_table_query_restrictions.hh"
#include "cql3/restrictions/statement_restrictions.hh"

namespace cql3 {
namespace restrictions {
using namespace expr;

analyzed_where_clause::analyzed_where_clause(data_dictionary::database db,
                                             schema_ptr schema,
                                             statements::statement_type type,
                                             const expr::expression& where_clause,
                                             prepare_context& ctx,
                                             bool selects_only_static_columns,
                                             bool for_view,
                                             bool allow_filtering) {
    query_restrictions = std::nullopt;
    restrictions = ::make_shared<statement_restrictions>(db, schema, type, where_clause, ctx,
                                                         selects_only_static_columns, for_view, allow_filtering);

    if (!restrictions->uses_secondary_indexing()) {
        expr::expression prepared_where_clause =
            expr::prepare_expression(where_clause, db, schema->ks_name(), schema.get(), nullptr);
        query_restrictions = single_table_query_restrictions::make(prepared_where_clause, schema);
    }
}

analyzed_where_clause::analyzed_where_clause(schema_ptr schema, bool allow_filtering) {
    query_restrictions = std::nullopt;
    restrictions = ::make_shared<statement_restrictions>(schema, allow_filtering);
}

// # Partition key
const expr::expression& analyzed_where_clause::get_partition_key_restrictions() const {
    return restrictions->get_partition_key_restrictions();
}

const expr::single_column_restrictions_map& analyzed_where_clause::get_single_column_partition_key_restrictions()
    const {
    return restrictions->get_single_column_partition_key_restrictions();
}

bool analyzed_where_clause::partition_key_restrictions_is_empty() const {
    return restrictions->partition_key_restrictions_is_empty();
}

bool analyzed_where_clause::partition_key_restrictions_is_all_eq() const {
    return restrictions->partition_key_restrictions_is_all_eq();
}

bool analyzed_where_clause::has_partition_key_unrestricted_components() const {
    return restrictions->has_partition_key_unrestricted_components();
}

bool analyzed_where_clause::has_token_restrictions() const {
    return restrictions->has_token_restrictions();
}

bool analyzed_where_clause::is_key_range() const {
    return restrictions->is_key_range();
}

bool analyzed_where_clause::key_is_in_relation() const {
    return restrictions->key_is_in_relation();
}

bool analyzed_where_clause::pk_restrictions_need_filtering() const {
    return restrictions->pk_restrictions_need_filtering();
}

dht::partition_range_vector analyzed_where_clause::get_partition_key_ranges(const query_options& options) const {
    return restrictions->get_partition_key_ranges(options);
}

// # Clustering key
const expr::expression& analyzed_where_clause::get_clustering_columns_restrictions() const {
    return restrictions->get_clustering_columns_restrictions();
}

const expr::single_column_restrictions_map& analyzed_where_clause::get_single_column_clustering_key_restrictions()
    const {
    return restrictions->get_single_column_clustering_key_restrictions();
}

bool analyzed_where_clause::has_clustering_columns_restriction() const {
    return restrictions->has_clustering_columns_restriction();
}

bool analyzed_where_clause::has_unrestricted_clustering_columns() const {
    return restrictions->has_unrestricted_clustering_columns();
}

bool analyzed_where_clause::ck_restrictions_need_filtering() const {
    return restrictions->ck_restrictions_need_filtering();
}

std::vector<query::clustering_range> analyzed_where_clause::get_clustering_bounds(const query_options& options) const {
    return restrictions->get_clustering_bounds(options);
}

// # Nonprimary columns
const expr::single_column_restrictions_map& analyzed_where_clause::get_non_pk_restriction() const {
    return restrictions->get_non_pk_restriction();
}

bool analyzed_where_clause::has_non_primary_key_restriction() const {
    return restrictions->has_non_primary_key_restriction();
}

// # Indexes
bool analyzed_where_clause::uses_secondary_indexing() const {
    return restrictions->uses_secondary_indexing();
}

std::pair<std::optional<secondary_index::index>, expr::expression> analyzed_where_clause::find_idx(
    const secondary_index::secondary_index_manager& sim) const {
    return restrictions->find_idx(sim);
}

void analyzed_where_clause::prepare_indexed_local(const schema& idx_tbl_schema) const {
    restrictions->prepare_indexed_local(idx_tbl_schema);
}

void analyzed_where_clause::prepare_indexed_global(const schema& idx_tbl_schema) const {
    restrictions->prepare_indexed_global(idx_tbl_schema);
}

std::vector<query::clustering_range> analyzed_where_clause::get_global_index_clustering_ranges(
    const query_options& options,
    const schema& idx_tbl_schema) const {
    return restrictions->get_global_index_clustering_ranges(options, idx_tbl_schema);
}

std::vector<query::clustering_range> analyzed_where_clause::get_global_index_token_clustering_ranges(
    const query_options& options,
    const schema& idx_tbl_schema) const {
    return restrictions->get_global_index_token_clustering_ranges(options, idx_tbl_schema);
}

std::vector<query::clustering_range> analyzed_where_clause::get_local_index_clustering_ranges(
    const query_options& options,
    const schema& idx_tbl_schema) const {
    return restrictions->get_local_index_clustering_ranges(options, idx_tbl_schema);
}

// # Other
bool analyzed_where_clause::is_restricted(const column_definition* cdef) const {
    return restrictions->is_restricted(cdef);
}

bool analyzed_where_clause::has_eq_restriction_on_column(const column_definition& column) const {
    return restrictions->has_eq_restriction_on_column(column);
}

bool analyzed_where_clause::need_filtering() const {
    return restrictions->need_filtering();
}

std::vector<const column_definition*> analyzed_where_clause::get_column_defs_for_filtering(
    data_dictionary::database db) const {
    return restrictions->get_column_defs_for_filtering(db);
}

}  // namespace restrictions
}  // namespace cql3
