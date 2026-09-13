/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include <span>
#include <vector>
#include "cql3/expr/expression.hh"
#include "cql3/expr/restrictions.hh"
#include "cql3/prepare_context.hh"
#include "cql3/restrictions/where_clause_analysis.hh"
#include "query/query-request.hh"
#include "schema/schema_fwd.hh"
#include "service/pager/query_plan.hh"

namespace cql3 {

namespace restrictions {

///In some cases checking if columns have indexes is undesired of even
///impossible, because e.g. the query runs on a pseudo-table, which does not
///have an index-manager, or even a table object.
using check_indexes = bool_class<class check_indexes_tag>;

/// The plan a continued paged query must keep scanning - the base table, or an
/// index view. The query fails if that plan is gone or cannot serve the query;
/// std::nullopt plans normally. See #18992.
using pinned_plan_opt = std::optional<service::pager::query_plan>;

// A mutation's WHERE clause has to name the rows it writes, so every restriction
// in it has to translate to a partition or clustering range.  There is no index
// to read and nothing to filter, and none of that machinery is reachable from
// either of the classes below.

/**
 * What an UPDATE statement's WHERE clause says about the rows it writes.
 *
 * An UPDATE writes whole rows, so its WHERE clause names whole rows: it may not
 * slice the clustering key, and must name all of it.
 *
 * Built by analyze_update_restrictions() below.
 */
class update_restrictions {
    where_clause_analysis _analysis;
public:
    // Marks the constructor as internal: a constructed object says nothing until
    // it is analyzed, so go through the analyze_*_restrictions() factories below.
    struct private_tag { explicit private_tag() = default; };

    update_restrictions(private_tag, schema_ptr schema);

    update_restrictions(const update_restrictions&) = delete;
    update_restrictions& operator=(const update_restrictions&) = delete;

    void analyze_update(
            data_dictionary::database db,
            const expr::expression& where_clause,
            prepare_context& ctx,
            bool applies_only_to_static_columns);

    /// Rejects a WHERE clause that does not name the whole clustering key.
    ///
    /// Takes the flag rather than remembering it: an IF [NOT] EXISTS condition
    /// is classified after the analysis and can change it.
    void reject_incomplete_clustering_key(bool applies_only_to_static_columns) const;

    /// Rejects a WHERE clause that does not name the whole partition key.
    void reject_incomplete_partition_key() const;

    bool key_is_in_relation() const { return _analysis.key_is_in_relation(); }
    bool clustering_key_restrictions_has_IN() const { return _analysis.clustering_key_restrictions_has_IN(); }
    bool has_clustering_columns_restriction() const { return _analysis.has_clustering_columns_restriction(); }

    dht::partition_range_vector get_partition_key_ranges(const query_options& options) const {
        return _analysis.get_partition_key_ranges(options);
    }
    std::vector<query::clustering_range> clustering_ranges(const query_options& options) const {
        return _analysis.get_clustering_bounds(options);
    }

    /// The rows the statement writes.  An UPDATE names whole rows, so each one is
    /// a clustering key rather than a range; the empty prefix is the static row.
    std::vector<clustering_key_prefix> clustering_rows(const query_options& options) const;

    /// The same, for a caller that already has the ranges and must not solve them
    /// a second time - a non-pure value in a key would not evaluate the same way
    /// twice, and the row written would not be the row read.
    static std::vector<clustering_key_prefix> clustering_rows(std::span<const query::clustering_range> ranges);

    /// Checks that the primary key restrictions don't contain null values, throws
    /// invalid_request_exception otherwise.
    void validate_primary_key(const query_options& options) const { _analysis.validate_primary_key(options); }
};

/**
 * What a DELETE statement's WHERE clause says about the rows it deletes.
 *
 * A DELETE is the only mutation that names a range of rows: it may slice the
 * clustering key, and need not name all of it.  What it may not then do is
 * delete a particular regular column of those rows.
 *
 * Built by analyze_delete_restrictions() below.
 */
class delete_restrictions {
    where_clause_analysis _analysis;
public:
    // Marks the constructor as internal: a constructed object says nothing until
    // it is analyzed, so go through the analyze_*_restrictions() factories below.
    struct private_tag { explicit private_tag() = default; };

    delete_restrictions(private_tag, schema_ptr schema);

    delete_restrictions(const delete_restrictions&) = delete;
    delete_restrictions& operator=(const delete_restrictions&) = delete;

    void analyze_delete(
            data_dictionary::database db,
            const expr::expression& where_clause,
            prepare_context& ctx,
            bool applies_only_to_static_columns);

    /// The clustering column the WHERE clause leaves unnamed while the statement
    /// may still delete regular columns; nullptr if it is free to do so.
    ///
    /// Takes the flag rather than remembering it: an IF [NOT] EXISTS condition
    /// is classified after the analysis and can change it.
    const column_definition* clustering_column_required_for_regular_columns(
            bool applies_only_to_static_columns) const;

    /// Rejects a WHERE clause that does not name the whole partition key.
    void reject_incomplete_partition_key() const;

    /// True if the WHERE clause names a range of rows rather than whole rows.
    bool deletes_a_range() const;

    /// True if the WHERE clause names exact rows: the whole clustering key, by
    /// equality.
    bool addresses_exact_rows() const;

    bool key_is_in_relation() const { return _analysis.key_is_in_relation(); }
    bool clustering_key_restrictions_has_IN() const { return _analysis.clustering_key_restrictions_has_IN(); }
    bool has_clustering_columns_restriction() const { return _analysis.has_clustering_columns_restriction(); }

    dht::partition_range_vector get_partition_key_ranges(const query_options& options) const {
        return _analysis.get_partition_key_ranges(options);
    }
    std::vector<query::clustering_range> clustering_ranges(const query_options& options) const {
        return _analysis.get_clustering_bounds(options);
    }

    /// Checks that the primary key restrictions don't contain null values, throws
    /// invalid_request_exception otherwise.
    void validate_primary_key(const query_options& options) const { _analysis.validate_primary_key(options); }
};

/**
 * What the SELECT defining a materialized view says about the base rows the view
 * has a row for.
 *
 * A view definition is not a query.  It is never executed, never reads an index
 * and never filters: it says which base rows the view covers, and which base
 * columns must be non-null for a view row to exist.  So there is no query plan
 * here at all - none of select_restrictions' index or filter state.
 *
 * Built by analyze_view_restrictions() below.
 */
class view_restrictions {
    where_clause_analysis _analysis;
public:
    // Marks the constructor as internal: a constructed object says nothing until
    // it is analyzed, so go through the analyze_*_restrictions() factories below.
    struct private_tag { explicit private_tag() = default; };

    view_restrictions(private_tag, schema_ptr schema);

    view_restrictions(const view_restrictions&) = delete;
    view_restrictions& operator=(const view_restrictions&) = delete;

    void analyze_view_definition(
            data_dictionary::database db,
            const expr::expression& where_clause,
            prepare_context& ctx);

    // The columns the view definition declares to be non-null, i.e. the base
    // columns a base row must have a value for to have a view row.  Handled
    // separately from the other restrictions: they select base rows rather than
    // filtering view rows, and so are not part of get_*_restrictions().
    const std::unordered_set<const column_definition*>& get_not_null_columns() const {
        return _analysis.not_null_columns;
    }

    /// True if the view definition restricts the column at all, IS NOT NULL
    /// included.  A view's primary key column has to be.
    bool is_restricted(const column_definition* cdef) const { return _analysis.is_restricted(cdef); }

    const expr::expression& get_partition_key_restrictions() const {
        return _analysis.partition_key_restrictions;
    }

    const expr::expression& get_clustering_columns_restrictions() const {
        return _analysis.clustering_columns_restrictions;
    }

    /// The restrictions on non-primary-key base columns - the view's filter.
    const expr::single_column_restrictions_map& get_non_pk_restriction() const {
        return _analysis.single_column_nonprimary_key_restrictions;
    }

    bool has_unrestricted_clustering_columns() const { return _analysis.has_unrestricted_clustering_columns(); }

    /// The clustering ranges of the base table the view covers.
    std::vector<query::clustering_range> clustering_ranges(const query_options& options) const {
        return _analysis.get_clustering_bounds(options);
    }
};

/**
 * What a SELECT statement's WHERE clause says about the rows it reads.
 *
 * A SELECT can read a secondary index and filter the rows it reads, so on top
 * of the restrictions themselves this holds the query plan: the index to read,
 * if any, and the filters to apply to what comes back.
 *
 * Built by analyze_select_restrictions().
 */
class select_restrictions {
    where_clause_analysis _analysis;

    /// True if the statement carries ALLOW FILTERING, so restrictions that no
    /// key order can express are permitted.
    bool _allow_filtering;

    check_indexes _check_indexes;

    /**
     * Scoring-function restrictions, e.g. WHERE BM25(col, 'term') > 0.
     *
     * Purely declarative. They express full-text matching intent,
     * but neither filter rows nor drive index selection themselves.
     * Extracted early and forwarded to the FTS layer as-is.
     */
    std::vector<expr::binary_operator> _scoring_function_restrictions;

    expr::expression _partition_level_filter = expr::conjunction({});
    expr::expression _clustering_row_level_filter = expr::conjunction({});

    /**
     * <code>true</code> if the secondary index need to be queried, <code>false</code> otherwise
     */
    bool _uses_secondary_indexing = false;

    /**
     * Specify if the query will return a range of partition keys.
     */
    bool _is_key_range = false;

    bool _has_queriable_regular_index = false, _has_queriable_pk_index = false, _has_queriable_ck_index = false;

    std::vector<const column_definition*> _column_defs_for_filtering;
    schema_ptr _view_schema;
    std::unique_ptr<secondary_index::index> _idx_opt;
    std::vector<predicate> _idx_column_predicates; ///< Predicates for the chosen index's target column.

    /// Like where_clause_analysis::clustering_prefix_restrictions, but for the indexing table (if this is an
    /// index-reading statement).
    /// Recall that the index-table CK is (token, PK, CK) of the base table for a global index and (indexed column,
    /// CK) for a local index.
    ///
    /// Elements are conjunctions of single-column binary operators with the same LHS.
    /// Element order follows the indexing-table clustering key.
    /// In case of a global index the first element's (token restriction) RHS is a dummy value, it is filled later.
    std::optional<std::vector<predicate>> _idx_tbl_ck_prefix;

    get_clustering_bounds_fn_t _get_global_index_clustering_ranges_fn;
    get_clustering_bounds_fn_t _get_global_index_token_clustering_ranges_fn;
    get_clustering_bounds_fn_t _get_local_index_clustering_ranges_fn;
    get_singleton_value_fn_t _value_for_index_partition_key_fn;

public:
    // Marks the constructor as internal: a constructed object says nothing until
    // it is analyzed, so go through the analyze_*_restrictions() factories below.
    struct private_tag { explicit private_tag() = default; };

    select_restrictions(private_tag, schema_ptr schema, bool allow_filtering, check_indexes do_check_indexes);

    select_restrictions(const select_restrictions&) = delete;
    select_restrictions& operator=(const select_restrictions&) = delete;

    /// Reads the WHERE clause of a SELECT statement and plans the query.
    void analyze_select(
            data_dictionary::database db,
            const expr::expression& where_clause,
            prepare_context& ctx,
            bool selects_only_static_columns,
            pinned_plan_opt pinned_plan);

    /// Initializes the object for a statement with no WHERE clause: every
    /// partition, every row, nothing to filter.
    void no_restrictions();

    const expr::expression& get_partition_key_restrictions() const {
        return _analysis.partition_key_restrictions;
    }

    const expr::expression& get_clustering_columns_restrictions() const {
        return _analysis.clustering_columns_restrictions;
    }

    const expr::expression& get_nonprimary_key_restrictions() const {
        return _analysis.nonprimary_key_restrictions;
    }

    const expr::single_column_restrictions_map& get_non_pk_restriction() const {
        return _analysis.single_column_nonprimary_key_restrictions;
    }

    bool key_is_in_relation() const { return _analysis.key_is_in_relation(); }
    bool clustering_key_restrictions_has_IN() const { return _analysis.clustering_key_restrictions_has_IN(); }
    bool clustering_key_restrictions_has_only_eq() const { return _analysis.ck_is_all_eq; }
    bool has_token_restrictions() const { return _analysis.has_token_restrictions(); }
    bool has_eq_restriction_on_column(const column_definition& column) const {
        return _analysis.has_eq_restriction_on_column(column);
    }
    bool has_partition_key_unrestricted_components() const {
        return _analysis.has_partition_key_unrestricted_components();
    }
    bool partition_key_restrictions_is_empty() const { return _analysis.partition_key_restrictions_is_empty(); }
    bool partition_key_restrictions_is_all_eq() const { return _analysis.pk_is_all_eq; }
    size_t partition_key_restrictions_size() const { return _analysis.partition_key_restrictions_size(); }
    size_t clustering_columns_restrictions_size() const { return _analysis.clustering_columns_restrictions_size(); }
    bool has_clustering_columns_restriction() const { return _analysis.has_clustering_columns_restriction(); }
    bool has_unrestricted_clustering_columns() const { return _analysis.has_unrestricted_clustering_columns(); }
    bool has_non_primary_key_restriction() const { return _analysis.has_non_primary_key_restriction(); }
    bool is_restricted(const column_definition* cdef) const { return _analysis.is_restricted(cdef); }
    bool is_empty() const { return _analysis.is_empty(); }

    dht::partition_range_vector get_partition_key_ranges(const query_options& options) const {
        return _analysis.get_partition_key_ranges(options);
    }
    std::vector<query::clustering_range> get_clustering_bounds(const query_options& options) const {
        return _analysis.get_clustering_bounds(options);
    }

    const std::vector<expr::binary_operator>& get_scoring_function_restrictions() const {
        return _scoring_function_restrictions;
    }

    /**
     * Checks if the query request a range of partition keys.
     *
     * @return <code>true</code> if the query request a range of partition keys, <code>false</code> otherwise.
     */
    bool is_key_range() const {
        return _is_key_range;
    }

    /**
     * Checks if the secondary index need to be queried.
     *
     * @return <code>true</code> if the secondary index need to be queried, <code>false</code> otherwise.
     */
    bool uses_secondary_indexing() const {
        return _uses_secondary_indexing;
    }

    /**
     * Builds a possibly empty collection of column definitions that will be used for filtering
     * @param db - the data_dictionary::database context
     * @return A list with the column definitions needed for filtering.
     */
    std::vector<const column_definition*> get_column_defs_for_filtering(data_dictionary::database db) const;

    /**
     * Determines the index to be used with the restriction.
     * @param sim - the index manager
     * @return If an index can be used, an optional containing this index, otherwise an empty optional.
     */
    std::optional<secondary_index::index> find_idx(const secondary_index::secondary_index_manager& sim) const;

    schema_ptr get_view_schema() const { return _view_schema; }

    /**
     * Checks if the query need to use filtering.
     * @return <code>true</code> if the query need to use filtering, <code>false</code> otherwise.
     */
    bool need_filtering() const;

    bool pk_restrictions_need_filtering() const { return _analysis.pk_restrictions_need_filtering(); }
    bool clustering_key_restrictions_need_filtering() const {
        return _analysis.clustering_key_restrictions_need_filtering();
    }
    bool ck_restrictions_need_filtering() const;

    // Returns any filter that needs to be applied to a row, but if it fails, it will fail for all rows in the partition.
    // If a column is used for a secondary index, it will not be in the filter.
    //
    // This filter will only reference partition key columns and static columns.
    const expr::expression& get_partition_level_filter() const {
        return _partition_level_filter;
    }

    // Returns any filter that needs to be applied to each clustering row. If one of the column restrictions is translated
    // to read_command, it will not be in the filter.
    const expr::expression& get_clustering_row_level_filter() const {
        return _clustering_row_level_filter;
    }

    /// Calculates clustering ranges for querying a global-index table.
    std::vector<query::clustering_range> get_global_index_clustering_ranges(
            const query_options& options) const;

    /// Calculates clustering ranges for querying a global-index table for queries with token restrictions present.
    std::vector<query::clustering_range> get_global_index_token_clustering_ranges(
            const query_options& options) const;

    /// Calculates clustering ranges for querying a local-index table.
    std::vector<query::clustering_range> get_local_index_clustering_ranges(
            const query_options& options) const;

    /// Finds the value of partition key of the index table
    bytes_opt value_for_index_partition_key(const query_options&) const;

private:
    /// The part of the analysis a view definition shares with an ordinary
    /// SELECT, starting from an already prepared WHERE clause.
    void analyze_read(
            data_dictionary::database db,
            where_clause_predicates where,
            bool selects_only_static_columns,
            pinned_plan_opt pinned_plan);

    /// Decides which index, if any, this query reads, and what it has to filter.
    void plan_query(
            data_dictionary::database db,
            const column_predicates& preds,
            bool selects_only_static_columns,
            pinned_plan_opt pinned_plan);

    void detect_queriable_indexes(
            data_dictionary::database db,
            const column_predicates& preds,
            bool force_base_plan,
            const std::optional<sstring>& pinned_index_name);

    void process_partition_key_restrictions();

    /**
     * Processes the clustering column restrictions.
     *
     * @throws InvalidRequestException if the request is invalid
     */
    void process_clustering_columns_restrictions();

    void build_filters(const column_predicates& preds);

    void calculate_column_defs_for_filtering_and_erase_restrictions_used_for_index(
            data_dictionary::database db,
            const column_predicates& preds);

    void validate_secondary_index_selections() const;

    /// Prepares internal data for evaluating index-table queries.  Must be called before
    /// get_local_index_clustering_ranges().
    void prepare_indexed_local(const schema& idx_tbl_schema, const column_predicates& preds);

    /// Prepares internal data for evaluating index-table queries.  Must be called before
    /// get_global_index_clustering_ranges() or get_global_index_token_clustering_ranges().
    void prepare_indexed_global(const schema& idx_tbl_schema);

    /**
     * Adds restrictions from where_clause_analysis::clustering_prefix_restrictions to _idx_tbl_ck_prefix.
     * Translates restrictions to use columns from the index schema instead of the base schema.
     *
     * @param idx_tbl_schema Schema of the index table
     */
    void add_clustering_restrictions_to_idx_ck_prefix(const schema& idx_tbl_schema);

    /// Builds the functions computing the ranges to read from an index table.
    void build_index_fns();
    get_clustering_bounds_fn_t build_get_global_index_clustering_ranges_fn() const;
    get_clustering_bounds_fn_t build_get_global_index_token_clustering_ranges_fn() const;
    get_clustering_bounds_fn_t build_get_local_index_clustering_ranges_fn() const;
    get_singleton_value_fn_t build_value_for_index_partition_key_fn() const;
};

// One entry point per statement type.  What a statement may do with a WHERE
// clause depends on the statement: only a SELECT can read an index or filter
// rows, a mutation has to name the rows it writes, and IS NOT NULL declares a
// materialized view's key columns rather than filtering.  Asking for the
// analysis by statement type keeps each caller from having to spell out the
// rules its statement plays by.

/// Analyzes the WHERE clause of a SELECT statement.
shared_ptr<const select_restrictions> analyze_select_restrictions(
        data_dictionary::database db,
        schema_ptr schema,
        const expr::expression& where_clause,
        prepare_context& ctx,
        bool selects_only_static_columns,
        bool allow_filtering,
        check_indexes do_check_indexes,
        pinned_plan_opt pinned_plan = std::nullopt);

/// Reads the WHERE clause of a materialized view's definition.
shared_ptr<const view_restrictions> analyze_view_restrictions(
        data_dictionary::database db,
        schema_ptr schema,
        const expr::expression& where_clause,
        prepare_context& ctx);

/// Analyzes the WHERE clause of an UPDATE statement.
shared_ptr<const update_restrictions> analyze_update_restrictions(
        data_dictionary::database db,
        schema_ptr schema,
        const expr::expression& where_clause,
        prepare_context& ctx,
        bool applies_only_to_static_columns);

/// Analyzes the WHERE clause of a DELETE statement.
shared_ptr<const delete_restrictions> analyze_delete_restrictions(
        data_dictionary::database db,
        schema_ptr schema,
        const expr::expression& where_clause,
        prepare_context& ctx,
        bool applies_only_to_static_columns);

/// Restrictions that restrict nothing, for a statement that does not work out
/// the rows it addresses from a WHERE clause.
///
/// The pager asks for these to put a query on the filtering path - which
/// re-applies the per-partition limit on every page - with no filter of its own.
shared_ptr<const select_restrictions> make_empty_select_restrictions(schema_ptr schema);


// Checks whether this expression is empty - doesn't restrict anything
bool is_empty_restriction(const expr::expression&);

}

}
