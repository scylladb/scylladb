/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <vector>
#include "bounds_slice.hh"
#include "cql3/expr/expression.hh"
#include "cql3/expr/restrictions.hh"
#include "schema/schema_fwd.hh"
#include "cql3/prepare_context.hh"
#include "cql3/statements/statement_type.hh"
#include "query-request.hh"

namespace cql3 {

namespace restrictions {

///In some cases checking if columns have indexes is undesired of even
///impossible, because e.g. the query runs on a pseudo-table, which does not
///have an index-manager, or even a table object.
using check_indexes = bool_class<class check_indexes_tag>;

/**
 * The restrictions corresponding to the relations specified on the where-clause of CQL query.
 */
class statement_restrictions {
private:
    schema_ptr _schema;

    /**
     * Restrictions on partitioning columns
     */
    expr::expression _partition_key_restrictions = expr::conjunction({});

    expr::single_column_restrictions_map _single_column_partition_key_restrictions;

    /**
     * Restrictions on clustering columns
     */
    expr::expression _clustering_columns_restrictions = expr::conjunction({});

    expr::single_column_restrictions_map _single_column_clustering_key_restrictions;

    /**
     * Restriction on non-primary key columns (i.e. secondary index restrictions)
     */
    expr::expression _nonprimary_key_restrictions = expr::conjunction({});

    expr::single_column_restrictions_map _single_column_nonprimary_key_restrictions;

    std::unordered_set<const column_definition*> _not_null_columns;

    /**
     * The restrictions used to build the index expressions
     */
    std::vector<expr::expression> _index_restrictions;

    /**
     * <code>true</code> if the secondary index need to be queried, <code>false</code> otherwise
     */
    bool _uses_secondary_indexing = false;

    /**
     * Specify if the query will return a range of partition keys.
     */
    bool _is_key_range = false;

    bool _has_queriable_regular_index = false, _has_queriable_pk_index = false, _has_queriable_ck_index = false;
    bool _has_multi_column; ///< True iff _clustering_columns_restrictions has a multi-column restriction.

    std::optional<expr::expression> _where; ///< The entire WHERE clause.

    /// Parts of _where defining the clustering slice.
    ///
    /// Meets all of the following conditions:
    /// 1. all elements must be simultaneously satisfied (as restrictions) for _where to be satisfied
    /// 2. each element is an atom or a conjunction of atoms
    /// 3. either all atoms (across all elements) are multi-column or they are all single-column
    /// 4. if single-column, then:
    ///   4.1 all atoms from an element have the same LHS, which we call the element's LHS
    ///   4.2 each element's LHS is different from any other element's LHS
    ///   4.3 the list of each element's LHS, in order, forms a clustering-key prefix
    ///   4.4 elements other than the last have only EQ or IN atoms
    ///   4.5 the last element has only EQ, IN, or is_slice() atoms
    /// 5. if multi-column, then each element is a binary_operator
    std::vector<expr::expression> _clustering_prefix_restrictions;

    /// Like _clustering_prefix_restrictions, but for the indexing table (if this is an index-reading statement).
    /// Recall that the index-table CK is (token, PK, CK) of the base table for a global index and (indexed column,
    /// CK) for a local index.
    ///
    /// Elements are conjunctions of single-column binary operators with the same LHS.
    /// Element order follows the indexing-table clustering key.
    /// In case of a global index the first element's (token restriction) RHS is a dummy value, it is filled later.
    std::optional<std::vector<expr::expression>> _idx_tbl_ck_prefix;

    /// Parts of _where defining the partition range.
    ///
    /// If the partition range is dictated by token restrictions, this is a single element that holds all the
    /// binary_operators on token.  If single-column restrictions define the partition range, each element holds
    /// restrictions for one partition column.  Each partition column has a corresponding element, but the elements
    /// are in arbitrary order.
    std::vector<expr::expression> _partition_range_restrictions;

    bool _partition_range_is_simple; ///< False iff _partition_range_restrictions imply a Cartesian product.


    check_indexes _check_indexes = check_indexes::yes;
public:
    /**
     * Creates a new empty <code>StatementRestrictions</code>.
     *
     * @param cfm the column family meta data
     * @return a new empty <code>StatementRestrictions</code>.
     */
    statement_restrictions(schema_ptr schema, bool allow_filtering);

    statement_restrictions(data_dictionary::database db,
        schema_ptr schema,
        statements::statement_type type,
        const expr::expression& where_clause,
        prepare_context& ctx,
        bool selects_only_static_columns,
        bool for_view = false,
        bool allow_filtering = false,
        check_indexes do_check_indexes = check_indexes::yes);

    const std::vector<expr::expression>& index_restrictions() const;

    /**
     * Checks if the restrictions on the partition key is an IN restriction.
     *
     * @return <code>true</code> the restrictions on the partition key is an IN restriction, <code>false</code>
     * otherwise.
     */
    bool key_is_in_relation() const;

    /**
     * Checks if the restrictions on the clustering key is an IN restriction.
     *
     * @return <code>true</code> the restrictions on the partition key is an IN restriction, <code>false</code>
     * otherwise.
     */
    bool clustering_key_restrictions_has_IN() const;

    bool clustering_key_restrictions_has_only_eq() const;

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

    const expr::expression& get_partition_key_restrictions() const {
        return _partition_key_restrictions;
    }

    const expr::expression& get_clustering_columns_restrictions() const {
        return _clustering_columns_restrictions;
    }

    // Get a set of columns restricted by the IS NOT NULL restriction.
    // IS NOT NULL is a special case that is handled separately from other restrictions.
    const std::unordered_set<const column_definition*> get_not_null_columns() const;

    bool has_token_restrictions() const;

    // Checks whether the given column has an EQ restriction.
    // EQ restriction is `col = ...` or `(col, col2) = ...`
    // IN restriction is NOT an EQ restriction, this function will not look for IN restrictions.
    // Uses column_defintion::operator== for comparison, columns with the same name but different schema will not be equal.
    bool has_eq_restriction_on_column(const column_definition&) const;

    /**
     * Builds a possibly empty collection of column definitions that will be used for filtering
     * @param db - the data_dictionary::database context
     * @return A list with the column definitions needed for filtering.
     */
    std::vector<const column_definition*> get_column_defs_for_filtering(data_dictionary::database db) const;

    /**
     * Gives a score that the index has - index with the highest score will be chosen
     * in find_idx()
     */
    int score(const secondary_index::index& index) const;

    /**
     * Determines the index to be used with the restriction.
     * @param db - the data_dictionary::database context (for extracting index manager)
     * @return If an index can be used, an optional containing this index, otherwise an empty optional.
     * In case the index is returned, second parameter returns the index restriction it uses.
     */
    std::pair<std::optional<secondary_index::index>, expr::expression> find_idx(const secondary_index::secondary_index_manager& sim) const;

    /**
     * Checks if the partition key has some unrestricted components.
     * @return <code>true</code> if the partition key has some unrestricted components, <code>false</code> otherwise.
     */
    bool has_partition_key_unrestricted_components() const;

    bool partition_key_restrictions_is_empty() const;

    bool partition_key_restrictions_is_all_eq() const;

    size_t partition_key_restrictions_size() const;

    bool parition_key_restrictions_have_supporting_index(const secondary_index::secondary_index_manager& index_manager, expr::allow_local_index allow_local) const;

    size_t clustering_columns_restrictions_size() const;

    bool clustering_columns_restrictions_have_supporting_index(
        const secondary_index::secondary_index_manager& index_manager,
        expr::allow_local_index allow_local) const;

    bool multi_column_clustering_restrictions_are_supported_by(const secondary_index::index& index) const;

    bounds_slice get_clustering_slice() const;

    /**
     * Checks if the clustering key has some unrestricted components.
     * @return <code>true</code> if the clustering key has some unrestricted components, <code>false</code> otherwise.
     */
    bool has_unrestricted_clustering_columns() const;

    /**
     * Returns the first unrestricted column for restrictions of the specified kind.
     * It's an error to call this function if there are no such columns.
     *
     * @param kind supported values are column_kind::partition_key and column_kind::clustering_key;
     * @return the <code>column_definition</code> for the unrestricted column.
     */
    const column_definition& unrestricted_column(column_kind kind) const;
private:
    void add_restriction(const expr::binary_operator& restr, schema_ptr schema, bool allow_filtering, bool for_view);
    void add_is_not_restriction(const expr::binary_operator& restr, schema_ptr schema, bool for_view);
    void add_single_column_parition_key_restriction(const expr::binary_operator& restr, schema_ptr schema, bool allow_filtering, bool for_view);
    void add_token_partition_key_restriction(const expr::binary_operator& restr);
    void add_single_column_clustering_key_restriction(const expr::binary_operator& restr, schema_ptr schema, bool allow_filtering);
    void add_multi_column_clustering_key_restriction(const expr::binary_operator& restr);
    void add_single_column_nonprimary_key_restriction(const expr::binary_operator& restr);

    void process_partition_key_restrictions(bool for_view, bool allow_filtering, statements::statement_type type);

    /**
     * Processes the clustering column restrictions.
     *
     * @param has_queriable_index <code>true</code> if some of the queried data are indexed, <code>false</code> otherwise
     * @throws InvalidRequestException if the request is invalid
     */
    void process_clustering_columns_restrictions(bool for_view, bool allow_filtering);

    /**
     * Returns the <code>Restrictions</code> for the specified type of columns.
     *
     * @param kind the column type
     * @return the <code>restrictions</code> for the specified type of columns
     */
    const expr::expression& get_restrictions(column_kind kind) const;

    /**
     * Adds restrictions from _clustering_prefix_restrictions to _idx_tbl_ck_prefix.
     * Translates restrictions to use columns from the index schema instead of the base schema.
     *
     * @param idx_tbl_schema Schema of the index table
     */
    void add_clustering_restrictions_to_idx_ck_prefix(const schema& idx_tbl_schema);

    unsigned int num_clustering_prefix_columns_that_need_not_be_filtered() const;

public:
    /**
     * Returns the specified range of the partition key.
     *
     * @param b the boundary type
     * @param options the query options
     * @return the specified bound of the partition key
     * @throws InvalidRequestException if the boundary cannot be retrieved
     */
    dht::partition_range_vector get_partition_key_ranges(const query_options& options) const;


public:
    std::vector<query::clustering_range> get_clustering_bounds(const query_options& options) const;

    /**
     * Checks if the query need to use filtering.
     * @return <code>true</code> if the query need to use filtering, <code>false</code> otherwise.
     */
    bool need_filtering() const;

    void validate_secondary_index_selections(bool selects_only_static_columns);

    /**
     * Checks if the query has some restrictions on the clustering columns.
     *
     * @return <code>true</code> if the query has some restrictions on the clustering columns,
     * <code>false</code> otherwise.
     */
    bool has_clustering_columns_restriction() const;

    /**
     * Checks if the restrictions contain any non-primary key restrictions
     *
     * @return <code>true</code> if the restrictions contain any non-primary key restrictions, <code>false</code> otherwise.
     */
    bool has_non_primary_key_restriction() const;

    bool pk_restrictions_need_filtering() const;

    bool ck_restrictions_need_filtering() const;

    bool clustering_key_restrictions_need_filtering() const;

    /**
     * @return true if column is restricted by some restriction, false otherwise
     */
    bool is_restricted(const column_definition* cdef) const;

     /**
      * @return the non-primary key restrictions.
      */
    const expr::single_column_restrictions_map& get_non_pk_restriction() const {
        return _single_column_nonprimary_key_restrictions;
    }

    /**
     * @return partition key restrictions split into single column restrictions (e.g. for filtering support).
     */
    const expr::single_column_restrictions_map& get_single_column_partition_key_restrictions() const;

    /**
     * @return clustering key restrictions split into single column restrictions (e.g. for filtering support).
     */
    const expr::single_column_restrictions_map& get_single_column_clustering_key_restrictions() const;

    /// Prepares internal data for evaluating index-table queries.  Must be called before
    /// get_local_index_clustering_ranges().
    void prepare_indexed_local(const schema& idx_tbl_schema);

    /// Prepares internal data for evaluating index-table queries.  Must be called before
    /// get_global_index_clustering_ranges() or get_global_index_token_clustering_ranges().
    void prepare_indexed_global(const schema& idx_tbl_schema);

    /// Calculates clustering ranges for querying a global-index table.
    std::vector<query::clustering_range> get_global_index_clustering_ranges(
            const query_options& options, const schema& idx_tbl_schema) const;

    /// Calculates clustering ranges for querying a global-index table for queries with token restrictions present.
    std::vector<query::clustering_range> get_global_index_token_clustering_ranges(
            const query_options& options, const schema& idx_tbl_schema) const;

    /// Calculates clustering ranges for querying a local-index table.
    std::vector<query::clustering_range> get_local_index_clustering_ranges(
            const query_options& options, const schema& idx_tbl_schema) const;

    sstring to_string() const;

    /// Checks that the primary key restrictions don't contain null values, throws invalid_request_exception otherwise.
    void validate_primary_key(const query_options& options) const;
};

}

}
