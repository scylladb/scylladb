/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include <vector>
#include "bounds_slice.hh"
#include "cql3/expr/expression.hh"
#include "cql3/expr/restrictions.hh"
#include "schema/schema_fwd.hh"
#include "cql3/prepare_context.hh"
#include "cql3/statements/statement_type.hh"
#include "query/query-request.hh"
#include "service/pager/query_plan.hh"

namespace cql3 {

namespace restrictions {

/// A set of discrete values.
using value_list = std::vector<managed_bytes>; // Sorted and deduped using value comparator.

/// General set of values.  Empty set and single-element sets are always value_list.  interval is
/// never singular and never has start > end.  Universal set is a interval with both bounds null.
using value_set = std::variant<value_list, interval<managed_bytes>>;

// For some boolean expression (say (X = 3) = TRUE, this represents a function that solves for X.
// (here, it would return 3). The expression is obtained by equating some factors of the WHERE
// clause to TRUE.
using solve_for_t = std::function<value_set (const query_options&)>;

struct on_row {
    bool operator==(const on_row&) const = default;
};

struct on_column {
    const column_definition* column;

    bool operator==(const on_column&) const = default;
};

// Placeholder type indicating we're solving for the partition key token.
struct on_partition_key_token {
    const ::schema* schema;

    bool operator==(const on_partition_key_token&) const = default;
};

struct on_clustering_key_prefix {
    std::vector<const column_definition*> columns;

    bool operator==(const on_clustering_key_prefix&) const = default;
};

// A predicate on a column or a combination of columns. The WHERE clause analyzer
// will attempt to convert predicates (that return true or false for a particular row)
// to solvers (that return the set of column values that satisfy the predicate) when possible.
struct predicate {
    // A function that returns the set of values that satisfy the filter. Can be unset,
    // in which case the filter must be interpreted.
    solve_for_t solve_for;
    // The original filter for this column.
    expr::expression filter;
    // What column the predicate can be solved for
    std::variant<
            on_row,                        // cannot determine, so predicate is on entire row
            on_column,                     // solving for a single column: e.g. c1 = 3
            on_partition_key_token,        // solving for the token, e.g. token(pk1, pk2) >= :var
            on_clustering_key_prefix       // solving for a clustering key prefix: e.g. (ck1, ck2) >= (3, 4)
    > on;
    // Whether the returned value_set will resolve to a single value.
    bool is_singleton = false;
    // Whether the returned value_set follows CQL comparison semantics
    bool comparable = true;
    bool is_multi_column = false;
    bool is_not_null_single_column = false;
    bool equality = false;        // operator is EQ
    bool is_in = false;           // operator is IN
    bool is_slice = false;        // operator is LT/LTE/GT/GTE
    bool is_upper_bound = false;  // operator is LT/LTE
    bool is_lower_bound = false;  // operator is GT/GTE
    expr::comparison_order order = expr::comparison_order::cql;
    std::optional<expr::oper_t> op;  // the binary operator, if any
    bool is_subscript = false;       // whether the LHS is a subscript (map element access)
};

///In some cases checking if columns have indexes is undesired of even
///impossible, because e.g. the query runs on a pseudo-table, which does not
///have an index-manager, or even a table object.
using check_indexes = bool_class<class check_indexes_tag>;

/// The plan a continued paged query must keep scanning - the base table, or an
/// index view. The query fails if that plan is gone or cannot serve the query;
/// std::nullopt plans normally. See #18992.
using pinned_plan_opt = std::optional<service::pager::query_plan>;

// A function that returns the partition key ranges for a query. It is the solver of
// WHERE clause fragments such as WHERE token(pk) > 1 or WHERE pk1 IN :list1 AND pk2 IN :list2.
using get_partition_key_ranges_fn_t = std::function<dht::partition_range_vector (const query_options&)>;

// A function that returns the clustering key ranges for a query. It is the solver of
// WHERE clause fragments such as WHERE ck > 1 or WHERE (ck1, ck2) > (1, 2).
using get_clustering_bounds_fn_t = std::function<std::vector<query::clustering_range> (const query_options& options)>;

// A function that returns a singleton value, usable for a key (e.g. bytes_opt)
using get_singleton_value_fn_t = std::function<bytes_opt (const query_options&)>;

struct no_partition_range_restrictions {
};

struct token_range_restrictions {
    predicate token_restrictions;
};

struct single_column_partition_range_restrictions {
    std::vector<predicate> per_column_restrictions;
};

using partition_range_restrictions = std::variant<
        no_partition_range_restrictions,
        token_range_restrictions,
        single_column_partition_range_restrictions>;

// A map of per-column predicate vectors, ordered by schema position.
using single_column_predicate_vectors = std::map<const column_definition*, std::vector<predicate>, expr::schema_pos_column_definition_comparator>;

// The per-column predicates the WHERE-clause analysis produced, grouped by the
// kind of column they restrict.  Only the stages that pick an index and decide
// what has to be filtered need them, so they are handed from the analysis to
// those stages rather than kept in the object.
struct column_predicates {
    single_column_predicate_vectors partition_key;
    single_column_predicate_vectors clustering_key;
    single_column_predicate_vectors other;
};

/**
 * The restrictions corresponding to the relations specified on the where-clause of CQL query.
 */
class statement_restrictions {
public:
    // Marks the constructors as internal: go through the analyze_*_restrictions()
    // factories at the bottom of this file.
    struct private_tag { explicit private_tag() = default; };

private:
    schema_ptr _schema;

    /**
     * Restrictions on partitioning columns
     */
    expr::expression _partition_key_restrictions = expr::conjunction({});

    expr::single_column_restrictions_map _single_column_partition_key_restrictions;
    expr::expression _partition_level_filter = expr::conjunction({});

    /**
     * Restrictions on clustering columns
     */
    expr::expression _clustering_columns_restrictions = expr::conjunction({});

    expr::single_column_restrictions_map _single_column_clustering_key_restrictions;
    expr::expression _clustering_row_level_filter = expr::conjunction({});

    /**
     * Restriction on non-primary key columns (i.e. secondary index restrictions)
     */
    expr::expression _nonprimary_key_restrictions = expr::conjunction({});

    expr::single_column_restrictions_map _single_column_nonprimary_key_restrictions;

    /**
     * Scoring-function restrictions, e.g. WHERE BM25(col, 'term') > 0.
     *
     * Purely declarative. They express full-text matching intent,
     * but neither filter rows nor drive index selection themselves.
     * Extracted early and forwarded to the FTS layer as-is.
     */
    std::vector<expr::binary_operator> _scoring_function_restrictions;


    std::unordered_set<const column_definition*> _not_null_columns;

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
    bool _ck_is_on_collection = false; ///< True iff _clustering_columns_restrictions has a collection restriction (CONTAINS/CONTAINS_KEY).
    bool _ck_is_all_eq = true; ///< True iff all CK restrictions use EQ operator only.
    bool _pk_is_all_eq = true; ///< True iff all PK restrictions use EQ operator only.

    std::vector<expr::expression> _where; ///< The entire WHERE clause (factorized).

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
    std::vector<predicate> _clustering_prefix_restrictions;

    /// Like _clustering_prefix_restrictions, but for the indexing table (if this is an index-reading statement).
    /// Recall that the index-table CK is (token, PK, CK) of the base table for a global index and (indexed column,
    /// CK) for a local index.
    ///
    /// Elements are conjunctions of single-column binary operators with the same LHS.
    /// Element order follows the indexing-table clustering key.
    /// In case of a global index the first element's (token restriction) RHS is a dummy value, it is filled later.
    std::optional<std::vector<predicate>> _idx_tbl_ck_prefix;

    /// Parts of _where defining the partition range.
    ///
    /// If the partition range is dictated by token restrictions, this is a single element that holds all the
    /// binary_operators on token.  If single-column restrictions define the partition range, each element holds
    /// restrictions for one partition column.  Each partition column has a corresponding element, but the elements
    /// are in arbitrary order.
    partition_range_restrictions _partition_range_restrictions;

    bool _partition_range_is_simple; ///< False iff _partition_range_restrictions imply a Cartesian product.
    bool _pk_has_slice_or_needs_filtering = false; ///< True iff any PK restriction has a slice or needs-filtering operator.


    /// True if the statement may read rows it does not need and drop them.  When
    /// it may not, every restriction has to translate to a partition or
    /// clustering range, which rules out slices no key order can express.
    bool _allow_filtering;

    check_indexes _check_indexes;
    /// Columns that appear on the LHS of an EQ restriction (not IN).
    /// For multi-column EQ like (ck1, ck2) = (1, 2), all columns in the tuple are included.
    std::unordered_set<const column_definition*> _columns_with_eq;
    std::vector<const column_definition*> _column_defs_for_filtering;
    schema_ptr _view_schema;
    std::unique_ptr<secondary_index::index> _idx_opt;
    std::vector<predicate> _idx_column_predicates; ///< Predicates for the chosen index's target column.
    get_partition_key_ranges_fn_t _get_partition_key_ranges_fn;
    get_clustering_bounds_fn_t _get_clustering_bounds_fn;
    get_clustering_bounds_fn_t _get_global_index_clustering_ranges_fn;
    get_clustering_bounds_fn_t _get_global_index_token_clustering_ranges_fn;
    get_clustering_bounds_fn_t _get_local_index_clustering_ranges_fn;
    get_singleton_value_fn_t _value_for_index_partition_key_fn;
public:
    statement_restrictions(private_tag, schema_ptr schema, bool allow_filtering, check_indexes do_check_indexes);

    // Important: objects of this class captures `this` extensively and so must remain non-copyable.
    statement_restrictions(const statement_restrictions&) = delete;
    statement_restrictions& operator=(const statement_restrictions&) = delete;

    // Each statement type runs the analysis steps that apply to it, and no
    // others.  The analyze_*_restrictions() functions at the bottom of this file
    // are the way in.

    /// Reads the WHERE clause of a SELECT statement and plans the query.
    void analyze_select(
        data_dictionary::database db,
        const expr::expression& where_clause,
        prepare_context& ctx,
        bool selects_only_static_columns,
        pinned_plan_opt pinned_plan);

    /// Reads the WHERE clause of the SELECT statement defining a materialized
    /// view and plans the query the view is refreshed by.
    void analyze_view_definition(
        data_dictionary::database db,
        const expr::expression& where_clause,
        prepare_context& ctx,
        bool selects_only_static_columns);

    /// Reads the WHERE clause of a mutation.  The type only picks the wording of
    /// the errors this may throw.
    void analyze_mutation(
        data_dictionary::database db,
        statements::statement_type type,
        const expr::expression& where_clause,
        prepare_context& ctx);

    /// Rejects a WHERE clause that restricts clustering columns although the
    /// statement writes only static columns: the clustering key names a row the
    /// statement then does not write, which is never what the user meant.
    ///
    /// Does not apply to an INSERT, which creates the row it names.
    void reject_clustering_restrictions(statements::statement_type type) const;

    /// Initializes the object for a statement that does not work out the rows it
    /// addresses from a WHERE clause: every partition, every row.
    void no_restrictions();

public:

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

    const std::vector<expr::binary_operator>& get_scoring_function_restrictions() const {
        return _scoring_function_restrictions;
    }

    const expr::expression& get_partition_key_restrictions() const {
        return _partition_key_restrictions;
    }

    const expr::expression& get_clustering_columns_restrictions() const {
        return _clustering_columns_restrictions;
    }

    const expr::expression& get_nonprimary_key_restrictions() const {
        return _nonprimary_key_restrictions;
    }

    // Get a set of columns restricted by the IS NOT NULL restriction.
    // IS NOT NULL is a special case that is handled separately from other restrictions.
    const std::unordered_set<const column_definition*> get_not_null_columns() const;

    bool has_token_restrictions() const;

    // Checks whether the given column has an EQ restriction (not IN).
    bool has_eq_restriction_on_column(const column_definition&) const;

    /**
     * Builds a possibly empty collection of column definitions that will be used for filtering
     * @param db - the data_dictionary::database context
     * @return A list with the column definitions needed for filtering.
     */
    std::vector<const column_definition*> get_column_defs_for_filtering(data_dictionary::database db) const;

    /**
     * Determines the index to be used with the restriction.
     * @param db - the data_dictionary::database context (for extracting index manager)
     * @return If an index can be used, an optional containing this index, otherwise an empty optional.
     */
    std::optional<secondary_index::index> find_idx(const secondary_index::secondary_index_manager& sim) const;

    /**
     * Checks if the partition key has some unrestricted components.
     * @return <code>true</code> if the partition key has some unrestricted components, <code>false</code> otherwise.
     */
    bool has_partition_key_unrestricted_components() const;

    bool partition_key_restrictions_is_empty() const;

    bool partition_key_restrictions_is_all_eq() const;

    size_t partition_key_restrictions_size() const;

    size_t clustering_columns_restrictions_size() const;

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

    schema_ptr get_view_schema() const { return _view_schema; }
private:
    // The WHERE clause, prepared and turned into predicates.  Scoring-function
    // restrictions are kept apart: they are purely declarative and never enter
    // the restriction, index or filtering machinery.
    struct where_clause_predicates {
        std::vector<predicate> predicates;
        std::vector<expr::binary_operator> scoring_functions;
    };

    /// Prepares the WHERE clause against the schema and turns it into predicates.
    where_clause_predicates prepare_where_clause(
            data_dictionary::database db,
            const expr::expression& where_clause,
            prepare_context& ctx);

    /// Sorts the predicates by the columns they restrict, and works out the
    /// clustering prefix and the partition range they address.
    column_predicates classify_predicates(std::vector<predicate> predicates, bool allow_filtering);

    /// The part of the analysis a view definition shares with an ordinary
    /// SELECT, starting from an already prepared WHERE clause.
    void analyze_read(
            data_dictionary::database db,
            where_clause_predicates where,
            bool selects_only_static_columns,
            pinned_plan_opt pinned_plan);

    /// Decides which index, if any, this query reads, and what it has to filter.
    /// Only a SELECT can read an index or filter, so only a SELECT runs this.
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

    void build_filters(const column_predicates& preds);

    /// Builds the functions computing the partition ranges and clustering bounds
    /// this statement addresses.
    void build_key_range_fns();

    /// Builds the functions computing the ranges to read from an index table.
    void build_index_fns();

    void process_partition_key_restrictions();

    /**
     * Processes the clustering column restrictions.
     *
     * @throws InvalidRequestException if the request is invalid
     */
    void process_clustering_columns_restrictions();

    /// Throws unless every restricted clustering column, in order, forms a
    /// prefix of the clustering key.
    void validate_clustering_columns_form_a_prefix() const;

    /// Rejects clustering-column restrictions that cannot be turned into a
    /// clustering slice.  For a statement with no index and no way to filter,
    /// this is the whole of the clustering-restriction validation.
    void validate_clustering_restrictions_are_a_slice() const;

    [[noreturn]] static void throw_collection_restriction_needs_index_or_filtering();

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
    void calculate_column_defs_for_filtering_and_erase_restrictions_used_for_index(
            data_dictionary::database db,
            const column_predicates& preds);
    get_partition_key_ranges_fn_t build_partition_key_ranges_fn() const;
    get_clustering_bounds_fn_t build_get_clustering_bounds_fn() const;
    get_clustering_bounds_fn_t build_get_global_index_clustering_ranges_fn() const;
    get_clustering_bounds_fn_t build_get_global_index_token_clustering_ranges_fn() const;
    get_clustering_bounds_fn_t build_get_local_index_clustering_ranges_fn() const;
    get_singleton_value_fn_t build_value_for_index_partition_key_fn() const;
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

    void validate_secondary_index_selections() const;

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

private:
    /// Prepares internal data for evaluating index-table queries.  Must be called before
    /// get_local_index_clustering_ranges().
    void prepare_indexed_local(const schema& idx_tbl_schema, const column_predicates& preds);

    /// Prepares internal data for evaluating index-table queries.  Must be called before
    /// get_global_index_clustering_ranges() or get_global_index_token_clustering_ranges().
    void prepare_indexed_global(const schema& idx_tbl_schema);

public:
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

    /// Checks that the primary key restrictions don't contain null values, throws invalid_request_exception otherwise.
    void validate_primary_key(const query_options& options) const;

    bool is_empty() const;
};

// One entry point per statement type.  What a statement may do with a WHERE
// clause depends on the statement: only a SELECT can read an index or filter
// rows, a mutation has to name the rows it writes, and IS NOT NULL declares a
// materialized view's key columns rather than filtering.  Asking for the
// analysis by statement type keeps each caller from having to spell out the
// rules its statement plays by.

/// Analyzes the WHERE clause of a SELECT statement.
shared_ptr<const statement_restrictions> analyze_select_restrictions(
        data_dictionary::database db,
        schema_ptr schema,
        const expr::expression& where_clause,
        prepare_context& ctx,
        bool selects_only_static_columns,
        bool allow_filtering,
        check_indexes do_check_indexes,
        pinned_plan_opt pinned_plan = std::nullopt);

/// Analyzes the WHERE clause of the SELECT statement defining a materialized view.
shared_ptr<const statement_restrictions> analyze_view_restrictions(
        data_dictionary::database db,
        schema_ptr schema,
        const expr::expression& where_clause,
        prepare_context& ctx,
        bool selects_only_static_columns,
        check_indexes do_check_indexes);

/// Analyzes the WHERE clause of an UPDATE statement.
shared_ptr<const statement_restrictions> analyze_update_restrictions(
        data_dictionary::database db,
        schema_ptr schema,
        const expr::expression& where_clause,
        prepare_context& ctx,
        bool applies_only_to_static_columns);

/// Analyzes the WHERE clause of a DELETE statement.
shared_ptr<const statement_restrictions> analyze_delete_restrictions(
        data_dictionary::database db,
        schema_ptr schema,
        const expr::expression& where_clause,
        prepare_context& ctx,
        bool applies_only_to_static_columns);

/// Analyzes the primary-key equalities an INSERT statement names.
shared_ptr<const statement_restrictions> analyze_insert_restrictions(
        data_dictionary::database db,
        schema_ptr schema,
        const expr::expression& where_clause,
        prepare_context& ctx);

/// Restrictions that restrict nothing, for a statement that does not work out
/// the rows it addresses from a WHERE clause.
///
/// The pager asks for these to put a query on the filtering path - which
/// re-applies the per-partition limit on every page - with no filter of its own.
shared_ptr<const statement_restrictions> make_empty_select_restrictions(schema_ptr schema);
/// INSERT ... JSON takes its primary key from the JSON document at execution
/// time, and computes the keys to write itself.
shared_ptr<const statement_restrictions> make_empty_insert_restrictions(schema_ptr schema);


// Checks whether this expression is empty - doesn't restrict anything
bool is_empty_restriction(const expr::expression&);

}

}
