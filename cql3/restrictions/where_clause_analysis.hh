/*
 * Copyright (C) 2026-present ScyllaDB
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
#include "query/query-request.hh"

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


// The WHERE clause, prepared and turned into predicates.  Scoring-function
// restrictions are kept apart: they are purely declarative and never enter the
// restriction, index or filtering machinery.
struct where_clause_predicates {
    std::vector<predicate> predicates;
    std::vector<expr::binary_operator> scoring_functions;
};

/// Reads a WHERE clause: which columns it restricts and how, and the partition
/// and clustering ranges that adds up to.
///
/// This is an implementation detail of the restrictions classes in
/// statement_restrictions.hh, which hold one and answer questions about it.  It
/// is the analysis every statement type shares, and it knows nothing about
/// statement types: which of its steps apply, in what order, and what to reject
/// is for the caller to decide.
///
/// The steps are meant to be run in the order they are declared below.
class where_clause_analysis {
public:
    schema_ptr schema;

    /**
     * Restrictions on partitioning columns
     */
    expr::expression partition_key_restrictions = expr::conjunction({});

    expr::single_column_restrictions_map single_column_partition_key_restrictions;

    /**
     * Restrictions on clustering columns
     */
    expr::expression clustering_columns_restrictions = expr::conjunction({});

    expr::single_column_restrictions_map single_column_clustering_key_restrictions;

    /**
     * Restriction on non-primary key columns (i.e. secondary index restrictions)
     */
    expr::expression nonprimary_key_restrictions = expr::conjunction({});

    expr::single_column_restrictions_map single_column_nonprimary_key_restrictions;

    /// The columns a view definition declared to be non-null.  Empty unless the
    /// caller asked for the view-definition reading of IS NOT NULL.
    std::unordered_set<const column_definition*> not_null_columns;

    std::vector<expr::expression> where_factors; ///< The entire WHERE clause (factorized).

    /// Parts of where_factors defining the clustering slice.
    ///
    /// Meets all of the following conditions:
    /// 1. all elements must be simultaneously satisfied (as restrictions) for where_factors to be satisfied
    /// 2. each element is an atom or a conjunction of atoms
    /// 3. either all atoms (across all elements) are multi-column or they are all single-column
    /// 4. if single-column, then:
    ///   4.1 all atoms from an element have the same LHS, which we call the element's LHS
    ///   4.2 each element's LHS is different from any other element's LHS
    ///   4.3 the list of each element's LHS, in order, forms a clustering-key prefix
    ///   4.4 elements other than the last have only EQ or IN atoms
    ///   4.5 the last element has only EQ, IN, or is_slice() atoms
    /// 5. if multi-column, then each element is a binary_operator
    std::vector<predicate> clustering_prefix_restrictions;

    /// Parts of where_factors defining the partition range.
    ///
    /// If the partition range is dictated by token restrictions, this is a single element that holds all the
    /// binary_operators on token.  If single-column restrictions define the partition range, each element holds
    /// restrictions for one partition column.  Each partition column has a corresponding element, but the elements
    /// are in arbitrary order.
    partition_range_restrictions partition_range;

    bool partition_range_is_simple = true; ///< False iff partition_range implies a Cartesian product.
    bool pk_has_slice_or_needs_filtering = false; ///< True iff any PK restriction has a slice or needs-filtering operator.
    bool has_multi_column = false; ///< True iff clustering_columns_restrictions has a multi-column restriction.
    bool ck_is_on_collection = false; ///< True iff clustering_columns_restrictions has a collection restriction (CONTAINS/CONTAINS_KEY).
    bool ck_is_all_eq = true; ///< True iff all CK restrictions use EQ operator only.
    bool pk_is_all_eq = true; ///< True iff all PK restrictions use EQ operator only.

    /// Columns that appear on the LHS of an EQ restriction (not IN).
    /// For multi-column EQ like (ck1, ck2) = (1, 2), all columns in the tuple are included.
    std::unordered_set<const column_definition*> columns_with_eq;

    get_partition_key_ranges_fn_t get_partition_key_ranges_fn;
    get_clustering_bounds_fn_t get_clustering_bounds_fn;

public:
    explicit where_clause_analysis(schema_ptr schema);

    // The range-solving functions capture `this`, so the analysis must stay put.
    where_clause_analysis(const where_clause_analysis&) = delete;
    where_clause_analysis& operator=(const where_clause_analysis&) = delete;

    // --- The analysis steps --------------------------------------------

    /// Prepares the WHERE clause against the schema and turns it into predicates.
    where_clause_predicates prepare_where_clause(
            data_dictionary::database db,
            const expr::expression& where_clause,
            prepare_context& ctx);

    /// Sorts the predicates by the columns they restrict, and works out the
    /// clustering prefix and the partition range they address.
    ///
    /// \param allow_filtering whether the statement may read rows it does not
    /// need and drop them.  When it may not, a restriction that no key order can
    /// express - a slice of the partition key, say - is rejected.
    column_predicates classify_predicates(std::vector<predicate> predicates, bool allow_filtering);

    /// Builds the functions computing the partition ranges and clustering bounds
    /// the statement addresses.
    void build_key_range_fns();

    // --- Validation the caller can ask for -----------------------------

    /// Throws unless every restricted clustering column, in order, forms a
    /// prefix of the clustering key.
    void validate_clustering_columns_form_a_prefix() const;

    /// Rejects clustering-column restrictions that cannot be turned into a
    /// clustering slice.  For a statement with no index and no way to filter,
    /// this is the whole of the clustering-restriction validation.
    void validate_clustering_restrictions_are_a_slice() const;

    [[noreturn]] static void throw_collection_restriction_needs_index_or_filtering();

    // --- What the analysis found ---------------------------------------

    bool key_is_in_relation() const;
    bool clustering_key_restrictions_has_IN() const;
    bool clustering_key_restrictions_has_only_eq() const;
    bool has_token_restrictions() const;
    bool has_eq_restriction_on_column(const column_definition&) const;
    bool has_partition_key_unrestricted_components() const;
    bool partition_key_restrictions_is_empty() const;
    bool partition_key_restrictions_is_all_eq() const;
    size_t partition_key_restrictions_size() const;
    size_t clustering_columns_restrictions_size() const;
    bool has_unrestricted_clustering_columns() const;
    const column_definition& unrestricted_column(column_kind kind) const;
    bool has_clustering_columns_restriction() const;
    bool has_non_primary_key_restriction() const;
    bool pk_restrictions_need_filtering() const;
    bool clustering_key_restrictions_need_filtering() const;
    unsigned int num_clustering_prefix_columns_that_need_not_be_filtered() const;
    bool is_restricted(const column_definition* cdef) const;
    dht::partition_range_vector get_partition_key_ranges(const query_options& options) const;
    std::vector<query::clustering_range> get_clustering_bounds(const query_options& options) const;
    void validate_primary_key(const query_options& options) const;
    bool is_empty() const;

    /// The restrictions on columns of the given kind.
    const expr::expression& get_restrictions(column_kind kind) const;

private:
    get_partition_key_ranges_fn_t build_partition_key_ranges_fn() const;
    get_clustering_bounds_fn_t build_get_clustering_bounds_fn() const;
};

}

}
