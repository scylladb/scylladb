// Copyright (C) 2023-present ScyllaDB
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#pragma once

#include "expression.hh"

#include "bytes.hh"
#include "keys.hh"
#include "interval.hh"
#include "cql3/expr/restrictions.hh"
#include "cql3/assignment_testable.hh"
#include "cql3/statements/bound.hh"

namespace cql3 {

struct prepare_context;

class column_identifier_raw;
class query_options;

namespace selection {
    class selection;
} // namespace selection

} // namespace cql3

namespace cql3::expr {

class evaluation_inputs;

/// Helper for generating evaluation_inputs::static_and_regular_columns
std::vector<managed_bytes_opt> get_non_pk_values(const cql3::selection::selection& selection, const query::result_row_view& static_row,
                                         const query::result_row_view* row);

/// Helper for accessing a column value from evaluation_inputs
managed_bytes_opt extract_column_value(const column_definition* cdef, const evaluation_inputs& inputs);

/// True iff restr evaluates to true, given these inputs
extern bool is_satisfied_by(
        const expression& restr, const evaluation_inputs& inputs);


extern bool recurse_until(const expression& e, const noncopyable_function<bool (const expression&)>& predicate_fun);

// Looks into the expression and finds the given expression variant
// for which the predicate function returns true.
// If nothing is found returns nullptr.
// For example:
// find_in_expression<binary_operator>(e, [](const binary_operator&) {return true;})
// Will return the first binary operator found in the expression
template<ExpressionElement ExprElem, class Fn>
requires std::invocable<Fn, const ExprElem&>
      && std::same_as<std::invoke_result_t<Fn, const ExprElem&>, bool>
const ExprElem* find_in_expression(const expression& e, Fn predicate_fun) {
    const ExprElem* ret = nullptr;
    recurse_until(e, [&] (const expression& e) {
        if (auto expr_elem = as_if<ExprElem>(&e)) {
            if (predicate_fun(*expr_elem)) {
                ret = expr_elem;
                return true;
            }
        }
        return false;
    });
    return ret;
}

/// If there is a binary_operator atom b for which f(b) is true, returns it.  Otherwise returns null.
template<class Fn>
requires std::invocable<Fn, const binary_operator&>
      && std::same_as<std::invoke_result_t<Fn, const binary_operator&>, bool>
const binary_operator* find_binop(const expression& e, Fn predicate_fun) {
    return find_in_expression<binary_operator>(e, predicate_fun);
}

// Goes over each expression of the specified type and calls for_each_func for each of them.
// For example:
// for_each_expression<column_vaue>(e, [](const column_value& cval) {std::cout << cval << '\n';});
// Will print all column values in an expression
template<ExpressionElement ExprElem, class Fn>
requires std::invocable<Fn, const ExprElem&>
void for_each_expression(const expression& e, Fn for_each_func) {
    recurse_until(e, [&] (const expression& cur_expr) -> bool {
        if (auto expr_elem = as_if<ExprElem>(&cur_expr)) {
            for_each_func(*expr_elem);
        }
        return false;
    });
}

/// Counts binary_operator atoms b for which f(b) is true.
size_t count_if(const expression& e, const noncopyable_function<bool (const binary_operator&)>& f);

inline const binary_operator* find(const expression& e, oper_t op) {
    return find_binop(e, [&] (const binary_operator& o) { return o.op == op; });
}

inline bool is_slice(oper_t op) {
    return (op == oper_t::LT) || (op == oper_t::LTE) || (op == oper_t::GT) || (op == oper_t::GTE);
}

inline bool has_slice(const expression& e) {
    return find_binop(e, [] (const binary_operator& bo) { return is_slice(bo.op); });
}

inline bool is_compare(oper_t op) {
    switch (op) {
    case oper_t::EQ:
    case oper_t::LT:
    case oper_t::LTE:
    case oper_t::GT:
    case oper_t::GTE:
    case oper_t::NEQ:
        return true;
    default:
        return false;
    }
}

// Check whether the given expression represents
// a call to the token() function.
bool is_token_function(const function_call&);
bool is_token_function(const expression&);

bool is_partition_token_for_schema(const function_call&, const schema&);
bool is_partition_token_for_schema(const expression&, const schema&);

/// Check whether the expression contains a binary_operator whose LHS is a call to the token
/// function representing a partition key token.
/// Examples:
/// For expression: "token(p1, p2, p3) < 123 AND c = 2" returns true
/// For expression: "p1 = token(1, 2, 3) AND c = 2" return false
inline bool has_partition_token(const expression& e, const schema& table_schema) {
    return find_binop(e, [&] (const binary_operator& o) { return is_partition_token_for_schema(o.lhs, table_schema); });
}

inline bool is_clustering_order(const binary_operator& op) {
    return op.order == comparison_order::clustering;
}

inline auto find_clustering_order(const expression& e) {
    return find_binop(e, is_clustering_order);
}

/// Given a Boolean expression, compute its factors such as e=f1 AND f2 AND f3 ...
/// If the expression is TRUE, may return no factors (happens today for an
/// empty conjunction).
std::vector<expression> boolean_factors(expression e);

/// Run the given function for each element in the top level conjunction.
void for_each_boolean_factor(const expression& e, const noncopyable_function<void (const expression&)>& for_each_func);

// Checks whether the given column occurs in the expression.
// Uses column_defintion::operator== for comparison, columns with the same name but different schema will not be equal.
bool contains_column(const column_definition& column, const expression& e);

// Checks whether this expression contains a nonpure function.
// The expression must be prepared, so that function names are converted to function pointers.
bool contains_nonpure_function(const expression&);

/// Replaces every column_definition in an expression with this one.  Throws if any LHS is not a single
/// column_value.
extern expression replace_column_def(const expression&, const column_definition*);

// Replaces all occurrences of token(p1, p2) on the left hand side with the given column.
// For example this changes token(p1, p2) < token(1, 2) to my_column_name < token(1, 2).
// Schema is needed to find out which calls to token() describe the partition token.
extern expression replace_partition_token(const expression&, const column_definition*, const schema&);

// Recursively copies e and returns it. Calls replace_candidate() on all nodes. If it returns nullopt,
// continue with the copying. If it returns an expression, that expression replaces the current node.
extern expression search_and_replace(const expression& e,
        const noncopyable_function<std::optional<expression> (const expression& candidate)>& replace_candidate);

// Adjust an expression for rows that were fetched using query::partition_slice::options::collections_as_maps
expression adjust_for_collection_as_maps(const expression& e);

extern expression prepare_expression(const expression& expr, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver);
std::optional<expression> try_prepare_expression(const expression& expr, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver);

// Check that a prepared expression has no aggregate functions. Throws on error.
void verify_no_aggregate_functions(const expression& expr, std::string_view context_for_errors);

// Prepares a binary operator received from the parser.
// Does some basic type checks but no advanced validation.
extern binary_operator prepare_binary_operator(binary_operator binop, data_dictionary::database db, const schema& table_schema);

// Pre-compile any constant LIKE patterns and return equivalent expression
expression optimize_like(const expression& e);


/**
 * @return whether this object can be assigned to the provided receiver. We distinguish
 * between 3 values: 
 *   - EXACT_MATCH if this object is exactly of the type expected by the receiver
 *   - WEAKLY_ASSIGNABLE if this object is not exactly the expected type but is assignable nonetheless
 *   - NOT_ASSIGNABLE if it's not assignable
 * Most caller should just call the is_assignable() method on the result, though functions have a use for
 * testing "strong" equality to decide the most precise overload to pick when multiple could match.
 */
extern assignment_testable::test_result test_assignment(const expression& expr, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver);

// Test all elements of exprs for assignment. If all are exact match, return exact match. If any is not assignable,
// return not assignable. Otherwise, return weakly assignable.
extern assignment_testable::test_result test_assignment_all(const std::vector<expression>& exprs, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver);

extern shared_ptr<assignment_testable> as_assignment_testable(expression e, std::optional<data_type> type_opt);

inline oper_t pick_operator(statements::bound b, bool inclusive) {
    return is_start(b) ?
            (inclusive ? oper_t::GTE : oper_t::GT) :
            (inclusive ? oper_t::LTE : oper_t::LT);
}

std::optional<bool> get_bool_value(const constant&);

utils::chunked_vector<managed_bytes_opt> get_list_elements(const cql3::raw_value&);
utils::chunked_vector<managed_bytes_opt> get_set_elements(const cql3::raw_value&);
std::vector<managed_bytes_opt> get_tuple_elements(const cql3::raw_value&, const abstract_type& type);
std::vector<managed_bytes> get_vector_elements(const cql3::raw_value&, const abstract_type& type);
std::vector<managed_bytes_opt> get_user_type_elements(const cql3::raw_value&, const abstract_type& type);
std::vector<std::pair<managed_bytes, managed_bytes>> get_map_elements(const cql3::raw_value&);

// Gets the elements of a constant which can be a list, set, tuple, vector or user type
std::vector<managed_bytes_opt> get_elements(const cql3::raw_value&, const abstract_type& type);

// Get elements of list<tuple<>> as vector<vector<managed_bytes_opt>
// It is useful with IN restrictions like (a, b) IN [(1, 2), (3, 4)].
// `type` parameter refers to the list<tuple<>> type.
utils::chunked_vector<std::vector<managed_bytes_opt>> get_list_of_tuples_elements(const cql3::raw_value&, const abstract_type& type);

// Retrieves information needed in prepare_context.
// Collects the column specification for the bind variables in this expression.
// Sets lwt_cache_id field in function_calls.
void fill_prepare_context(expression&, cql3::prepare_context&);

// Checks whether there is a bind_variable inside this expression
// It's important to note, that even when there are no bind markers,
// there can be other things that prevent immediate evaluation of an expression.
// For example an expression can contain calls to nonpure functions.
bool contains_bind_marker(const expression& e);

// Extracts column_defs from the expression and sorts them using schema_pos_column_definition_comparator.
std::vector<const column_definition*> get_sorted_column_defs(const expression&);

// Extracts column_defs and returns the last one according to schema_pos_column_definition_comparator.
const column_definition* get_last_column_def(const expression&);

// Finds common columns between both expressions and prints them to a string.
// Uses schema_pos_column_definition_comparator for comparison.
sstring get_columns_in_commons(const expression& a, const expression& b);

/// Finds the data type of writetime(x) or ttl(x)
data_type column_mutation_attribute_type(const column_mutation_attribute& e);


// How deep aggregations are nested. e.g. sum(avg(count(col))) == 3
unsigned aggregation_depth(const cql3::expr::expression& e);

// Make sure every column_value or column_mutation_attribute is nested in exactly `depth` aggregations, by adding
// first() calls at the deepest level. e.g. if depth=3, then
//
//    my_agg(sum(x), y)
//
// becomes
//
//    my_agg(sum(first(x)), first(first(y)))
//
cql3::expr::expression levellize_aggregation_depth(const cql3::expr::expression& e, unsigned depth);


struct aggregation_split_result {
    std::vector<expression> inner_loop;
    std::vector<expression> outer_loop;
    std::vector<cql3::raw_value> initial_values_for_temporaries; // same size as inner_loop
};

// Given a vector of aggergation expressions, split them into an inner loop that
// calls the aggregating function on each input row, and an outer loop that calls
// the final function on temporaries and generate the result.
//
// inner_loop should be evaluated with for each input row in a group, and its
// results stored in temporaries seeded from initial_values_for_temporaries
//
// outer_loop should be evaluated once for each group, just with temporaries
// as input.
//
// If the expressions don't contain aggregates, inner_loop and initial_values_for_temporaries
// are empty, and outer_loop should be evaluated for each loop.
aggregation_split_result split_aggregation(std::span<const expression> aggregation);

}