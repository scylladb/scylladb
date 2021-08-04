/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <fmt/core.h>
#include <ostream>
#include <seastar/core/shared_ptr.hh>
#include <variant>
#include <concepts>

#include "bytes.hh"
#include "cql3/statements/bound.hh"
#include "cql3/term.hh"
#include "cql3/cql3_type.hh"
#include "cql3/functions/function_name.hh"
#include "database_fwd.hh"
#include "gc_clock.hh"
#include "range.hh"
#include "seastarx.hh"
#include "utils/overloaded_functor.hh"
#include "utils/variant_element.hh"

class row;

namespace secondary_index {
class index;
class secondary_index_manager;
} // namespace secondary_index

namespace query {
    class result_row_view;
} // namespace query

namespace cql3 {

class column_identifier_raw;
class query_options;
class term_raw;

namespace selection {
    class selection;
} // namespace selection

namespace functions {

class function;

}

namespace expr {

struct allow_local_index_tag {};
using allow_local_index = bool_class<allow_local_index_tag>;

class binary_operator;
class conjunction;
struct column_value;
struct column_value_tuple;
struct token;
class unresolved_identifier;
class column_mutation_attribute;
class function_call;
class cast;
class field_selection;
using term_raw_ptr = ::shared_ptr<term_raw>;
struct null;
struct bind_variable;

/// A CQL expression -- union of all possible expression types.  bool means a Boolean constant.
using expression = std::variant<bool, conjunction, binary_operator, column_value, column_value_tuple, token,
                                unresolved_identifier, column_mutation_attribute, function_call, cast,
                                field_selection, term_raw_ptr, null, bind_variable>;

template <typename T>
concept ExpressionElement = utils::VariantElement<T, expression>;

// An expression variant element can't contain an expression by value, since the size of the variant
// will be infinite. `nested_expression` contains an expression indirectly, but has value semantics and
// is copyable.
class nested_expression {
    std::unique_ptr<expression> _e;
public:
    // not explicit, an expression _is_ a nested expression
    nested_expression(expression e);
    // not explicit, an ExpressionElement _is_ an expression
    nested_expression(ExpressionElement auto e);
    nested_expression(const nested_expression&);
    nested_expression(nested_expression&&) = default;
    nested_expression& operator=(const nested_expression&);
    nested_expression& operator=(nested_expression&&) = default;
    const expression* operator->() const { return _e.get(); }
    expression* operator->() { return _e.get(); }
    const expression& operator*() const { return *_e.get(); }
    expression& operator*() { return *_e.get(); }
};

/// A column, optionally subscripted by a term (eg, c1 or c2['abc']).
struct column_value {
    const column_definition* col;
    ::shared_ptr<term> sub; ///< If present, this LHS is col[sub], otherwise just col.
    /// For easy creation of vector<column_value> from vector<column_definition*>.
    column_value(const column_definition* col) : col(col) {}
    /// The compiler doesn't auto-generate this due to the other constructor's existence.
    column_value(const column_definition* col, ::shared_ptr<term> sub) : col(col), sub(sub) {}
};

/// A tuple of column selectors. Usually used for range constraints on clustering keys.
struct column_value_tuple {
    std::vector<column_value> elements;
    explicit column_value_tuple(std::vector<column_value>);
    template <typename Range>
            requires requires (Range r) {
                { r.begin() } -> std::input_iterator;
                { r.end() } -> std::input_iterator;
                { *r.begin() } -> std::convertible_to<column_value>;
            }
    explicit column_value_tuple(Range r);
};

/// Represents token function on LHS of an operator relation.  No need to list column definitions
/// here -- token takes exactly the partition key as its argument.
struct token {};

enum class oper_t { EQ, NEQ, LT, LTE, GTE, GT, IN, CONTAINS, CONTAINS_KEY, IS_NOT, LIKE };

/// Describes the nature of clustering-key comparisons.  Useful for implementing SCYLLA_CLUSTERING_BOUND.
enum class comparison_order : char {
    cql, ///< CQL order. (a,b)>(1,1) is equivalent to a>1 OR (a=1 AND b>1).
    clustering, ///< Table's clustering order. (a,b)>(1,1) means any row past (1,1) in storage.
};

/// Operator restriction: LHS op RHS.
struct binary_operator {
    nested_expression lhs;
    oper_t op;
    ::shared_ptr<term> rhs;
    comparison_order order;

    binary_operator(expression lhs, oper_t op, ::shared_ptr<term> rhs, comparison_order order = comparison_order::cql);
};

/// A conjunction of restrictions.
struct conjunction {
    std::vector<expression> children;
};

// Gets resolved eventually into a column_value.
struct unresolved_identifier {
    ::shared_ptr<column_identifier_raw> ident;

    ~unresolved_identifier();
};

// An attribute attached to a column mutation: writetime or ttl
struct column_mutation_attribute {
    enum class attribute_kind { writetime, ttl };

    attribute_kind kind;
    // note: only unresolved_identifier is legal here now. One day, when prepare()
    // on expressions yields expressions, column_value will also be legal here.
    nested_expression column;
};

struct function_call {
    std::variant<functions::function_name, shared_ptr<functions::function>> func;
    std::vector<expression> args;
};

struct cast {
    nested_expression arg;
    std::variant<cql3_type, shared_ptr<cql3_type::raw>> type;
};

struct field_selection {
    nested_expression structure;
    shared_ptr<column_identifier_raw> field;
};

struct null {
};

struct bind_variable {
    enum class shape_type { scalar, scalar_in, tuple, tuple_in };
    // FIXME: infer shape from expression rather than from grammar
    shape_type shape;
    int32_t bind_index;
};

/// Creates a conjunction of a and b.  If either a or b is itself a conjunction, its children are inserted
/// directly into the resulting conjunction's children, flattening the expression tree.
extern expression make_conjunction(expression a, expression b);

extern std::ostream& operator<<(std::ostream&, oper_t);

/// True iff restr is satisfied with respect to the row provided from a partition slice.
extern bool is_satisfied_by(
        const expression& restr,
        const std::vector<bytes>& partition_key, const std::vector<bytes>& clustering_key,
        const query::result_row_view& static_row, const query::result_row_view* row,
        const selection::selection&, const query_options&);

/// Finds the first binary_operator in restr that represents a bound and returns its RHS as a tuple.  If no
/// such binary_operator exists, returns an empty vector.  The search is depth first.
extern std::vector<managed_bytes_opt> first_multicolumn_bound(const expression&, const query_options&, statements::bound);

/// A set of discrete values.
using value_list = std::vector<managed_bytes>; // Sorted and deduped using value comparator.

/// General set of values.  Empty set and single-element sets are always value_list.  nonwrapping_range is
/// never singular and never has start > end.  Universal set is a nonwrapping_range with both bounds null.
using value_set = std::variant<value_list, nonwrapping_range<managed_bytes>>;

/// A set of all column values that would satisfy an expression.  If column is null, a set of all token values
/// that satisfy.
///
/// An expression restricts possible values of a column or token:
/// - `A>5` restricts A from below
/// - `A>5 AND A>6 AND B<10 AND A=12 AND B>0` restricts A to 12 and B to between 0 and 10
/// - `A IN (1, 3, 5)` restricts A to 1, 3, or 5
/// - `A IN (1, 3, 5) AND A>3` restricts A to just 5
/// - `A=1 AND A<=0` restricts A to an empty list; no value is able to satisfy the expression
/// - `A>=NULL` also restricts A to an empty list; all comparisons to NULL are false
/// - an expression without A "restricts" A to unbounded range
extern value_set possible_lhs_values(const column_definition*, const expression&, const query_options&);

/// Turns value_set into a range, unless it's a multi-valued list (in which case this throws).
extern nonwrapping_range<managed_bytes> to_range(const value_set&);

/// A range of all X such that X op val.
nonwrapping_range<clustering_key_prefix> to_range(oper_t op, const clustering_key_prefix& val);

/// True iff the index can support the entire expression.
extern bool is_supported_by(const expression&, const secondary_index::index&);

/// True iff any of the indices from the manager can support the entire expression.  If allow_local, use all
/// indices; otherwise, use only global indices.
extern bool has_supporting_index(
        const expression&, const secondary_index::secondary_index_manager&, allow_local_index allow_local);

extern sstring to_string(const expression&);

extern std::ostream& operator<<(std::ostream&, const column_value&);

extern std::ostream& operator<<(std::ostream&, const expression&);

/// If there is a binary_operator atom b for which f(b) is true, returns it.  Otherwise returns null.
template<typename Fn>
requires std::regular_invocable<Fn, const binary_operator&>
const binary_operator* find_atom(const expression& e, Fn f) {
    return std::visit(overloaded_functor{
            [&] (const binary_operator& op) { return f(op) ? &op : nullptr; },
            [] (bool) -> const binary_operator* { return nullptr; },
            [&] (const conjunction& conj) -> const binary_operator* {
                for (auto& child : conj.children) {
                    if (auto found = find_atom(child, f)) {
                        return found;
                    }
                }
                return nullptr;
            },
            [] (const column_value&) -> const binary_operator* { return nullptr; },
            [] (const column_value_tuple&) -> const binary_operator* { return nullptr; },
            [] (const token&) -> const binary_operator* { return nullptr; },
            [] (const unresolved_identifier&) -> const binary_operator* { return nullptr; },
            [] (const column_mutation_attribute&) -> const binary_operator* { return nullptr; },
            [&] (const function_call& fc) -> const binary_operator* {
                for (auto& arg : fc.args) {
                    if (auto found = find_atom(arg, f)) {
                        return found;
                    }
                }
                return nullptr;
            },
            [&] (const cast& c) -> const binary_operator* {
                return find_atom(*c.arg, f);
            },
            [&] (const field_selection& fs) -> const binary_operator* {
                return find_atom(*fs.structure, f);
            },
            [&] (const term_raw_ptr&) -> const binary_operator* {
                return nullptr;
            },
            [&] (const null&) -> const binary_operator* {
                return nullptr;
            },
            [&] (const bind_variable&) -> const binary_operator* {
                return nullptr;
            },
        }, e);
}

/// Counts binary_operator atoms b for which f(b) is true.
template<typename Fn>
requires std::regular_invocable<Fn, const binary_operator&>
size_t count_if(const expression& e, Fn f) {
    return std::visit(overloaded_functor{
            [&] (const binary_operator& op) -> size_t { return f(op) ? 1 : 0; },
            [&] (const conjunction& conj) {
                return std::accumulate(conj.children.cbegin(), conj.children.cend(), size_t{0},
                                       [&] (size_t acc, const expression& c) { return acc + count_if(c, f); });
            },
            [] (bool) -> size_t { return 0; },
            [] (const column_value&) -> size_t { return 0; },
            [] (const column_value_tuple&) -> size_t { return 0; },
            [] (const token&) -> size_t { return 0; },
            [] (const unresolved_identifier&) -> size_t { return 0; },
            [] (const column_mutation_attribute&) -> size_t { return 0; },
            [&] (const function_call& fc) -> size_t {
                return std::accumulate(fc.args.cbegin(), fc.args.cend(), size_t{0},
                                       [&] (size_t acc, const expression& c) { return acc + count_if(c, f); });
            },
            [&] (const cast& c) -> size_t {
                return count_if(*c.arg, f); },
            [&] (const field_selection& fs) -> size_t {
                return count_if(*fs.structure, f);
            },
            [&] (const term_raw_ptr&) -> size_t {
                return 0;
            },
            [&] (const null&) -> size_t {
                return 0;
            },
            [&] (const bind_variable&) -> size_t {
                return 0;
            },
        }, e);
}

inline const binary_operator* find(const expression& e, oper_t op) {
    return find_atom(e, [&] (const binary_operator& o) { return o.op == op; });
}

inline bool needs_filtering(oper_t op) {
    return (op == oper_t::CONTAINS) || (op == oper_t::CONTAINS_KEY) || (op == oper_t::LIKE) ||
           (op == oper_t::IS_NOT) || (op == oper_t::NEQ) ;
}

inline auto find_needs_filtering(const expression& e) {
    return find_atom(e, [] (const binary_operator& bo) { return needs_filtering(bo.op); });
}

inline bool is_slice(oper_t op) {
    return (op == oper_t::LT) || (op == oper_t::LTE) || (op == oper_t::GT) || (op == oper_t::GTE);
}

inline bool has_slice(const expression& e) {
    return find_atom(e, [] (const binary_operator& bo) { return is_slice(bo.op); });
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

inline bool is_multi_column(const binary_operator& op) {
    return holds_alternative<column_value_tuple>(*op.lhs);
}

inline bool has_token(const expression& e) {
    return find_atom(e, [] (const binary_operator& o) { return std::holds_alternative<token>(*o.lhs); });
}

inline bool has_slice_or_needs_filtering(const expression& e) {
    return find_atom(e, [] (const binary_operator& o) { return is_slice(o.op) || needs_filtering(o.op); });
}

inline bool is_clustering_order(const binary_operator& op) {
    return op.order == comparison_order::clustering;
}

inline auto find_clustering_order(const expression& e) {
    return find_atom(e, is_clustering_order);
}

/// True iff binary_operator involves a collection.
extern bool is_on_collection(const binary_operator&);

/// Replaces every column_definition in an expression with this one.  Throws if any LHS is not a single
/// column_value.
extern expression replace_column_def(const expression&, const column_definition*);

// Replaces all occurences of token(p1, p2) on the left hand side with the given colum.
// For example this changes token(p1, p2) < token(1, 2) to my_column_name < token(1, 2).
extern expression replace_token(const expression&, const column_definition*);

extern ::shared_ptr<term_raw> as_term_raw(const expression& e);

extern expression as_expression(::shared_ptr<term::raw> t);

inline oper_t pick_operator(statements::bound b, bool inclusive) {
    return is_start(b) ?
            (inclusive ? oper_t::GTE : oper_t::GT) :
            (inclusive ? oper_t::LTE : oper_t::LT);
}

inline
column_value_tuple::column_value_tuple(std::vector<column_value> e)
        : elements(std::move(e)) {
}

template <typename Range>
        requires requires (Range r) {
            { r.begin() } -> std::input_iterator;
            { r.end() } -> std::input_iterator;
            { *r.begin() } -> std::convertible_to<column_value>;
        }
column_value_tuple::column_value_tuple(Range r)
        : column_value_tuple(std::vector<column_value>(r.begin(), r.end())) {
}

nested_expression::nested_expression(ExpressionElement auto e)
        : nested_expression(expression(std::move(e))) {
}

// Extracts all binary operators which have the given column on their left hand side.
// Extracts only single-column restrictions.
// Does not include multi-column restrictions.
// Does not include token() restrictions.
// Does not include boolean constant restrictions.
// For example "WHERE c = 1 AND (a, c) = (2, 1) AND token(p) < 2 AND FALSE" will return {"c = 1"}.
std::vector<expression> extract_single_column_restrictions_for_column(const expression&, const column_definition&);

} // namespace expr

} // namespace cql3

/// Required for fmt::join() to work on expression.
template <>
struct fmt::formatter<cql3::expr::expression> {
    constexpr auto parse(format_parse_context& ctx) {
        return ctx.end();
    }

    template <typename FormatContext>
    auto format(const cql3::expr::expression& expr, FormatContext& ctx) {
        std::ostringstream os;
        os << expr;
        return format_to(ctx.out(), "{}", os.str());
    }
};

/// Required for fmt::join() to work on column_value.
template <>
struct fmt::formatter<cql3::expr::column_value> {
    constexpr auto parse(format_parse_context& ctx) {
        return ctx.end();
    }

    template <typename FormatContext>
    auto format(const cql3::expr::column_value& col, FormatContext& ctx) {
        std::ostringstream os;
        os << col;
        return format_to(ctx.out(), "{}", os.str());
    }
};
