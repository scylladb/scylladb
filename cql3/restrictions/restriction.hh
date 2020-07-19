/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2019 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include <concepts>
#include <numeric>
#include <optional>
#include <sstream>
#include <variant>
#include <vector>

#include <fmt/ostream.h>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include "utils/overloaded_functor.hh"
#include "cql3/query_options.hh"
#include "cql3/term.hh"
#include "cql3/statements/bound.hh"
#include "index/secondary_index_manager.hh"
#include "query-result-reader.hh"
#include "range.hh"
#include "types.hh"

class row;

namespace cql3 {

namespace selection {
class selection;
} // namespace selection

namespace restrictions {

struct allow_local_index_tag {};
using allow_local_index = bool_class<allow_local_index_tag>;

class binary_operator;
class conjunction;

/// A restriction expression -- union of all possible restriction types.  bool means a Boolean constant.
using expression = std::variant<bool, conjunction, binary_operator>;

/// A column, optionally subscripted by a term (eg, c1 or c2['abc']).
struct column_value {
    const column_definition* col;
    ::shared_ptr<term> sub; ///< If present, this LHS is col[sub], otherwise just col.
    /// For easy creation of vector<column_value> from vector<column_definition*>.
    column_value(const column_definition* col) : col(col) {}
    /// The compiler doesn't auto-generate this due to the other constructor's existence.
    column_value(const column_definition* col, ::shared_ptr<term> sub) : col(col), sub(sub) {}
};

/// Represents token function on LHS of an operator relation.  No need to list column definitions
/// here -- token takes exactly the partition key as its argument.
struct token {};

/// Operator restriction: LHS op RHS.
struct binary_operator {
    std::variant<column_value, std::vector<column_value>, token> lhs;
    const operator_type* op; // Pointer because operator_type isn't copyable or assignable.
    ::shared_ptr<term> rhs;
};

/// A conjunction of restrictions.
struct conjunction {
    std::vector<expression> children;
};

/// Creates a conjunction of a and b.  If either a or b is itself a conjunction, its children are inserted
/// directly into the resulting conjunction's children, flattening the expression tree.
extern expression make_conjunction(expression a, expression b);

/// True iff restr is satisfied with respect to the row provided from a partition slice.
extern bool is_satisfied_by(
        const expression& restr,
        const std::vector<bytes>& partition_key, const std::vector<bytes>& clustering_key,
        const query::result_row_view& static_row, const query::result_row_view* row,
        const selection::selection&, const query_options&);

/// True iff restr is satisfied with respect to the row provided from a mutation.
extern bool is_satisfied_by(
        const expression& restr,
        const schema& schema, const partition_key& key, const clustering_key_prefix& ckey, const row& cells,
        const query_options& options, gc_clock::time_point now);

/// Finds the first binary_operator in restr that represents a bound and returns its RHS as a tuple.  If no
/// such binary_operator exists, returns an empty vector.  The search is depth first.
extern std::vector<bytes_opt> first_multicolumn_bound(const expression&, const query_options&, statements::bound);

/// A set of discrete values.
using value_list = std::vector<bytes>; // Sorted and deduped using value comparator.

/// General set of values.  Empty set and single-element sets are always value_list.  nonwrapping_range is
/// never singular and never has start > end.  Universal set is a nonwrapping_range with both bounds null.
using value_set = std::variant<value_list, nonwrapping_range<bytes>>;

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
extern nonwrapping_range<bytes> to_range(const value_set&);

/// True iff expr references the function.
extern bool uses_function(const expression& expr, const sstring& ks_name, const sstring& function_name);

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
        }, e);
}

inline const binary_operator* find(const expression& e, const operator_type& op) {
    return find_atom(e, [&] (const binary_operator& o) { return *o.op == op; });
}

inline bool needs_filtering(const expression& e) {
    return find_atom(e, [] (const binary_operator& o) { return o.op->needs_filtering(); });
}

inline bool has_slice(const expression& e) {
    return find_atom(e, [] (const binary_operator& o) { return o.op->is_slice(); });
}

inline bool has_token(const expression& e) {
    return find_atom(e, [] (const binary_operator& o) { return std::holds_alternative<token>(o.lhs); });
}

inline bool has_slice_or_needs_filtering(const expression& e) {
    return find_atom(e, [] (const binary_operator& o) { return o.op->is_slice() || o.op->needs_filtering(); });
}

/// True iff binary_operator involves a collection.
extern bool is_on_collection(const binary_operator&);

/// Replaces every column_definition in an expression with this one.  Throws if any LHS is not a single
/// column_value.
extern expression replace_column_def(const expression&, const column_definition*);

/// Makes a binary_operator on a column_definition.
inline expression make_column_op(const column_definition* cdef, const operator_type& op, ::shared_ptr<term> value) {
    return binary_operator{column_value(cdef), &op, std::move(value)};
}

inline const operator_type* pick_operator(statements::bound b, bool inclusive) {
    return is_start(b) ?
            (inclusive ? &operator_type::GTE : &operator_type::GT) :
            (inclusive ? &operator_type::LTE : &operator_type::LT);
}

/**
 * Result of relation::to_restriction().  TODO: remove this class and rewrite to_restriction to return
 * expression.
 */
class restriction {
public:
    // Init to false for now, to easily detect errors.  This whole class is going away.
    cql3::restrictions::expression expression = false;
    virtual ~restriction() {}
};

}

}

/// Required for fmt::join() to work on expression.
template <>
struct fmt::formatter<cql3::restrictions::expression> {
    constexpr auto parse(format_parse_context& ctx) {
        return ctx.end();
    }

    template <typename FormatContext>
    auto format(const cql3::restrictions::expression& expr, FormatContext& ctx) {
        std::ostringstream os;
        os << expr;
        return format_to(ctx.out(), "{}", os.str());
    }
};

/// Required for fmt::join() to work on column_value.
template <>
struct fmt::formatter<cql3::restrictions::column_value> {
    constexpr auto parse(format_parse_context& ctx) {
        return ctx.end();
    }

    template <typename FormatContext>
    auto format(const cql3::restrictions::column_value& col, FormatContext& ctx) {
        std::ostringstream os;
        os << col;
        return format_to(ctx.out(), "{}", os.str());
    }
};
