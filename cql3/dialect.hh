// Copyright (C) 2024-present ScyllaDB
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#pragma once

#include <limits>

#include <fmt/core.h>

namespace cql3 {

struct dialect {
    bool duplicate_bind_variable_names_refer_to_same_variable = true;  // if :a is found twice in a query, the two references are to the same variable (see #15559)
    unsigned max_relations_in_where_clause = 100; // maximum number of relations in a WHERE clause
    // the bind variable of an IN restriction is named "IN(column)" (if false, the
    // operator is spelled in lowercase, as Cassandra spells it)
    bool in_bind_variable_name_uses_uppercase_operator = true;
    // "(x)" around a single term is a one-element tuple, as CQL has always read
    // it.  Read as a parenthesized term instead, it is simply x - which is what
    // a grammar with operators needs it to be, so that "(a + b) * c" multiplies
    // rather than building a tuple.  tuple(x) spells the one-element tuple
    // either way.
    bool parentheses_around_a_single_term_make_a_tuple = true;
    bool operator==(const dialect&) const = default;
};

// Dialect for CQL the database writes for itself in this source tree.
// Guardrails meant for client queries must not apply here: such a statement
// is ours, and rejecting it would make the database unusable.
inline
dialect
internal_dialect() {
    return dialect{
        .duplicate_bind_variable_names_refer_to_same_variable = true,
        .max_relations_in_where_clause = std::numeric_limits<unsigned>::max(),
        .in_bind_variable_name_uses_uppercase_operator = true,
    };
}

// Dialect for CQL the database stored and reads back: a materialized view's
// WHERE clause, a user-defined aggregate's INITCOND.  Such text was written by
// our own printers, and how they spelled a one-element tuple decides how "(x)"
// has to be read.  They spelled it "(x)" until the TUPLE_CONSTRUCTOR feature
// was enabled, and tuple(x) since; the feature is enabled in the same group0
// command that rewrites every stored text the old way, so a node that sees the
// feature enabled never finds a "(x)" tuple, and one that does not never finds
// a tuple(x) it cannot read.  As for internal_dialect(), the statement was
// already accepted when the user submitted it, so the client limits do not
// apply again.
inline
dialect
stored_statement_dialect(bool one_element_tuples_spelled_with_constructor) {
    auto d = internal_dialect();
    d.parentheses_around_a_single_term_make_a_tuple = !one_element_tuples_spelled_with_constructor;
    return d;
}

}

template <>
struct fmt::formatter<cql3::dialect> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const cql3::dialect& d, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "cql3::dialect{{duplicate_bind_variable_names_refer_to_same_variable={}, max_relations_in_where_clause={}, in_bind_variable_name_uses_uppercase_operator={}, parentheses_around_a_single_term_make_a_tuple={}}}",
                d.duplicate_bind_variable_names_refer_to_same_variable, d.max_relations_in_where_clause, d.in_bind_variable_name_uses_uppercase_operator, d.parentheses_around_a_single_term_make_a_tuple);
    }
};
