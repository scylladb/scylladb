/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <vector>

#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptor/transformed.hpp>

#include <seastar/core/sstring.hh>

#include "cql3/column_identifier.hh"
#include "cql3/CqlParser.hpp"
#include "cql3/error_collector.hh"
#include "cql3/statements/raw/select_statement.hh"

namespace cql3 {

namespace util {


void do_with_parser_impl(const sstring_view& cql, noncopyable_function<void (cql3_parser::CqlParser& p)> func);

template <typename Func, typename Result = std::result_of_t<Func(cql3_parser::CqlParser&)>>
Result do_with_parser(const sstring_view& cql, Func&& f) {
    std::optional<Result> ret;
    do_with_parser_impl(cql, [&] (cql3_parser::CqlParser& parser) {
        ret.emplace(f(parser));
    });
    return std::move(*ret);
}

inline
sstring relations_to_where_clause(const expr::expression& e) {
    auto expr_to_pretty_string = [](const expr::expression& e) -> sstring {
        expr::expression::printer p {
            .expr_to_print = e,
            .debug_mode = false,
        };
        return fmt::format("{}", p);
    };
    auto relations = expr::boolean_factors(e);
    auto expressions = relations | boost::adaptors::transformed(expr_to_pretty_string);
    return boost::algorithm::join(expressions, " AND ");
}

static expr::expression where_clause_to_relations(const sstring_view& where_clause) {
    return expr::conjunction{do_with_parser(where_clause, std::mem_fn(&cql3_parser::CqlParser::whereClause))};
}

inline sstring rename_column_in_where_clause(const sstring_view& where_clause, column_identifier::raw from, column_identifier::raw to) {
    std::vector<expr::expression> relations = boolean_factors(where_clause_to_relations(where_clause));
    std::vector<expr::expression> new_relations;
    new_relations.reserve(relations.size());

    for (const expr::expression& old_relation : relations) {
        expr::expression new_relation = expr::search_and_replace(old_relation,
            [&](const expr::expression& e) -> std::optional<expr::expression> {
                if (auto ident = expr::as_if<expr::unresolved_identifier>(&e)) {
                    if (*ident->ident == from) {
                        return expr::unresolved_identifier{
                            ::make_shared<column_identifier::raw>(to)
                        };
                    }
                }
                return std::nullopt;
            }
        );

        new_relations.emplace_back(std::move(new_relation));
    }

    return relations_to_where_clause(expr::conjunction{std::move(new_relations)});
}

/// build a CQL "select" statement with the desired parameters.
/// If select_all_columns==true, all columns are selected and the value of
/// selected_columns is ignored.
std::unique_ptr<cql3::statements::raw::select_statement> build_select_statement(
        const sstring_view& cf_name,
        const sstring_view& where_clause,
        bool select_all_columns,
        const std::vector<column_definition>& selected_columns);

/// maybe_quote() takes an identifier - the name of a column, table or
/// keyspace name - and transforms it to a string which can be used in CQL
/// commands. Namely, if the identifier is not entirely lower-case (including
/// digits and underscores), it needs to be quoted to be represented in CQL.
/// Without this quoting, CQL folds uppercase letters to lower case, and
/// forbids non-alpha-numeric characters in identifier names.
/// Quoting involves wrapping the string in double-quotes ("). A double-quote
/// character itself is quoted by doubling it.
/// maybe_quote() also quotes reserved CQL keywords (e.g., "to", "where")
/// but doesn't quote *unreserved* keywords (like ttl, int or as).
/// Note that this means that if new reserved keywords are added to the
/// parser, a saved output of maybe_quote() may no longer be parsable by
/// parser. To avoid this forward-compatibility issue, use quote() instead
/// of maybe_quote() - to unconditionally quote an identifier even if it is
/// lowercase and not (yet) a keyword.
sstring maybe_quote(const sstring& s);

/// quote() takes an identifier - the name of a column, table or keyspace -
/// and transforms it to a string which can be safely used in CQL commands.
/// Quoting involves wrapping the name in double-quotes ("). A double-quote
/// character itself is quoted by doubling it.
/// Quoting is necessary when the identifier contains non-alpha-numeric
/// characters, when it contains uppercase letters (which will be folded to
/// lowercase if not quoted), or when the identifier is one of many CQL
/// keywords. But it's allowed - and easier - to just unconditionally
/// quote the identifier name in CQL, so that is what this function does does.
sstring quote(const sstring& s);

// Check whether timestamp is not too far in the future as this probably
// indicates its incorrectness (for example using other units than microseconds).
void validate_timestamp(const query_options& options, const std::unique_ptr<attributes>& attrs);

} // namespace util

} // namespace cql3
