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

template <typename Func, typename Result = cql3_parser::unwrap_uninitialized_t<std::invoke_result_t<Func, cql3_parser::CqlParser&>>>
Result do_with_parser(const sstring_view& cql, Func&& f) {
    std::optional<Result> ret;
    do_with_parser_impl(cql, [&] (cql3_parser::CqlParser& parser) {
        ret.emplace(f(parser));
    });
    return std::move(*ret);
}

sstring relations_to_where_clause(const expr::expression& e);

expr::expression where_clause_to_relations(const sstring_view& where_clause);

sstring rename_column_in_where_clause(const sstring_view& where_clause, column_identifier::raw from, column_identifier::raw to);

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

/// single_quote() takes a string and transforms it to a string 
/// which can be safely used in CQL commands.
/// Single quoting involves wrapping the name in single-quotes ('). A sigle-quote
/// character itself is quoted by doubling it.
/// Single quoting is necessary for dates, IP addresses or string literals.
sstring single_quote(const sstring& s);

// Check whether timestamp is not too far in the future as this probably
// indicates its incorrectness (for example using other units than microseconds).
void validate_timestamp(const db::config& config, const query_options& options, const std::unique_ptr<attributes>& attrs);

} // namespace util

} // namespace cql3
