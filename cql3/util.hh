/*
 * Copyright (C) 2016 ScyllaDB
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

#include <vector>

#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptor/transformed.hpp>

#include <seastar/core/sstring.hh>

#include "cql3/column_identifier.hh"
#include "cql3/CqlParser.hpp"
#include "cql3/error_collector.hh"
#include "cql3/relation.hh"
#include "cql3/statements/raw/select_statement.hh"

namespace cql3 {

namespace util {

template <typename Func, typename Result = std::result_of_t<Func(cql3_parser::CqlParser&)>>
Result do_with_parser(const sstring_view& cql, Func&& f) {
    cql3_parser::CqlLexer::collector_type lexer_error_collector(cql);
    cql3_parser::CqlParser::collector_type parser_error_collector(cql);
    cql3_parser::CqlLexer::InputStreamType input{reinterpret_cast<const ANTLR_UINT8*>(cql.begin()), ANTLR_ENC_UTF8, static_cast<ANTLR_UINT32>(cql.size()), nullptr};
    cql3_parser::CqlLexer lexer{&input};
    lexer.set_error_listener(lexer_error_collector);
    cql3_parser::CqlParser::TokenStreamType tstream(ANTLR_SIZE_HINT, lexer.get_tokSource());
    cql3_parser::CqlParser parser{&tstream};
    parser.set_error_listener(parser_error_collector);
    auto result = f(parser);
    return result;
}

template<typename Range> // Range<cql3::relation_ptr>
sstring relations_to_where_clause(Range&& relations) {
    auto expressions = relations | boost::adaptors::transformed(std::mem_fn(&relation::to_string));
    return boost::algorithm::join(expressions, " AND ");
}

static std::vector<relation_ptr> where_clause_to_relations(const sstring_view& where_clause) {
    return do_with_parser(where_clause, std::mem_fn(&cql3_parser::CqlParser::whereClause));
}

inline sstring rename_column_in_where_clause(const sstring_view& where_clause, column_identifier::raw from, column_identifier::raw to) {
    auto relations = where_clause_to_relations(where_clause);
    auto new_relations = relations | boost::adaptors::transformed([&] (auto&& rel) {
        return rel->maybe_rename_identifier(from, to);
    });
    return relations_to_where_clause(std::move(new_relations));
}

/// build a CQL "select" statement with the desired parameters.
/// If select_all_columns==true, all columns are selected and the value of
/// selected_columns is ignored.
shared_ptr<cql3::statements::raw::select_statement> build_select_statement(
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
sstring maybe_quote(const sstring& s);

} // namespace util

} // namespace cql3
