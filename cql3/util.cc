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

/* Copyright 2020 ScyllaDB */

#include "util.hh"

namespace cql3::util {

void do_with_parser_impl(const sstring_view& cql, noncopyable_function<void (cql3_parser::CqlParser& parser)> f) {
    cql3_parser::CqlLexer::collector_type lexer_error_collector(cql);
    cql3_parser::CqlParser::collector_type parser_error_collector(cql);
    cql3_parser::CqlLexer::InputStreamType input{reinterpret_cast<const ANTLR_UINT8*>(cql.begin()), ANTLR_ENC_UTF8, static_cast<ANTLR_UINT32>(cql.size()), nullptr};
    cql3_parser::CqlLexer lexer{&input};
    lexer.set_error_listener(lexer_error_collector);
    cql3_parser::CqlParser::TokenStreamType tstream(ANTLR_SIZE_HINT, lexer.get_tokSource());
    cql3_parser::CqlParser parser{&tstream};
    parser.set_error_listener(parser_error_collector);
    f(parser);
}

}
