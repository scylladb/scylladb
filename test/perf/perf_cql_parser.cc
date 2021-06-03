
/*
 * Copyright (C) 2015-present ScyllaDB
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

#include "test/perf/perf.hh"

#include "cql3/error_collector.hh"
#include "cql3/CqlParser.hpp"

using namespace cql3;

int main(int argc, char* argv[]) {
    sstring query = "UPDATE \"standard1\" SET \"C0\" = 0xce7990de95e1516101cbbd6ca3bdc2819e799c8f9b1bfd1b08aa1d1edf09dd409b7d,\"C1\" = 0xc99b2076286ee4d4be742508653ed1178fb04192ae192d31745235e57dead6bf7f45,\"C2\" = 0xb492df82f1f2055af30694f135d3c99b0eac4e8d7d4d8e8b2d8ce49a9a3e50e3c63c,\"C3\" = 0xc42bcb9b1a215a8d9629887bee918437fd580f0d15c48e1402fe11f6caab069e95aa,\"C4\" = 0x329f193b16024ea72ace70571848e56b36496a05896454d13e1696c5c21053b5bcbb WHERE KEY=0x30374b37384e364c3531";

    std::cout << "Timing CQL statement parsing...\n";

    time_it([&] {
        cql3_parser::CqlLexer::collector_type lexer_error_collector(query);
        cql3_parser::CqlParser::collector_type parser_error_collector(query);
        cql3_parser::CqlLexer::InputStreamType input{reinterpret_cast<const ANTLR_UINT8*>(query.data()), ANTLR_ENC_UTF8, static_cast<ANTLR_UINT32>(query.size()), nullptr};
        cql3_parser::CqlLexer lexer{&input};
        lexer.set_error_listener(lexer_error_collector);
        cql3_parser::CqlParser::TokenStreamType tstream(ANTLR_SIZE_HINT, lexer.get_tokSource());
        cql3_parser::CqlParser parser{&tstream};
        parser.set_error_listener(parser_error_collector);
        parser.query();
    });
}
