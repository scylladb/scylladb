
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "test/perf/perf.hh"

#include "cql3/query_processor.hh"
#include "cql3/statements/raw/parsed_statement.hh"

using namespace cql3;

int main(int argc, char* argv[]) {
    sstring query = "UPDATE \"standard1\" SET \"C0\" = 0xce7990de95e1516101cbbd6ca3bdc2819e799c8f9b1bfd1b08aa1d1edf09dd409b7d,\"C1\" = 0xc99b2076286ee4d4be742508653ed1178fb04192ae192d31745235e57dead6bf7f45,\"C2\" = 0xb492df82f1f2055af30694f135d3c99b0eac4e8d7d4d8e8b2d8ce49a9a3e50e3c63c,\"C3\" = 0xc42bcb9b1a215a8d9629887bee918437fd580f0d15c48e1402fe11f6caab069e95aa,\"C4\" = 0x329f193b16024ea72ace70571848e56b36496a05896454d13e1696c5c21053b5bcbb WHERE KEY=0x30374b37384e364c3531";

    std::cout << "Timing CQL statement parsing...\n";

    time_it([&] {
        auto _ = cql3::query_processor::parse_statement(query);
    });
}
