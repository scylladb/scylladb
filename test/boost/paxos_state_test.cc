/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>

#include <string_view>
#include <utility>

#include "test/lib/cql_test_env.hh"

#include "service/paxos/paxos_state.hh"

using namespace std::chrono_literals;

BOOST_AUTO_TEST_SUITE(paxos_state_test)

SEASTAR_TEST_CASE(test_prepared_statement_cache_prunes_after_table_drop) {
    return seastar::async([] {
        auto check = [] (std::string_view name, std::string_view drop_statement) {
            BOOST_TEST_CONTEXT(name) {
                cql_test_config cfg;
                // With vnodes, system.paxos is never dropped, so the weak pointers aren't invalidated immediately after
                // dropping the base table. Hence, we run the test only with tablets.
                cfg.initial_tablets = 1;
                cfg.need_remote_proxy = true;

                do_with_cql_env_thread([drop_statement] (cql_test_env& env) {
                    auto paxos_prepared_statements_cache_size = [&env] {
                        return env.get_paxos_store().local().get_prepared_statements_cache().size();
                    };

                    env.get_paxos_store().local().set_prepared_statements_prune_period(1ms);
                    BOOST_REQUIRE_EQUAL(paxos_prepared_statements_cache_size(), 0);

                    env.execute_cql("CREATE TABLE t (pk int PRIMARY KEY, v int)").get();
                    env.execute_cql("INSERT INTO t (pk, v) VALUES (0, 0) IF NOT EXISTS").get();

                    BOOST_REQUIRE_GT(paxos_prepared_statements_cache_size(), 0);

                    env.execute_cql(drop_statement).get();

                    const auto deadline = seastar::lowres_clock::now() + 10s;
                    while (paxos_prepared_statements_cache_size() != 0 && seastar::lowres_clock::now() < deadline) {
                        seastar::sleep(2ms).get();
                    }

                    BOOST_REQUIRE_EQUAL(paxos_prepared_statements_cache_size(), 0);
                }, std::move(cfg)).get();
            }
        };

        check("drop table", "DROP TABLE t");
        check("drop keyspace", "DROP KEYSPACE ks");
    });
}

BOOST_AUTO_TEST_SUITE_END()
