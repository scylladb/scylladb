/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */


#include <boost/test/unit_test.hpp>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/eventually.hh"

BOOST_AUTO_TEST_SUITE(view_schema_ckey_test)

using namespace std::literals::chrono_literals;

// This is a second reproducer for issue #4340. Here we create a more useful
// materialized view than the trivial one in the previous test, but in the
// view, put all key columns as partition keys, none of them in clustering
// keys. There is no reason why this shouldn't be allowed.
SEASTAR_TEST_CASE(test_no_clustering_key_2) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (a int, b int, c int, primary key (a))").get();
        // Before #4340 was fixed, we couldn't even create the following view:
        e.execute_cql("create materialized view mv as select a, b from cf "
                      "where a is not null and b is not null "
                      "primary key ((a,b))").get();
        // Let's check that after fixing #4340, we can not only create the
        // view, it also works as expected:
        e.execute_cql("insert into cf (a, b, c) values (1, 2, 3)").get();
        BOOST_TEST_PASSPOINT();
        auto msg = e.execute_cql("select * from cf").get();
        assert_that(msg).is_rows().with_size(1)
                .with_row({{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}});
        BOOST_TEST_PASSPOINT();
        eventually([&] {
            auto msg = e.execute_cql("select * from mv where a = 1 and b = 2").get();
            assert_that(msg).is_rows().with_size(1)
                    .with_row({{int32_type->decompose(1)}, {int32_type->decompose(2)}});
        });
    });
}

BOOST_AUTO_TEST_SUITE_END()
