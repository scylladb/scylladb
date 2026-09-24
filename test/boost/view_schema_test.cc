/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */


#include <boost/test/unit_test.hpp>
#include "db/view/node_view_update_backlog.hh"

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/eventually.hh"
#include "utils/chunked_string.hh"

BOOST_AUTO_TEST_SUITE(view_schema_test)

using namespace std::literals::chrono_literals;

// Unlike everything else which used to be in this file, this is not a test of
// CQL behavior but a unit test of the db::view::node_update_backlog class, so
// it was not moved to Python along with the rest in issue #16134 - there is no
// way to reach this class from CQL, and no reason to want to.
SEASTAR_THREAD_TEST_CASE(node_view_update_backlog) {
    // This test was originally written assuming we have (at least) two
    // shards and the test doesn't run on shard 1...
    BOOST_ASSERT(this_shard_id() != 1);
    BOOST_ASSERT(this_smp_shard_count() >= 2);

    // First, check that a db::view::node_update_backlog object doesn't
    // recalculate the backlog if the interval hasn't yet passed (we use
    // a long 10 second interval that will certainly not pass during this
    // test).
    db::view::node_update_backlog b(2, 10s);
    auto backlog = [] (size_t size) { return db::view::update_backlog{size, 1000}; };
    smp::submit_to(0, [&b, &backlog] {
        b.add(backlog(10));
        b.fetch();
    }).get();
    smp::submit_to(1, [&b, &backlog] {
        b.add(backlog(50));
        b.fetch();
    }).get();
    BOOST_REQUIRE(b.load() == backlog(10));
    // Second, check that the backlog *is* recalculated if the interval
    // has passed. We use a very short interval (10ms) and sleep a bit more
    // to make sure it has passed.
    db::view::node_update_backlog b2(2, 10ms);
    smp::submit_to(0, [&b2, &backlog] {
        b2.add(backlog(10));
        b2.fetch();
    }).get();
    sleep(11ms).get();
    smp::submit_to(1, [&b2, &backlog] {
        b2.add(backlog(100));
        b2.fetch();
    }).get();
    BOOST_REQUIRE(b2.load() == backlog(100));
}

// The rest of this test - the long series of writes at assorted timestamps
// which it used to begin with - was moved to Python in issue #16134, as
// test_no_base_column_in_view_pk_complex_timestamp in
// test/cqlpy/test_materialized_view_old.py. Only these last few steps stayed
// behind, because they need forward_jump_clocks() to expire a TTL instantly
// and cqlpy has no equivalent of that.
SEASTAR_TEST_CASE(test_no_base_column_in_view_pk_complex_timestamp_ttl) {
    return do_with_cql_env_thread([] (cql_test_env& e) {

        e.execute_cql("CREATE TABLE t (k int, c int, a int, b int, e int, f int, primary key(k, c))").get();
        e.execute_cql("CREATE MATERIALIZED VIEW mv AS SELECT k,c,a,b FROM t "
                         "WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, k)").get();

        ::shared_ptr<cql_transport::messages::result_message> msg;

        // add selected with ttl=1
        e.execute_cql("UPDATE t USING TTL 30 SET a=1 WHERE k=1 AND c=1;").get();
        eventually([&] {
            msg = e.execute_cql("SELECT * FROM t").get();
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(1), int32_type->decompose(1), {}, {}, {} },
            });
            msg = e.execute_cql("SELECT * FROM mv").get();
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(1), int32_type->decompose(1), {} },
            });
        });

        forward_jump_clocks(31s);

        eventually([&] {
            msg = e.execute_cql("SELECT * FROM mv").get();
            assert_that(msg).is_rows().with_size(0);
        });

        // update unselected with ttl=1, view row should be alive
        e.execute_cql("UPDATE t USING TTL 30 SET f=1 WHERE k=1 AND c=1;").get();

        eventually([&] {
            msg = e.execute_cql("SELECT * FROM t").get();
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(1), {}, {}, {}, int32_type->decompose(1) },
            });
            msg = e.execute_cql("SELECT * FROM mv").get();
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(1), {}, {} },
            });
        });

        forward_jump_clocks(31s);

        eventually([&] {
            msg = e.execute_cql("SELECT * FROM t").get();
            assert_that(msg).is_rows().with_size(0);
            msg = e.execute_cql("SELECT * FROM mv").get();
            assert_that(msg).is_rows().with_size(0);
        });
    });
}

// As with test_no_base_column_in_view_pk_complex_timestamp_ttl above, the
// bulk of this test - its long series of writes at assorted timestamps - was
// moved to Python in issue #16134, as
// test_base_column_in_view_pk_complex_timestamp in
// test/cqlpy/test_materialized_view_old.py. Only these last steps stayed
// behind, because they need forward_jump_clocks() to expire a TTL instantly.
SEASTAR_TEST_CASE(test_base_column_in_view_pk_complex_timestamp_ttl) {
    return do_with_cql_env_thread([] (cql_test_env& e) {

        e.execute_cql("CREATE TABLE t (k int, c int, a int, b int, e int, f int, primary key(k, c))").get();
        e.execute_cql("CREATE MATERIALIZED VIEW mv AS SELECT k, c, a, b FROM t "
                         "WHERE k IS NOT NULL AND c IS NOT NULL AND a IS NOT NULL PRIMARY KEY (k, c, a)").get();
        ::shared_ptr<cql_transport::messages::result_message> msg;

        // add selected with ttl=1
        e.execute_cql("UPDATE t USING TTL 30 SET a=1, b=1 WHERE k=1 AND c=1;").get();
        eventually([&] {
            msg = e.execute_cql("SELECT * FROM t").get();
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(1), int32_type->decompose(1), int32_type->decompose(1), {}, {} },
            });
            msg = e.execute_cql("SELECT * FROM mv").get();
            assert_that(msg).is_rows().with_rows({
                { int32_type->decompose(1), int32_type->decompose(1), int32_type->decompose(1), int32_type->decompose(1) },
            });
        });

        forward_jump_clocks(31s);

        eventually([&] {
            msg = e.execute_cql("SELECT * FROM mv").get();
            assert_that(msg).is_rows().with_size(0);
        });
    });
}

BOOST_AUTO_TEST_SUITE_END()
