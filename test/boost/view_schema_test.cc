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

// This test was deliberately *not* moved to Python in issue #16134, and is
// one of the few left here. It uses forward_jump_clocks() to make TTLs expire
// instantly, and we have no equivalent of that in the Python (cqlpy) tests -
// there, the test would have to really sleep for the TTLs to pass, which for
// this test means over 10 seconds of sleeping. That is far too slow for
// cqlpy, where the entire file of materialized-view tests runs in seconds, so
// this test earns its keep by staying in C++.
SEASTAR_TEST_CASE(test_ttl) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c int, v1 int, v2 int, v3 int, primary key (p, c));").get();
        e.execute_cql("create materialized view mv as select p, c, v1, v2 from cf "
                      "where p is not null and c is not null and v1 is not null primary key (v1, c, p)").get();

        e.execute_cql("insert into cf (p, c, v1, v2, v3) values (0, 0, 0, 0, 0) using ttl 3").get();
        eventually([&] {
        auto msg = e.execute_cql("select * from mv").get();
        assert_that(msg).is_rows().with_size(1);
        forward_jump_clocks(4s);
        msg = e.execute_cql("select * from mv").get();
        assert_that(msg).is_rows().with_size(0);
        });

        e.execute_cql("insert into cf (p, c, v1, v2, v3) values (1, 1, 1, 1, 1) using ttl 3").get();
        forward_jump_clocks(1s);
        eventually([&] {
        auto msg = e.execute_cql("select v2 from mv").get();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(1)} });
        });

        e.execute_cql("insert into cf (p, c, v1) values (1, 1, 1)").get();
        forward_jump_clocks(4s);
        eventually([&] {
        auto msg = e.execute_cql("select v2 from mv").get();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ { } });
        });

        e.execute_cql("insert into cf (p, c, v1, v2, v3) values (2, 2, 2, 2, 2) using ttl 3").get();
        eventually([&] {
        auto msg = e.execute_cql("select * from mv where v1 = 2").get();
        assert_that(msg).is_rows().with_size(1);
        });
        forward_jump_clocks(2s);
        e.execute_cql("update cf using ttl 8 set v3 = 4 where p = 2 and c = 2").get();
        forward_jump_clocks(2s);
        eventually([&] {
        auto msg = e.execute_cql("select * from mv where v1 = 2").get();
        assert_that(msg).is_rows().with_size(0);
        msg = e.execute_cql("select * from cf where p = 2 and c = 2").get();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_row({ {int32_type->decompose(2)}, {int32_type->decompose(2)}, { }, { }, {int32_type->decompose(4)} });
        });
    });
}

// Like test_ttl above, this test was deliberately not moved to Python in
// issue #16134, while its twin test_non_primary_key_restrictions_update_vk
// was. It uses forward_jump_clocks() to expire a TTL instantly, and cqlpy has
// no equivalent - a Python version has to really sleep out the TTL, which
// measured at 1.8 seconds, more than a quarter of the run time of the whole
// materialized-view test file. So this one earns its keep by staying in C++.
SEASTAR_TEST_CASE(test_non_primary_key_restrictions_ttl_vk) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (a int, c int, primary key (a))").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where a is not null and c is not null and c = 1"
                      "primary key (a, c)").get();
        // Insert a base row without c, and set c=1 (matching the filter)
        // with a TTL. The view will then have a row, but it should disappear
        // when the TTL expires.
        // We later re-add c=1, and expect to see the view row appear again.
        BOOST_TEST_PASSPOINT();
        e.execute_cql("insert into cf (a, c) values (1, 0)").get();
        eventually([&] {
            auto msg = e.execute_cql("select a, c from vcf").get();
            assert_that(msg).is_rows().is_empty();
        });
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf using ttl 5 set c = 1 where a = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select a, c from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                { {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
        BOOST_TEST_PASSPOINT();
        forward_jump_clocks(6s);
        eventually([&] {
            auto msg = e.execute_cql("select a, c from vcf").get();
            assert_that(msg).is_rows().is_empty();
        });
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf set c = 1 where a = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select a, c from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                { {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
    });
}

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

// Test whether it is possible to drop columns from a base table which has
// materialized views. This should be allowed, unless one of the views "needs"
// the column, where needs means either this column was selected by the view,
// or is a virtual column (i.e., the *liveness* of this column matters).
// Reproduces issue #4448.
// Because our secondary indexes are also implemented on top of materialized
// views, the ability or inability to drop columns where secondary indexes
// exist also needs to be tested - see the separate test case
// test_secondary_index_allow_some_column_drops() in secondary_index_test.cc.
SEASTAR_TEST_CASE(test_mv_allow_some_column_drops) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        // When the view has a new key column that didn't exist in the base,
        // virtual columns aren't needed, so unselected columns aren't needed
        // by the view and may be dropped. Check that the drop is allowed and
        // the view still works properly afterwards.
        e.execute_cql("create table cf (p int primary key, a int, b int, c int)").get();
        e.execute_cql("create materialized view mv as select c from cf where a is not null primary key (a, p)").get();
        e.execute_cql("insert into cf (p, a, b, c) VALUES (1, 2, 3, 4)").get();
        BOOST_TEST_PASSPOINT();
        auto res = e.execute_cql("select * from cf").get();
        assert_that(res).is_rows().with_rows({
            {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(4)}}});
        e.execute_cql("alter table cf drop b").get();
        BOOST_TEST_PASSPOINT();
        res = e.execute_cql("select * from cf").get();
        assert_that(res).is_rows().with_rows({
            {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(4)}}});
        eventually([&] {
            auto res = e.execute_cql("select * from mv where a = 2").get();
            assert_that(res).is_rows().with_rows({
                {{int32_type->decompose(2)}, {int32_type->decompose(1)}, {int32_type->decompose(4)}}});
        });
        // Test that we cannot drop a selected column of a view. Both
        // c and a are selected (one as a new key column, one as a regular
        // column).
        BOOST_REQUIRE_THROW(e.execute_cql("alter table cf drop c").get(), exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("alter table cf drop a").get(), exceptions::invalid_request_exception);
        // We also cannot drop a base's primary key column, of course.
        BOOST_REQUIRE_THROW(e.execute_cql("alter table cf drop p").get(), exceptions::invalid_request_exception);
        // Also cannot drop a non existent column :-)
        BOOST_REQUIRE_THROW(e.execute_cql("alter table cf drop xyz").get(), exceptions::invalid_request_exception);

        // When a view has the same key columns as the base, virtual columns
        // are added for all unselected columns, because the *liveness* is
        // important for the view rows, even if the value isn't. In this case,
        // we do not allow to drop any base columns.
        e.execute_cql("create table cf2 (p int, c int, a int, b int, d int, primary key (p, c))").get();
        e.execute_cql("create materialized view mv2 as select d from cf2 where c is not null primary key (c, p)").get();
        BOOST_REQUIRE_THROW(e.execute_cql("alter table cf2 drop p").get(), exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("alter table cf2 drop c").get(), exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("alter table cf2 drop a").get(), exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("alter table cf2 drop b").get(), exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("alter table cf2 drop d").get(), exceptions::invalid_request_exception);
    });
}

BOOST_AUTO_TEST_SUITE_END()
