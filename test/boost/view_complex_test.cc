/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>
#include <fmt/ranges.h>

#include "replica/database.hh"
#include "db/view/view_builder.hh"
#include "compaction/compaction_manager.hh"

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/eventually.hh"

#include "db/config.hh"

BOOST_AUTO_TEST_SUITE(view_complex_test)

using namespace std::literals::chrono_literals;

// This is another reproducer for issue #3362, using TTLs instead of
// numerous back-and-forth additions and deletions.
//
// Not moved to Python in issue #16134: using a TTL instead of the
// back-and-forth is the whole difference between this test and
// test_3362_no_ttls, which did move, and expiring one needs
// forward_jump_clocks(), which cqlpy has no equivalent of.
SEASTAR_TEST_CASE(test_3362_with_ttls) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c int, a int, b int, primary key (p, c))").get();
        e.execute_cql("create materialized view vcf as select p, c from cf "
                      "where p is not null and c is not null "
                      "primary key (p, c)").get();

        // In row p=1 c=1, insert two cells - a=1 with ttl, and b=1 without
        // ttl. The ttl'ed cell is inserted first, with a newer timestamp.
        // The problem is that the view row's marker gets, with a new
        // timestamp, a ttl. Then, when we go to add another column with an
        // older timestamp, and try to set the row marker without a
        // ttl - the older timestamp of this update looses, and we wrongly
        // remain with a ttl on the view row marker.
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf using timestamp 2 and ttl 100 set a = 1 where p = 1 and c = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf using timestamp 1 set b = 1 where p = 1 and c = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
        // Pass the time 101 seconds forward. Cell 'a' will have expired, but
        // cell 'b' will still exist, so the base row still exists and the
        // corresponding view row should also exist too.
        forward_jump_clocks(101s);
        BOOST_TEST_PASSPOINT();
        // verify that the base row still exists (cell b didn't expire)
        eventually([&] {
            auto msg = e.execute_cql("select * from cf where p = 1 and c = 1").get();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)}, {}, {{int32_type->decompose(1)}} }});
        });
        BOOST_TEST_PASSPOINT();
        // verify that the view row still exists too.
        // This check failing is issue #3362.
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
    });
}

// Not moved to Python in issue #16134: these are the TTL versions of the
// collection tests whose non-TTL versions did move (as
// test_3362_no_ttls_with_collections in
// test/cqlpy/test_materialized_view_old.py). Using a TTL is the whole
// difference, and expiring one needs forward_jump_clocks(), which cqlpy has
// no equivalent of.
enum class collection_kind { set, list, map };
void do_test_3362_with_ttls_with_collections(cql_test_env& e, collection_kind t) {
    sstring type, pref, suf;
    switch (t) {
    case collection_kind::set:
        type = "set<int>";
        pref = "{";
        suf = "}";
        break;
    case collection_kind::list:
        type = "list<int>";
        pref = "[";
        suf = "]";
        break;
    case collection_kind::map:
        type = "map<int, int>";
        pref = "{";
        suf = " : 17}";
        break;
    }
    e.execute_cql(format("create table cf (p int, c int, a {}, primary key (p, c))", type)).get();
    e.execute_cql("create materialized view vcf as select p, c from cf "
            "where p is not null and c is not null "
            "primary key (p, c)").get();
    e.execute_cql(format("update cf using timestamp 2 and ttl 100 set a = a + {}1{} where p = 1 and c = 1", pref, suf)).get();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get();
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
    });
    e.execute_cql(format("update cf using timestamp 1 set a = a + {}2{} where p = 1 and c = 1", pref, suf)).get();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get();
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
    });
    forward_jump_clocks(101s);
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get();
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
    });
}
SEASTAR_TEST_CASE(test_3362_with_ttls_with_set) {
    return do_with_cql_env_thread([] (auto& e) {
        do_test_3362_with_ttls_with_collections(e, collection_kind::set);
    });
}
SEASTAR_TEST_CASE(test_3362_with_ttls_with_list) {
    return do_with_cql_env_thread([] (auto& e) {
        do_test_3362_with_ttls_with_collections(e, collection_kind::list);
    });
}
SEASTAR_TEST_CASE(test_3362_with_ttls_with_map) {
    return do_with_cql_env_thread([] (auto& e) {
        do_test_3362_with_ttls_with_collections(e, collection_kind::map);
    });
}

// This is a version of test_3362_with_ttls with frozen collection fields
// instead of integer fields in test_3362_with_ttls. The intention is to
// verify that we properly fixed #3362 in this case - by replacing the
// frozen collection by a single virtual cell, not a collection.
//
// Not moved to Python in issue #16134, for the same reason as the tests above
// it: it needs forward_jump_clocks() to expire its TTL.
SEASTAR_TEST_CASE(test_3362_with_ttls_frozen) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c int, a frozen<set<int>>, b frozen<set<int>>, primary key (p, c))").get();
        e.execute_cql("create materialized view vcf as select p, c from cf "
                      "where p is not null and c is not null "
                      "primary key (p, c)").get();
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf using timestamp 2 and ttl 100 set a = {1,2} where p = 1 and c = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf using timestamp 1 set b = {3,4} where p = 1 and c = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
        forward_jump_clocks(101s);
        BOOST_TEST_PASSPOINT();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
    });
}

// This is a version of test_3362_with_ttls with the added twist that the
// unselected column involved did not exist when the base table and view
// were originally created, but only added later with an "alter table".
// For this test to work, "alter table" will need to add the virtual
// columns in the view table for the newly created unselected column in
// the base table.
//
// Not moved to Python in issue #16134, for the same reason as the tests above
// it: it needs forward_jump_clocks() to expire its TTL.
SEASTAR_TEST_CASE(test_3362_with_ttls_alter_add) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c int, primary key (p, c))").get();
        e.execute_cql("create materialized view vcf as select p, c from cf "
                      "where p is not null and c is not null "
                      "primary key (p, c)").get();
        // Add with "alter table" two additional columns to the base table -
        // a and b. These are not selected in the materialized view, and we
        // want to check that they are treated like unselected columns
        // (namely, virtual columns are added to the view).
        e.execute_cql("alter table cf add a int").get();
        e.execute_cql("alter table cf add b int").get();
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf using timestamp 2 and ttl 100 set a = 1 where p = 1 and c = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf using timestamp 1 set b = 1 where p = 1 and c = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
        forward_jump_clocks(101s);
        BOOST_TEST_PASSPOINT();
        eventually([&] {
            auto msg = e.execute_cql("select * from cf where p = 1 and c = 1").get();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)}, {}, {{int32_type->decompose(1)}} }});
        });
        BOOST_TEST_PASSPOINT();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
    });
}

// test_3362_with_ttls_alter_add() above is about handling changes to virtual
// columns as the base table columns change, but only for the "add" case.
// Theoretically we could have had problems in the "drop" and "rename" cases
// as well, but today, those are not supported:
// 1. Today we do not allow "alter table drop" to drop any column from a base
//    table with views - even unselected columns.
//    If we every do allow this, we need to also check that we drop the
//    virtual column from the view.
// 2. Today we do not allow "alter table rename" to rename any non-pk
//    column, so unselected columns also cannot be renamed. If this
//    limitation is ever lifted, we will need to check that if we
//    rename an unselected base column, the virtual column in the view is
//    also renamed.


BOOST_AUTO_TEST_SUITE_END()
