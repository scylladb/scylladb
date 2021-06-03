/*
 * Copyright (C) 2018-present ScyllaDB
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

#include <seastar/testing/test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"
#include "transport/messages/result_message.hh"
#include "service/pager/paging_state.hh"
#include "types/map.hh"
#include "types/list.hh"
#include "types/set.hh"
#include "test/lib/exception_utils.hh"
#include "cql3/statements/select_statement.hh"
#include "test/lib/select_statement_utils.hh"


SEASTAR_TEST_CASE(test_secondary_index_regular_column_query) {
    return do_with_cql_env([] (cql_test_env& e) {
        return e.execute_cql("CREATE TABLE users (userid int, name text, email text, country text, PRIMARY KEY (userid));").discard_result().then([&e] {
            return e.execute_cql("CREATE INDEX ON users (email);").discard_result();
        }).then([&e] {
            return e.execute_cql("CREATE INDEX ON users (country);").discard_result();
        }).then([&e] {
            return e.execute_cql("INSERT INTO users (userid, name, email, country) VALUES (0, 'Bondie Easseby', 'beassebyv@house.gov', 'France');").discard_result();
        }).then([&e] {
            return e.execute_cql("INSERT INTO users (userid, name, email, country) VALUES (1, 'Demetri Curror', 'dcurrorw@techcrunch.com', 'France');").discard_result();
        }).then([&e] {
            return e.execute_cql("INSERT INTO users (userid, name, email, country) VALUES (2, 'Langston Paulisch', 'lpaulischm@reverbnation.com', 'United States');").discard_result();
        }).then([&e] {
            return e.execute_cql("INSERT INTO users (userid, name, email, country) VALUES (3, 'Channa Devote', 'cdevote14@marriott.com', 'Denmark');").discard_result();
        }).then([&e] {
            return e.execute_cql("SELECT email FROM users WHERE country = 'France';");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows().with_rows({
                { utf8_type->decompose(sstring("dcurrorw@techcrunch.com")) },
                { utf8_type->decompose(sstring("beassebyv@house.gov")) },
            });
        });
    });
}

SEASTAR_TEST_CASE(test_secondary_index_clustering_key_query) {
    return do_with_cql_env([] (cql_test_env& e) {
        return e.execute_cql("CREATE TABLE users (userid int, name text, email text, country text, PRIMARY KEY (userid, country));").discard_result().then([&e] {
            return e.execute_cql("CREATE INDEX ON users (country);").discard_result();
        }).then([&e] {
            return e.execute_cql("INSERT INTO users (userid, name, email, country) VALUES (0, 'Bondie Easseby', 'beassebyv@house.gov', 'France');").discard_result();
        }).then([&e] {
            return e.execute_cql("INSERT INTO users (userid, name, email, country) VALUES (1, 'Demetri Curror', 'dcurrorw@techcrunch.com', 'France');").discard_result();
        }).then([&e] {
            return e.execute_cql("INSERT INTO users (userid, name, email, country) VALUES (2, 'Langston Paulisch', 'lpaulischm@reverbnation.com', 'United States');").discard_result();
        }).then([&e] {
            return e.execute_cql("INSERT INTO users (userid, name, email, country) VALUES (3, 'Channa Devote', 'cdevote14@marriott.com', 'Denmark');").discard_result();
        }).then([&e] {
            return e.execute_cql("SELECT email FROM users WHERE country = 'France';");
        }).then([&e] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                { utf8_type->decompose(sstring("dcurrorw@techcrunch.com")) },
                { utf8_type->decompose(sstring("beassebyv@house.gov")) },
            });
        }).then([&e] {
            return e.execute_cql("select country from users where country='France' and country='Denmark'"); // #7772
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows().is_empty();
        }).then([&e] {
            return e.execute_cql("select country from users where country='Denmark' and country='Denmark'");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows().with_rows({{utf8_type->decompose(sstring("Denmark"))}});
        });
    });
}

// If there is a single partition key column, creating an index on this
// column is not necessary - it is already indexed as the partition key!
// So Scylla, as does Cassandra, forbids it. The user should just drop
// the "create index" attempt and searches will work anyway.
// This test verifies that this case is indeed forbidden.
SEASTAR_TEST_CASE(test_secondary_index_single_column_partition_key) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p int primary key, a int)").get();
        // Expecting exception: "exceptions::invalid_request_exception:
        // Cannot create secondary index on partition key column p"
        assert_that_failed(e.execute_cql("create index on cf (p)"));
        // The same happens if we also have a clustering key, but still just
        // one partition key column and we want to index it
        e.execute_cql("create table cf2 (p int, c1 int, c2 int, a int, primary key (p, c1, c2))").get();
        // Expecting exception: "exceptions::invalid_request_exception:
        // Cannot create secondary index on partition key column p"
        assert_that_failed(e.execute_cql("create index on cf2 (p)"));
    });
}

// However, if there are multiple partition key columns (a so-called composite
// partition key), we *should* be able to index each one of them separately.
// It is useful, and Cassandra allows it, so should we (this was issue #3404)
SEASTAR_TEST_CASE(test_secondary_index_multi_column_partition_key) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p1 int, p2 int, a int, primary key ((p1, p2)))").get();
        e.execute_cql("create index on cf (a)").get();
        e.execute_cql("create index on cf (p1)").get();
        e.execute_cql("create index on cf (p2)").get();
    });
}

// CQL usually folds identifier names - keyspace, table and column names -
// to lowercase. That is, unless the identifier is enclosed in double
// quotation marks ("). Let's test that case-sensitive (quoted) column
// names can be indexed. This reproduces issues #3154, #3388, #3391, #3401.
SEASTAR_TEST_CASE(test_secondary_index_case_sensitive) {
    return do_with_cql_env_thread([] (auto& e) {
        // Test case-sensitive *table* name.
        e.execute_cql("CREATE TABLE \"FooBar\" (a int PRIMARY KEY, b int, c int)").get();
        e.execute_cql("CREATE INDEX ON \"FooBar\" (b)").get();
        e.execute_cql("INSERT INTO \"FooBar\" (a, b, c) VALUES (1, 2, 3)").get();
        e.execute_cql("SELECT * from \"FooBar\" WHERE b = 1").get();

        // Test case-sensitive *indexed column* name.
        // This not working was issue #3154. The symptom was that the SELECT
        // below threw a "No index found." runtime error.
        e.execute_cql("CREATE TABLE tab (a int PRIMARY KEY, \"FooBar\" int, c int)").get();
        e.execute_cql("CREATE INDEX ON tab (\"FooBar\")").get();
        // This INSERT also had problems (issue #3401)
        e.execute_cql("INSERT INTO tab (a, \"FooBar\", c) VALUES (1, 2, 3)").get();
        e.execute_cql("SELECT * from tab WHERE \"FooBar\" = 2").get();

        // Test case-sensitive *partition column* name.
        // This used to have multiple bugs in SI and MV code, detailed below:
        e.execute_cql("CREATE TABLE tab2 (\"FooBar\" int PRIMARY KEY, b int, c int)").get();
        e.execute_cql("CREATE INDEX ON tab2 (b)").get();
        // The following INSERT didn't work because of issues #3388 and #3391.
        e.execute_cql("INSERT INTO tab2 (\"FooBar\", b, c) VALUES (1, 2, 3)").get();
        // After the insert works, add the SELECT and see it works. It used
        // to fail before the patch to #3210 fixed this incidentally.
        e.execute_cql("SELECT * from tab2 WHERE b = 2").get();
    });
}

SEASTAR_TEST_CASE(test_cannot_drop_secondary_index_backing_mv) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p int primary key, a int)").get();
        e.execute_cql("create index on cf (a)").get();
        auto s = e.local_db().find_schema(sstring("ks"), sstring("cf"));
        auto index_name = s->index_names().front();
        assert_that_failed(e.execute_cql(format("drop materialized view {}_index", index_name)));
    });
}

// Issue #3210 is about searching the secondary index not working properly
// when the *partition key* has multiple columns (a compound partition key),
// and this is what we test here.
SEASTAR_TEST_CASE(test_secondary_index_case_compound_partition_key) {
    return do_with_cql_env_thread([] (auto& e) {
        // Test case-sensitive *table* name.
        e.execute_cql("CREATE TABLE tab (a int, b int, c int, PRIMARY KEY ((a, b)))").get();
        e.execute_cql("CREATE INDEX ON tab (c)").get();
        e.execute_cql("INSERT INTO tab (a, b, c) VALUES (1, 2, 3)").get();
        eventually([&] {
            // We expect this search to find the single row, with the compound
            // partition key (a, b) = (1, 2).
            auto res = e.execute_cql("SELECT * from tab WHERE c = 3").get0();
            assert_that(res).is_rows()
                    .with_size(1)
                    .with_row({
                        {int32_type->decompose(1)},
                        {int32_type->decompose(2)},
                        {int32_type->decompose(3)},
                    });
        });
    });
}

// Tests for issue #2991 - test that "IF NOT EXISTS" works as expected for
// index creation, and "IF EXISTS" for index drop.
SEASTAR_TEST_CASE(test_secondary_index_if_exists) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p int primary key, a int)").get();
        e.execute_cql("create index on cf (a)").get();
        // Confirm that creating the same index again with "if not exists" is
        // fine, but without "if not exists", it's an error.
        e.execute_cql("create index if not exists on cf (a)").get();
        assert_that_failed(e.execute_cql("create index on cf (a)"));
        // Confirm that after dropping the index, dropping it again with
        // "if exists" is fine, but an error without it.
        e.execute_cql("drop index cf_a_idx").get();
        e.execute_cql("drop index if exists cf_a_idx").get();
        // Expect exceptions::invalid_request_exception: Index 'cf_a_idx'
        // could not be found in any of the tables of keyspace 'ks'
        assert_that_failed(seastar::futurize_invoke([&e] { return e.execute_cql("drop index cf_a_idx"); }));
    });
}

// An index can be named, and if it isn't, the name defaults to
// <table>_<column>_idx. There is little consequence for the name
// chosen, but it needs to be known for dropping an index.
SEASTAR_TEST_CASE(test_secondary_index_name) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        // Default name
        e.execute_cql("create table cf (abc int primary key, xyz int)").get();
        e.execute_cql("create index on cf (xyz)").get();
        e.execute_cql("insert into cf (abc, xyz) VALUES (1, 2)").get();
        e.execute_cql("select * from cf WHERE xyz = 2").get();
        e.execute_cql("drop index cf_xyz_idx").get();
        // Default name, both cf and column name are case-sensitive but
        // still alphanumeric.
        e.execute_cql("create table \"TableName\" (abc int primary key, \"FooBar\" int)").get();
        e.execute_cql("create index on \"TableName\" (\"FooBar\")").get();
        e.execute_cql("insert into \"TableName\" (abc, \"FooBar\") VALUES (1, 2)").get();
        e.execute_cql("select * from \"TableName\" WHERE \"FooBar\" = 2").get();
        e.execute_cql("drop index \"TableName_FooBar_idx\"").get();
        // Scylla, as does Cassandra, forces table names to be alphanumeric
        // and cannot contain weird characters such as space. But column names
        // may! So when creating the default index name, these characters are
        // dropped, so that the index name resembles a legal table name.
        e.execute_cql("create table \"TableName2\" (abc int primary key, \"Foo Bar\" int)").get();
        e.execute_cql("create index on \"TableName2\" (\"Foo Bar\")").get();
        e.execute_cql("insert into \"TableName2\" (abc, \"Foo Bar\") VALUES (1, 2)").get();
        e.execute_cql("select * from \"TableName2\" WHERE \"Foo Bar\" = 2").get();
        // To be 100% compatible with Cassandra, we should drop non-alpha numeric
        // from the default index name. But we don't, yet. This is issue #3403:
#if 0
        e.execute_cql("drop index \"TableName2_FooBar_idx\"").get(); // note no space
#else
        e.execute_cql("drop index \"TableName2_Foo Bar_idx\"").get(); // note space
#endif
        // User-chosen name
        e.execute_cql("create table cf2 (abc int primary key, xyz int)").get();
        e.execute_cql("create index \"IndexName\" on cf2 (xyz)").get();
        e.execute_cql("insert into cf2 (abc, xyz) VALUES (1, 2)").get();
        e.execute_cql("select * from cf2 WHERE xyz = 2").get();
        e.execute_cql("drop index \"IndexName\"").get();
    });
}

// Test that if we have multiple columns of all types - multiple regular
// columns, multiple clustering columns, and multiple partition columns,
// we can index *all* of these columns at the same time, and all the indexes
// can be used to find the correct rows.
// This reproduced issue #3405 as we have here multiple clustering columns.
SEASTAR_TEST_CASE(test_many_columns) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("CREATE TABLE tab (a int, b int, c int, d int, e int, f int, PRIMARY KEY ((a, b), c, d))").get();
        e.execute_cql("CREATE INDEX ON tab (a)").get();
        e.execute_cql("CREATE INDEX ON tab (b)").get();
        e.execute_cql("CREATE INDEX ON tab (c)").get();
        e.execute_cql("CREATE INDEX ON tab (d)").get();
        e.execute_cql("CREATE INDEX ON tab (e)").get();
        e.execute_cql("CREATE INDEX ON tab (f)").get();
        e.execute_cql("INSERT INTO tab (a, b, c, d, e, f) VALUES (1, 2, 3, 4, 5, 6)").get();
        e.execute_cql("INSERT INTO tab (a, b, c, d, e, f) VALUES (1, 0, 0, 0, 0, 0)").get();
        e.execute_cql("INSERT INTO tab (a, b, c, d, e, f) VALUES (0, 2, 0, 0, 0, 0)").get();
        e.execute_cql("INSERT INTO tab (a, b, c, d, e, f) VALUES (0, 0, 3, 0, 0, 0)").get();
        e.execute_cql("INSERT INTO tab (a, b, c, d, e, f) VALUES (0, 0, 0, 4, 0, 0)").get();
        e.execute_cql("INSERT INTO tab (a, b, c, d, e, f) VALUES (0, 0, 0, 0, 5, 0)").get();
        e.execute_cql("INSERT INTO tab (a, b, c, d, e, f) VALUES (0, 0, 0, 7, 0, 6)").get();
        e.execute_cql("INSERT INTO tab (a, b, c, d, e, f) VALUES (1, 2, 3, 7, 5, 0)").get();
        // We expect each search below to find two or three of the rows that
        // we inserted above.
        BOOST_TEST_PASSPOINT();
        eventually([&] {
            auto res = e.execute_cql("SELECT * from tab WHERE a = 1").get0();
            assert_that(res).is_rows().with_size(3)
                .with_rows({
                {{int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}},
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(4)}, {int32_type->decompose(5)}, {int32_type->decompose(6)}},
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(7)}, {int32_type->decompose(5)}, {int32_type->decompose(0)}},
            });
        });
        BOOST_TEST_PASSPOINT();
        eventually([&] {
            auto res = e.execute_cql("SELECT * from tab WHERE b = 2").get0();
            assert_that(res).is_rows().with_size(3)
                .with_rows({
                {{int32_type->decompose(0)}, {int32_type->decompose(2)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}},
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(4)}, {int32_type->decompose(5)}, {int32_type->decompose(6)}},
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(7)}, {int32_type->decompose(5)}, {int32_type->decompose(0)}},
            });
        });
        BOOST_TEST_PASSPOINT();
        eventually([&] {
            auto res = e.execute_cql("SELECT * from tab WHERE c = 3").get0();
            assert_that(res).is_rows().with_size(3)
                .with_rows({
                {{int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(3)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}},
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(4)}, {int32_type->decompose(5)}, {int32_type->decompose(6)}},
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(7)}, {int32_type->decompose(5)}, {int32_type->decompose(0)}},
            });
        });
        BOOST_TEST_PASSPOINT();
        eventually([&] {
            auto res = e.execute_cql("SELECT * from tab WHERE d = 4").get0();
            assert_that(res).is_rows().with_size(2)
                .with_rows({
                {{int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(4)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}},
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(4)}, {int32_type->decompose(5)}, {int32_type->decompose(6)}},
            });
        });
        BOOST_TEST_PASSPOINT();
        eventually([&] {
            auto res = e.execute_cql("SELECT * from tab WHERE e = 5").get0();
            assert_that(res).is_rows().with_size(3)
                .with_rows({
                {{int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(5)}, {int32_type->decompose(0)}},
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(4)}, {int32_type->decompose(5)}, {int32_type->decompose(6)}},
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(7)}, {int32_type->decompose(5)}, {int32_type->decompose(0)}},
            });
        });
        BOOST_TEST_PASSPOINT();
        eventually([&] {
            auto res = e.execute_cql("SELECT * from tab WHERE f = 6").get0();
            assert_that(res).is_rows().with_size(2)
                .with_rows({
                {{int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(7)}, {int32_type->decompose(0)}, {int32_type->decompose(6)}},
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(4)}, {int32_type->decompose(5)}, {int32_type->decompose(6)}},
            });
        });
        BOOST_TEST_PASSPOINT();
        eventually([&] {
            // #7659
            cquery_nofail(e, "SELECT * FROM tab WHERE d=0 AND f>0 ALLOW FILTERING");
            cquery_nofail(e, "SELECT * FROM tab WHERE f=0 AND d>0 ALLOW FILTERING");
            cquery_nofail(e, "SELECT * FROM tab WHERE f=0 AND f>0 ALLOW FILTERING");
        });
    });
}

SEASTAR_TEST_CASE(test_index_with_partition_key) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("CREATE TABLE tab (a int, b int, c int, d int, e int, f int, PRIMARY KEY ((a, b), c, d))").get();
        e.execute_cql("CREATE INDEX ON tab (e)").get();
        e.execute_cql("INSERT INTO tab (a, b, c, d, e, f) VALUES (1, 2, 3, 4, 5, 6)").get();
        e.execute_cql("INSERT INTO tab (a, b, c, d, e, f) VALUES (1, 0, 0, 0, 0, 0)").get();
        e.execute_cql("INSERT INTO tab (a, b, c, d, e, f) VALUES (0, 2, 0, 0, 0, 0)").get();
        e.execute_cql("INSERT INTO tab (a, b, c, d, e, f) VALUES (0, 0, 3, 0, 0, 0)").get();
        e.execute_cql("INSERT INTO tab (a, b, c, d, e, f) VALUES (0, 0, 0, 4, 0, 0)").get();
        e.execute_cql("INSERT INTO tab (a, b, c, d, e, f) VALUES (0, 0, 0, 0, 5, 0)").get();
        e.execute_cql("INSERT INTO tab (a, b, c, d, e, f) VALUES (0, 0, 0, 7, 0, 6)").get();
        e.execute_cql("INSERT INTO tab (a, b, c, d, e, f) VALUES (1, 2, 3, 7, 5, 0)").get();

        // Queries that restrict the whole partition key and an index should not require filtering - they are not performance-heavy
        eventually([&] {
            auto res = e.execute_cql("SELECT * from tab WHERE a = 1 and b = 2 and e = 5").get0();
            assert_that(res).is_rows().with_rows({
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(4)}, {int32_type->decompose(5)}, {int32_type->decompose(6)}},
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(7)}, {int32_type->decompose(5)}, {int32_type->decompose(0)}}
            });
        });

        // Queries that restrict only a part of the partition key and an index require filtering, because we need to compute token
        // in order to create a valid index view query
        BOOST_REQUIRE_THROW(e.execute_cql("SELECT * from tab WHERE a = 1 and e = 5").get(), exceptions::invalid_request_exception);

        // Indexed queries with full primary key are allowed without filtering as well
        eventually([&] {
            auto res = e.execute_cql("SELECT * from tab WHERE a = 1 and b = 2 and c = 3 and d = 4 and e = 5").get0();
            assert_that(res).is_rows().with_rows({
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(4)}, {int32_type->decompose(5)}, {int32_type->decompose(6)}}
            });
        });

        // And it's also sufficient if only full parition key + clustering key prefix is present
        eventually([&] {
            auto res = e.execute_cql("SELECT * from tab WHERE a = 1 and b = 2 and c = 3 and e = 5").get0();
            assert_that(res).is_rows().with_rows({
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(4)}, {int32_type->decompose(5)}, {int32_type->decompose(6)}},
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(7)}, {int32_type->decompose(5)}, {int32_type->decompose(0)}}
            });
        });

        // This query needs filtering, because clustering key restrictions do not form a prefix
        BOOST_REQUIRE_THROW(e.execute_cql("SELECT * from tab WHERE a = 1 and b = 2 and d = 4 and e = 5").get(), exceptions::invalid_request_exception);
        eventually([&] {
            auto res = e.execute_cql("SELECT * from tab WHERE a = 1 and b = 2 and d = 4 and e = 5 ALLOW FILTERING").get0();
            assert_that(res).is_rows().with_rows({
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(4)}, {int32_type->decompose(5)}, {int32_type->decompose(6)}}
            });
        });

        eventually([&] {
            auto res = e.execute_cql("SELECT * from tab WHERE a = 1 and b IN (2, 3) and d IN (4, 5, 6, 7) and e = 5 ALLOW FILTERING").get0();
            assert_that(res).is_rows().with_rows({
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(4)}, {int32_type->decompose(5)}, {int32_type->decompose(6)}},
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(7)}, {int32_type->decompose(5)}, {int32_type->decompose(0)}}
            });
        });

        eventually([&] {
            auto res = e.execute_cql("SELECT * from tab WHERE a = 1 and b = 2 and (c, d) in ((3, 4), (1, 1), (3, 7)) and e = 5 ALLOW FILTERING").get0();
            assert_that(res).is_rows().with_rows({
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(4)}, {int32_type->decompose(5)}, {int32_type->decompose(6)}},
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(7)}, {int32_type->decompose(5)}, {int32_type->decompose(0)}}
            });
        });
    });
}

SEASTAR_TEST_CASE(test_index_on_pk_ck_with_paging) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("CREATE TABLE tab (pk int, pk2 int, ck text, ck2 text, v int, v2 int, v3 text, PRIMARY KEY ((pk, pk2), ck, ck2))").get();
        e.execute_cql("CREATE INDEX ON tab (v)").get();
        e.execute_cql("CREATE INDEX ON tab (pk2)").get();
        e.execute_cql("CREATE INDEX ON tab (ck2)").get();

        sstring big_string(1024 * 1024 + 7, 'j');
        for (int i = 0; i < 4; ++i) {
            e.execute_cql(format("INSERT INTO tab (pk, pk2, ck, ck2, v, v2, v3) VALUES ({}, {}, 'hello{}', 'world{}', 1, {}, '{}')", i % 3, i, i, i, i, big_string)).get();
        }
        for (int i = 4; i < 2052; ++i) {
            e.execute_cql(format("INSERT INTO tab (pk, pk2, ck, ck2, v, v2, v3) VALUES ({}, {}, 'hello{}', 'world{}', 1, {}, '{}')", i % 3, i, i, i, i, "small_string")).get();
        }

        eventually([&] {
            auto qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{101, nullptr, {}, api::new_timestamp()});
            auto res = e.execute_cql("SELECT * FROM tab WHERE v = 1", std::move(qo)).get0();
            assert_that(res).is_rows().with_size(101);
        });

        eventually([&] {
            auto res = e.execute_cql("SELECT * FROM tab WHERE v = 1").get0();
            assert_that(res).is_rows().with_size(2052);
        });

        eventually([&] {
            auto qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{100, nullptr, {}, api::new_timestamp()});
            auto res = e.execute_cql("SELECT * FROM tab WHERE pk2 = 1", std::move(qo)).get0();
            assert_that(res).is_rows().with_rows({{
                {int32_type->decompose(1)}, {int32_type->decompose(1)}, {utf8_type->decompose("hello1")}, {utf8_type->decompose("world1")},
                {int32_type->decompose(1)}, {int32_type->decompose(1)}, {utf8_type->decompose(big_string)}
            }});
        });

        eventually([&] {
            auto qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{100, nullptr, {}, api::new_timestamp()});
            auto res = e.execute_cql("SELECT * FROM tab WHERE ck2 = 'world8'", std::move(qo)).get0();
            assert_that(res).is_rows().with_rows({{
                {int32_type->decompose(2)}, {int32_type->decompose(8)}, {utf8_type->decompose("hello8")}, {utf8_type->decompose("world8")},
                {int32_type->decompose(1)}, {int32_type->decompose(8)}, {utf8_type->decompose("small_string")}
            }});
        });
    });
}

SEASTAR_TEST_CASE(test_simple_index_paging) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("CREATE TABLE tab (p int, c int, v int, PRIMARY KEY (p, c))").get();
        e.execute_cql("CREATE INDEX ON tab (v)").get();
        e.execute_cql("CREATE INDEX ON tab (c)").get();

        e.execute_cql("INSERT INTO tab (p, c, v) VALUES (1, 2, 1)").get();
        e.execute_cql("INSERT INTO tab (p, c, v) VALUES (1, 1, 1)").get();
        e.execute_cql("INSERT INTO tab (p, c, v) VALUES (3, 2, 1)").get();

        auto extract_paging_state = [] (::shared_ptr<cql_transport::messages::result_message> res) {
            auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(res);
            auto paging_state = rows->rs().get_metadata().paging_state();
            assert(paging_state);
            return make_lw_shared<service::pager::paging_state>(*paging_state);
        };

        auto expect_more_pages = [] (::shared_ptr<cql_transport::messages::result_message> res, bool more_pages_expected) {
            auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(res);
            if(more_pages_expected != rows->rs().get_metadata().flags().contains(cql3::metadata::flag::HAS_MORE_PAGES)) {
                throw std::runtime_error(format("Expected {}more pages", more_pages_expected ? "" : "no "));
            }
        };

        eventually([&] {
            auto qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{1, nullptr, {}, api::new_timestamp()});
            auto res = e.execute_cql("SELECT * FROM tab WHERE v = 1", std::move(qo)).get0();
            auto paging_state = extract_paging_state(res);
            expect_more_pages(res, true);

            assert_that(res).is_rows().with_rows({{
                {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)},
            }});

            qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{1, paging_state, {}, api::new_timestamp()});
            res = e.execute_cql("SELECT * FROM tab WHERE v = 1", std::move(qo)).get0();
            expect_more_pages(res, true);
            paging_state = extract_paging_state(res);

            assert_that(res).is_rows().with_rows({{
                {int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(1)},
            }});

            qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{1, paging_state, {}, api::new_timestamp()});
            res = e.execute_cql("SELECT * FROM tab WHERE v = 1", std::move(qo)).get0();
            paging_state = extract_paging_state(res);

            assert_that(res).is_rows().with_rows({{
                {int32_type->decompose(3)}, {int32_type->decompose(2)}, {int32_type->decompose(1)},
            }});

            // Due to implementation differences from origin (Scylla is allowed to return empty pages with has_more_pages == true
            // and it's a legal operation), paging indexes may result in finding an additional empty page at the end of the results,
            // but never more than one. This case used to be buggy (see #4569).
            try {
                expect_more_pages(res, false);
            } catch (...) {
                qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                        cql3::query_options::specific_options{1, paging_state, {}, api::new_timestamp()});
                res = e.execute_cql("SELECT * FROM tab WHERE v = 1", std::move(qo)).get0();
                assert_that(res).is_rows().with_size(0);
                expect_more_pages(res, false);
            }

        });

        eventually([&] {
            auto qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{1, nullptr, {}, api::new_timestamp()});
            auto res = e.execute_cql("SELECT * FROM tab WHERE c = 2", std::move(qo)).get0();
            auto paging_state = extract_paging_state(res);

            assert_that(res).is_rows().with_rows({{
                {int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(1)},
            }});

            qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{1, paging_state, {}, api::new_timestamp()});
            res = e.execute_cql("SELECT * FROM tab WHERE c = 2", std::move(qo)).get0();

            assert_that(res).is_rows().with_rows({{
                {int32_type->decompose(3)}, {int32_type->decompose(2)}, {int32_type->decompose(1)},
            }});
        });

        {
            auto qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{1, nullptr, {}, api::new_timestamp()});
            auto res = e.execute_cql("SELECT * FROM tab WHERE c = 2", std::move(qo)).get0();
            auto paging_state = extract_paging_state(res);

            assert_that(res).is_rows().with_rows({{
                {int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(1)},
            }});

            // Override the actual paging state with one with empty keys,
            // which is a valid paging state as well, and should return
            // no rows.
            paging_state = make_lw_shared<service::pager::paging_state>(partition_key::make_empty(),
                    std::nullopt, paging_state->get_remaining(), paging_state->get_query_uuid(),
                    paging_state->get_last_replicas(), paging_state->get_query_read_repair_decision(),
                    paging_state->get_rows_fetched_for_last_partition());

            qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{1, paging_state, {}, api::new_timestamp()});
            res = e.execute_cql("SELECT * FROM tab WHERE c = 2", std::move(qo)).get0();

            assert_that(res).is_rows().with_size(0);
        }

        {
            // An artificial paging state with an empty key pair is also valid and is expected
            // not to return rows (since no row matches an empty partition key)
            auto paging_state = make_lw_shared<service::pager::paging_state>(partition_key::make_empty(), std::nullopt,
                    1, utils::make_random_uuid(), service::pager::paging_state::replicas_per_token_range{}, std::nullopt, 1);
            auto qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{1, paging_state, {}, api::new_timestamp()});
            auto res = e.execute_cql("SELECT * FROM tab WHERE v = 1", std::move(qo)).get0();

            assert_that(res).is_rows().with_size(0);
        }
    });
}

SEASTAR_TEST_CASE(test_secondary_index_collections) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table t (p int primary key, s1 set<int>, m1 map<int, text>, l1 list<int>, s2 frozen<set<int>>, m2 frozen<map<int, text>>, l2 frozen<list<int>>)").get();

        using ire = exceptions::invalid_request_exception;
        using exception_predicate::message_contains;
        auto non_frozen = message_contains("index on non-frozen");
        auto non_full = message_contains("Cannot create index");
        auto duplicate = message_contains("duplicate");
        auto entry_eq = message_contains("entry equality predicates on frozen map");

        //NOTICE(sarna): should be lifted after issue #2962 is resolved
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("create index on t(s1)").get(), ire, non_frozen);
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("create index on t(m1)").get(), ire, non_frozen);
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("create index on t(l1)").get(), ire, non_frozen);

        BOOST_REQUIRE_EXCEPTION(e.execute_cql("create index on t(FULL(s1))").get(), ire, non_frozen);
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("create index on t(FULL(m1))").get(), ire, non_frozen);
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("create index on t(FULL(l1))").get(), ire, non_frozen);

        BOOST_REQUIRE_EXCEPTION(e.execute_cql("create index on t(     s2 )").get(), ire, non_full);
        e.execute_cql("create index on t(FULL(s2))").get();
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("create index on t(FULL(s2))").get(), ire, duplicate);

        BOOST_REQUIRE_EXCEPTION(e.execute_cql("create index on t(     m2 )").get(), ire, non_full);
        e.execute_cql("create index on t(FULL(m2))").get();
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("create index on t(FULL(m2))").get(), ire, duplicate);

        BOOST_REQUIRE_EXCEPTION(e.execute_cql("create index on t(     l2 )").get(), ire, non_full);
        e.execute_cql("create index on t(FULL(l2))").get();
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("create index on t(FULL(l2))").get(), ire, duplicate);

        BOOST_REQUIRE_EXCEPTION(e.execute_cql("select * from t where m2[1] = '1'").get(), ire, entry_eq);

        e.execute_cql("insert into t(p, s2, m2, l2) values (1, {1}, {1: 'one', 2: 'two'}, [2])").get();
        e.execute_cql("insert into t(p, s2, m2, l2) values (2, {2}, {3: 'three'}, [3, 4, 5])").get();
        e.execute_cql("insert into t(p, s2, m2, l2) values (3, {3}, {5: 'five', 7: 'seven'}, [7, 8, 9])").get();

        auto set_type = set_type_impl::get_instance(int32_type, true);
        auto map_type = map_type_impl::get_instance(int32_type, utf8_type, true);
        auto list_type = list_type_impl::get_instance(int32_type, true);

        eventually([&] {
            auto res = e.execute_cql("SELECT p from t where s2 = {2}").get0();
            assert_that(res).is_rows().with_rows({{{int32_type->decompose(2)}}});
            res = e.execute_cql("SELECT p from t where s2 = {}").get0();
            assert_that(res).is_rows().with_size(0);
        });
        eventually([&] {
            auto res = e.execute_cql("SELECT p from t where m2 = {5: 'five', 7: 'seven'}").get0();
            assert_that(res).is_rows().with_rows({{{int32_type->decompose(3)}}});
            res = e.execute_cql("SELECT p from t where m2 = {1: 'one', 2: 'three'}").get0();
            assert_that(res).is_rows().with_size(0);
        });
        eventually([&] {
            auto res = e.execute_cql("SELECT p from t where l2 = [2]").get0();
            assert_that(res).is_rows().with_rows({{{int32_type->decompose(1)}}});
            res = e.execute_cql("SELECT p from t where l2 = [3]").get0();
            assert_that(res).is_rows().with_size(0);
        });
    });
}

// Test for issue #3977 - we do not support SASI, nor any other types of
// custom index implementations, so "create custom index" commands should
// fail, rather than be silently ignored. Also check that various improper
// combination of parameters related to custom indexes are rejected as well.
SEASTAR_TEST_CASE(test_secondary_index_create_custom_index) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p int primary key, a int)").get();
        // Creating an index on column a works, obviously.
        e.execute_cql("create index on cf (a)").get();
        // The following is legal syntax on Cassandra, to create a SASI index.
        // However, we don't support SASI, so this should fail. Not be silently
        // ignored as it was before #3977 was fixed.
        assert_that_failed(e.execute_cql("create custom index on cf (a) using 'org.apache.cassandra.index.sasi.SASIIndex'"));
        // Even if we ever support SASI (and the above check should be
        // changed to expect success), we'll never support a custom index
        // class with the following ridiculous name, so the following should
        // continue to fail.
        assert_that_failed(e.execute_cql("create custom index on cf (a) using 'a.ridiculous.name'"));
        // It's a syntax error to try to create a "custom index" without
        // specifying a class name in "USING". We expect exception:
        // "exceptions::invalid_request_exception: CUSTOM index requires
        // specifying the index class"
        assert_that_failed(e.execute_cql("create custom index on cf (a)"));
        // It's also a syntax error to try to specify a "USING" without
        // specifying CUSTOM. We expect the exception:
        // "exceptions::invalid_request_exception: Cannot specify index class
        // for a non-CUSTOM index"
        assert_that_failed(e.execute_cql("create index on cf (a) using 'org.apache.cassandra.index.sasi.SASIIndex'"));
    });
}

// Reproducer for #4144
SEASTAR_TEST_CASE(test_secondary_index_contains_virtual_columns) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p int, c int, v int, primary key(p, c))").get();
        e.execute_cql("create index on cf (c)").get();
        e.execute_cql("update cf set v = 1 where p = 1 and c = 1").get();
        eventually([&] {
            auto res = e.execute_cql("select * from cf where c = 1").get0();
            assert_that(res).is_rows().with_rows({{{int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}}});
        });
        // Similar test to the above, just indexing a partition-key column
        // instead of a clustering key-column in the test above.
        e.execute_cql("create table cf2 (p1 int, p2 int, c int, v int, primary key((p1, p2), c))").get();
        e.execute_cql("create index on cf2 (p1)").get();
        e.execute_cql("update cf2 set v = 1 where p1 = 1 and p2 = 1 and c = 1").get();
        eventually([&] {
            auto res = e.execute_cql("select * from cf2 where p1 = 1").get0();
            assert_that(res).is_rows().with_rows({{{int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}}});
        });
    });
}

SEASTAR_TEST_CASE(test_local_secondary_index) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table t (p int, c int, v1 int, v2 int, primary key(p, c))").get();
        e.execute_cql("create index local_t_v1 on t ((p),v1)").get();
        BOOST_REQUIRE_THROW(e.execute_cql("create index local_t_p on t(p, v2)").get(), std::exception);
        BOOST_REQUIRE_THROW(e.execute_cql("create index local_t_p on t((v1), v2)").get(), std::exception);

        e.execute_cql("insert into t (p,c,v1,v2) values (1,1,1,1)").get();
        e.execute_cql("insert into t (p,c,v1,v2) values (1,2,3,2)").get();
        e.execute_cql("insert into t (p,c,v1,v2) values (1,3,3,3)").get();
        e.execute_cql("insert into t (p,c,v1,v2) values (1,4,5,6)").get();
        e.execute_cql("insert into t (p,c,v1,v2) values (2,1,3,4)").get();
        e.execute_cql("insert into t (p,c,v1,v2) values (2,1,3,5)").get();

        BOOST_REQUIRE_THROW(e.execute_cql("select * from t where v1 = 1").get(), exceptions::invalid_request_exception);

        auto get_local_index_read_count = [&] {
            return e.db().map_reduce0([] (database& local_db) {
                return local_db.find_column_family("ks", "local_t_v1_index").get_stats().reads.hist.count;
            }, 0, std::plus<int64_t>()).get0();
        };

        int64_t expected_read_count = 0;
        eventually([&] {
            auto res = e.execute_cql("select * from t where p = 1 and v1 = 3").get0();
            assert_that(res).is_rows().with_rows({
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(2)}},
                {{int32_type->decompose(1)}, {int32_type->decompose(3)}, {int32_type->decompose(3)}, {int32_type->decompose(3)}},
            });
            ++expected_read_count;
            BOOST_REQUIRE_EQUAL(get_local_index_read_count(), expected_read_count);
        });

        // Even with local indexes present, filtering should work without issues
        auto res = e.execute_cql("select * from t where v1 = 1 ALLOW FILTERING").get0();
        assert_that(res).is_rows().with_rows({
            {{int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}},
        });
        BOOST_REQUIRE_EQUAL(get_local_index_read_count(), expected_read_count);
    });
}

SEASTAR_TEST_CASE(test_local_and_global_secondary_index) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table t (p int, c int, v1 int, v2 int, primary key(p, c))").get();
        e.execute_cql("create index local_t_v1 on t ((p),v1)").get();
        e.execute_cql("create index global_t_v1 on t(v1)").get();

        e.execute_cql("insert into t (p,c,v1,v2) values (1,1,1,1)").get();
        e.execute_cql("insert into t (p,c,v1,v2) values (1,2,3,2)").get();
        e.execute_cql("insert into t (p,c,v1,v2) values (1,3,3,3)").get();
        e.execute_cql("insert into t (p,c,v1,v2) values (1,4,5,6)").get();
        e.execute_cql("insert into t (p,c,v1,v2) values (2,1,3,4)").get();
        e.execute_cql("insert into t (p,c,v1,v2) values (2,6,3,5)").get();

        auto get_local_index_read_count = [&] {
            return e.db().map_reduce0([] (database& local_db) {
                return local_db.find_column_family("ks", "local_t_v1_index").get_stats().reads.hist.count;
            }, 0, std::plus<int64_t>()).get0();
        };
        auto get_global_index_read_count = [&] {
            return e.db().map_reduce0([] (database& local_db) {
                return local_db.find_column_family("ks", "global_t_v1_index").get_stats().reads.hist.count;
            }, 0, std::plus<int64_t>()).get0();
        };

        int64_t expected_local_index_read_count = 0;
        int64_t expected_global_index_read_count = 0;

        eventually([&] {
            auto res = e.execute_cql("select * from t where p = 1 and v1 = 3").get0();
            assert_that(res).is_rows().with_rows({
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(2)}},
                {{int32_type->decompose(1)}, {int32_type->decompose(3)}, {int32_type->decompose(3)}, {int32_type->decompose(3)}},
            });
            ++expected_local_index_read_count;
            BOOST_REQUIRE_EQUAL(get_local_index_read_count(), expected_local_index_read_count);
            BOOST_REQUIRE_EQUAL(get_global_index_read_count(), expected_global_index_read_count);
        });

        eventually([&] {
            auto res = e.execute_cql("select * from t where v1 = 3").get0();
            ++expected_global_index_read_count;
            BOOST_REQUIRE_EQUAL(get_local_index_read_count(), expected_local_index_read_count);
            BOOST_REQUIRE_EQUAL(get_global_index_read_count(), expected_global_index_read_count);
            assert_that(res).is_rows().with_rows_ignore_order({
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(2)}},
                {{int32_type->decompose(1)}, {int32_type->decompose(3)}, {int32_type->decompose(3)}, {int32_type->decompose(3)}},
                {{int32_type->decompose(2)}, {int32_type->decompose(1)}, {int32_type->decompose(3)}, {int32_type->decompose(4)}},
                {{int32_type->decompose(2)}, {int32_type->decompose(6)}, {int32_type->decompose(3)}, {int32_type->decompose(5)}},
            });
        });
    });
}

SEASTAR_TEST_CASE(test_local_index_paging) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("CREATE TABLE tab (p int, c1 int, c2 int, v int, PRIMARY KEY (p, c1, c2))").get();
        e.execute_cql("CREATE INDEX ON tab ((p),v)").get();
        e.execute_cql("CREATE INDEX ON tab ((p),c2)").get();

        e.execute_cql("INSERT INTO tab (p, c1, c2, v) VALUES (1, 1, 2, 1)").get();
        e.execute_cql("INSERT INTO tab (p, c1, c2, v) VALUES (1, 1, 1, 1)").get();
        e.execute_cql("INSERT INTO tab (p, c1, c2, v) VALUES (1, 2, 2, 4)").get();
        e.execute_cql("INSERT INTO tab (p, c1, c2, v) VALUES (3, 1, 2, 1)").get();

        auto extract_paging_state = [] (::shared_ptr<cql_transport::messages::result_message> res) {
            auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(res);
            auto paging_state = rows->rs().get_metadata().paging_state();
            assert(paging_state);
            return make_lw_shared<service::pager::paging_state>(*paging_state);
        };

        eventually([&] {
            auto qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{1, nullptr, {}, api::new_timestamp()});
            auto res = e.execute_cql("SELECT * FROM tab WHERE p = 1 and v = 1", std::move(qo)).get0();
            auto paging_state = extract_paging_state(res);

            assert_that(res).is_rows().with_rows({{
                {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)},
            }});

            qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{1, paging_state, {}, api::new_timestamp()});
            res = e.execute_cql("SELECT * FROM tab WHERE p = 1 and v = 1", std::move(qo)).get0();

            assert_that(res).is_rows().with_rows({{
                {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(1)},
            }});
        });

        eventually([&] {
            auto qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{1, nullptr, {}, api::new_timestamp()});
            auto res = e.execute_cql("SELECT * FROM tab WHERE p = 1 and c2 = 2", std::move(qo)).get0();
            auto paging_state = extract_paging_state(res);

            assert_that(res).is_rows().with_rows({{
                {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(1)},
            }});

            qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{1, paging_state, {}, api::new_timestamp()});
            res = e.execute_cql("SELECT * FROM tab WHERE p = 1 and c2 = 2", std::move(qo)).get0();

            assert_that(res).is_rows().with_rows({{
                {int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(2)}, {int32_type->decompose(4)},
            }});
        });
    });
}

SEASTAR_TEST_CASE(test_malformed_local_index) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("CREATE TABLE tab (p1 int, p2 int, c1 int, c2 int, v int, PRIMARY KEY ((p1, p2), c1, c2))").get();

        BOOST_REQUIRE_THROW(e.execute_cql("CREATE INDEX ON tab ((p1),v)").get(), exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("CREATE INDEX ON tab ((p2),v)").get(), exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("CREATE INDEX ON tab ((p1,p2,p1),v)").get(), exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("CREATE INDEX ON tab ((p1,c1),v)").get(), exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("CREATE INDEX ON tab ((c1,c2),v)").get(), exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("CREATE INDEX ON tab ((p1,p2),c1,v)").get(), exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("CREATE INDEX ON tab ((p1,p2))").get(), exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("CREATE INDEX ON tab ((p1,p2),p1)").get(), exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("CREATE INDEX ON tab ((p1,p2),p2)").get(), exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("CREATE INDEX ON tab ((p1,p2),(c1,c2))").get(), exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("CREATE INDEX ON tab ((p2,p1),v)").get(), exceptions::invalid_request_exception);
    });
}

SEASTAR_TEST_CASE(test_local_index_multi_pk_columns) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("CREATE TABLE tab (p1 int, p2 int, c1 int, c2 int, v int, PRIMARY KEY ((p1, p2), c1, c2))").get();
        e.execute_cql("CREATE INDEX ON tab ((p1,p2),v)").get();
        e.execute_cql("CREATE INDEX ON tab ((p1,p2),c2)").get();

        e.execute_cql("INSERT INTO tab (p1, p2, c1, c2, v) VALUES (1, 2, 1, 2, 1)").get();
        e.execute_cql("INSERT INTO tab (p1, p2, c1, c2, v) VALUES (1, 2, 1, 1, 1)").get();
        e.execute_cql("INSERT INTO tab (p1, p2, c1, c2, v) VALUES (1, 3, 2, 2, 4)").get();
        e.execute_cql("INSERT INTO tab (p1, p2, c1, c2, v) VALUES (1, 2, 3, 2, 4)").get();
        e.execute_cql("INSERT INTO tab (p1, p2, c1, c2, v) VALUES (1, 2, 3, 7, 4)").get();
        e.execute_cql("INSERT INTO tab (p1, p2, c1, c2, v) VALUES (3, 3, 1, 2, 1)").get();

        eventually([&] {
            auto res = e.execute_cql("select * from tab where p1 = 1 and p2 = 2 and v = 4").get0();
            assert_that(res).is_rows().with_rows({
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(2)}, {int32_type->decompose(4)}},
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(7)}, {int32_type->decompose(4)}},
            });
        });

        eventually([&] {
            auto res = e.execute_cql("select * from tab where p1 = 1 and p2 = 2 and v = 5").get0();
            assert_that(res).is_rows().with_size(0);
        });

        BOOST_REQUIRE_THROW(e.execute_cql("select * from tab where p1 = 1 and v = 3").get(), exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("select * from tab where p2 = 2 and v = 3").get(), exceptions::invalid_request_exception);
    });
}

SEASTAR_TEST_CASE(test_local_index_case_sensitive) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("CREATE TABLE \"FooBar\" (a int PRIMARY KEY, b int, c int)").get();
        e.execute_cql("CREATE INDEX ON \"FooBar\" ((a),b)").get();
        e.execute_cql("INSERT INTO \"FooBar\" (a, b, c) VALUES (1, 2, 3)").get();
        e.execute_cql("SELECT * from \"FooBar\" WHERE a = 1 AND b = 1").get();

        e.execute_cql("CREATE TABLE tab (a int PRIMARY KEY, \"FooBar\" int, c int)").get();
        e.execute_cql("CREATE INDEX ON tab ((a),\"FooBar\")").get();

        e.execute_cql("INSERT INTO tab (a, \"FooBar\", c) VALUES (1, 2, 3)").get();
        e.execute_cql("SELECT * from tab WHERE a = 1 and \"FooBar\" = 2").get();

        e.execute_cql("CREATE TABLE tab2 (\"FooBar\" int PRIMARY KEY, b int, c int)").get();
        e.execute_cql("CREATE INDEX ON tab2 ((\"FooBar\"),b)").get();
        e.execute_cql("INSERT INTO tab2 (\"FooBar\", b, c) VALUES (1, 2, 3)").get();

        e.execute_cql("SELECT * from tab2 WHERE \"FooBar\" = 1 AND b = 2").get();
    });
}

SEASTAR_TEST_CASE(test_local_index_unorthodox_name) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("CREATE TABLE tab (a int PRIMARY KEY, \"Comma\\,,\" int, c int)").get();
        e.execute_cql("CREATE INDEX ON tab ((a),\"Comma\\,,\")").get();
        e.execute_cql("INSERT INTO tab (a, \"Comma\\,,\", c) VALUES (1, 2, 3)").get();
        e.execute_cql("SELECT * from tab WHERE a = 1 and \"Comma\\,,\" = 2").get();

        e.execute_cql("CREATE TABLE tab2 (\"CommaWithParentheses,abc)\" int PRIMARY KEY, b int, c int)").get();
        e.execute_cql("CREATE INDEX ON tab2 ((\"CommaWithParentheses,abc)\"),b)").get();
        e.execute_cql("INSERT INTO tab2 (\"CommaWithParentheses,abc)\", b, c) VALUES (1, 2, 3)").get();
        e.execute_cql("SELECT * from tab2 WHERE \"CommaWithParentheses,abc)\" = 1 AND b = 2").get();

        e.execute_cql("CREATE TABLE tab3 (\"YetAnotherComma\\,ff,a\" int PRIMARY KEY, b int, c int)").get();
        e.execute_cql("CREATE INDEX ON tab3 ((\"YetAnotherComma\\,ff,a\"),b)").get();
        e.execute_cql("INSERT INTO tab3 (\"YetAnotherComma\\,ff,a\", b, c) VALUES (1, 2, 3)").get();
        e.execute_cql("SELECT * from tab3 WHERE \"YetAnotherComma\\,ff,a\" = 1 AND b = 2").get();

        e.execute_cql("CREATE TABLE tab4 (\"escapedcomma\\,inthemiddle\" int PRIMARY KEY, b int, c int)").get();
        e.execute_cql("CREATE INDEX ON tab4 ((\"escapedcomma\\,inthemiddle\"),b)").get();
        e.execute_cql("INSERT INTO tab4 (\"escapedcomma\\,inthemiddle\", b, c) VALUES (1, 2, 3)").get();
        e.execute_cql("SELECT * from tab4 WHERE \"escapedcomma\\,inthemiddle\" = 1 AND b = 2").get();

        e.execute_cql("CREATE TABLE tab5 (a int PRIMARY KEY, \"(b)\" int, c int)").get();
        e.execute_cql("CREATE INDEX ON tab5 (\"(b)\")").get();
        e.execute_cql("INSERT INTO tab5 (a, \"(b)\", c) VALUES (1, 2, 3)").get();
        e.execute_cql("SELECT * from tab5 WHERE \"(b)\" = 1").get();

        e.execute_cql("CREATE TABLE tab6 (\"trailingbacklash\\\" int, b int, c int, d int, primary key ((\"trailingbacklash\\\", b)))").get();
        e.execute_cql("CREATE INDEX ON tab6((\"trailingbacklash\\\", b),c)").get();
        e.execute_cql("INSERT INTO tab6 (\"trailingbacklash\\\", b, c, d) VALUES (1, 2, 3, 4)").get();
        e.execute_cql("SELECT * FROM tab6 WHERE c = 3 and \"trailingbacklash\\\" = 1 and b = 2").get();
    });
}

SEASTAR_TEST_CASE(test_local_index_operations) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("CREATE TABLE t (p1 int, p2 int, c int, v1 int, v2 int, PRIMARY KEY ((p1,p2),c))").get();
        // Both global and local indexes can be created
        e.execute_cql("CREATE INDEX ON t (v1)").get();
        e.execute_cql("CREATE INDEX ON t ((p1,p2),v1)").get();

        // Duplicate index cannot be created, even if it's named
        BOOST_REQUIRE_THROW(e.execute_cql("CREATE INDEX ON t ((p1,p2),v1)").get(), exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("CREATE INDEX named_idx ON t ((p1,p2),v1)").get(), exceptions::invalid_request_exception);
        e.execute_cql("CREATE INDEX IF NOT EXISTS named_idx ON t ((p1,p2),v1)").get();

        // Even with global index dropped, duplicated local index cannot be created
        e.execute_cql("DROP INDEX t_v1_idx").get();
        BOOST_REQUIRE_THROW(e.execute_cql("CREATE INDEX named_idx ON t ((p1,p2),v1)").get(), exceptions::invalid_request_exception);

        e.execute_cql("DROP INDEX t_v1_idx_1").get();
        e.execute_cql("CREATE INDEX named_idx ON t ((p1,p2),v1)").get();
        e.execute_cql("DROP INDEX named_idx").get();

        BOOST_REQUIRE_THROW(e.execute_cql("DROP INDEX named_idx").get(), exceptions::invalid_request_exception);
        e.execute_cql("DROP INDEX IF EXISTS named_idx").get();

        // Even if a default name is taken, it's possible to create a local index
        e.execute_cql("CREATE INDEX t_v1_idx ON t(v2)").get();
        e.execute_cql("CREATE INDEX ON t(v1)").get();
    });
}

SEASTAR_TEST_CASE(test_local_index_prefix_optimization) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("CREATE TABLE t (p1 int, p2 int, c1 int, c2 int, v int, PRIMARY KEY ((p1,p2),c1,c2))").get();
        // Both global and local indexes can be created
        e.execute_cql("CREATE INDEX ON t ((p1,p2),v)").get();

        e.execute_cql("INSERT INTO t (p1,p2,c1,c2,v) VALUES (1,2,3,4,5);").get();
        e.execute_cql("INSERT INTO t (p1,p2,c1,c2,v) VALUES (2,3,4,5,6);").get();
        e.execute_cql("INSERT INTO t (p1,p2,c1,c2,v) VALUES (3,4,5,6,7);").get();

        eventually([&] {
            auto res = e.execute_cql("select * from t where p1 = 1 and p2 = 2 and c1 = 3 and v = 5").get0();
            assert_that(res).is_rows().with_rows({
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(4)}, {int32_type->decompose(5)}},
            });
        });
        eventually([&] {
            auto res = e.execute_cql("select * from t where p1 = 1 and p2 = 2 and c1 = 3 and c2 = 4 and v = 5").get0();
            assert_that(res).is_rows().with_rows({
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(4)}, {int32_type->decompose(5)}},
            });
        });
        BOOST_REQUIRE_THROW(e.execute_cql("select * from t where p1 = 1 and p2 = 2 and c2 = 4 and v = 5").get(), exceptions::invalid_request_exception);
        eventually([&] {
            auto res = e.execute_cql("select * from t where p1 = 2 and p2 = 3 and c2 = 5 and v = 6 ALLOW FILTERING").get0();
            assert_that(res).is_rows().with_rows({
                {{int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(4)}, {int32_type->decompose(5)}, {int32_type->decompose(6)}},
            });
        });
    });
}

// A secondary index allows a query involving both the indexed column and
// the primary key. The relation on the primary key cannot be an IN query
// or we get the exception "Select on indexed columns and with IN clause for
// the PRIMARY KEY are not supported". We inherited this limitation from
// Cassandra, where I guess the thinking was that such query can just split
// into several separate queries. But if the IN clause only lists a single
// value, this is nothing more than an equality and can be supported anyway.
// This test reproduces issue #4455.
SEASTAR_TEST_CASE(test_secondary_index_single_value_in) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p int primary key, a int)").get();
        e.execute_cql("create index on cf (a)").get();
        e.execute_cql("insert into cf (p, a) VALUES (1, 2)").get();
        e.execute_cql("insert into cf (p, a) VALUES (3, 4)").get();
        // An ordinary "p=3 and a=4" query should work
        BOOST_TEST_PASSPOINT();
        eventually([&] {
            auto res = e.execute_cql("select * from cf where p = 3 and a = 4").get0();
            assert_that(res).is_rows().with_rows({
                {{int32_type->decompose(3)}, {int32_type->decompose(4)}}});
        });
        // Querying "p IN (3) and a=4" can do the same, even if a general
        // IN with multiple values isn't yet supported. Before fixing
        // #4455, this wasn't supported.
        BOOST_TEST_PASSPOINT();
        auto res = e.execute_cql("select * from cf where p IN (3) and a = 4").get0();
        assert_that(res).is_rows().with_rows({
            {{int32_type->decompose(3)}, {int32_type->decompose(4)}}});

        // Beyond the specific issue of #4455 involving a partition key,
        // in general, any IN with a single value should be equivalent to
        // a "=", so should be accepted in additional contexts where a
        // multi-value IN is not currently supported. For example in
        // queries over the indexed column: Since "a=4" works, so
        // should "a IN (4)":
        BOOST_TEST_PASSPOINT();
        res = e.execute_cql("select * from cf where a = 4").get0();
        assert_that(res).is_rows().with_rows({
            {{int32_type->decompose(3)}, {int32_type->decompose(4)}}});
        BOOST_TEST_PASSPOINT();
        res = e.execute_cql("select * from cf where a IN (4)").get0();
        assert_that(res).is_rows().with_rows({
            {{int32_type->decompose(3)}, {int32_type->decompose(4)}}});

        // The following test is not strictly related to secondary indexes,
        // but since above we tested single-column restrictions, let's also
        // exercise multi-column restrictions. In other words, that a multi-
        // column EQ can be written as a single-value IN.
        e.execute_cql("create table cf2 (p int, c1 int, c2 int, primary key (p, c1, c2))").get();
        e.execute_cql("insert into cf2 (p, c1, c2) VALUES (1, 2, 3)").get();
        e.execute_cql("insert into cf2 (p, c1, c2) VALUES (4, 5, 6)").get();
        res = e.execute_cql("select * from cf2 where p = 1 and (c1, c2) = (2, 3)").get0();
        assert_that(res).is_rows().with_rows({
            {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}}});
        res = e.execute_cql("select * from cf2 where p = 1 and (c1, c2) IN ((2, 3))").get0();
        assert_that(res).is_rows().with_rows({
            {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}}});

    });
}

// Test that even though a table has a secondary index it is allowed to drop
// unindexed columns.
// However, if the index is on one of the primary key columns, we can't allow
// dropping a drop any column from the base table. The problem is that such
// column's value be responsible for keeping a base row alive, and therefore
// (when the index is on a primary key column) also the view row.
// Reproduces issue #4448.
SEASTAR_TEST_CASE(test_secondary_index_allow_some_column_drops) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        // Test that if the index is on a non-pk column, we can drop any other
        // non-pk column from the base table. Check that the drop is allowed and
        // the index still works afterwards.
        e.execute_cql("create table cf (p int primary key, a int, b int)").get();
        e.execute_cql("create index on cf (a)").get();
        e.execute_cql("insert into cf (p, a, b) VALUES (1, 2, 3)").get();
        BOOST_TEST_PASSPOINT();
        auto res = e.execute_cql("select * from cf").get0();
        assert_that(res).is_rows().with_rows({
            {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}}});
        e.execute_cql("alter table cf drop b").get();
        BOOST_TEST_PASSPOINT();
        res = e.execute_cql("select * from cf").get0();
        assert_that(res).is_rows().with_rows({
            {{int32_type->decompose(1)}, {int32_type->decompose(2)}}});
        eventually([&] {
            auto res = e.execute_cql("select * from cf where a = 2").get0();
            assert_that(res).is_rows().with_rows({
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}}});
        });
        // Test that we cannot drop the indexed column, because the index
        // (or rather, its backing materialized-view) needs it:
        // Expected exception: "exceptions::invalid_request_exception:
        // Cannot drop column a from base table ks.cf with a materialized
        // view cf_a_idx_index that needs this column".
        BOOST_REQUIRE_THROW(e.execute_cql("alter table cf drop a").get(), exceptions::invalid_request_exception);
        // Also cannot drop a primary key column, of course. Exception is:
        // "exceptions::invalid_request_exception: Cannot drop PRIMARY KEY part p"
        BOOST_REQUIRE_THROW(e.execute_cql("alter table cf drop p").get(), exceptions::invalid_request_exception);
        // Also cannot drop a non existent column :-) Exception is:
        // "exceptions::invalid_request_exception: Column xyz was not found in table cf"
        BOOST_REQUIRE_THROW(e.execute_cql("alter table cf drop xyz").get(), exceptions::invalid_request_exception);

        // If the index is on a pk column, we don't allow dropping columns...
        // In such case because the rows of the index are identical to those
        // of the base, the unselected columns become "virtual columns"
        // in the view, and we don't support deleting them.
        e.execute_cql("create table cf2 (p int, c int, a int, b int, primary key (p, c))").get();
        e.execute_cql("create index on cf2 (c)").get();
        e.execute_cql("insert into cf2 (p, c, a, b) VALUES (1, 2, 3, 4)").get();
        BOOST_TEST_PASSPOINT();
        res = e.execute_cql("select * from cf2").get0();
        assert_that(res).is_rows().with_rows({
            {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(4)}}});
        BOOST_REQUIRE_THROW(e.execute_cql("alter table cf2 drop b").get(), exceptions::invalid_request_exception);

        // Verify that even if just one of many indexes needs a column, it
        // still cannot be deleted.
        e.execute_cql("create table cf3 (p int, c int, a int, b int, d int, primary key (p, c))").get();
        e.execute_cql("create index on cf3 (b)").get();
        e.execute_cql("create index on cf3 (d)").get();
        e.execute_cql("create index on cf3 (a)").get();
        BOOST_REQUIRE_THROW(e.execute_cql("alter table cf2 drop d").get(), exceptions::invalid_request_exception);
    });
}

// Reproduces issue #4539 - a partition key index should not influence a filtering decision for regular columns.
// Previously, given sequence resulted in a "No index found" error.
SEASTAR_TEST_CASE(test_secondary_index_on_partition_key_with_filtering) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE test_a(a int, b int, c int, PRIMARY KEY ((a, b)));").get();
        e.execute_cql("CREATE INDEX ON test_a(a);").get();
        e.execute_cql("INSERT INTO test_a (a, b, c) VALUES (1, 2, 3);").get();
        eventually([&] {
            auto res = e.execute_cql("SELECT * FROM test_a WHERE a = 1 AND b = 2 AND c = 3 ALLOW FILTERING;").get0();
            assert_that(res).is_rows().with_rows({
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}}});
        });
    });
}

SEASTAR_TEST_CASE(test_indexing_paging_and_aggregation) {
    static constexpr int row_count = 2 * cql3::statements::select_statement::DEFAULT_COUNT_PAGE_SIZE + 120;

    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "CREATE TABLE fpa (id int primary key, v int)");
        cquery_nofail(e, "CREATE INDEX ON fpa(v)");
        for (int i = 0; i < row_count; ++i) {
            cquery_nofail(e, format("INSERT INTO fpa (id, v) VALUES ({}, {})", i + 1, i % 2).c_str());
        }

      eventually([&] {
        auto qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                cql3::query_options::specific_options{2, nullptr, {}, api::new_timestamp()});
        auto msg = cquery_nofail(e, "SELECT sum(id) FROM fpa WHERE v = 0;", std::move(qo));
        // Even though we set up paging, we still expect a single result from an aggregation function.
        // Also, instead of the user-provided page size, internal DEFAULT_COUNT_PAGE_SIZE is expected to be used.
        assert_that(msg).is_rows().with_rows({
            { int32_type->decompose(row_count * row_count / 4)},
        });

        // Even if paging is not explicitly used, the query will be internally paged to avoid OOM.
        msg = cquery_nofail(e, "SELECT sum(id) FROM fpa WHERE v = 1;");
        assert_that(msg).is_rows().with_rows({
            { int32_type->decompose(row_count * row_count / 4 + row_count / 2)},
        });

        qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                cql3::query_options::specific_options{3, nullptr, {}, api::new_timestamp()});
        msg = cquery_nofail(e, "SELECT avg(id) FROM fpa WHERE v = 1;", std::move(qo));
        assert_that(msg).is_rows().with_rows({
            { int32_type->decompose(row_count / 2 + 1)},
        });
      });

        // Similar, but this time a non-prefix clustering key part is indexed (wrt. issue 3405, after which we have
        // a special code path for indexing composite non-prefix clustering keys).
        cquery_nofail(e, "CREATE TABLE fpa2 (id int, c1 int, c2 int, primary key (id, c1, c2))");
        cquery_nofail(e, "CREATE INDEX ON fpa2(c2)");

      eventually([&] {
        for (int i = 0; i < row_count; ++i) {
            cquery_nofail(e, format("INSERT INTO fpa2 (id, c1, c2) VALUES ({}, {}, {})", i + 1, i + 1, i % 2).c_str());
        }

        auto qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                cql3::query_options::specific_options{2, nullptr, {}, api::new_timestamp()});
        auto msg = cquery_nofail(e, "SELECT sum(id) FROM fpa2 WHERE c2 = 0;", std::move(qo));
        // Even though we set up paging, we still expect a single result from an aggregation function
        assert_that(msg).is_rows().with_rows({
            { int32_type->decompose(row_count * row_count / 4)},
        });

        qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                cql3::query_options::specific_options{3, nullptr, {}, api::new_timestamp()});
        msg = cquery_nofail(e, "SELECT avg(id) FROM fpa2 WHERE c2 = 1;", std::move(qo));
        assert_that(msg).is_rows().with_rows({
            { int32_type->decompose(row_count / 2 + 1)},
        });
      });
    });
}

// Verifies that both "SELECT * [rest_of_query]" and "SELECT count(*) [rest_of_query]" 
// return expected count of rows.
void assert_select_count_and_select_rows_has_size(
        cql_test_env& e, 
        const sstring& rest_of_query, int64_t expected_count, 
        const std::experimental::source_location& loc = std::experimental::source_location::current()) {
    eventually([&] { 
        require_rows(e, "SELECT count(*) " + rest_of_query, {
            { long_type->decompose(expected_count) }
        }, loc);
        auto res = cquery_nofail(e, "SELECT * " + rest_of_query, nullptr, loc);
        try {
            assert_that(res).is_rows().with_size(expected_count);
        } catch (const std::exception& e) {
            BOOST_FAIL(format("is_rows/with_size failed: {}\n{}:{}: originally from here",
                              e.what(), loc.file_name(), loc.line()));
        }
    });
}

static constexpr int page_scenarios_page_size = 20;
static constexpr int page_scenarios_row_count = 2 * page_scenarios_page_size + 5;
static constexpr int page_scenarios_initial_count = 3;
static constexpr int page_scenarios_window_size = 4;
static constexpr int page_scenarios_just_before_first_page = page_scenarios_page_size - page_scenarios_window_size;
static constexpr int page_scenarios_just_after_first_page = page_scenarios_page_size + page_scenarios_window_size;    
static constexpr int page_scenarios_just_before_second_page = 2 * page_scenarios_page_size - page_scenarios_window_size;
static constexpr int page_scenarios_just_after_second_page = 2 * page_scenarios_page_size + page_scenarios_window_size;    

static_assert(page_scenarios_initial_count < page_scenarios_row_count);
static_assert(page_scenarios_window_size < page_scenarios_page_size);
static_assert(page_scenarios_just_after_second_page < page_scenarios_row_count);

// Executes `insert` lambda page_scenarios_row_count times. 
// Runs `validate` lambda in a few scenarios:
//
// 1. After a small number of `insert`s
// 2. In a window from just before and just after `insert`s were executed
//    DEFAULT_COUNT_PAGE_SIZE times
// 3. In a window from just before and just after `insert`s were executed
//    2 * DEFAULT_COUNT_PAGE_SIZE times
// 4. After all `insert`s
void test_with_different_page_scenarios(
    noncopyable_function<void (int)> insert, noncopyable_function<void (int)> validate) {

    int current_row = 0;
    for (; current_row < page_scenarios_initial_count; current_row++) {
        insert(current_row);
        validate(current_row + 1);
    }

    for (; current_row < page_scenarios_just_before_first_page; current_row++) {
        insert(current_row);
    }

    for (; current_row < page_scenarios_just_after_first_page; current_row++) {
        insert(current_row);
        validate(current_row + 1);
    }

    for (; current_row < page_scenarios_just_before_second_page; current_row++) {
        insert(current_row);
    }

    for (; current_row < page_scenarios_just_after_second_page; current_row++) {
        insert(current_row);
        validate(current_row + 1);
    }   

    for (; current_row < page_scenarios_row_count; current_row++) {
        insert(current_row);
    }

    // No +1, because we just left for loop and current_row was incremented.
    validate(current_row);
}

SEASTAR_TEST_CASE(test_secondary_index_on_ck_first_column_and_aggregation) {
    // Tests aggregation on table with secondary index on first column
    // of clustering key. This is the "partition_slices" case of 
    // indexed_table_select_statement::do_execute.

    return do_with_cql_env_thread([] (cql_test_env& e) {
        cql3::statements::set_internal_paging_size_guard g(page_scenarios_page_size);

        // Explicitly reproduce the first failing example in issue #7355.
        cquery_nofail(e, "CREATE TABLE t1 (pk1 int, pk2 int, ck int, primary key((pk1, pk2), ck))");
        cquery_nofail(e, "CREATE INDEX ON t1(ck)");

        cquery_nofail(e, "INSERT INTO t1(pk1, pk2, ck) VALUES (1, 2, 3)");
        assert_select_count_and_select_rows_has_size(e, "FROM t1 WHERE ck = 3", 1);

        cquery_nofail(e, "INSERT INTO t1(pk1, pk2, ck) VALUES (1, 2, 4)");
        cquery_nofail(e, "INSERT INTO t1(pk1, pk2, ck) VALUES (1, 2, 5)");
        assert_select_count_and_select_rows_has_size(e, "FROM t1 WHERE ck = 3", 1);

        cquery_nofail(e, "INSERT INTO t1(pk1, pk2, ck) VALUES (2, 2, 3)");
        assert_select_count_and_select_rows_has_size(e, "FROM t1 WHERE ck = 3", 2);

        cquery_nofail(e, "INSERT INTO t1(pk1, pk2, ck) VALUES (2, 1, 3)");
        assert_select_count_and_select_rows_has_size(e, "FROM t1 WHERE ck = 3", 3);

        // Test a case when there are a lot of small partitions (more than a page size).
        cquery_nofail(e, "CREATE TABLE t2 (pk int, ck int, primary key(pk, ck))");
        cquery_nofail(e, "CREATE INDEX ON t2(ck)");

        // "Decoy" rows - they should be not counted (previously they were incorrectly counted in,
        // see issue #7355).
        cquery_nofail(e, "INSERT INTO t2(pk, ck) VALUES (0, -2)");
        cquery_nofail(e, "INSERT INTO t2(pk, ck) VALUES (0, 3)");
        cquery_nofail(e, format("INSERT INTO t2(pk, ck) VALUES ({}, 3)", page_scenarios_just_after_first_page).c_str());

        test_with_different_page_scenarios([&](int current_row) {
            cquery_nofail(e, format("INSERT INTO t2(pk, ck) VALUES ({}, 1)", current_row).c_str());
        }, [&](int rows_inserted) {
            assert_select_count_and_select_rows_has_size(e, "FROM t2 WHERE ck = 1", rows_inserted);
          eventually([&] { 
            auto res = cquery_nofail(e, "SELECT pk FROM t2 WHERE ck = 1 GROUP BY pk");
            assert_that(res).is_rows().with_size(rows_inserted);
            res = cquery_nofail(e, "SELECT pk, ck FROM t2 WHERE ck = 1 GROUP BY pk, ck");
            assert_that(res).is_rows().with_size(rows_inserted);
            require_rows(e, "SELECT sum(pk) FROM t2 WHERE ck = 1", {
               { int32_type->decompose(int32_t(rows_inserted * (rows_inserted - 1) / 2)) }
            });
          });
        });

        // Test a case when there is a single large partition (larger than a page size).
        cquery_nofail(e, "CREATE TABLE t3 (pk int, ck1 int, ck2 int, primary key(pk, ck1, ck2))");
        cquery_nofail(e, "CREATE INDEX ON t3(ck1)");

        // "Decoy" rows
        cquery_nofail(e, "INSERT INTO t3(pk, ck1, ck2) VALUES (1, 0, 0)");
        cquery_nofail(e, "INSERT INTO t3(pk, ck1, ck2) VALUES (1, 2, 0)");

        test_with_different_page_scenarios([&](int current_row) {
            cquery_nofail(e, format("INSERT INTO t3(pk, ck1, ck2) VALUES (1, 1, {})", current_row).c_str());
        }, [&](int rows_inserted) {
            assert_select_count_and_select_rows_has_size(e, "FROM t3 WHERE ck1 = 1", rows_inserted);
          eventually([&] { 
            auto res = cquery_nofail(e, "SELECT pk FROM t3 WHERE ck1 = 1 GROUP BY pk");
            assert_that(res).is_rows().with_size(1);
            res = cquery_nofail(e, "SELECT pk, ck1 FROM t3 WHERE ck1 = 1 GROUP BY pk, ck1");
            assert_that(res).is_rows().with_size(1);
            res = cquery_nofail(e, "SELECT pk, ck1, ck2 FROM t3 WHERE ck1 = 1 GROUP BY pk, ck1, ck2");
            assert_that(res).is_rows().with_size(rows_inserted);
            require_rows(e, "SELECT avg(ck2) FROM t3 WHERE ck1 = 1", {
                { int32_type->decompose(int32_t((rows_inserted * (rows_inserted - 1) / 2) / rows_inserted)) }
            }); 
          });
        });
    });
}

SEASTAR_TEST_CASE(test_secondary_index_on_pk_column_and_aggregation) {
    // Tests aggregation on table with secondary index on a column
    // of partition key. This is the "whole_partitions" case of 
    // indexed_table_select_statement::do_execute.

    return do_with_cql_env_thread([] (cql_test_env& e) {
        cql3::statements::set_internal_paging_size_guard g(page_scenarios_page_size);

        // Explicitly reproduce the second failing example in issue #7355.
        // This a case with a single large partition.
        cquery_nofail(e, "CREATE TABLE t1 (pk1 int, pk2 int, ck int, primary key((pk1, pk2), ck))");
        cquery_nofail(e, "CREATE INDEX ON t1(pk2)");

        test_with_different_page_scenarios([&](int current_row) {
            cquery_nofail(e, format("INSERT INTO t1(pk1, pk2, ck) VALUES (1, 1, {})", current_row).c_str());
        }, [&](int rows_inserted) {
            assert_select_count_and_select_rows_has_size(e, "FROM t1 WHERE pk2 = 1", rows_inserted);
          eventually([&] { 
            auto res = cquery_nofail(e, "SELECT pk1, pk2 FROM t1 WHERE pk2 = 1 GROUP BY pk1, pk2");
            assert_that(res).is_rows().with_size(1);
            res = cquery_nofail(e, "SELECT pk1, pk2, ck FROM t1 WHERE pk2 = 1 GROUP BY pk1, pk2, ck");
            assert_that(res).is_rows().with_size(rows_inserted);
            require_rows(e, "SELECT min(pk1) FROM t1 WHERE pk2 = 1", {
                { int32_type->decompose(1) }
            });
          });
        });

        // Test a case when there are a lot of small partitions (more than a page size)
        // and there is a clustering key in base table.
        cquery_nofail(e, "CREATE TABLE t2 (pk1 int, pk2 int, ck int, primary key((pk1, pk2), ck))");
        cquery_nofail(e, "CREATE INDEX ON t2(pk2)");

        test_with_different_page_scenarios([&](int current_row) {
            cquery_nofail(e, format("INSERT INTO t2(pk1, pk2, ck) VALUES ({}, 1, {})", 
                current_row, current_row % 20).c_str());
        }, [&](int rows_inserted) {
            assert_select_count_and_select_rows_has_size(e, "FROM t2 WHERE pk2 = 1", rows_inserted);
          eventually([&] { 
            auto res = cquery_nofail(e, "SELECT pk1, pk2 FROM t2 WHERE pk2 = 1 GROUP BY pk1, pk2");
            assert_that(res).is_rows().with_size(rows_inserted);
            require_rows(e, "SELECT max(pk1) FROM t2 WHERE pk2 = 1", {
                { int32_type->decompose(int32_t(rows_inserted - 1)) }
            });
          });
        });

        // Test a case when there are a lot of small partitions (more than a page size)
        // and there is NO clustering key in base table.
        cquery_nofail(e, "CREATE TABLE t3 (pk1 int, pk2 int, primary key((pk1, pk2)))");
        cquery_nofail(e, "CREATE INDEX ON t3(pk2)");

        test_with_different_page_scenarios([&](int current_row) {
            cquery_nofail(e, format("INSERT INTO t3(pk1, pk2) VALUES ({}, 1)", current_row).c_str());
        }, [&](int rows_inserted) {
            assert_select_count_and_select_rows_has_size(e, "FROM t3 WHERE pk2 = 1", rows_inserted);
        });
    });
}

SEASTAR_TEST_CASE(test_secondary_index_on_non_pk_ck_column_and_aggregation) {
    // Tests aggregation on table with secondary index on a column
    // that is not a part of partition key and clustering key. 
    // This is the non-"whole_partitions" and non-"partition_slices"
    // case of indexed_table_select_statement::do_execute.

    return do_with_cql_env_thread([] (cql_test_env& e) {
        cql3::statements::set_internal_paging_size_guard g(page_scenarios_page_size);

        // Test a case when there are a lot of small partitions (more than a page size)
        // and there is a clustering key in base table.
        cquery_nofail(e, "CREATE TABLE t (pk int, ck int, v int, primary key(pk, ck))");
        cquery_nofail(e, "CREATE INDEX ON t(v)");

        test_with_different_page_scenarios([&](int current_row) {
            cquery_nofail(e, format("INSERT INTO t(pk, ck, v) VALUES ({}, {}, 1)", 
                current_row, current_row % 20).c_str());
        }, [&](int rows_inserted) {
            assert_select_count_and_select_rows_has_size(e, "FROM t WHERE v = 1", rows_inserted);
          eventually([&] { 
            auto res = cquery_nofail(e, "SELECT pk FROM t WHERE v = 1 GROUP BY pk");
            assert_that(res).is_rows().with_size(rows_inserted);
            require_rows(e, "SELECT sum(v) FROM t WHERE v = 1", {
                { int32_type->decompose(int32_t(rows_inserted)) }
            });
          });
        });

        // Test a case when there are a lot of small partitions (more than a page size)
        // and there is NO clustering key in base table.
        cquery_nofail(e, "CREATE TABLE t2 (pk int, v int, primary key(pk))");
        cquery_nofail(e, "CREATE INDEX ON t2(v)");

        test_with_different_page_scenarios([&](int current_row) {
            cquery_nofail(e, format("INSERT INTO t2(pk, v) VALUES ({}, 1)", current_row).c_str());
        }, [&](int rows_inserted) {
            assert_select_count_and_select_rows_has_size(e, "FROM t2 WHERE v = 1", rows_inserted);
          eventually([&] { 
            auto res = cquery_nofail(e, "SELECT pk FROM t2 WHERE v = 1 GROUP BY pk");
            assert_that(res).is_rows().with_size(rows_inserted);
            require_rows(e, "SELECT sum(pk) FROM t2 WHERE v = 1", {
                { int32_type->decompose(int32_t(rows_inserted * (rows_inserted - 1) / 2)) }
            });
          });
        });

        // Test a case when there is a single large partition (larger than a page size).
        cquery_nofail(e, "CREATE TABLE t3 (pk int, ck int, v int, primary key(pk, ck))");
        cquery_nofail(e, "CREATE INDEX ON t3(v)");

        test_with_different_page_scenarios([&](int current_row) {
            cquery_nofail(e, format("INSERT INTO t3(pk, ck, v) VALUES (1, {}, 1)", current_row).c_str());
        }, [&](int rows_inserted) {
            assert_select_count_and_select_rows_has_size(e, "FROM t3 WHERE v = 1", rows_inserted);
          eventually([&] { 
            auto res = cquery_nofail(e, "SELECT pk FROM t3 WHERE v = 1 GROUP BY pk");
            assert_that(res).is_rows().with_size(1);
            res = cquery_nofail(e, "SELECT pk, ck FROM t3 WHERE v = 1 GROUP BY pk, ck");
            assert_that(res).is_rows().with_size(rows_inserted);
            require_rows(e, "SELECT max(ck) FROM t3 WHERE v = 1", {
                { int32_type->decompose(int32_t(rows_inserted - 1)) }
            });
          });
        });
    });
}

SEASTAR_TEST_CASE(test_computed_columns) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("CREATE TABLE t (p1 int, p2 int, c1 int, c2 int, v int, PRIMARY KEY ((p1,p2),c1,c2))").get();
        e.execute_cql("CREATE INDEX local1 ON t ((p1,p2),v)").get();
        e.execute_cql("CREATE INDEX global1 ON t (v)").get();
        e.execute_cql("CREATE INDEX global2 ON t (c2)").get();
        e.execute_cql("CREATE INDEX local2 ON t ((p1,p2),c2)").get();

        auto local1 = e.local_db().find_schema("ks", "local1_index");
        auto local2 = e.local_db().find_schema("ks", "local2_index");
        auto global1 = e.local_db().find_schema("ks", "global1_index");
        auto global2 = e.local_db().find_schema("ks", "global2_index");

        bytes token_column_name("idx_token");
        data_value token_computation(token_column_computation().serialize());
        BOOST_REQUIRE_EQUAL(local1->get_column_definition(token_column_name), nullptr);
        BOOST_REQUIRE_EQUAL(local2->get_column_definition(token_column_name), nullptr);
        BOOST_REQUIRE(global1->get_column_definition(token_column_name)->is_computed());
        BOOST_REQUIRE(global2->get_column_definition(token_column_name)->is_computed());

        auto msg = e.execute_cql("SELECT computation FROM system_schema.computed_columns WHERE keyspace_name='ks'").get0();
        assert_that(msg).is_rows().with_rows({
            {{bytes_type->decompose(token_computation)}},
            {{bytes_type->decompose(token_computation)}}
        });
    });
}

// Ref: #3423 - rows should be returned in token order,
// using signed comparison (ref: #7443)
SEASTAR_TEST_CASE(test_token_order) {
    return do_with_cql_env_thread([] (auto& e) {
        cquery_nofail(e, "CREATE TABLE t (pk int, v int, PRIMARY KEY(pk))");
        cquery_nofail(e, "CREATE INDEX ON t(v)");

        for (int i = 0; i < 7; i++) {
            cquery_nofail(e, format("INSERT INTO t (pk, v) VALUES ({}, 1)", i).c_str());
        }

        std::vector<std::vector<bytes_opt>> expected_rows = {
            { int32_type->decompose(5) }, // token(pk) = -7509452495886106294
            { int32_type->decompose(1) }, // token(pk) = -4069959284402364209
            { int32_type->decompose(0) }, // token(pk) = -3485513579396041028
            { int32_type->decompose(2) }, // token(pk) = -3248873570005575792
            { int32_type->decompose(4) }, // token(pk) = -2729420104000364805
            { int32_type->decompose(6) }, // token(pk) = +2705480034054113608
            { int32_type->decompose(3) }, // token(pk) = +9010454139840013625
        };

        eventually([&] {
            auto nonindex_order = cquery_nofail(e, "SELECT pk FROM t");
            auto index_order = cquery_nofail(e, "SELECT pk FROM t WHERE v = 1");

            assert_that(nonindex_order).is_rows().with_rows(expected_rows);
            assert_that(index_order).is_rows().with_rows(expected_rows);
        });
    });
}

// Ref: #5708 - filtering should be applied on an indexed column
// if the restriction is not eligible for indexing (it's not EQ)
SEASTAR_TEST_CASE(test_filtering_indexed_column) {
    return do_with_cql_env_thread([] (auto& e) {
        cquery_nofail(e, "CREATE TABLE test_index (a INT, b INT, c INT, d INT, e INT, PRIMARY KEY ((a, b),c));");
        cquery_nofail(e, "CREATE INDEX ON test_index(d);");
        cquery_nofail(e, "INSERT INTO test_index (a, b, c, d, e) VALUES (1, 2, 3, 4, 5);");
        cquery_nofail(e, "INSERT INTO test_index (a, b, c, d, e) VALUES (11, 22, 33, 44, 55);");
        cquery_nofail(e, "select c,e from ks.test_index where d = 44 ALLOW FILTERING;");
        cquery_nofail(e, "select a from ks.test_index where d > 43 ALLOW FILTERING;");

        eventually([&] {
            auto msg = cquery_nofail(e, "select c,e from ks.test_index where d = 44 ALLOW FILTERING;");
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(33), int32_type->decompose(55)}
            });
        });
        eventually([&] {
            auto msg = cquery_nofail(e, "select a from ks.test_index where d > 43 ALLOW FILTERING;");
            // NOTE: Column d will also be fetched, because it needs to be filtered.
            // It's not the case in the previous select, where d was only used as an index.
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(11), int32_type->decompose(44)}
            });
        });

        cquery_nofail(e, "CREATE INDEX ON test_index(b);");
        cquery_nofail(e, "CREATE INDEX ON test_index(c);");
        eventually([&] {
            auto msg = cquery_nofail(e, "select e from ks.test_index where b > 12 ALLOW FILTERING;");
            assert_that(msg).is_rows().with_rows({
                {int32_type->decompose(55), int32_type->decompose(22)}
            });
        });
        eventually([&] {
            auto msg = cquery_nofail(e, "select d from ks.test_index where c > 25;");
            assert_that(msg).is_rows().with_rows({{int32_type->decompose(44)}});
        });
    });
}

namespace {

auto I(int32_t x) { return int32_type->decompose(x); }

} // anonymous namespace

SEASTAR_TEST_CASE(indexed_multicolumn_where) {
    return do_with_cql_env_thread([] (auto& e) {
        cquery_nofail(e, "create table t (p int, c1 int, c2 int, primary key(p, c1, c2))");
        cquery_nofail(e, "create index i1 on t(c1)");
        cquery_nofail(e, "insert into t(p, c1, c2) values (1, 11, 21)");
        cquery_nofail(e, "insert into t(p, c1, c2) values (2, 12, 22)");
        eventually_require_rows(e, "select c1 from t where (c1,c2)=(11,21)", {{I(11)}});
        eventually_require_rows(e, "select c1 from t where (c1,c2)=(11,22)", {});
        eventually_require_rows(e, "select c1 from t where (c1)=(12)", {{I(12)}});
        cquery_nofail(e, "create index i2 on t(c2)");
        eventually_require_rows(e, "select c1 from t where (c1,c2)=(11,21)", {{I(11)}});
        eventually_require_rows(e, "select c1 from t where (c1,c2)=(11,22)", {});
        eventually_require_rows(e, "select c1 from t where (c1)=(12)", {{I(12)}});
        cquery_nofail(e, "drop index i1");
        eventually_require_rows(e, "select c1 from t where (c1,c2)=(11,21)", {{I(11)}});
        eventually_require_rows(e, "select c1 from t where (c1,c2)=(11,22)", {});
        eventually_require_rows(e, "select c1 from t where (c1)=(12)", {{I(12)}});
    });
}
