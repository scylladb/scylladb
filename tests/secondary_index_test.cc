/*
 * Copyright (C) 2018 ScyllaDB
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

#include "tests/test-utils.hh"
#include "tests/cql_test_env.hh"
#include "tests/cql_assertions.hh"


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
        assert_that_failed(e.execute_cql(sprint("drop materialized view %s_index", index_name)));
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
        assert_that_failed(seastar::futurize_apply([&e] { return e.execute_cql("drop index cf_a_idx"); }));
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
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(4)}, {int32_type->decompose(5)}, {int32_type->decompose(6)}},
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(7)}, {int32_type->decompose(5)}, {int32_type->decompose(0)}},
                {{int32_type->decompose(0)}, {int32_type->decompose(2)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}},
            });
        });
        BOOST_TEST_PASSPOINT();
        eventually([&] {
            auto res = e.execute_cql("SELECT * from tab WHERE c = 3").get0();
            assert_that(res).is_rows().with_size(3)
                .with_rows({
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(4)}, {int32_type->decompose(5)}, {int32_type->decompose(6)}},
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(7)}, {int32_type->decompose(5)}, {int32_type->decompose(0)}},
                {{int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(3)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}},
            });
        });
        BOOST_TEST_PASSPOINT();
        eventually([&] {
            auto res = e.execute_cql("SELECT * from tab WHERE d = 4").get0();
            assert_that(res).is_rows().with_size(2)
                .with_rows({
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(4)}, {int32_type->decompose(5)}, {int32_type->decompose(6)}},
                {{int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(4)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}},
            });
        });
        BOOST_TEST_PASSPOINT();
        eventually([&] {
            auto res = e.execute_cql("SELECT * from tab WHERE e = 5").get0();
            assert_that(res).is_rows().with_size(3)
                .with_rows({
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(4)}, {int32_type->decompose(5)}, {int32_type->decompose(6)}},
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(7)}, {int32_type->decompose(5)}, {int32_type->decompose(0)}},
                {{int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(5)}, {int32_type->decompose(0)}},
            });
        });
        BOOST_TEST_PASSPOINT();
        eventually([&] {
            auto res = e.execute_cql("SELECT * from tab WHERE f = 6").get0();
            assert_that(res).is_rows().with_size(2)
                .with_rows({
                {{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}, {int32_type->decompose(4)}, {int32_type->decompose(5)}, {int32_type->decompose(6)}},
                {{int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(7)}, {int32_type->decompose(0)}, {int32_type->decompose(6)}},
            });
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
        BOOST_REQUIRE_THROW(e.execute_cql("SELECT * from tab WHERE a = 1 and e = 5"), exceptions::invalid_request_exception);
    });
}
