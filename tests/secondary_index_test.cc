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

SEASTAR_TEST_CASE(test_index_with_paging) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("CREATE TABLE tab (pk int, ck text, v int, v2 int, v3 text, PRIMARY KEY (pk, ck))").get();
        e.execute_cql("CREATE INDEX ON tab (v)").get();

        sstring big_string(4096, 'j');
        // There should be enough rows to use multiple pages
        for (int i = 0; i < 64 * 1024; ++i) {
            e.execute_cql(sprint("INSERT INTO tab (pk, ck, v, v2, v3) VALUES (%s, 'hello%s', 1, %s, '%s')", i % 3, i, i, big_string)).get();
        }

        eventually([&] {
            auto qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, infinite_timeout_config, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{4321, nullptr, {}, api::new_timestamp()});
            auto res = e.execute_cql("SELECT * FROM tab WHERE v = 1", std::move(qo)).get0();
            assert_that(res).is_rows().with_size(4321);
        });

        eventually([&] {
            auto res = e.execute_cql("SELECT * FROM tab WHERE v = 1").get0();
            assert_that(res).is_rows().with_size(64 * 1024);
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
            e.execute_cql(sprint("INSERT INTO tab (pk, pk2, ck, ck2, v, v2, v3) VALUES (%s, %s, 'hello%s', 'world%s', 1, %s, '%s')", i % 3, i, i, i, i, big_string)).get();
        }
        for (int i = 4; i < 2052; ++i) {
            e.execute_cql(sprint("INSERT INTO tab (pk, pk2, ck, ck2, v, v2, v3) VALUES (%s, %s, 'hello%s', 'world%s', 1, %s, '%s')", i % 3, i, i, i, i, "small_string")).get();
        }

        eventually([&] {
            auto qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, infinite_timeout_config, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{101, nullptr, {}, api::new_timestamp()});
            auto res = e.execute_cql("SELECT * FROM tab WHERE v = 1", std::move(qo)).get0();
            assert_that(res).is_rows().with_size(101);
        });

        eventually([&] {
            auto res = e.execute_cql("SELECT * FROM tab WHERE v = 1").get0();
            assert_that(res).is_rows().with_size(2052);
        });

        eventually([&] {
            auto qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, infinite_timeout_config, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{100, nullptr, {}, api::new_timestamp()});
            auto res = e.execute_cql("SELECT * FROM tab WHERE pk2 = 1", std::move(qo)).get0();
            assert_that(res).is_rows().with_rows({{
                {int32_type->decompose(1)}, {int32_type->decompose(1)}, {utf8_type->decompose("hello1")}, {utf8_type->decompose("world1")},
                {int32_type->decompose(1)}, {int32_type->decompose(1)}, {utf8_type->decompose(big_string)}
            }});
        });

        eventually([&] {
            auto qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, infinite_timeout_config, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{100, nullptr, {}, api::new_timestamp()});
            auto res = e.execute_cql("SELECT * FROM tab WHERE ck2 = 'world8'", std::move(qo)).get0();
            assert_that(res).is_rows().with_rows({{
                {int32_type->decompose(2)}, {int32_type->decompose(8)}, {utf8_type->decompose("hello8")}, {utf8_type->decompose("world8")},
                {int32_type->decompose(1)}, {int32_type->decompose(8)}, {utf8_type->decompose("small_string")}
            }});
        });
    });
}

SEASTAR_TEST_CASE(test_secondary_index_collections) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table t (p int primary key, s1 set<int>, m1 map<int, text>, l1 list<int>, s2 frozen<set<int>>, m2 frozen<map<int, text>>, l2 frozen<list<int>>)").get();

        //NOTICE(sarna): should be lifted after issue #2962 is resolved
        BOOST_REQUIRE_THROW(e.execute_cql("create index on t(s1)").get(), exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("create index on t(m1)").get(), exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("create index on t(l1)").get(), exceptions::invalid_request_exception);

        e.execute_cql("create index on t(FULL(s2))").get();
        e.execute_cql("create index on t(FULL(m2))").get();
        e.execute_cql("create index on t(FULL(l2))").get();

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
