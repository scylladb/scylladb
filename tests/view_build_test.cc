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

#include <boost/test/unit_test.hpp>

#include "database.hh"
#include "db/view/view_builder.hh"
#include "db/system_keyspace.hh"

#include "tests/test-utils.hh"
#include "tests/cql_test_env.hh"
#include "tests/cql_assertions.hh"

using namespace std::literals::chrono_literals;

SEASTAR_TEST_CASE(test_builder_with_large_partition) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p int, c int, v int, primary key (p, c))").get();

        for (auto i = 0; i < 1024; ++i) {
            e.execute_cql(format("insert into cf (p, c, v) values (0, {:d}, 0)", i)).get();
        }

        auto f = e.local_view_builder().wait_until_built("ks", "vcf", lowres_clock::now() + 10s);
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c is not null and v is not null "
                      "primary key (v, c, p)").get();

        f.get();
        auto built = db::system_keyspace::load_built_views().get0();
        BOOST_REQUIRE_EQUAL(built.size(), 1);
        BOOST_REQUIRE_EQUAL(built[0].second, sstring("vcf"));

        auto msg = e.execute_cql("select count(*) from vcf where v = 0").get0();
        assert_that(msg).is_rows().with_size(1);
        assert_that(msg).is_rows().with_rows({{{long_type->decompose(1024L)}}});
    });
}

SEASTAR_TEST_CASE(test_builder_with_multiple_partitions) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p int, c int, v int, primary key (p, c))").get();

        for (auto i = 0; i < 1024; ++i) {
            e.execute_cql(format("insert into cf (p, c, v) values ({:d}, {:d}, 0)", i % 5, i)).get();
        }

        auto f = e.local_view_builder().wait_until_built("ks", "vcf", lowres_clock::now() + 10s);
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c is not null and v is not null "
                      "primary key (v, c, p)").get();

        f.get();
        auto built = db::system_keyspace::load_built_views().get0();
        BOOST_REQUIRE_EQUAL(built.size(), 1);
        BOOST_REQUIRE_EQUAL(built[0].second, sstring("vcf"));

        auto msg = e.execute_cql("select count(*) from vcf where v = 0").get0();
        assert_that(msg).is_rows().with_size(1);
        assert_that(msg).is_rows().with_rows({{{long_type->decompose(1024L)}}});
    });
}

SEASTAR_TEST_CASE(test_builder_with_multiple_partitions_of_batch_size_rows) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p int, c int, v int, primary key (p, c))").get();

        for (auto i = 0; i < 1024; ++i) {
            e.execute_cql(format("insert into cf (p, c, v) values ({:d}, {:d}, 0)", i % db::view::view_builder::batch_size, i)).get();
        }

        auto f = e.local_view_builder().wait_until_built("ks", "vcf", lowres_clock::now() + 10s);
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c is not null and v is not null "
                      "primary key (v, c, p)").get();

        f.get();
        auto built = db::system_keyspace::load_built_views().get0();
        BOOST_REQUIRE_EQUAL(built.size(), 1);
        BOOST_REQUIRE_EQUAL(built[0].second, sstring("vcf"));

        auto msg = e.execute_cql("select count(*) from vcf where v = 0").get0();
        assert_that(msg).is_rows().with_size(1);
        assert_that(msg).is_rows().with_rows({{{long_type->decompose(1024L)}}});
    });
}

SEASTAR_TEST_CASE(test_builder_view_added_during_ongoing_build) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p int, c int, v int, primary key (p, c))").get();

        for (auto i = 0; i < 5000; ++i) {
            e.execute_cql(format("insert into cf (p, c, v) values (0, {:d}, 0)", i)).get();
        }

        auto f1 = e.local_view_builder().wait_until_built("ks", "vcf1", lowres_clock::now() + 60s);
        auto f2 = e.local_view_builder().wait_until_built("ks", "vcf2", lowres_clock::now() + 30s);

        e.execute_cql("create materialized view vcf1 as select * from cf "
                      "where p is not null and c is not null and v is not null "
                      "primary key (v, c, p)").get();

        sleep(1s).get();

        e.execute_cql("create materialized view vcf2 as select * from cf "
                      "where p is not null and c is not null and v is not null "
                      "primary key (v, p, c)").get();

        f2.get();
        f1.get();
        auto built = db::system_keyspace::load_built_views().get0();
        BOOST_REQUIRE_EQUAL(built.size(), 2);

        auto msg = e.execute_cql("select count(*) from vcf1 where v = 0").get0();
        assert_that(msg).is_rows().with_size(1);
        assert_that(msg).is_rows().with_rows({{{long_type->decompose(5000L)}}});

        msg = e.execute_cql("select count(*) from vcf2 where v = 0").get0();
        assert_that(msg).is_rows().with_size(1);
        assert_that(msg).is_rows().with_rows({{{long_type->decompose(5000L)}}});
    });
}

std::mt19937 random_generator() {
    std::random_device rd;
    // In case of errors, replace the seed with a fixed value to get a deterministic run.
    auto seed = rd();
    std::cout << "Random seed: " << seed << "\n";
    return std::mt19937(seed);
}

bytes random_bytes(size_t size, std::mt19937& gen) {
    bytes result(bytes::initialized_later(), size);
    static thread_local std::uniform_int_distribution<int> dist(0, std::numeric_limits<uint8_t>::max());
    for (size_t i = 0; i < size; ++i) {
        result[i] = dist(gen);
    }
    return result;
}

SEASTAR_TEST_CASE(test_builder_across_tokens_with_large_partitions) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto gen = random_generator();

        e.execute_cql("create table cf (p blob, c int, v int, primary key (p, c))").get();
        auto s = e.local_db().find_schema("ks", "cf");

        auto make_key = [&] (auto) { return to_hex(random_bytes(128, gen));  };
        for (auto&& k : boost::irange(0, 4) | boost::adaptors::transformed(make_key)) {
            for (auto i = 0; i < 1000; ++i) {
                e.execute_cql(format("insert into cf (p, c, v) values (0x{}, {:d}, 0)", k, i)).get();
            }
        }

        auto f1 = e.local_view_builder().wait_until_built("ks", "vcf1", lowres_clock::now() + 60s);
        auto f2 = e.local_view_builder().wait_until_built("ks", "vcf2", lowres_clock::now() + 30s);

        e.execute_cql("create materialized view vcf1 as select * from cf "
                      "where p is not null and c is not null and v is not null "
                      "primary key (v, c, p)").get();

        sleep(1s).get();

        e.execute_cql("create materialized view vcf2 as select * from cf "
                      "where p is not null and c is not null and v is not null "
                      "primary key (v, p, c)").get();

        f2.get();
        f1.get();
        auto built = db::system_keyspace::load_built_views().get0();
        BOOST_REQUIRE_EQUAL(built.size(), 2);

        auto msg = e.execute_cql("select count(*) from vcf1 where v = 0").get0();
        assert_that(msg).is_rows().with_size(1);
        assert_that(msg).is_rows().with_rows({{{long_type->decompose(4000L)}}});

        msg = e.execute_cql("select count(*) from vcf2 where v = 0").get0();
        assert_that(msg).is_rows().with_size(1);
        assert_that(msg).is_rows().with_rows({{{long_type->decompose(4000L)}}});
    });
}

SEASTAR_TEST_CASE(test_builder_across_tokens_with_small_partitions) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto gen = random_generator();

        e.execute_cql("create table cf (p blob, c int, v int, primary key (p, c))").get();
        auto s = e.local_db().find_schema("ks", "cf");

        auto make_key = [&] (auto) { return to_hex(random_bytes(128, gen));  };
        for (auto&& k : boost::irange(0, 1000) | boost::adaptors::transformed(make_key)) {
            for (auto i = 0; i < 4; ++i) {
                e.execute_cql(format("insert into cf (p, c, v) values (0x{}, {:d}, 0)", k, i)).get();
            }
        }

        auto f1 = e.local_view_builder().wait_until_built("ks", "vcf1", lowres_clock::now() + 60s);
        auto f2 = e.local_view_builder().wait_until_built("ks", "vcf2", lowres_clock::now() + 30s);

        e.execute_cql("create materialized view vcf1 as select * from cf "
                      "where p is not null and c is not null and v is not null "
                      "primary key (v, c, p)").get();

        sleep(1s).get();

        e.execute_cql("create materialized view vcf2 as select * from cf "
                      "where p is not null and c is not null and v is not null "
                      "primary key (v, p, c)").get();

        f2.get();
        f1.get();

        auto built = db::system_keyspace::load_built_views().get0();
        BOOST_REQUIRE_EQUAL(built.size(), 2);

        auto msg = e.execute_cql("select count(*) from vcf1 where v = 0").get0();
        assert_that(msg).is_rows().with_size(1);
        assert_that(msg).is_rows().with_rows({{{long_type->decompose(4000L)}}});

        msg = e.execute_cql("select count(*) from vcf2 where v = 0").get0();
        assert_that(msg).is_rows().with_size(1);
        assert_that(msg).is_rows().with_rows({{{long_type->decompose(4000L)}}});
    });
}

SEASTAR_TEST_CASE(test_builder_with_tombstones) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p int, c1 int, c2 int, v int, primary key (p, c1, c2))").get();

        for (auto i = 0; i < 100; ++i) {
            e.execute_cql(format("insert into cf (p, c1, c2, v) values (0, {:d}, {:d}, 1)", i % 2, i)).get();
        }

        e.execute_cql("delete from cf where p = 0 and c1 = 0").get();
        e.execute_cql("delete from cf where p = 0 and c1 = 1 and c2 >= 50 and c2 < 101").get();

        auto f = e.local_view_builder().wait_until_built("ks", "vcf", lowres_clock::now() + 30s);
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c1 is not null and c2 is not null and v is not null "
                      "primary key ((v, p), c1, c2)").get();

        f.get();
        auto built = db::system_keyspace::load_built_views().get0();
        BOOST_REQUIRE_EQUAL(built.size(), 1);

        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_size(25);
    });
}

SEASTAR_TEST_CASE(test_builder_with_concurrent_writes) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto gen = random_generator();

        e.execute_cql("create table cf (p blob, c int, v int, primary key (p, c))").get();

        const size_t rows = 6864;
        const size_t rows_per_partition = 4;
        const size_t partitions = rows / rows_per_partition;

        std::unordered_set<sstring> keys;
        while (keys.size() != partitions) {
            keys.insert(to_hex(random_bytes(128, gen)));
        }

        auto half = keys.begin();
        std::advance(half, keys.size() / 2);
        auto k = keys.begin();
        for (; k != half; ++k) {
            for (size_t i = 0; i < rows_per_partition; ++i) {
                e.execute_cql(format("insert into cf (p, c, v) values (0x{}, {:d}, 0)", *k, i)).get();
            }
        }

        auto f = e.local_view_builder().wait_until_built("ks", "vcf", lowres_clock::now() + 60s);
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c is not null and v is not null "
                      "primary key (v, c, p)").get();

        for (; k != keys.end(); ++k) {
            for (size_t i = 0; i < rows_per_partition; ++i) {
                e.execute_cql(format("insert into cf (p, c, v) values (0x{}, {:d}, 0)", *k, i)).get();
            }
        }

        f.get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf").get0();
            assert_that(msg).is_rows().with_size(rows);
        });
    });
}

SEASTAR_TEST_CASE(test_builder_with_concurrent_drop) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto gen = random_generator();

        e.execute_cql("create table cf (p blob, c int, v int, primary key (p, c))").get();

        auto make_key = [&] (auto) { return to_hex(random_bytes(128, gen));  };
        for (auto&& k : boost::irange(0, 1000) | boost::adaptors::transformed(make_key)) {
            for (auto i = 0; i < 5; ++i) {
                e.execute_cql(format("insert into cf (p, c, v) values (0x{}, {:d}, 0)", k, i)).get();
            }
        }

        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c is not null and v is not null "
                      "primary key (v, c, p)").get();

        e.execute_cql("drop materialized view vcf").get();

        eventually([&] {
            auto msg = e.execute_cql("select * from system.scylla_views_builds_in_progress").get0();
            assert_that(msg).is_rows().is_empty();
            msg = e.execute_cql("select * from system.built_views").get0();
            assert_that(msg).is_rows().is_empty();
            msg = e.execute_cql("select * from system.views_builds_in_progress").get0();
            assert_that(msg).is_rows().is_empty();
            msg = e.execute_cql("select * from system_distributed.view_build_status").get0();
            assert_that(msg).is_rows().is_empty();
        });
    });
}
