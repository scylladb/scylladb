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
#include "schema_builder.hh"
#include "service/priority_manager.hh"

using namespace std::literals::chrono_literals;

static db::nop_large_partition_handler nop_lp_handler;

schema_ptr test_table_schema() {
    static thread_local auto s = [] {
        schema_builder builder(make_lw_shared(schema(
                generate_legacy_id("try1", "data"), "try1", "data",
        // partition key
        {{"p", utf8_type}},
        // clustering key
        {{"c", utf8_type}},
        // regular columns
        {{"v", utf8_type}},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        ""
       )));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return s;
}

SEASTAR_TEST_CASE(test_builder_with_large_partition) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p int, c int, v int, primary key (p, c))").get();

        for (auto i = 0; i < 1024; ++i) {
            e.execute_cql(sprint("insert into cf (p, c, v) values (0, %d, 0)", i)).get();
        }

        auto f = e.local_view_builder().wait_until_built("ks", "vcf");
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

// This test reproduces issue #4213. We have a large base partition with
// many rows, and the view has the *same* partition key as the base, so all
// the generated view rows will go to the same view partition. The view
// builder used to batch up to 128 (view_builder::batch_size) of these view
// rows into one view mutation, and if the individual rows are big (i.e.,
// contain very long strings or blobs), this 128-row mutation can be too
// large to be applied because of our limitation on commit-log segment size
// (commitlog_segment_size_in_mb, by default 32 MB). When applying the
// mutation fails, view building retries indefinitely and this test hung.
SEASTAR_TEST_CASE(test_builder_with_large_partition_2) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        const int nrows = 64; // meant to be lower than view_builder::batch_size
        const int target_size = 33*1024*1024; // meant to be higher than commitlog_segment_size_in_mb
        e.execute_cql("create table cf (p int, c int, s ascii, primary key (p, c))").get();
        const sstring longstring = sstring(target_size / nrows, 'x');
        for (auto i = 0; i < nrows; ++i) {
            e.execute_cql(format("insert into cf (p, c, s) values (0, {:d}, '{}')", i, longstring)).get();
        }

        auto f = e.local_view_builder().wait_until_built("ks", "vcf");
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c is not null "
                      "primary key (p, c)").get();

        f.get();
        auto built = db::system_keyspace::load_built_views().get0();
        BOOST_REQUIRE_EQUAL(built.size(), 1);
        BOOST_REQUIRE_EQUAL(built[0].second, sstring("vcf"));

        auto msg = e.execute_cql("select count(*) from vcf").get0();
        assert_that(msg).is_rows().with_size(1);
        assert_that(msg).is_rows().with_rows({{{long_type->decompose(long(nrows))}}});
    });
}


SEASTAR_TEST_CASE(test_builder_with_multiple_partitions) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p int, c int, v int, primary key (p, c))").get();

        for (auto i = 0; i < 1024; ++i) {
            e.execute_cql(sprint("insert into cf (p, c, v) values (%d, %d, 0)", i % 5, i)).get();
        }

        auto f = e.local_view_builder().wait_until_built("ks", "vcf");
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
            e.execute_cql(sprint("insert into cf (p, c, v) values (%d, %d, 0)", i % db::view::view_builder::batch_size, i)).get();
        }

        auto f = e.local_view_builder().wait_until_built("ks", "vcf");
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
            e.execute_cql(sprint("insert into cf (p, c, v) values (0, %d, 0)", i)).get();
        }

        auto f1 = e.local_view_builder().wait_until_built("ks", "vcf1");
        auto f2 = e.local_view_builder().wait_until_built("ks", "vcf2");

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
                e.execute_cql(sprint("insert into cf (p, c, v) values (0x%s, %d, 0)", k, i)).get();
            }
        }

        auto f1 = e.local_view_builder().wait_until_built("ks", "vcf1");
        auto f2 = e.local_view_builder().wait_until_built("ks", "vcf2");

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
                e.execute_cql(sprint("insert into cf (p, c, v) values (0x%s, %d, 0)", k, i)).get();
            }
        }

        auto f1 = e.local_view_builder().wait_until_built("ks", "vcf1");
        auto f2 = e.local_view_builder().wait_until_built("ks", "vcf2");

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
            e.execute_cql(sprint("insert into cf (p, c1, c2, v) values (0, %d, %d, 1)", i % 2, i)).get();
        }

        e.execute_cql("delete from cf where p = 0 and c1 = 0").get();
        e.execute_cql("delete from cf where p = 0 and c1 = 1 and c2 >= 50 and c2 < 101").get();

        auto f = e.local_view_builder().wait_until_built("ks", "vcf");
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
                e.execute_cql(sprint("insert into cf (p, c, v) values (0x%s, %d, 0)", *k, i)).get();
            }
        }

        auto f = e.local_view_builder().wait_until_built("ks", "vcf");
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c is not null and v is not null "
                      "primary key (v, c, p)").get();

        for (; k != keys.end(); ++k) {
            for (size_t i = 0; i < rows_per_partition; ++i) {
                e.execute_cql(sprint("insert into cf (p, c, v) values (0x%s, %d, 0)", *k, i)).get();
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
                e.execute_cql(sprint("insert into cf (p, c, v) values (0x%s, %d, 0)", k, i)).get();
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

SEASTAR_TEST_CASE(test_view_update_generator) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table t (p text, c text, v text, primary key (p, c))").get();
        for (auto i = 0; i < 1024; ++i) {
            e.execute_cql(fmt::format("insert into t (p, c, v) values ('a', 'c{}', 'x')", i)).get();
        }
        for (auto i = 0; i < 512; ++i) {
            e.execute_cql(fmt::format("insert into t (p, c, v) values ('b', 'c{}', '{}')", i, i + 1)).get();
        }
        auto& view_update_generator = e.local_view_update_generator();
        auto s = test_table_schema();

        auto key = partition_key::from_exploded(*s, {to_bytes("a")});
        mutation m(s, key);
        auto col = s->get_column_definition("v");
        for (int i = 1024; i < 1280; ++i) {
            auto& row = m.partition().clustered_row(*s, clustering_key::from_exploded(*s, {to_bytes(fmt::format("c{}", i))}));
            row.cells().apply(*col, atomic_cell::make_live(*col->type, 2345, col->type->decompose(sstring(fmt::format("v{}", i)))));
        }
        lw_shared_ptr<table> t = e.local_db().find_column_family("ks", "t").shared_from_this();

        auto sst = t->make_streaming_staging_sstable();
        sstables::sstable_writer_config sst_cfg;
        sst_cfg.large_partition_handler = &nop_lp_handler;
        auto& pc = service::get_local_streaming_write_priority();
        sst->write_components(flat_mutation_reader_from_mutations({m}), 1ul, s, sst_cfg, {}, pc).get();
        sst->open_data().get();
        t->add_sstable_and_update_cache(sst).get();
        view_update_generator.register_staging_sstable(sst, t).get();

        eventually([&] {
            auto msg = e.execute_cql("SELECT * FROM t WHERE p = 'a'").get0();
            assert_that(msg).is_rows().with_size(1280);
            msg = e.execute_cql("SELECT * FROM t WHERE p = 'b'").get0();
            assert_that(msg).is_rows().with_size(512);

            for (int i = 0; i < 1024; ++i) {
                auto msg = e.execute_cql(fmt::format("SELECT * FROM t WHERE p = 'a' and c = 'c{}'", i)).get0();
                assert_that(msg).is_rows().with_size(1).with_row({
                     {utf8_type->decompose(sstring("a"))},
                     {utf8_type->decompose(sstring(fmt::format("c{}", i)))},
                     {utf8_type->decompose(sstring("x"))}
                 });

            }
            for (int i = 1024; i < 1280; ++i) {
                auto msg = e.execute_cql(fmt::format("SELECT * FROM t WHERE p = 'a' and c = 'c{}'", i)).get0();
                assert_that(msg).is_rows().with_size(1).with_row({
                     {utf8_type->decompose(sstring("a"))},
                     {utf8_type->decompose(sstring(fmt::format("c{}", i)))},
                     {utf8_type->decompose(sstring(fmt::format("v{}", i)))}
                 });

            }
        });
    });
}
