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

#include <boost/test/unit_test.hpp>

#include "database.hh"
#include "db/view/view_builder.hh"
#include "db/view/view_updating_consumer.hh"
#include "db/system_keyspace.hh"
#include "db/system_keyspace_view_types.hh"
#include "db/config.hh"

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/closeable.hh>

#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/sstable_utils.hh"
#include "schema_builder.hh"
#include "service/priority_manager.hh"
#include "test/lib/test_services.hh"
#include "test/lib/data_model.hh"
#include "test/lib/log.hh"
#include "test/lib/random_utils.hh"
#include "utils/ranges.hh"

using namespace std::literals::chrono_literals;

schema_ptr test_table_schema() {
    static thread_local auto s = [] {
        schema_builder builder(make_shared_schema(
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
       ));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return s;
}

SEASTAR_TEST_CASE(test_builder_with_large_partition) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p int, c int, v int, primary key (p, c))").get();

        for (auto i = 0; i < 1024; ++i) {
            e.execute_cql(format("insert into cf (p, c, v) values (0, {:d}, 0)", i)).get();
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
            e.execute_cql(format("insert into cf (p, c, v) values ({:d}, {:d}, 0)", i % 5, i)).get();
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
            e.execute_cql(format("insert into cf (p, c, v) values ({:d}, {:d}, 0)", i % db::view::view_builder::batch_size, i)).get();
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
            e.execute_cql(format("insert into cf (p, c, v) values (0, {:d}, 0)", i)).get();
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
    // In case of errors, replace the seed with a fixed value to get a deterministic run.
    auto seed = tests::random::get_int<uint32_t>();
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
                e.execute_cql(format("insert into cf (p, c, v) values (0x{}, {:d}, 0)", k, i)).get();
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
            e.execute_cql(format("insert into cf (p, c1, c2, v) values (0, {:d}, {:d}, 1)", i % 2, i)).get();
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
                e.execute_cql(format("insert into cf (p, c, v) values (0x{}, {:d}, 0)", *k, i)).get();
            }
        }

        auto f = e.local_view_builder().wait_until_built("ks", "vcf");
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

SEASTAR_TEST_CASE(test_view_update_generator) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table t (p text, c text, v text, primary key (p, c))").get();

        auto msb = e.local_db().get_config().murmur3_partitioner_ignore_msb_bits();
        auto key_token_pair = token_generation_for_shard(2, this_shard_id(), msb);
        auto key1 = key_token_pair[0].first;
        auto key2 = key_token_pair[1].first;

        for (auto i = 0; i < 1024; ++i) {
            e.execute_cql(fmt::format("insert into t (p, c, v) values ('{}', 'c{}', 'x')", key1, i)).get();
        }
        for (auto i = 0; i < 512; ++i) {
            e.execute_cql(fmt::format("insert into t (p, c, v) values ('{}', 'c{}', '{}')", key2, i, i + 1)).get();
        }
        auto& view_update_generator = e.local_view_update_generator();
        auto s = test_table_schema();

        std::vector<shared_sstable> ssts;

        lw_shared_ptr<table> t = e.local_db().find_column_family("ks", "t").shared_from_this();

        auto write_to_sstable = [&] (mutation m) {
            auto sst = t->make_streaming_staging_sstable();
            sstables::sstable_writer_config sst_cfg = e.db().local().get_user_sstables_manager().configure_writer("test");
            auto& pc = service::get_local_streaming_priority();

            auto permit = e.local_db().get_reader_concurrency_semaphore().make_tracking_only_permit(s.get(), "test");
            sst->write_components(flat_mutation_reader_from_mutations(std::move(permit), {m}), 1ul, s, sst_cfg, {}, pc).get();
            sst->open_data().get();
            t->add_sstable_and_update_cache(sst).get();
            return sst;
        };

        auto key = partition_key::from_exploded(*s, {to_bytes(key1)});
        mutation m(s, key);
        auto col = s->get_column_definition("v");
        for (int i = 1024; i < 1280; ++i) {
            auto& row = m.partition().clustered_row(*s, clustering_key::from_exploded(*s, {to_bytes(fmt::format("c{}", i))}));
            row.cells().apply(*col, atomic_cell::make_live(*col->type, 2345, col->type->decompose(sstring(fmt::format("v{}", i)))));
            // Scatter the data in a bunch of different sstables, so we
            // can test the registration semaphore of the view update
            // generator
            if (!(i % 10)) {
                ssts.push_back(write_to_sstable(std::exchange(m, mutation(s, key))));
            }
        }
        ssts.push_back(write_to_sstable(std::move(m)));

        BOOST_REQUIRE_EQUAL(view_update_generator.available_register_units(), db::view::view_update_generator::registration_queue_size);

        parallel_for_each(ssts.begin(), ssts.begin() + 10, [&] (shared_sstable& sst) {
            return view_update_generator.register_staging_sstable(sst, t);
        }).get();

        BOOST_REQUIRE_EQUAL(view_update_generator.available_register_units(), db::view::view_update_generator::registration_queue_size);

        parallel_for_each(ssts.begin() + 10, ssts.end(), [&] (shared_sstable& sst) {
            return view_update_generator.register_staging_sstable(sst, t);
        }).get();

        BOOST_REQUIRE_EQUAL(view_update_generator.available_register_units(), db::view::view_update_generator::registration_queue_size);

        eventually([&, key1, key2] {
            auto msg = e.execute_cql(fmt::format("SELECT * FROM t WHERE p = '{}'", key1)).get0();
            assert_that(msg).is_rows().with_size(1280);
            msg = e.execute_cql(fmt::format("SELECT * FROM t WHERE p = '{}'", key2)).get0();
            assert_that(msg).is_rows().with_size(512);

            for (int i = 0; i < 1024; ++i) {
                auto msg = e.execute_cql(fmt::format("SELECT * FROM t WHERE p = '{}' and c = 'c{}'", key1, i)).get0();
                assert_that(msg).is_rows().with_size(1).with_row({
                     {utf8_type->decompose(key1)},
                     {utf8_type->decompose(sstring(fmt::format("c{}", i)))},
                     {utf8_type->decompose(sstring("x"))}
                 });

            }
            for (int i = 1024; i < 1280; ++i) {
                auto msg = e.execute_cql(fmt::format("SELECT * FROM t WHERE p = '{}' and c = 'c{}'", key1, i)).get0();
                assert_that(msg).is_rows().with_size(1).with_row({
                     {utf8_type->decompose(key1)},
                     {utf8_type->decompose(sstring(fmt::format("c{}", i)))},
                     {utf8_type->decompose(sstring(fmt::format("v{}", i)))}
                 });

            }
        });

        BOOST_REQUIRE_EQUAL(view_update_generator.available_register_units(), db::view::view_update_generator::registration_queue_size);
    });
}

SEASTAR_THREAD_TEST_CASE(test_view_update_generator_deadlock) {
    cql_test_config test_cfg;
    auto& db_cfg = *test_cfg.db_config;

    db_cfg.enable_cache(false);
    db_cfg.enable_commitlog(false);

    do_with_cql_env_thread([] (cql_test_env& e) -> future<> {
        e.execute_cql("create table t (p text, c text, v text, primary key (p, c))").get();
        e.execute_cql("create materialized view tv as select * from t "
                      "where p is not null and c is not null and v is not null "
                      "primary key (v, c, p)").get();

        auto msb = e.local_db().get_config().murmur3_partitioner_ignore_msb_bits();
        auto key1 = token_generation_for_shard(1, this_shard_id(), msb).front().first;

        for (auto i = 0; i < 1024; ++i) {
            e.execute_cql(fmt::format("insert into t (p, c, v) values ('{}', 'c{}', 'x')", key1, i)).get();
        }

        // We need data on the disk so that the pre-image reader is forced to go to disk.
        e.db().invoke_on_all([] (database& db) {
            return db.flush_all_memtables();
        }).get();

        auto& view_update_generator = e.local_view_update_generator();
        auto s = test_table_schema();

        lw_shared_ptr<table> t = e.local_db().find_column_family("ks", "t").shared_from_this();

        auto key = partition_key::from_exploded(*s, {to_bytes(key1)});
        mutation m(s, key);
        auto col = s->get_column_definition("v");
        const auto filler_val_size = 4 * 1024;
        const auto filler_val = sstring(filler_val_size, 'a');
        for (int i = 0; i < 1024; ++i) {
            auto& row = m.partition().clustered_row(*s, clustering_key::from_exploded(*s, {to_bytes(fmt::format("c{}", i))}));
            row.cells().apply(*col, atomic_cell::make_live(*col->type, 2345, col->type->decompose(filler_val)));
        }

        auto sst = t->make_streaming_staging_sstable();
        sstables::sstable_writer_config sst_cfg = e.local_db().get_user_sstables_manager().configure_writer("test");
        auto& pc = service::get_local_streaming_priority();

        auto permit = e.local_db().get_reader_concurrency_semaphore().make_tracking_only_permit(s.get(), "test");
        sst->write_components(flat_mutation_reader_from_mutations(std::move(permit), {m}), 1ul, s, sst_cfg, {}, pc).get();
        sst->open_data().get();
        t->add_sstable_and_update_cache(sst).get();

        auto& sem = *with_scheduling_group(e.local_db().get_streaming_scheduling_group(), [&] () {
            return &e.local_db().get_reader_concurrency_semaphore();
        }).get0();

        // consume all units except what is needed to admit a single reader.
        const auto consumed_resources = sem.initial_resources() - reader_resources{1, new_reader_base_cost};
        sem.consume(consumed_resources);
        auto release_resources = defer([&sem, consumed_resources] {
            sem.signal(consumed_resources);
        });

        testlog.info("res = [.count={}, .memory={}]", sem.available_resources().count, sem.available_resources().memory);

        BOOST_REQUIRE_EQUAL(sem.get_stats().permit_based_evictions, 0);

        view_update_generator.register_staging_sstable(sst, t).get();

        eventually_true([&] {
            return sem.get_stats().permit_based_evictions > 0;
        });

        return make_ready_future<>();
    }, std::move(test_cfg)).get();
}

// Test that registered sstables (and semaphore units) are not leaked when
// sstables are register *while* a batch of sstables are processed.
SEASTAR_THREAD_TEST_CASE(test_view_update_generator_register_semaphore_unit_leak) {
    cql_test_config test_cfg;
    auto& db_cfg = *test_cfg.db_config;

    db_cfg.enable_cache(false);
    db_cfg.enable_commitlog(false);

    do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table t (p text, c text, v text, primary key (p, c))").get();
        e.execute_cql("create materialized view tv as select * from t "
                      "where p is not null and c is not null and v is not null "
                      "primary key (v, c, p)").get();

        auto msb = e.local_db().get_config().murmur3_partitioner_ignore_msb_bits();
        auto key1 = token_generation_for_shard(1, this_shard_id(), msb).front().first;

        for (auto i = 0; i < 1024; ++i) {
            e.execute_cql(fmt::format("insert into t (p, c, v) values ('{}', 'c{}', 'x')", key1, i)).get();
        }

        // We need data on the disk so that the pre-image reader is forced to go to disk.
        e.db().invoke_on_all([] (database& db) {
            return db.flush_all_memtables();
        }).get();

        auto& view_update_generator = e.local_view_update_generator();
        auto s = test_table_schema();

        lw_shared_ptr<table> t = e.local_db().find_column_family("ks", "t").shared_from_this();

        auto key = partition_key::from_exploded(*s, {to_bytes(key1)});

        auto make_sstable = [&] {
            mutation m(s, key);
            auto col = s->get_column_definition("v");
            const auto val = sstring(10, 'a');
            for (int i = 0; i < 1024; ++i) {
                auto& row = m.partition().clustered_row(*s, clustering_key::from_exploded(*s, {to_bytes(fmt::format("c{}", i))}));
                row.cells().apply(*col, atomic_cell::make_live(*col->type, 2345, col->type->decompose(val)));
            }

            auto sst = t->make_streaming_staging_sstable();
            sstables::sstable_writer_config sst_cfg = e.local_db().get_user_sstables_manager().configure_writer("test");
            auto& pc = service::get_local_streaming_priority();

            auto permit = e.local_db().get_reader_concurrency_semaphore().make_tracking_only_permit(s.get(), "test");
            sst->write_components(flat_mutation_reader_from_mutations(std::move(permit), {m}), 1ul, s, sst_cfg, {}, pc).get();
            sst->open_data().get();
            t->add_sstable_and_update_cache(sst).get();
            return sst;
        };

        std::vector<sstables::shared_sstable> prepared_sstables;

        // We need 2 * N + 1 sstables. N should be at least 5 (number of units
        // on the register semaphore) + 1 (just to make sure the returned future
        // blocks). While the initial batch is processed we want to register N
        // more sstables, + 1 to detect the leak (N units will be returned from
        // the initial batch). See below for more details.
        const auto num_sstables = (view_update_generator.available_register_units() + 1) * 2 + 1;
        for (auto i = 0; i < num_sstables; ++i) {
            prepared_sstables.push_back(make_sstable());
        }

        // First batch: register N sstables.
        while (view_update_generator.available_register_units()) {
            auto fut = view_update_generator.register_staging_sstable(std::move(prepared_sstables.back()), t);
            prepared_sstables.pop_back();
            BOOST_REQUIRE(fut.available());
        }

        // Make sure we consumed all units and thus the register future blocks.
        auto fut1 = view_update_generator.register_staging_sstable(std::move(prepared_sstables.back()), t);
        prepared_sstables.pop_back();
        BOOST_REQUIRE(!fut1.available());

        std::vector<future<>> futures;
        futures.reserve(prepared_sstables.size());

        // While the first batch is processed, concurrently register the
        // remaining N + 1 sstables, yielding in-between so the first batch
        // processing can progress.
        while (!prepared_sstables.empty()) {
            thread::yield();
            futures.emplace_back(view_update_generator.register_staging_sstable(std::move(prepared_sstables.back()), t));
            prepared_sstables.pop_back();
        }

        // Make sure the first batch is processed.
        fut1.get();

        auto fut_res = when_all_succeed(futures.begin(), futures.end());

        auto watchdog_timer_done = make_ready_future<>();

        // Watchdog timer which will break out of the deadlock and fail the test.
        timer watchdog_timer([&view_update_generator, &watchdog_timer_done] {
            // Re-start it so stop() on shutdown doesn't crash.
            watchdog_timer_done = watchdog_timer_done.then([&] {
                return view_update_generator.stop().then([&] {
                    return view_update_generator.start();
                });
            });
        });

        watchdog_timer.arm(std::chrono::seconds(60));

        // Wait on the second batch, will fail if the watchdog timer fails.
        fut_res.get();

        watchdog_timer.cancel();

        watchdog_timer_done.get();
    }, std::move(test_cfg)).get();
}

SEASTAR_THREAD_TEST_CASE(test_view_update_generator_buffering) {
    using partition_size_map = std::map<dht::decorated_key, size_t, dht::ring_position_less_comparator>;

    class consumer_verifier {
        schema_ptr _schema;
        reader_concurrency_semaphore& _semaphore;
        const partition_size_map& _partition_rows;
        std::vector<mutation>& _collected_muts;
        std::unique_ptr<row_locker> _rl;
        std::unique_ptr<row_locker::stats> _rl_stats;
        clustering_key::less_compare _less_cmp;
        const size_t _max_rows_soft;
        const size_t _max_rows_hard;
        size_t _buffer_rows = 0;
        bool& _ok;

    private:
        static size_t rows_in_limit(size_t l) {
            const size_t _100kb = 100 * 1024;
            // round up
            return l / _100kb + std::min(size_t(1), l % _100kb);
        }

        static size_t rows_in_mut(const mutation& m) {
            return std::distance(m.partition().clustered_rows().begin(), m.partition().clustered_rows().end());
        }

        void check(mutation mut) {
            // First we check that we would be able to create a reader, even
            // though the staging reader consumed all resources.
            auto permit = _semaphore.obtain_permit(_schema.get(), "consumer_verifier", new_reader_base_cost, db::timeout_clock::now()).get0();

            const size_t current_rows = rows_in_mut(mut);
            const auto total_rows = _partition_rows.at(mut.decorated_key());
            _buffer_rows += current_rows;

            testlog.trace("consumer_verifier::check(): key={}, rows={}/{}, _buffer={}",
                    partition_key::with_schema_wrapper(*_schema, mut.key()),
                    current_rows,
                    total_rows,
                    _buffer_rows);

            BOOST_REQUIRE(current_rows);
            BOOST_REQUIRE(current_rows <= _max_rows_hard);
            BOOST_REQUIRE(_buffer_rows <= _max_rows_hard);

            // The current partition doesn't have all of its rows yet, verify
            // that the new mutation contains the next rows for the same
            // partition
            if (!_collected_muts.empty() && rows_in_mut(_collected_muts.back()) < _partition_rows.at(_collected_muts.back().decorated_key())) {
                BOOST_REQUIRE(_collected_muts.back().decorated_key().equal(*mut.schema(), mut.decorated_key()));
                const auto& previous_ckey = (--_collected_muts.back().partition().clustered_rows().end())->key();
                const auto& next_ckey = mut.partition().clustered_rows().begin()->key();
                BOOST_REQUIRE(_less_cmp(previous_ckey, next_ckey));
                mutation_application_stats stats;
                _collected_muts.back().partition().apply(*_schema, mut.partition(), *mut.schema(), stats);
            // The new mutation is a new partition.
            } else {
                if (!_collected_muts.empty()) {
                    BOOST_REQUIRE(!_collected_muts.back().decorated_key().equal(*mut.schema(), mut.decorated_key()));
                }
                _collected_muts.push_back(std::move(mut));
            }

            if (_buffer_rows >= _max_rows_hard) { // buffer flushed on hard limit
                _buffer_rows = 0;
                testlog.trace("consumer_verifier::check(): buffer ends on hard limit");
            } else if (_buffer_rows >= _max_rows_soft) { // buffer flushed on soft limit
                _buffer_rows = 0;
                testlog.trace("consumer_verifier::check(): buffer ends on soft limit");
            }
        }

    public:
        consumer_verifier(schema_ptr schema, reader_concurrency_semaphore& sem, const partition_size_map& partition_rows, std::vector<mutation>& collected_muts, bool& ok)
            : _schema(std::move(schema))
            , _semaphore(sem)
            , _partition_rows(partition_rows)
            , _collected_muts(collected_muts)
            , _rl(std::make_unique<row_locker>(_schema))
            , _rl_stats(std::make_unique<row_locker::stats>())
            , _less_cmp(*_schema)
            , _max_rows_soft(rows_in_limit(db::view::view_updating_consumer::buffer_size_soft_limit))
            , _max_rows_hard(rows_in_limit(db::view::view_updating_consumer::buffer_size_hard_limit))
            , _ok(ok)
        { }

        future<row_locker::lock_holder> operator()(mutation mut) {
            try {
                check(std::move(mut));
            } catch (...) {
                _ok = false;
                BOOST_FAIL(fmt::format("consumer_verifier::operator(): caught unexpected exception {}", std::current_exception()));
            }
            return _rl->lock_pk(_collected_muts.back().decorated_key(), true, db::no_timeout, *_rl_stats);
        }
    };

    reader_concurrency_semaphore sem(1, new_reader_base_cost, get_name());
    auto stop_sem = deferred_stop(sem);

    auto schema = schema_builder("ks", "cf")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("v", bytes_type)
            .build();

    const auto blob_100kb = bytes(100 * 1024, bytes::value_type(0xab));
    const abort_source as;

    const auto partition_size_sets = std::vector<std::vector<int>>{{12}, {8, 4}, {8, 16}, {22}, {8, 8, 8, 8}, {8, 8, 8, 16, 8}, {8, 20, 16, 16}, {50}, {21}, {21, 2}};
    const auto max_partition_set_size = std::ranges::max_element(partition_size_sets, [] (const std::vector<int>& a, const std::vector<int>& b) { return a.size() < b.size(); })->size();
    auto pkeys = ranges::to<std::vector<dht::decorated_key>>(boost::irange(size_t{0}, max_partition_set_size) | boost::adaptors::transformed([schema] (int i) {
        return dht::decorate_key(*schema, partition_key::from_single_value(*schema, int32_type->decompose(data_value(i))));
    }));
    std::ranges::sort(pkeys, dht::ring_position_less_comparator(*schema));

    for (auto partition_sizes_100kb : partition_size_sets) {
        testlog.debug("partition_sizes_100kb={}", partition_sizes_100kb);
        partition_size_map partition_rows{dht::ring_position_less_comparator(*schema)};
        std::vector<mutation> muts;
        auto pk = 0;
        for (auto partition_size_100kb : partition_sizes_100kb) {
            auto mut_desc = tests::data_model::mutation_description(pkeys.at(pk++).key().explode(*schema));
            for (auto ck = 0; ck < partition_size_100kb; ++ck) {
                mut_desc.add_clustered_cell({int32_type->decompose(data_value(ck))}, "v", tests::data_model::mutation_description::value(blob_100kb));
            }
            muts.push_back(mut_desc.build(schema));
            partition_rows.emplace(muts.back().decorated_key(), partition_size_100kb);
        }

        std::ranges::sort(muts, [less = dht::ring_position_less_comparator(*schema)] (const mutation& a, const mutation& b) {
            return less(a.decorated_key(), b.decorated_key());
        });

        auto permit = sem.obtain_permit(schema.get(), get_name(), new_reader_base_cost, db::no_timeout).get0();

        auto mt = make_lw_shared<memtable>(schema);
        for (const auto& mut : muts) {
            mt->apply(mut);
        }

        auto p = make_manually_paused_evictable_reader(
                mt->as_data_source(),
                schema,
                permit,
                query::full_partition_range,
                schema->full_slice(),
                service::get_local_streaming_priority(),
                nullptr,
                ::mutation_reader::forwarding::no);
        auto& staging_reader = std::get<0>(p);
        auto& staging_reader_handle = std::get<1>(p);
        auto close_staging_reader = deferred_close(staging_reader);

        std::vector<mutation> collected_muts;
        bool ok = true;

        staging_reader.consume_in_thread(db::view::view_updating_consumer(schema, permit, as, staging_reader_handle,
                    consumer_verifier(schema, sem, partition_rows, collected_muts, ok)), db::no_timeout);

        BOOST_REQUIRE(ok);

        BOOST_REQUIRE_EQUAL(muts.size(), collected_muts.size());
        for (size_t i = 0; i < muts.size(); ++i) {
            testlog.trace("compare mutation {}", i);
            BOOST_REQUIRE_EQUAL(muts[i], collected_muts[i]);
        }
    }
}

// Checking for view_builds_in_progress used to assume that certain values
// (e.g. first_token) are always present in every row, which lead to using
// freed memory. In order to check for regressions, a row with missing
// values is inserted and ensured that it produces a status without any
// views in progress.
SEASTAR_TEST_CASE(test_load_view_build_progress_with_values_missing) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, format("INSERT INTO system.{} (keyspace_name, view_name, cpu_id) VALUES ('ks', 'v', {})",
                db::system_keyspace::v3::SCYLLA_VIEWS_BUILDS_IN_PROGRESS, this_shard_id()));
        BOOST_REQUIRE(db::system_keyspace::load_view_build_progress().get0().empty());
    });
}
