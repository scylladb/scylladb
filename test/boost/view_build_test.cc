/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <boost/test/unit_test.hpp>
#include <fmt/ranges.h>

#include "replica/database.hh"
#include "db/view/view_builder.hh"
#include "db/view/view_updating_consumer.hh"
#include "db/view/view_update_generator.hh"
#include "db/system_keyspace.hh"
#include "db/config.hh"
#include "cql3/query_options.hh"

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/closeable.hh>

#include "schema/schema_builder.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/eventually.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/data_model.hh"
#include "test/lib/log.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/key_utils.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/mutation_assertions.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/test_utils.hh"

#include "readers/from_mutations_v2.hh"
#include "readers/evictable.hh"

BOOST_AUTO_TEST_SUITE(view_build_test)

using namespace std::literals::chrono_literals;

schema_ptr test_table_schema() {
    static thread_local auto s = [] {
        schema_builder builder("try1", "data", generate_legacy_id("try1", "data"));
        builder.with_column("p", utf8_type, column_kind::partition_key);
        builder.with_column("c", utf8_type, column_kind::clustering_key);
        builder.with_column("v", utf8_type);
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
        auto built = e.get_system_keyspace().local().load_built_views().get();
        BOOST_REQUIRE_EQUAL(built.size(), 1);
        BOOST_REQUIRE_EQUAL(built[0].second, sstring("vcf"));

        auto msg = e.execute_cql("select count(*) from vcf where v = 0").get();
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
        auto built = e.get_system_keyspace().local().load_built_views().get();
        BOOST_REQUIRE_EQUAL(built.size(), 1);
        BOOST_REQUIRE_EQUAL(built[0].second, sstring("vcf"));

        auto msg = e.execute_cql("select count(*) from vcf").get();
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
        auto built = e.get_system_keyspace().local().load_built_views().get();
        BOOST_REQUIRE_EQUAL(built.size(), 1);
        BOOST_REQUIRE_EQUAL(built[0].second, sstring("vcf"));

        auto msg = e.execute_cql("select count(*) from vcf where v = 0").get();
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
        auto built = e.get_system_keyspace().local().load_built_views().get();
        BOOST_REQUIRE_EQUAL(built.size(), 1);
        BOOST_REQUIRE_EQUAL(built[0].second, sstring("vcf"));

        auto msg = e.execute_cql("select count(*) from vcf where v = 0").get();
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
        auto built = e.get_system_keyspace().local().load_built_views().get();
        BOOST_REQUIRE_EQUAL(built.size(), 2);

        auto msg = e.execute_cql("select count(*) from vcf1 where v = 0").get();
        assert_that(msg).is_rows().with_size(1);
        assert_that(msg).is_rows().with_rows({{{long_type->decompose(5000L)}}});

        msg = e.execute_cql("select count(*) from vcf2 where v = 0").get();
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
        for (auto&& k : std::views::iota(0, 4) | std::views::transform(make_key)) {
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
        auto built = e.get_system_keyspace().local().load_built_views().get();
        BOOST_REQUIRE_EQUAL(built.size(), 2);

        auto msg = e.execute_cql("select count(*) from vcf1 where v = 0").get();
        assert_that(msg).is_rows().with_size(1);
        assert_that(msg).is_rows().with_rows({{{long_type->decompose(4000L)}}});

        msg = e.execute_cql("select count(*) from vcf2 where v = 0").get();
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
        for (auto&& k : std::views::iota(0, 1000) | std::views::transform(make_key)) {
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

        auto built = e.get_system_keyspace().local().load_built_views().get();
        BOOST_REQUIRE_EQUAL(built.size(), 2);

        auto msg = e.execute_cql("select count(*) from vcf1 where v = 0").get();
        assert_that(msg).is_rows().with_size(1);
        assert_that(msg).is_rows().with_rows({{{long_type->decompose(4000L)}}});

        msg = e.execute_cql("select count(*) from vcf2 where v = 0").get();
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
        auto built = e.get_system_keyspace().local().load_built_views().get();
        BOOST_REQUIRE_EQUAL(built.size(), 1);

        auto msg = e.execute_cql("select * from vcf").get();
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
            auto msg = e.execute_cql("select * from vcf").get();
            assert_that(msg).is_rows().with_size(rows);
        });
    });
}

SEASTAR_TEST_CASE(test_builder_with_concurrent_drop) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto gen = random_generator();

        e.execute_cql("create table cf (p blob, c int, v int, primary key (p, c))").get();

        auto make_key = [&] (auto) { return to_hex(random_bytes(128, gen));  };
        for (auto&& k : std::views::iota(0, 1000) | std::views::transform(make_key)) {
            for (auto i = 0; i < 5; ++i) {
                e.execute_cql(format("insert into cf (p, c, v) values (0x{}, {:d}, 0)", k, i)).get();
            }
        }

        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c is not null and v is not null "
                      "primary key (v, c, p)").get();

        e.execute_cql("drop materialized view vcf").get();

        eventually([&] {
            auto msg = e.execute_cql("select * from system.scylla_views_builds_in_progress").get();
            assert_that(msg).is_rows().is_empty();
            msg = e.execute_cql("select * from system.built_views").get();
            assert_that(msg).is_rows().is_empty();
            msg = e.execute_cql("select * from system.views_builds_in_progress").get();
            assert_that(msg).is_rows().is_empty();
            msg = e.execute_cql("select * from system_distributed.view_build_status").get();
            assert_that(msg).is_rows().is_empty();
        });
    });
}

SEASTAR_TEST_CASE(test_view_update_generator) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table t (p text, c text, v text, primary key (p, c))").get();
        auto s = test_table_schema();

        auto insert_id = e.prepare("insert into t (p, c, v) values (?, ?, ?)").get();

        auto keys = tests::generate_partition_keys(2, s);
        const auto& key1 = keys[0];
        const auto& key2 = keys[1];

        const auto key1_raw = cql3::raw_value::make_value(key1.key().explode().front());
        const auto key2_raw = cql3::raw_value::make_value(key2.key().explode().front());

        for (auto i = 0; i < 1024; ++i) {
            e.execute_prepared(insert_id, {
                    key1_raw,
                    cql3::raw_value::make_value(serialized(format("c{}", i))),
                    cql3::raw_value::make_value(serialized("x"))}).get();
        }
        for (auto i = 0; i < 512; ++i) {
            e.execute_prepared(insert_id, {
                    cql3::raw_value::make_value(key2.key().explode().front()),
                    cql3::raw_value::make_value(serialized(format("c{}", i))),
                    cql3::raw_value::make_value(serialized(format("{}", i + 1)))}).get();
        }
        auto& view_update_generator = e.local_view_update_generator();

        std::vector<shared_sstable> ssts;

        lw_shared_ptr<replica::table> t = e.local_db().find_column_family("ks", "t").shared_from_this();

        auto write_to_sstable = [&] (mutation m) {
            auto sst = t->make_streaming_staging_sstable();
            sstables::sstable_writer_config sst_cfg = e.db().local().get_user_sstables_manager().configure_writer("test");
            auto permit = e.local_db().get_reader_concurrency_semaphore().make_tracking_only_permit(s, "test", db::no_timeout, {});
            sst->write_components(make_mutation_reader_from_mutations_v2(m.schema(), std::move(permit), m), 1ul, s, sst_cfg, {}).get();
            sst->open_data().get();
            t->add_sstable_and_update_cache(sst).get();
            return sst;
        };

        mutation m(s, key1);
        auto col = s->get_column_definition("v");
        for (int i = 1024; i < 1280; ++i) {
            auto& row = m.partition().clustered_row(*s, clustering_key::from_exploded(*s, {to_bytes(fmt::format("c{}", i))}));
            row.cells().apply(*col, atomic_cell::make_live(*col->type, 2345, col->type->decompose(sstring(fmt::format("v{}", i)))));
            // Scatter the data in a bunch of different sstables, so we
            // can test the registration semaphore of the view update
            // generator
            if (!(i % 10)) {
                ssts.push_back(write_to_sstable(std::exchange(m, mutation(s, key1))));
            }
        }
        ssts.push_back(write_to_sstable(std::move(m)));

        BOOST_REQUIRE_EQUAL(view_update_generator.available_register_units(), db::view::view_update_generator::registration_queue_size);

        auto register_and_check_semaphore = [&view_update_generator, t] (std::vector<shared_sstable>::iterator b, std::vector<shared_sstable>::iterator e) {
            std::vector<future<>> register_futures;
            for (auto it = b; it != e; ++it) {
                register_futures.emplace_back(view_update_generator.register_staging_sstable(*it, t));
            }
            const auto qsz = db::view::view_update_generator::registration_queue_size;
            when_all(register_futures.begin(), register_futures.end()).get();
            REQUIRE_EVENTUALLY_EQUAL<ssize_t>([&] { return view_update_generator.available_register_units(); }, qsz);
        };
        register_and_check_semaphore(ssts.begin(), ssts.begin() + 10);
        register_and_check_semaphore(ssts.begin() + 10, ssts.end());

        auto select_by_p_id = e.prepare("SELECT * FROM t WHERE p = ?").get();
        auto select_by_p_and_c_id = e.prepare("SELECT * FROM t WHERE p = ? and c = ?").get();

        eventually([&, key1, key2] {
            auto msg = e.execute_prepared(select_by_p_id, {key1_raw}).get();
            assert_that(msg).is_rows().with_size(1280);
            msg = e.execute_prepared(select_by_p_id, {key2_raw}).get();
            assert_that(msg).is_rows().with_size(512);
            const auto key1_dv = key1.key().explode().front();

            for (int i = 0; i < 1024; ++i) {
                auto msg = e.execute_prepared(select_by_p_and_c_id, {key1_raw, cql3::raw_value::make_value(serialized(format("c{}", i)))}).get();
                assert_that(msg).is_rows().with_size(1).with_row({
                     {key1_dv},
                     {utf8_type->decompose(sstring(fmt::format("c{}", i)))},
                     {utf8_type->decompose(sstring("x"))}
                 });

            }
            for (int i = 1024; i < 1280; ++i) {
                auto msg = e.execute_prepared(select_by_p_and_c_id, {key1_raw, cql3::raw_value::make_value(serialized(format("c{}", i)))}).get();
                assert_that(msg).is_rows().with_size(1).with_row({
                     {key1_dv},
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

        auto s = test_table_schema();
        const auto key = tests::generate_partition_key(s);
        const auto key1_raw = cql3::raw_value::make_value(key.key().explode().front());

        auto insert_id = e.prepare("insert into t (p, c, v) values (?, ?, 'x')").get();

        for (auto i = 0; i < 1024; ++i) {
            e.execute_prepared(insert_id, {key1_raw, cql3::raw_value::make_value(serialized(format("c{}", i)))}).get();
        }

        // We need data on the disk so that the pre-image reader is forced to go to disk.
        e.db().invoke_on_all([] (replica::database& db) {
            return db.flush_all_memtables();
        }).get();

        auto& view_update_generator = e.local_view_update_generator();

        lw_shared_ptr<replica::table> t = e.local_db().find_column_family("ks", "t").shared_from_this();

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
        auto permit = e.local_db().get_reader_concurrency_semaphore().make_tracking_only_permit(s, "test", db::no_timeout, {});
        sst->write_components(make_mutation_reader_from_mutations_v2(m.schema(), std::move(permit), m), 1ul, s, sst_cfg, {}).get();
        sst->open_data().get();
        t->add_sstable_and_update_cache(sst).get();

        auto& sem = *with_scheduling_group(e.local_db().get_streaming_scheduling_group(), [&] () {
            return &e.local_db().get_reader_concurrency_semaphore();
        }).get();

        // consume all units except what is needed to admit a single reader.
        auto sponge_permit = sem.make_tracking_only_permit(s, "sponge", db::no_timeout, {});
        auto resources = sponge_permit.consume_resources(sem.available_resources() - reader_resources{1, replica::new_reader_base_cost});

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

        auto s = test_table_schema();
        const auto key = tests::generate_partition_key(s);
        const auto key1_raw = cql3::raw_value::make_value(key.key().explode().front());

        auto insert_id = e.prepare("insert into t (p, c, v) values (?, ?, 'x')").get();

        for (auto i = 0; i < 1024; ++i) {
            e.execute_prepared(insert_id, {key1_raw, cql3::raw_value::make_value(serialized(format("c{}", i)))}).get();
        }

        // We need data on the disk so that the pre-image reader is forced to go to disk.
        e.db().invoke_on_all([] (replica::database& db) {
            return db.flush_all_memtables();
        }).get();

        auto& view_update_generator = e.local_view_update_generator();

        lw_shared_ptr<replica::table> t = e.local_db().find_column_family("ks", "t").shared_from_this();

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
            auto permit = e.local_db().get_reader_concurrency_semaphore().make_tracking_only_permit(s, "test", db::no_timeout, {});
            sst->write_components(make_mutation_reader_from_mutations_v2(m.schema(), std::move(permit), m), 1ul, s, sst_cfg, {}).get();
            sst->open_data().get();
            t->add_sstable_and_update_cache(sst).get();
            return sst;
        };

        BOOST_REQUIRE_EQUAL(view_update_generator.queued_batches_count(), 0);

        std::vector<sstables::shared_sstable> prepared_sstables;

        // We need 2 * N sstables. While the initial batch is processed we want
        // to register N more sstables, + 1 to detect the leak (N units will be
        // returned from the initial batch). See below for more details.
        const auto num_sstables = 5 * 2;
        for (auto i = 0; i < num_sstables; ++i) {
            prepared_sstables.push_back(make_sstable());
        }

        BOOST_REQUIRE_EQUAL(view_update_generator.queued_batches_count(), 0);

        for (auto i = 0; i < num_sstables / 2; ++i) {
            auto fut = view_update_generator.register_staging_sstable(std::move(prepared_sstables.back()), t);
            prepared_sstables.pop_back();
            BOOST_REQUIRE(fut.available());
        }

        BOOST_REQUIRE_EQUAL(view_update_generator.queued_batches_count(), 1);

        thread::yield();

        // After the yield above, the first batch should have started processing.
        eventually_true([&] { return view_update_generator.queued_batches_count() == 0; });

        std::vector<future<>> futures;
        futures.reserve(num_sstables);

        // While the first batch is processed, concurrently register the
        // remaining N sstables, yielding in-between so the first batch
        // processing can progress.
        while (!prepared_sstables.empty()) {
            thread::yield();
            futures.emplace_back(view_update_generator.register_staging_sstable(std::move(prepared_sstables.back()), t));
            prepared_sstables.pop_back();
        }

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
            auto permit = _semaphore.obtain_permit(_schema, "consumer_verifier", replica::new_reader_base_cost, db::timeout_clock::now(), {}).get();

            const size_t current_rows = rows_in_mut(mut);
            const auto total_rows = _partition_rows.at(mut.decorated_key());
            _buffer_rows += current_rows;

            testlog.trace("consumer_verifier::check(): key={}, rows={}/{}, _buffer={}",
                    partition_key::with_schema_wrapper(*_schema, mut.key()),
                    current_rows,
                    total_rows,
                    _buffer_rows);

            BOOST_REQUIRE(!mut.partition().empty());
            BOOST_REQUIRE(current_rows <= _max_rows_hard);
            BOOST_REQUIRE(_buffer_rows <= _max_rows_hard);

            // The current partition doesn't have all of its rows yet, verify
            // that the new mutation contains the next rows for the same
            // partition
            if (!_collected_muts.empty() && _collected_muts.back().decorated_key().equal(*mut.schema(), mut.decorated_key())) {
                if (rows_in_mut(_collected_muts.back()) && rows_in_mut(mut)) {
                    const auto& previous_ckey = (--_collected_muts.back().partition().clustered_rows().end())->key();
                    const auto& next_ckey = mut.partition().clustered_rows().begin()->key();
                    BOOST_REQUIRE(_less_cmp(previous_ckey, next_ckey));
                }
                mutation_application_stats stats;
                _collected_muts.back().partition().apply(*_schema, mut.partition(), *mut.schema(), stats);
            // The new mutation is a new partition.
            } else {
                if (!_collected_muts.empty()) {
                    BOOST_REQUIRE(rows_in_mut(_collected_muts.back()) == _partition_rows.at(_collected_muts.back().decorated_key()));
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
            , _max_rows_soft(rows_in_limit(db::view::view_updating_consumer::buffer_size_soft_limit_default))
            , _max_rows_hard(rows_in_limit(db::view::view_updating_consumer::buffer_size_hard_limit_default))
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

    reader_concurrency_semaphore sem(reader_concurrency_semaphore::for_tests{}, get_name(), 1, replica::new_reader_base_cost);
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
    auto pkeys = std::views::iota(size_t{0}, max_partition_set_size) | std::views::transform([schema] (int i) {
        return dht::decorate_key(*schema, partition_key::from_single_value(*schema, int32_type->decompose(data_value(i))));
    }) | std::ranges::to<std::vector>();
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
            // Reproduces #14503
            mut_desc.add_range_tombstone(interval<tests::data_model::mutation_description::key>::make_open_ended_both_sides());
            muts.push_back(mut_desc.build(schema));
            partition_rows.emplace(muts.back().decorated_key(), partition_size_100kb);
        }

        std::ranges::sort(muts, [less = dht::ring_position_less_comparator(*schema)] (const mutation& a, const mutation& b) {
            return less(a.decorated_key(), b.decorated_key());
        });

        auto permit = sem.obtain_permit(schema, get_name(), replica::new_reader_base_cost, db::no_timeout, {}).get();

        auto mt = make_memtable(schema, muts);
        auto p = make_manually_paused_evictable_reader_v2(
                mt->as_data_source(),
                schema,
                permit,
                query::full_partition_range,
                schema->full_slice(),
                nullptr,
                ::mutation_reader::forwarding::no);
        auto& staging_reader = std::get<0>(p);
        auto& staging_reader_handle = std::get<1>(p);
        auto close_staging_reader = deferred_close(staging_reader);

        std::vector<mutation> collected_muts;
        bool ok = true;

        staging_reader.consume_in_thread(db::view::view_updating_consumer(schema, permit, as, staging_reader_handle,
                    consumer_verifier(schema, sem, partition_rows, collected_muts, ok)));

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
        BOOST_REQUIRE(e.get_system_keyspace().local().load_view_build_progress().get().empty());
    });
}

// A random mutation test for view_updating_consumer's buffering logic.
// Passes random mutations through a view_updating_consumer with a extremely
// small buffer, which should cause a buffer flush after every mutation fragment.
// Should check that flushing works correctly in every position, and regardless
// of the last fragment and the last range tombstone change,
//
// Inspired by #14503.
SEASTAR_THREAD_TEST_CASE(test_view_update_generator_buffering_with_random_mutations) {
    // Collects the mutations produced by the tested view_updating_consumer into a vector.
    class consumer_verifier {
        schema_ptr _schema;
        std::vector<mutation>& _collected_muts;
        std::unique_ptr<row_locker> _rl;
        std::unique_ptr<row_locker::stats> _rl_stats;
        bool& _ok;

    private:
        void check(mutation mut) {
            BOOST_REQUIRE(!mut.partition().empty());
            _collected_muts.push_back(std::move(mut));
        }

    public:
        consumer_verifier(schema_ptr schema, std::vector<mutation>& collected_muts, bool& ok)
            : _schema(std::move(schema))
            , _collected_muts(collected_muts)
            , _rl(std::make_unique<row_locker>(_schema))
            , _rl_stats(std::make_unique<row_locker::stats>())
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

    // Create a random mutation.
    // We don't really want a random `mutation`, but a random valid mutation fragment
    // stream. But I don't know a better way to get that other than to create a random
    // `mutation` and shove it through readers.
    random_mutation_generator gen(random_mutation_generator::generate_counters::no);
    mutation mut = gen();
    schema_ptr schema = gen.schema();

    // Turn the random mutation into a mutation fragment stream,
    // so it can be fed to the view_updating_consumer.
    // Quite verbose. Perhaps there exists a simpler way to do this.
    reader_concurrency_semaphore sem(reader_concurrency_semaphore::for_tests{}, get_name(), 1, replica::new_reader_base_cost);
    auto stop_sem = deferred_stop(sem);
    const abort_source as;
    auto mt = make_memtable(schema, {mut});
    auto permit = sem.obtain_permit(schema, get_name(), replica::new_reader_base_cost, db::no_timeout, {}).get();
    auto p = make_manually_paused_evictable_reader_v2(
            mt->as_data_source(),
            schema,
            permit,
            query::full_partition_range,
            schema->full_slice(),
            nullptr,
            ::mutation_reader::forwarding::no);
    auto& staging_reader = std::get<0>(p);
    auto& staging_reader_handle = std::get<1>(p);
    auto close_staging_reader = deferred_close(staging_reader);

    // Feed the random valid mutation fragment stream to the view_updating_consumer,
    // and collect its outputs.
    std::vector<mutation> collected_muts;
    bool ok = true;
    auto vuc = db::view::view_updating_consumer(schema, permit, as, staging_reader_handle,
                    consumer_verifier(schema, collected_muts, ok));
    vuc.set_buffer_size_limit_for_testing_purposes(1);
    staging_reader.consume_in_thread(std::move(vuc));

    // Check that the outputs sum up to the initial mutation.
    // We could also check that they are non-overlapping, which is
    // expected from the view_updating_consumer flushes, but it's
    // not necessary for correctness.
    BOOST_REQUIRE(ok);
    mutation total(schema, mut.decorated_key());
    for (const auto& x : collected_muts) {
        total += x;
    }
    assert_that(total).is_equal_to_compacted(mut);
}

// Reproducer for #14819
// Push an partition containing only a tombstone to the view update generator
// (with soft limit set to 1) and expect it to trigger flushing the buffer on
// finishing the partition.
SEASTAR_THREAD_TEST_CASE(test_view_update_generator_buffering_with_empty_mutations) {
    class consumer_verifier {
        std::unique_ptr<row_locker> _rl;
        std::unique_ptr<row_locker::stats> _rl_stats;
        std::optional<dht::decorated_key> _last_dk;
        bool& _buffer_flushed;

    public:
        consumer_verifier(schema_ptr schema, bool& buffer_flushed)
            : _rl(std::make_unique<row_locker>(std::move(schema)))
            , _rl_stats(std::make_unique<row_locker::stats>())
            , _buffer_flushed(buffer_flushed)
        { }
        future<row_locker::lock_holder> operator()(mutation mut) {
            _buffer_flushed = true;
            _last_dk = mut.decorated_key();
            return _rl->lock_pk(*_last_dk, true, db::no_timeout, *_rl_stats);
        }
    };

    simple_schema ss;
    auto schema = ss.schema();
    reader_concurrency_semaphore sem(reader_concurrency_semaphore::for_tests{}, get_name(), 1, replica::new_reader_base_cost);
    auto stop_sem = deferred_stop(sem);
    auto permit = sem.make_tracking_only_permit(schema, "test", db::no_timeout, {});
    abort_source as;
    auto [staging_reader, staging_reader_handle] = make_manually_paused_evictable_reader_v2(make_empty_mutation_source(), schema, permit,
            query::full_partition_range, schema->full_slice(), {}, mutation_reader::forwarding::no);
    auto close_staging_reader = deferred_close(staging_reader);
    bool buffer_flushed = false;

    auto vuc = db::view::view_updating_consumer(schema, permit, as, staging_reader_handle, consumer_verifier(schema, buffer_flushed));
    vuc.set_buffer_size_limit_for_testing_purposes(1);

    vuc.consume_new_partition(ss.make_pkey(0));
    vuc.consume(ss.new_tombstone());
    vuc.consume_end_of_partition();

    // consume_end_of_stream() forces a flush, so we need to check before it.
    BOOST_REQUIRE(buffer_flushed);

    vuc.consume_end_of_stream();
}

BOOST_AUTO_TEST_SUITE_END()
