/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include <fmt/ranges.h>

#include <seastar/core/sstring.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/align.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/util/closeable.hh>

#include "sstables/sstables.hh"
#include "sstables/compress.hh"
#include "sstables/metadata_collector.hh"
#include <seastar/testing/thread_test_case.hh>
#include "schema/schema.hh"
#include "schema/schema_builder.hh"
#include "replica/database.hh"
#include "sstables/sstable_writer.hh"
#include <memory>
#include "test/boost/sstable_test.hh"
#include <seastar/core/seastar.hh>
#include <seastar/core/do_with.hh>
#include <seastar/testing/test_case.hh>
#include "dht/i_partitioner.hh"
#include "test/lib/mutation_reader_assertions.hh"
#include "test/lib/mutation_assertions.hh"
#include "counters.hh"
#include "test/lib/index_reader_assertions.hh"
#include "test/lib/make_random_string.hh"
#include "test/lib/simple_schema.hh"
#include "dht/ring_position.hh"
#include "partition_slice_builder.hh"
#include "replica/memtable-sstable.hh"

#include <stdio.h>
#include <ftw.h>
#include <unistd.h>
#include <boost/range/algorithm/find_if.hpp>
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/algorithm/cxx11/is_sorted.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/icl/interval_map.hpp>
#include "test/lib/sstable_utils.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/test_utils.hh"
#include "readers/from_mutations_v2.hh"
#include "readers/from_fragments_v2.hh"
#include "test/lib/random_schema.hh"
#include "test/lib/exception_utils.hh"

namespace fs = std::filesystem;

using namespace sstables;

static const sstring some_keyspace("ks");
static const sstring some_column_family("cf");

atomic_cell make_atomic_cell(data_type dt, bytes_view value, uint32_t ttl = 0, uint32_t expiration = 0) {
    if (ttl) {
        return atomic_cell::make_live(*dt, 0, value,
            gc_clock::time_point(gc_clock::duration(expiration)), gc_clock::duration(ttl));
    } else {
        return atomic_cell::make_live(*dt, 0, value);
    }
}

atomic_cell make_dead_atomic_cell(uint32_t deletion_time) {
    return atomic_cell::make_dead(0, gc_clock::time_point(gc_clock::duration(deletion_time)));
}

static void assert_sstable_set_size(const sstable_set& s, size_t expected_size) {
    BOOST_REQUIRE(s.size() == expected_size && s.size() == s.all()->size());
}

SEASTAR_TEST_CASE(datafile_generation_09) {
    // Test that generated sstable components can be successfully loaded.
    return test_env::do_with_async([] (test_env& env) {
        auto s = make_shared_schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}}, {}, utf8_type);

        const column_definition& r1_col = *s->get_column_definition("r1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

        mutation m(s, key);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));

        auto sst = make_sstable_containing(env.make_sstable(s), {std::move(m)});
        auto sst2 = env.reusable_sst(sst).get();

        sstables::test(sst2).read_summary().get();
        summary& sst1_s = sstables::test(sst).get_summary();
        summary& sst2_s = sstables::test(sst2).get_summary();

        BOOST_REQUIRE(::memcmp(&sst1_s.header, &sst2_s.header, sizeof(summary::header)) == 0);
        BOOST_REQUIRE(sst1_s.positions == sst2_s.positions);
        BOOST_REQUIRE(sst1_s.entries == sst2_s.entries);
        BOOST_REQUIRE(sst1_s.first_key.value == sst2_s.first_key.value);
        BOOST_REQUIRE(sst1_s.last_key.value == sst2_s.last_key.value);

        sstables::test(sst2).read_toc().get();
        auto& sst1_c = sstables::test(sst).get_components();
        auto& sst2_c = sstables::test(sst2).get_components();

        BOOST_REQUIRE(sst1_c == sst2_c);
    });
}

SEASTAR_TEST_CASE(datafile_generation_11) {
    return test_env::do_with_async([] (test_env& env) {
        auto s = complex_schema();

        const column_definition& set_col = *s->get_column_definition("reg_set");
        const column_definition& static_set_col = *s->get_column_definition("static_collection");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("c1"), to_bytes("c2")});

        mutation m(s, key);

        tombstone tomb(api::new_timestamp(), gc_clock::now());
        collection_mutation_description set_mut;
        set_mut.tomb = tomb;
        set_mut.cells.emplace_back(to_bytes("1"), make_atomic_cell(bytes_type, {}));
        set_mut.cells.emplace_back(to_bytes("2"), make_atomic_cell(bytes_type, {}));
        set_mut.cells.emplace_back(to_bytes("3"), make_atomic_cell(bytes_type, {}));

        m.set_clustered_cell(c_key, set_col, set_mut.serialize(*set_col.type));

        m.set_static_cell(static_set_col, set_mut.serialize(*static_set_col.type));

        auto key2 = partition_key::from_exploded(*s, {to_bytes("key2")});
        mutation m2(s, key2);
        collection_mutation_description set_mut_single;
        set_mut_single.cells.emplace_back(to_bytes("4"), make_atomic_cell(bytes_type, {}));

        m2.set_clustered_cell(c_key, set_col, set_mut_single.serialize(*set_col.type));

        auto mt = make_memtable(s, {std::move(m), std::move(m2)});

        auto verifier = [s, set_col, c_key] (auto& mutation) {
            auto& mp = mutation->partition();
            BOOST_REQUIRE(mp.clustered_rows().calculate_size() == 1);
            auto r = mp.find_row(*s, c_key);
            BOOST_REQUIRE(r);
            BOOST_REQUIRE(r->size() == 1);
            auto cell = r->find_cell(set_col.id);
            BOOST_REQUIRE(cell);
            return cell->as_collection_mutation().with_deserialized(*set_col.type, [&] (collection_mutation_view_description m) {
                return m.materialize(*set_col.type);
            });
        };

        auto sstp = verify_mutation(env, env.make_sstable(s), mt, "key1", [&] (mutation_opt& mutation) {
            auto verify_set = [&tomb] (const collection_mutation_description& m) {
                BOOST_REQUIRE(bool(m.tomb) == true);
                BOOST_REQUIRE(m.tomb == tomb);
                BOOST_REQUIRE(m.cells.size() == 3);
                BOOST_REQUIRE(m.cells[0].first == to_bytes("1"));
                BOOST_REQUIRE(m.cells[1].first == to_bytes("2"));
                BOOST_REQUIRE(m.cells[2].first == to_bytes("3"));
            };

            auto& mp = mutation->partition();
            auto& ssr = mp.static_row();
            auto scol = ssr.find_cell(static_set_col.id);
            BOOST_REQUIRE(scol);

            // The static set
            scol->as_collection_mutation().with_deserialized(*static_set_col.type, [&] (collection_mutation_view_description mut) {
                verify_set(mut.materialize(*static_set_col.type));
            });

            // The clustered set
            auto m = verifier(mutation);
            verify_set(m);
        });

        verify_mutation(env, sstp, "key2", [&] (mutation_opt& mutation) {
            auto m = verifier(mutation);
            BOOST_REQUIRE(!m.tomb);
            BOOST_REQUIRE(m.cells.size() == 1);
            BOOST_REQUIRE(m.cells[0].first == to_bytes("4"));
        });
    });
}

SEASTAR_TEST_CASE(datafile_generation_12) {
    return test_env::do_with_async([] (test_env& env) {
        auto s = complex_schema();

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto cp = clustering_key_prefix::from_exploded(*s, {to_bytes("c1")});

        mutation m(s, key);

        tombstone tomb(api::new_timestamp(), gc_clock::now());
        m.partition().apply_delete(*s, cp, tomb);

        auto mt = make_memtable(s, {std::move(m)});

        verify_mutation(env, env.make_sstable(s), mt, "key1", [&] (mutation_opt& mutation) {
            auto& mp = mutation->partition();
            BOOST_REQUIRE(mp.row_tombstones().size() == 1);
            for (auto& rt: mp.row_tombstones()) {
                BOOST_REQUIRE(rt.tombstone().tomb == tomb);
            }
        });
    });
}

static future<> sstable_compression_test(compressor_ptr c) {
    return test_env::do_with_async([c] (test_env& env) {
        // NOTE: set a given compressor algorithm to schema.
        schema_builder builder(complex_schema());
        builder.set_compressor_params(c);
        auto s = builder.build(schema_builder::compact_storage::no);

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto cp = clustering_key_prefix::from_exploded(*s, {to_bytes("c1")});

        mutation m(s, key);

        tombstone tomb(api::new_timestamp(), gc_clock::now());
        m.partition().apply_delete(*s, cp, tomb);
        auto mtp = make_memtable(s, {std::move(m)});

        verify_mutation(env, env.make_sstable(s), mtp, "key1", [&] (mutation_opt& mutation) {
            auto& mp = mutation->partition();
            BOOST_REQUIRE(mp.row_tombstones().size() == 1);
            for (auto& rt: mp.row_tombstones()) {
                BOOST_REQUIRE(rt.tombstone().tomb == tomb);
            }
        });
    });
}

SEASTAR_TEST_CASE(datafile_generation_13) {
    return sstable_compression_test(compressor::lz4);
}

SEASTAR_TEST_CASE(datafile_generation_14) {
    return sstable_compression_test(compressor::snappy);
}

SEASTAR_TEST_CASE(datafile_generation_15) {
    return sstable_compression_test(compressor::deflate);
}

future<> test_datafile_generation_16(test_env_config cfg) {
    return test_env::do_with_async([] (test_env& env) {
        auto s = uncompressed_schema();

        auto mtp = make_lw_shared<replica::memtable>(s);
        // Create a number of keys that is a multiple of the sampling level
        for (int i = 0; i < 0x80; ++i) {
            sstring k = "key" + to_sstring(i);
            auto key = partition_key::from_exploded(*s, {to_bytes(k)});
            mutation m(s, key);

            auto c_key = clustering_key::make_empty();
            m.set_clustered_cell(c_key, to_bytes("col2"), i, api::max_timestamp);
            mtp->apply(std::move(m));
        }

        auto sst = make_sstable_containing(env.make_sstable(s), mtp);
        // Not crashing is enough
        BOOST_REQUIRE(sst);
        sst->destroy().get();
    }, std::move(cfg));
}

SEASTAR_TEST_CASE(datafile_generation_16) {
    return test_datafile_generation_16({});
}

SEASTAR_TEST_CASE(datafile_generation_16_s3, *boost::unit_test::precondition(tests::has_scylla_test_env)) {
    return test_datafile_generation_16(test_env_config{ .storage = make_test_object_storage_options() });
}

// mutation_reader for sstable keeping all the required objects alive.
static mutation_reader sstable_reader_v2(shared_sstable sst, schema_ptr s, reader_permit permit) {
    return sst->as_mutation_source().make_reader_v2(s, std::move(permit), query::full_partition_range, s->full_slice());

}

static mutation_reader sstable_reader_v2(shared_sstable sst, schema_ptr s, reader_permit permit, const dht::partition_range& pr) {
    return sst->as_mutation_source().make_reader_v2(s, std::move(permit), pr, s->full_slice());
}

SEASTAR_TEST_CASE(datafile_generation_37) {
    return test_env::do_with_async([] (test_env& env) {
        auto s = compact_simple_dense_schema();

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        mutation m(s, key);

        auto c_key = clustering_key_prefix::from_exploded(*s, {to_bytes("cl1")});
        const column_definition& cl2 = *s->get_column_definition("cl2");

        m.set_clustered_cell(c_key, cl2, make_atomic_cell(bytes_type, bytes_type->decompose(data_value(to_bytes("cl2")))));
        auto mtp = make_memtable(s, {std::move(m)});

        verify_mutation(env, env.make_sstable(s), mtp, "key1", [&] (mutation_opt& mutation) {
            auto& mp = mutation->partition();

            auto clustering = clustering_key_prefix::from_exploded(*s, {to_bytes("cl1")});

            auto& row = mp.clustered_row(*s, clustering);
            match_live_cell(row.cells(), *s, "cl2", data_value(to_bytes("cl2")));
        });
    });
}

SEASTAR_TEST_CASE(datafile_generation_38) {
    return test_env::do_with_async([] (test_env& env) {
        auto s = compact_dense_schema();

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        mutation m(s, key);

        auto c_key = clustering_key_prefix::from_exploded(*s, {to_bytes("cl1"), to_bytes("cl2")});

        const column_definition& cl3 = *s->get_column_definition("cl3");
        m.set_clustered_cell(c_key, cl3, make_atomic_cell(bytes_type, bytes_type->decompose(data_value(to_bytes("cl3")))));
        auto mtp = make_memtable(s, {std::move(m)});

        verify_mutation(env, env.make_sstable(s), mtp, "key1", [&] (mutation_opt& mutation) {
            auto& mp = mutation->partition();
            auto clustering = clustering_key_prefix::from_exploded(*s, {to_bytes("cl1"), to_bytes("cl2")});

            auto& row = mp.clustered_row(*s, clustering);
            match_live_cell(row.cells(), *s, "cl3", data_value(to_bytes("cl3")));
        });
    });
}

SEASTAR_TEST_CASE(datafile_generation_39) {
    return test_env::do_with_async([] (test_env& env) {
        auto s = compact_sparse_schema();

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        mutation m(s, key);

        auto c_key = clustering_key::make_empty();

        const column_definition& cl1 = *s->get_column_definition("cl1");
        m.set_clustered_cell(c_key, cl1, make_atomic_cell(bytes_type, bytes_type->decompose(data_value(to_bytes("cl1")))));
        const column_definition& cl2 = *s->get_column_definition("cl2");
        m.set_clustered_cell(c_key, cl2, make_atomic_cell(bytes_type, bytes_type->decompose(data_value(to_bytes("cl2")))));
        auto mtp = make_memtable(s, {std::move(m)});

        verify_mutation(env, env.make_sstable(s), mtp, "key1", [&] (mutation_opt& mutation) {
            auto& mp = mutation->partition();
            auto& row = mp.clustered_row(*s, clustering_key::make_empty());
            match_live_cell(row.cells(), *s, "cl1", data_value(data_value(to_bytes("cl1"))));
            match_live_cell(row.cells(), *s, "cl2", data_value(data_value(to_bytes("cl2"))));
        });
    });
}

SEASTAR_TEST_CASE(datafile_generation_41) {
    return test_env::do_with_async([] (test_env& env) {
        auto s = make_shared_schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}, {"r2", int32_type}}, {}, utf8_type);

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("c1")});
        mutation m(s, key);

        tombstone tomb(api::new_timestamp(), gc_clock::now());
        m.partition().apply_delete(*s, std::move(c_key), tomb);
        auto mt = make_memtable(s, {std::move(m)});

        verify_mutation(env, env.make_sstable(s), mt, "key1", [&] (mutation_opt& mutation) {
            auto& mp = mutation->partition();
            BOOST_REQUIRE(mp.clustered_rows().calculate_size() == 1);
            auto& c_row = *(mp.clustered_rows().begin());
            BOOST_REQUIRE(c_row.row().deleted_at().tomb() == tomb);
        });
    });
}

SEASTAR_TEST_CASE(datafile_generation_47) {
    // Tests the problem in which the sstable row parser would hang.
    return test_env::do_with_async([] (test_env& env) {
        auto s = make_shared_schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", utf8_type}}, {}, utf8_type);

        const column_definition& r1_col = *s->get_column_definition("r1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("c1")});
        mutation m(s, key);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(utf8_type, bytes(512*1024, 'a')));

        auto sstp = make_sstable_containing(env.make_sstable(s), {std::move(m)});
        auto reader = sstable_reader_v2(sstp, s, env.make_reader_permit());
        auto close_reader = deferred_close(reader);
        while (reader().get()) {
        }
    });
}

SEASTAR_TEST_CASE(test_counter_write) {
    return test_env::do_with_async([] (test_env& env) {
        auto s = schema_builder(some_keyspace, some_column_family)
                .with_column("p1", utf8_type, column_kind::partition_key)
                .with_column("c1", utf8_type, column_kind::clustering_key)
                .with_column("r1", counter_type)
                .with_column("r2", counter_type)
                .build();
        auto& r1_col = *s->get_column_definition("r1");
        auto& r2_col = *s->get_column_definition("r2");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("c1")});
        auto c_key2 = clustering_key::from_exploded(*s, {to_bytes("c2")});

        mutation m(s, key);

        std::vector<counter_id> ids;
        std::generate_n(std::back_inserter(ids), 3, counter_id::create_random_id);
        boost::range::sort(ids);

        counter_cell_builder b1;
        b1.add_shard(counter_shard(ids[0], 5, 1));
        b1.add_shard(counter_shard(ids[1], -4, 1));
        b1.add_shard(counter_shard(ids[2], 9, 1));
        auto ts = api::new_timestamp();
        m.set_clustered_cell(c_key, r1_col, b1.build(ts));

        counter_cell_builder b2;
        b2.add_shard(counter_shard(ids[1], -1, 1));
        b2.add_shard(counter_shard(ids[2], 2, 1));
        m.set_clustered_cell(c_key, r2_col, b2.build(ts));

        m.set_clustered_cell(c_key2, r1_col, make_dead_atomic_cell(1));

        auto sstp = make_sstable_containing(env.make_sstable(s), {m});
        assert_that(sstable_reader_v2(sstp, s, env.make_reader_permit()))
            .produces(m)
            .produces_end_of_stream();
    });
}

static shared_sstable sstable_for_overlapping_test(test_env& env, const schema_ptr& schema,
        const partition_key& first_key, const partition_key& last_key, uint32_t level = 0) {
    auto sst = env.make_sstable(schema);
    sstables::test(sst).set_values_for_leveled_strategy(1 /* data_size */, level, 0 /* max_timestamp */, first_key, last_key);
    return sst;
}

SEASTAR_TEST_CASE(check_read_indexes) {
    return test_env::do_with([] (test_env& env) {
        return for_each_sstable_version([&env] (const sstables::sstable::version_types version) {
            return seastar::async([&env, version] {
                auto builder = schema_builder("test", "summary_test")
                    .with_column("a", int32_type, column_kind::partition_key);
                builder.set_min_index_interval(256);
                auto s = builder.build();

                auto sst = env.reusable_sst(s, get_test_dir("summary_test", s), 1, version).get();
                    auto list = sstables::test(sst).read_indexes(env.make_reader_permit()).get();
                        BOOST_REQUIRE(list.size() == 130);
            });
        });
    });
}

SEASTAR_TEST_CASE(check_multi_schema) {
    // Schema used to write sstable:
    // CREATE TABLE multi_schema_test (
    //        a int PRIMARY KEY,
    //        b int,
    //        c int,
    //        d set<int>,
    //        e int
    //);

    // Schema used to read sstable:
    // CREATE TABLE multi_schema_test (
    //        a int PRIMARY KEY,
    //        c set<int>,
    //        d int,
    //        e blob
    //);
    return test_env::do_with([] (test_env& env) {
        return for_each_sstable_version([&env] (const sstables::sstable::version_types version) {
            return seastar::async([&env, version] {
                auto set_of_ints_type = set_type_impl::get_instance(int32_type, true);
                auto builder = schema_builder("test", "test_multi_schema")
                    .with_column("a", int32_type, column_kind::partition_key)
                    .with_column("c", set_of_ints_type)
                    .with_column("d", int32_type)
                    .with_column("e", bytes_type);
                auto s = builder.build();

                auto sst = env.reusable_sst(s, get_test_dir("multi_schema_test", s), 1, version).get();
                auto reader = sstable_reader_v2(sst, s, env.make_reader_permit());
                auto close_reader = deferred_close(reader);
                std::invoke([&] {
                    mutation_opt m = read_mutation_from_mutation_reader(reader).get();
                    BOOST_REQUIRE(m);
                    BOOST_REQUIRE(m->key().equal(*s, partition_key::from_singular(*s, 0)));
                    auto rows = m->partition().clustered_rows();
                    BOOST_REQUIRE_EQUAL(rows.calculate_size(), 1);
                    auto& row = rows.begin()->row();
                    BOOST_REQUIRE(!row.deleted_at());
                    auto& cells = row.cells();
                    BOOST_REQUIRE_EQUAL(cells.size(), 1);
                    auto& cdef = *s->get_column_definition("e");
                    BOOST_REQUIRE_EQUAL(cells.cell_at(cdef.id).as_atomic_cell(cdef).value(), managed_bytes(int32_type->decompose(5)));
                });

                std::invoke([&] {
                    auto m = reader().get();
                    BOOST_REQUIRE(!m);
                });
            });
        });
    });
}

void test_sliced_read_row_presence(shared_sstable sst, schema_ptr s, reader_permit permit, const query::partition_slice& ps,
    std::vector<std::pair<partition_key, std::vector<clustering_key>>> expected)
{
    auto reader = sst->as_mutation_source().make_reader_v2(s, std::move(permit), query::full_partition_range, ps);
    auto close_reader = deferred_close(reader);

    partition_key::equality pk_eq(*s);
    clustering_key::equality ck_eq(*s);

    auto mfopt = reader().get();
    while (mfopt) {
        BOOST_REQUIRE(mfopt->is_partition_start());
        auto it = std::find_if(expected.begin(), expected.end(), [&] (auto&& x) {
            return pk_eq(x.first, mfopt->as_partition_start().key().key());
        });
        BOOST_REQUIRE(it != expected.end());
        auto expected_cr = std::move(it->second);
        expected.erase(it);

        mfopt = reader().get();
        BOOST_REQUIRE(mfopt);
        while (!mfopt->is_end_of_partition()) {
            if (mfopt->is_clustering_row()) {
                auto& cr = mfopt->as_clustering_row();
                auto it = std::find_if(expected_cr.begin(), expected_cr.end(), [&] (auto&& x) {
                    return ck_eq(x, cr.key());
                });
                if (it == expected_cr.end()) {
                    fmt::print(std::cout, "unexpected clustering row: {}\n", cr.key());
                }
                BOOST_REQUIRE(it != expected_cr.end());
                expected_cr.erase(it);
            }
            mfopt = reader().get();
            BOOST_REQUIRE(mfopt);
        }
        BOOST_REQUIRE(expected_cr.empty());

        mfopt = reader().get();
    }
    BOOST_REQUIRE(expected.empty());
}

SEASTAR_TEST_CASE(test_sliced_mutation_reads) {
    // CREATE TABLE sliced_mutation_reads_test (
    //        pk int,
    //        ck int,
    //        v1 int,
    //        v2 set<int>,
    //        PRIMARY KEY (pk, ck)
    //);
    //
    // insert into sliced_mutation_reads_test (pk, ck, v1) values (0, 0, 1);
    // insert into sliced_mutation_reads_test (pk, ck, v2) values (0, 1, { 0, 1 });
    // update sliced_mutation_reads_test set v1 = 3 where pk = 0 and ck = 2;
    // insert into sliced_mutation_reads_test (pk, ck, v1) values (0, 3, null);
    // insert into sliced_mutation_reads_test (pk, ck, v2) values (0, 4, null);
    // insert into sliced_mutation_reads_test (pk, ck, v1) values (1, 1, 1);
    // insert into sliced_mutation_reads_test (pk, ck, v1) values (1, 3, 1);
    // insert into sliced_mutation_reads_test (pk, ck, v1) values (1, 5, 1);
    return test_env::do_with_async([] (test_env& env) {
      for (auto version : all_sstable_versions) {
        auto set_of_ints_type = set_type_impl::get_instance(int32_type, true);
        auto builder = schema_builder("ks", "sliced_mutation_reads_test")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("v1", int32_type)
            .with_column("v2", set_of_ints_type);
        auto s = builder.build();

        auto sst = env.reusable_sst(s, get_test_dir("sliced_mutation_reads", s), 1, version).get();

        {
            auto ps = partition_slice_builder(*s)
                          .with_range(query::clustering_range::make_singular(
                              clustering_key_prefix::from_single_value(*s, int32_type->decompose(0))))
                          .with_range(query::clustering_range::make_singular(
                              clustering_key_prefix::from_single_value(*s, int32_type->decompose(5))))
                          .build();
            test_sliced_read_row_presence(sst, s, env.make_reader_permit(), ps, {
                std::make_pair(partition_key::from_single_value(*s, int32_type->decompose(0)),
                    std::vector<clustering_key> { clustering_key_prefix::from_single_value(*s, int32_type->decompose(0)) }),
                std::make_pair(partition_key::from_single_value(*s, int32_type->decompose(1)),
                    std::vector<clustering_key> { clustering_key_prefix::from_single_value(*s, int32_type->decompose(5)) }),
            });
        }
        {
            auto ps = partition_slice_builder(*s)
                          .with_range(query::clustering_range {
                             query::clustering_range::bound { clustering_key_prefix::from_single_value(*s, int32_type->decompose(0)) },
                             query::clustering_range::bound { clustering_key_prefix::from_single_value(*s, int32_type->decompose(3)), false },
                          }).build();
            test_sliced_read_row_presence(sst, s, env.make_reader_permit(), ps, {
                std::make_pair(partition_key::from_single_value(*s, int32_type->decompose(0)),
                    std::vector<clustering_key> {
                        clustering_key_prefix::from_single_value(*s, int32_type->decompose(0)),
                        clustering_key_prefix::from_single_value(*s, int32_type->decompose(1)),
                        clustering_key_prefix::from_single_value(*s, int32_type->decompose(2)),
                    }),
                std::make_pair(partition_key::from_single_value(*s, int32_type->decompose(1)),
                    std::vector<clustering_key> { clustering_key_prefix::from_single_value(*s, int32_type->decompose(1)) }),
            });
        }
        {
            auto ps = partition_slice_builder(*s)
                          .with_range(query::clustering_range {
                             query::clustering_range::bound { clustering_key_prefix::from_single_value(*s, int32_type->decompose(3)) },
                             query::clustering_range::bound { clustering_key_prefix::from_single_value(*s, int32_type->decompose(9)) },
                          }).build();
            test_sliced_read_row_presence(sst, s, env.make_reader_permit(), ps, {
                std::make_pair(partition_key::from_single_value(*s, int32_type->decompose(0)),
                    std::vector<clustering_key> {
                        clustering_key_prefix::from_single_value(*s, int32_type->decompose(3)),
                        clustering_key_prefix::from_single_value(*s, int32_type->decompose(4)),
                    }),
                std::make_pair(partition_key::from_single_value(*s, int32_type->decompose(1)),
                    std::vector<clustering_key> {
                        clustering_key_prefix::from_single_value(*s, int32_type->decompose(3)),
                        clustering_key_prefix::from_single_value(*s, int32_type->decompose(5)),
                    }),
            });
        }
      }
    });
}

SEASTAR_TEST_CASE(test_wrong_range_tombstone_order) {
    // create table wrong_range_tombstone_order (
    //        p int,
    //        a int,
    //        b int,
    //        c int,
    //        r int,
    //        primary key (p,a,b,c)
    // ) with compact storage;
    //
    // delete from wrong_range_tombstone_order where p = 0 and a = 0;
    // insert into wrong_range_tombstone_order (p,a,r) values (0,1,1);
    // insert into wrong_range_tombstone_order (p,a,b,r) values (0,1,1,2);
    // insert into wrong_range_tombstone_order (p,a,b,r) values (0,1,2,3);
    // insert into wrong_range_tombstone_order (p,a,b,c,r) values (0,1,2,3,4);
    // delete from wrong_range_tombstone_order where p = 0 and a = 1 and b = 3;
    // insert into wrong_range_tombstone_order (p,a,b,r) values (0,1,3,5);
    // insert into wrong_range_tombstone_order (p,a,b,c,r) values (0,1,3,4,6);
    // insert into wrong_range_tombstone_order (p,a,b,r) values (0,1,4,7);
    // insert into wrong_range_tombstone_order (p,a,b,c,r) values (0,1,4,0,8);
    // delete from wrong_range_tombstone_order where p = 0 and a = 1 and b = 4 and c = 0;
    // delete from wrong_range_tombstone_order where p = 0 and a = 2;
    // delete from wrong_range_tombstone_order where p = 0 and a = 2 and b = 1;
    // delete from wrong_range_tombstone_order where p = 0 and a = 2 and b = 2;

    return test_env::do_with_async([] (test_env& env) {
      for (const auto version : all_sstable_versions) {
        auto s = schema_builder("ks", "wrong_range_tombstone_order")
            .with(schema_builder::compact_storage::yes)
            .with_column("p", int32_type, column_kind::partition_key)
            .with_column("a", int32_type, column_kind::clustering_key)
            .with_column("b", int32_type, column_kind::clustering_key)
            .with_column("c", int32_type, column_kind::clustering_key)
            .with_column("r", int32_type)
            .build();
        clustering_key::equality ck_eq(*s);
        auto pkey = partition_key::from_exploded(*s, { int32_type->decompose(0) });
        auto dkey = dht::decorate_key(*s, std::move(pkey));

        auto sst = env.reusable_sst(s, get_test_dir("wrong_range_tombstone_order", s), 1, version).get();
        auto reader = sstable_reader_v2(sst, s, env.make_reader_permit());

        using kind = mutation_fragment_v2::kind;
        assert_that(std::move(reader))
            .produces_partition_start(dkey)
            .produces(kind::range_tombstone_change, { 0 })
            .produces(kind::range_tombstone_change, { 0 })
            .produces(kind::clustering_row, { 1 })
            .produces(kind::clustering_row, { 1, 1 })
            .produces(kind::clustering_row, { 1, 2 })
            .produces(kind::clustering_row, { 1, 2, 3 })
            .produces(kind::range_tombstone_change, { 1, 3 })
            .produces(kind::clustering_row, { 1, 3 })
            .produces(kind::clustering_row, { 1, 3, 4 })
            .produces(kind::range_tombstone_change, { 1, 3 })
            .produces(kind::clustering_row, { 1, 4 })
            .produces(kind::clustering_row, { 1, 4, 0 })
            .produces(kind::range_tombstone_change, { 2 })
            .produces(kind::range_tombstone_change, { 2, 1 })
            .produces(kind::range_tombstone_change, { 2, 1 })
            .produces(kind::range_tombstone_change, { 2, 2 })
            .produces(kind::range_tombstone_change, { 2, 2 })
            .produces(kind::range_tombstone_change, { 2 })
            .produces_partition_end()
            .produces_end_of_stream();
      }
    });
}

SEASTAR_TEST_CASE(test_counter_read) {
        // create table counter_test (
        //      pk int,
        //      ck int,
        //      c1 counter,
        //      c2 counter,
        //      primary key (pk, ck)
        // );
        //
        // Node 1:
        // update counter_test set c1 = c1 + 8 where pk = 0 and ck = 0;
        // update counter_test set c2 = c2 - 99 where pk = 0 and ck = 0;
        // update counter_test set c1 = c1 + 3 where pk = 0 and ck = 0;
        // update counter_test set c1 = c1 + 42 where pk = 0 and ck = 1;
        //
        // Node 2:
        // update counter_test set c2 = c2 + 7 where pk = 0 and ck = 0;
        // update counter_test set c1 = c1 + 2 where pk = 0 and ck = 0;
        // delete c1 from counter_test where pk = 0 and ck = 1;
        //
        // select * from counter_test;
        // pk | ck | c1 | c2
        // ----+----+----+-----
        //  0 |  0 | 13 | -92

        return test_env::do_with_async([] (test_env& env) {
          for (const auto version : all_sstable_versions) {
            auto s = schema_builder("ks", "counter_test")
                    .with_column("pk", int32_type, column_kind::partition_key)
                    .with_column("ck", int32_type, column_kind::clustering_key)
                    .with_column("c1", counter_type)
                    .with_column("c2", counter_type)
                    .build();

            auto node1 = counter_id(utils::UUID("8379ab99-4507-4ab1-805d-ac85a863092b"));
            auto node2 = counter_id(utils::UUID("b8a6c3f3-e222-433f-9ce9-de56a8466e07"));

            auto sst = env.reusable_sst(s, get_test_dir("counter_test", s), 5, version).get();
            auto reader = sstable_reader_v2(sst, s, env.make_reader_permit());
            auto close_reader = deferred_close(reader);

            auto mfopt = reader().get();
            BOOST_REQUIRE(mfopt);
            BOOST_REQUIRE(mfopt->is_partition_start());

            mfopt = reader().get();
            BOOST_REQUIRE(mfopt);
            BOOST_REQUIRE(mfopt->is_clustering_row());
            const clustering_row* cr = &mfopt->as_clustering_row();
            cr->cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& c) {
                counter_cell_view ccv(c.as_atomic_cell(s->regular_column_at(id)));
                auto& col = s->column_at(column_kind::regular_column, id);
                if (col.name_as_text() == "c1") {
                    BOOST_REQUIRE_EQUAL(ccv.total_value(), 13);
                    BOOST_REQUIRE_EQUAL(ccv.shard_count(), 2);

                    auto it = ccv.shards().begin();
                    auto shard = *it++;
                    BOOST_REQUIRE_EQUAL(shard.id(), node1);
                    BOOST_REQUIRE_EQUAL(shard.value(), 11);
                    BOOST_REQUIRE_EQUAL(shard.logical_clock(), 2);

                    shard = *it++;
                    BOOST_REQUIRE_EQUAL(shard.id(), node2);
                    BOOST_REQUIRE_EQUAL(shard.value(), 2);
                    BOOST_REQUIRE_EQUAL(shard.logical_clock(), 1);
                } else if (col.name_as_text() == "c2") {
                    BOOST_REQUIRE_EQUAL(ccv.total_value(), -92);
                } else {
                    BOOST_FAIL(format("Unexpected column \'{}\'", col.name_as_text()));
                }
            });

            mfopt = reader().get();
            BOOST_REQUIRE(mfopt);
            BOOST_REQUIRE(mfopt->is_clustering_row());
            cr = &mfopt->as_clustering_row();
            cr->cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& c) {
                auto& col = s->column_at(column_kind::regular_column, id);
                if (col.name_as_text() == "c1") {
                    BOOST_REQUIRE(!c.as_atomic_cell(col).is_live());
                } else {
                    BOOST_FAIL(format("Unexpected column \'{}\'", col.name_as_text()));
                }
            });

            mfopt = reader().get();
            BOOST_REQUIRE(mfopt);
            BOOST_REQUIRE(mfopt->is_end_of_partition());

            mfopt = reader().get();
            BOOST_REQUIRE(!mfopt);
          }
        });
}

SEASTAR_TEST_CASE(test_sstable_max_local_deletion_time) {
    return test_env::do_with_async([] (test_env& env) {
            for (const auto version : writable_sstable_versions) {
                schema_builder builder(some_keyspace, some_column_family);
                builder.with_column("p1", utf8_type, column_kind::partition_key);
                builder.with_column("c1", utf8_type, column_kind::clustering_key);
                builder.with_column("r1", utf8_type);
                schema_ptr s = builder.build(schema_builder::compact_storage::no);
                auto mt = make_lw_shared<replica::memtable>(s);
                int32_t last_expiry = 0;
                for (auto i = 0; i < 10; i++) {
                    auto key = partition_key::from_exploded(*s, {to_bytes("key" + to_sstring(i))});
                    mutation m(s, key);
                    auto c_key = clustering_key::from_exploded(*s, {to_bytes("c1")});
                    last_expiry = (gc_clock::now() + gc_clock::duration(3600 + i)).time_since_epoch().count();
                    m.set_clustered_cell(c_key, *s->get_column_definition("r1"),
                                         make_atomic_cell(utf8_type, bytes("a"), 3600 + i, last_expiry));
                    mt->apply(std::move(m));
                }
                auto sstp = make_sstable_containing(env.make_sstable(s, version), mt);
                BOOST_REQUIRE(last_expiry == sstp->get_stats_metadata().max_local_deletion_time);
            }
    });
}

SEASTAR_TEST_CASE(test_promoted_index_read) {
    // create table promoted_index_read (
    //        pk int,
    //        ck1 int,
    //        ck2 int,
    //        v int,
    //        primary key (pk, ck1, ck2)
    // );
    //
    // column_index_size_in_kb: 0
    //
    // delete from promoted_index_read where pk = 0 and ck1 = 0;
    // insert into promoted_index_read (pk, ck1, ck2, v) values (0, 0, 0, 0);
    // insert into promoted_index_read (pk, ck1, ck2, v) values (0, 0, 1, 1);
    //
    // SSTable:
    // [
    // {"key": "0",
    //  "cells": [["0:_","0:!",1468923292708929,"t",1468923292],
    //            ["0:_","0:!",1468923292708929,"t",1468923292],
    //            ["0:0:","",1468923308379491],
    //            ["0:_","0:!",1468923292708929,"t",1468923292],
    //            ["0:0:v","0",1468923308379491],
    //            ["0:_","0:!",1468923292708929,"t",1468923292],
    //            ["0:1:","",1468923311744298],
    //            ["0:_","0:!",1468923292708929,"t",1468923292],
    //            ["0:1:v","1",1468923311744298]]}
    // ]

    return test_env::do_with_async([] (test_env& env) {
      for (const auto version : all_sstable_versions) {
        auto s = schema_builder("ks", "promoted_index_read")
                .with_column("pk", int32_type, column_kind::partition_key)
                .with_column("ck1", int32_type, column_kind::clustering_key)
                .with_column("ck2", int32_type, column_kind::clustering_key)
                .with_column("v", int32_type)
                .build();
        auto sst = env.reusable_sst(s, get_test_dir("promoted_index_read", s), 1, version).get();
        auto pkey = partition_key::from_exploded(*s, { int32_type->decompose(0) });
        auto dkey = dht::decorate_key(*s, std::move(pkey));

        auto ck1 = clustering_key::from_exploded(*s, {int32_type->decompose(0)});
        auto ck2 = clustering_key::from_exploded(*s, {int32_type->decompose(0), int32_type->decompose(0)});
        auto ck3 = clustering_key::from_exploded(*s, {int32_type->decompose(0), int32_type->decompose(1)});

        auto rd = sstable_reader_v2(sst, s, env.make_reader_permit());
        using kind = mutation_fragment_v2::kind;
        assert_that(std::move(rd))
                .produces_partition_start(dkey)
                .produces(kind::range_tombstone_change, { 0 })
                .produces(kind::clustering_row, { 0, 0 })
                .produces(kind::clustering_row, { 0, 1 })
                .produces(kind::range_tombstone_change, { 0 })
                .produces_partition_end()
                .produces_end_of_stream();
      }
    });
}

static void check_min_max_column_names(const sstable_ptr& sst, std::vector<bytes> min_components, std::vector<bytes> max_components) {
    const auto& st = sst->get_stats_metadata();
    BOOST_TEST_MESSAGE(fmt::format("min {}/{} max {}/{}", st.min_column_names.elements.size(), min_components.size(), st.max_column_names.elements.size(), max_components.size()));
    BOOST_REQUIRE(st.min_column_names.elements.size() == min_components.size());
    for (auto i = 0U; i < st.min_column_names.elements.size(); i++) {
        BOOST_REQUIRE(min_components[i] == st.min_column_names.elements[i].value);
    }
    BOOST_REQUIRE(st.max_column_names.elements.size() == max_components.size());
    for (auto i = 0U; i < st.max_column_names.elements.size(); i++) {
        BOOST_REQUIRE(max_components[i] == st.max_column_names.elements[i].value);
    }
}

static void test_min_max_clustering_key(test_env& env, schema_ptr s, std::function<shared_sstable()> sst_gen, std::vector<bytes> exploded_pk, std::vector<std::vector<bytes>> exploded_cks,
        std::vector<bytes> min_components, std::vector<bytes> max_components, sstable_version_types version, bool remove = false) {
    auto mt = make_lw_shared<replica::memtable>(s);
    auto insert_data = [&mt, &s] (std::vector<bytes>& exploded_pk, std::vector<bytes>&& exploded_ck) {
        const column_definition& r1_col = *s->get_column_definition("r1");
        auto key = partition_key::from_exploded(*s, exploded_pk);
        auto c_key = clustering_key::make_empty();
        if (!exploded_ck.empty()) {
            c_key = clustering_key::from_exploded(*s, exploded_ck);
        }
        mutation m(s, key);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
        mt->apply(std::move(m));
    };
    auto remove_data = [&mt, &s] (std::vector<bytes>& exploded_pk, std::vector<bytes>&& exploded_ck) {
        auto key = partition_key::from_exploded(*s, exploded_pk);
        auto c_key = clustering_key::from_exploded(*s, exploded_ck);
        mutation m(s, key);
        tombstone tomb(api::new_timestamp(), gc_clock::now());
        m.partition().apply_delete(*s, c_key, tomb);
        mt->apply(std::move(m));
    };

    if (exploded_cks.empty()) {
        insert_data(exploded_pk, {});
    } else {
        for (auto& exploded_ck : exploded_cks) {
            if (remove) {
                remove_data(exploded_pk, std::move(exploded_ck));
            } else {
                insert_data(exploded_pk, std::move(exploded_ck));
            }
        }
    }
    auto sst = make_sstable_containing(env.make_sstable(s, version), mt);
    check_min_max_column_names(sst, std::move(min_components), std::move(max_components));
    sst->unlink().get();
}

SEASTAR_TEST_CASE(min_max_clustering_key_test) {
    return test_env::do_with_async([] (test_env& env) {
        for (auto version : writable_sstable_versions) {
            {
                auto s = schema_builder("ks", "cf")
                        .with_column("pk", utf8_type, column_kind::partition_key)
                        .with_column("ck1", utf8_type, column_kind::clustering_key)
                        .with_column("ck2", utf8_type, column_kind::clustering_key)
                        .with_column("r1", int32_type)
                        .build();
                auto sst_gen = env.make_sst_factory(s, version);
                test_min_max_clustering_key(env, s, sst_gen, {"key1"}, {{"a", "b"},
                                                          {"a", "c"}}, {"a", "b"}, {"a", "c"}, version);
            }
            {
                auto s = schema_builder("ks", "cf")
                        .with(schema_builder::compact_storage::yes)
                        .with_column("pk", utf8_type, column_kind::partition_key)
                        .with_column("ck1", utf8_type, column_kind::clustering_key)
                        .with_column("ck2", utf8_type, column_kind::clustering_key)
                        .with_column("r1", int32_type)
                        .build();
                auto sst_gen = env.make_sst_factory(s, version);
                test_min_max_clustering_key(env, s, sst_gen, {"key1"}, {{"a", "b"},
                                                          {"a", "c"}}, {"a", "b"}, {"a", "c"}, version);
            }
            {
                auto s = schema_builder("ks", "cf")
                        .with_column("pk", utf8_type, column_kind::partition_key)
                        .with_column("ck1", utf8_type, column_kind::clustering_key)
                        .with_column("ck2", utf8_type, column_kind::clustering_key)
                        .with_column("r1", int32_type)
                        .build();
                BOOST_TEST_MESSAGE(fmt::format("min_max_clustering_key_test: min={{\"a\", \"c\"}} max={{\"b\", \"a\"}} version={}", version));
                auto sst_gen = env.make_sst_factory(s, version);
                test_min_max_clustering_key(env, s, sst_gen, {"key1"}, {{"b", "a"}, {"a", "c"}}, {"a", "c"}, {"b", "a"}, version);
            }
            {
                auto s = schema_builder("ks", "cf")
                        .with(schema_builder::compact_storage::yes)
                        .with_column("pk", utf8_type, column_kind::partition_key)
                        .with_column("ck1", utf8_type, column_kind::clustering_key)
                        .with_column("ck2", utf8_type, column_kind::clustering_key)
                        .with_column("r1", int32_type)
                        .build();
                BOOST_TEST_MESSAGE(fmt::format("min_max_clustering_key_test: min={{\"a\", \"c\"}} max={{\"b\", \"a\"}} with compact storage version={}", version));
                auto sst_gen = env.make_sst_factory(s, version);
                test_min_max_clustering_key(env, s, sst_gen, {"key1"}, {{"b", "a"}, {"a", "c"}}, {"a", "c"}, {"b", "a"}, version);
            }
            {
                auto s = schema_builder("ks", "cf")
                        .with_column("pk", utf8_type, column_kind::partition_key)
                        .with_column("ck1", utf8_type, column_kind::clustering_key)
                        .with_column("ck2", reversed_type_impl::get_instance(utf8_type), column_kind::clustering_key)
                        .with_column("r1", int32_type)
                        .build();
                BOOST_TEST_MESSAGE(fmt::format("min_max_clustering_key_test: reversed order: min={{\"a\", \"z\"}} max={{\"a\", \"a\"}} version={}", version));
                auto sst_gen = env.make_sst_factory(s, version);
                test_min_max_clustering_key(env, s, sst_gen, {"key1"}, {{"a", "a"}, {"a", "z"}}, {"a", "z"}, {"a", "a"}, version);
            }
            {
                auto s = schema_builder("ks", "cf")
                        .with_column("pk", utf8_type, column_kind::partition_key)
                        .with_column("ck1", utf8_type, column_kind::clustering_key)
                        .with_column("ck2", reversed_type_impl::get_instance(utf8_type), column_kind::clustering_key)
                        .with_column("r1", int32_type)
                        .build();
                BOOST_TEST_MESSAGE(fmt::format("min_max_clustering_key_test: reversed order: min={{\"a\", \"a\"}} max={{\"b\", \"z\"}} version={}", version));
                auto sst_gen = env.make_sst_factory(s, version);
                test_min_max_clustering_key(env, s, sst_gen, {"key1"}, {{"b", "z"}, {"a", "a"}}, {"a", "a"}, {"b", "z"}, version);
            }
            {
                auto s = schema_builder("ks", "cf")
                        .with_column("pk", utf8_type, column_kind::partition_key)
                        .with_column("ck1", utf8_type, column_kind::clustering_key)
                        .with_column("r1", int32_type)
                        .build();
                auto sst_gen = env.make_sst_factory(s, version);
                test_min_max_clustering_key(env, s, sst_gen, {"key1"}, {{"a"},
                                                          {"z"}}, {"a"}, {"z"}, version);
            }
            {
                auto s = schema_builder("ks", "cf")
                        .with_column("pk", utf8_type, column_kind::partition_key)
                        .with_column("ck1", utf8_type, column_kind::clustering_key)
                        .with_column("r1", int32_type)
                        .build();
                auto sst_gen = env.make_sst_factory(s, version);
                test_min_max_clustering_key(env, s, sst_gen, {"key1"}, {{"a"},
                                                          {"z"}}, {"a"}, {"z"}, version, true);
            }
            {
                auto s = schema_builder("ks", "cf")
                        .with_column("pk", utf8_type, column_kind::partition_key)
                        .with_column("r1", int32_type)
                        .build();
                auto sst_gen = env.make_sst_factory(s, version);
                test_min_max_clustering_key(env, s, sst_gen, {"key1"}, {}, {}, {}, version);
            }
            if (version >= sstable_version_types::mc) {
                {
                    auto s = schema_builder("ks", "cf")
                            .with(schema_builder::compact_storage::yes)
                            .with_column("pk", utf8_type, column_kind::partition_key)
                            .with_column("ck1", utf8_type, column_kind::clustering_key)
                            .with_column("ck2", reversed_type_impl::get_instance(utf8_type), column_kind::clustering_key)
                            .with_column("r1", int32_type)
                            .build();
                    BOOST_TEST_MESSAGE(fmt::format("min_max_clustering_key_test: reversed order: min={{\"a\"}} max={{\"a\"}} with compact storage version={}", version));
                auto sst_gen = env.make_sst_factory(s, version);
                    test_min_max_clustering_key(env, s, sst_gen, {"key1"}, {{"a", "z"}, {"a"}}, {"a"}, {"a"}, version);
                }
            }
        }
    });
}

SEASTAR_TEST_CASE(sstable_tombstone_metadata_check) {
    return test_env::do_with_async([] (test_env& env) {
        for (const auto version : writable_sstable_versions) {
            auto s = schema_builder("ks", "cf")
                    .with_column("pk", utf8_type, column_kind::partition_key)
                    .with_column("ck1", utf8_type, column_kind::clustering_key)
                    .with_column("r1", int32_type)
                    .build();
            auto sst_gen = env.make_sst_factory(s, version);
            auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
            auto c_key = clustering_key_prefix::from_exploded(*s, {to_bytes("c1")});
            const column_definition& r1_col = *s->get_column_definition("r1");

            BOOST_TEST_MESSAGE(fmt::format("version {}", version));

            {
                mutation m(s, key);
                tombstone tomb(api::new_timestamp(), gc_clock::now());
                m.partition().apply_delete(*s, c_key, tomb);
                auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                check_min_max_column_names(sst, {"c1"}, {"c1"});
            }

            {
                mutation m(s, key);
                m.set_clustered_cell(c_key, r1_col, make_dead_atomic_cell(3600));
                auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                check_min_max_column_names(sst, {"c1"}, {"c1"});
            }

            {
                mutation m(s, key);
                m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
                auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                BOOST_REQUIRE(!sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                check_min_max_column_names(sst, {"c1"}, {"c1"});
            }

            {
                mutation m(s, key);
                tombstone tomb(api::new_timestamp(), gc_clock::now());
                m.partition().apply_delete(*s, c_key, tomb);

                auto key2 = partition_key::from_exploded(*s, {to_bytes("key2")});
                mutation m2(s, key2);
                m2.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));

                auto sst = make_sstable_containing(sst_gen, {std::move(m), std::move(m2)});
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                check_min_max_column_names(sst, {"c1"}, {"c1"});
            }

            {
                mutation m(s, key);
                tombstone tomb(api::new_timestamp(), gc_clock::now());
                m.partition().apply(tomb);
                auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                check_min_max_column_names(sst, {}, {});
            }

            {
                mutation m(s, key);
                tombstone tomb(api::new_timestamp(), gc_clock::now());
                range_tombstone rt(clustering_key_prefix::from_single_value(*s, bytes(
                "a")), clustering_key_prefix::from_single_value(*s, bytes("a")), tomb);
                m.partition().apply_delete(*s, std::move(rt));
                auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                if (version >= sstable_version_types::mc) {
                    check_min_max_column_names(sst, {"a"}, {"a"});
                }
            }

            {
                mutation m(s, key);
                m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
                tombstone tomb(api::new_timestamp(), gc_clock::now());
                range_tombstone rt(
                        clustering_key_prefix::from_single_value(*s, bytes("a")),
                        clustering_key_prefix::from_single_value(*s, bytes("a")),
                        tomb);
                m.partition().apply_delete(*s, std::move(rt));
                auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                if (version >= sstable_version_types::mc) {
                    check_min_max_column_names(sst, {"a"}, {"c1"});
                }
            }

            {
                mutation m(s, key);
                m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
                tombstone tomb(api::new_timestamp(), gc_clock::now());
                range_tombstone rt(
                        clustering_key_prefix::from_single_value(*s, bytes("c")),
                        clustering_key_prefix::from_single_value(*s, bytes("d")),
                        tomb);
                m.partition().apply_delete(*s, std::move(rt));
                auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                if (version >= sstable_version_types::mc) {
                    check_min_max_column_names(sst, {"c"}, {"d"});
                }
            }

            {
                mutation m(s, key);
                m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
                tombstone tomb(api::new_timestamp(), gc_clock::now());
                range_tombstone rt(
                        clustering_key_prefix::from_single_value(*s, bytes("d")),
                        clustering_key_prefix::from_single_value(*s, bytes("z")),
                        tomb);
                m.partition().apply_delete(*s, std::move(rt));
                auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                if (version >= sstable_version_types::mc) {
                    check_min_max_column_names(sst, {"c1"}, {"z"});
                }
            }

            if (version >= sstable_version_types::mc) {
                {
                    mutation m(s, key);
                    m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
                    tombstone tomb(api::new_timestamp(), gc_clock::now());
                    range_tombstone rt(
                            bound_view::bottom(),
                            bound_view(clustering_key_prefix::from_single_value(*s, bytes("z")), bound_kind::incl_end),
                            tomb);
                    m.partition().apply_delete(*s, std::move(rt));
                    auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                    BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                    check_min_max_column_names(sst, {}, {"z"});
                }

                {
                    mutation m(s, key);
                    m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
                    tombstone tomb(api::new_timestamp(), gc_clock::now());
                    range_tombstone rt(
                            bound_view(clustering_key_prefix::from_single_value(*s, bytes("a")), bound_kind::incl_start),
                            bound_view::top(),
                            tomb);
                    m.partition().apply_delete(*s, std::move(rt));
                    auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                    BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                    check_min_max_column_names(sst, {"a"}, {});
                }

                {
                    mutation m(s, key);
                    m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
                    tombstone tomb(api::new_timestamp(), gc_clock::now());
                    m.partition().apply_delete(*s, clustering_key_prefix::make_empty(), tomb);
                    auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                    BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                    check_min_max_column_names(sst, {}, {});
                }
            }
        }
    });
}

SEASTAR_TEST_CASE(sstable_composite_tombstone_metadata_check) {
    return test_env::do_with_async([] (test_env& env) {
        for (const auto version : writable_sstable_versions) {
            auto s = schema_builder("ks", "cf")
                    .with_column("pk", utf8_type, column_kind::partition_key)
                    .with_column("ck1", utf8_type, column_kind::clustering_key)
                    .with_column("ck2", utf8_type, column_kind::clustering_key)
                    .with_column("r1", int32_type)
                    .build();
            auto sst_gen = env.make_sst_factory(s, version);
            auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
            auto c_key = clustering_key_prefix::from_exploded(*s, {to_bytes("c1"), to_bytes("c2")});
            const column_definition& r1_col = *s->get_column_definition("r1");

            BOOST_TEST_MESSAGE(fmt::format("version {}", version));

            {
                mutation m(s, key);
                tombstone tomb(api::new_timestamp(), gc_clock::now());
                m.partition().apply_delete(*s, c_key, tomb);
                auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                check_min_max_column_names(sst, {"c1", "c2"}, {"c1", "c2"});
            }

            {
                mutation m(s, key);
                m.set_clustered_cell(c_key, r1_col, make_dead_atomic_cell(3600));
                auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                check_min_max_column_names(sst, {"c1", "c2"}, {"c1", "c2"});
            }

            {
                mutation m(s, key);
                m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
                auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                BOOST_REQUIRE(!sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                check_min_max_column_names(sst, {"c1", "c2"}, {"c1", "c2"});
            }

            {
                mutation m(s, key);
                tombstone tomb(api::new_timestamp(), gc_clock::now());
                m.partition().apply_delete(*s, c_key, tomb);

                auto key2 = partition_key::from_exploded(*s, {to_bytes("key2")});
                mutation m2(s, key2);
                m2.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));

                auto sst = make_sstable_containing(sst_gen, {std::move(m), std::move(m2)});
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                check_min_max_column_names(sst, {"c1", "c2"}, {"c1", "c2"});
            }

            {
                mutation m(s, key);
                tombstone tomb(api::new_timestamp(), gc_clock::now());
                m.partition().apply(tomb);
                auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                check_min_max_column_names(sst, {}, {});
            }

            {
                mutation m(s, key);
                tombstone tomb(api::new_timestamp(), gc_clock::now());
                range_tombstone rt(
                        clustering_key_prefix::from_exploded(*s, {to_bytes("a"), to_bytes("aa")}),
                        clustering_key_prefix::from_exploded(*s, {to_bytes("z"), to_bytes("zz")}),
                        tomb);
                m.partition().apply_delete(*s, std::move(rt));
                auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                if (version >= sstable_version_types::mc) {
                    check_min_max_column_names(sst, {"a", "aa"}, {"z", "zz"});
                }
            }

            {
                mutation m(s, key);
                m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
                tombstone tomb(api::new_timestamp(), gc_clock::now());
                range_tombstone rt(
                        clustering_key_prefix::from_exploded(*s, {to_bytes("a")}),
                        clustering_key_prefix::from_exploded(*s, {to_bytes("a"), to_bytes("zz")}),
                        tomb);
                m.partition().apply_delete(*s, std::move(rt));
                auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                if (version >= sstable_version_types::mc) {
                    check_min_max_column_names(sst, {"a"}, {"c1", "c2"});
                }
            }

            {
                mutation m(s, key);
                m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
                tombstone tomb(api::new_timestamp(), gc_clock::now());
                range_tombstone rt(
                        clustering_key_prefix::from_exploded(*s, {to_bytes("c1"), to_bytes("aa")}),
                        clustering_key_prefix::from_exploded(*s, {to_bytes("c1"), to_bytes("zz")}),
                        tomb);
                m.partition().apply_delete(*s, std::move(rt));
                auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                if (version >= sstable_version_types::mc) {
                    check_min_max_column_names(sst, {"c1", "aa"}, {"c1", "zz"});
                }
            }

            {
                mutation m(s, key);
                m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
                tombstone tomb(api::new_timestamp(), gc_clock::now());
                range_tombstone rt(
                        clustering_key_prefix::from_exploded(*s, {to_bytes("c1"), to_bytes("d")}),
                        clustering_key_prefix::from_exploded(*s, {to_bytes("z"), to_bytes("zz")}),
                        tomb);
                m.partition().apply_delete(*s, std::move(rt));
                auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                if (version >= sstable_version_types::mc) {
                    check_min_max_column_names(sst, {"c1", "c2"}, {"z", "zz"});
                }
            }

            if (version >= sstable_version_types::mc) {
                {
                    mutation m(s, key);
                    m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
                    tombstone tomb(api::new_timestamp(), gc_clock::now());
                    range_tombstone rt(
                            bound_view::bottom(),
                            bound_view(clustering_key_prefix::from_single_value(*s, bytes("z")), bound_kind::incl_end),
                            tomb);
                    m.partition().apply_delete(*s, std::move(rt));
                    auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                    BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                    check_min_max_column_names(sst, {}, {"z"});
                }

                {
                    mutation m(s, key);
                    m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
                    tombstone tomb(api::new_timestamp(), gc_clock::now());
                    range_tombstone rt(
                            bound_view(clustering_key_prefix::from_single_value(*s, bytes("a")), bound_kind::incl_start),
                            bound_view::top(),
                            tomb);
                    m.partition().apply_delete(*s, std::move(rt));
                    auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                    BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                    check_min_max_column_names(sst, {"a"}, {});
                }
            }
        }
    });
}

SEASTAR_TEST_CASE(sstable_composite_reverse_tombstone_metadata_check) {
    return test_env::do_with_async([] (test_env& env) {
        for (const auto version : writable_sstable_versions) {
            auto s = schema_builder("ks", "cf")
                    .with_column("pk", utf8_type, column_kind::partition_key)
                    .with_column("ck1", utf8_type, column_kind::clustering_key)
                    .with_column("ck2", reversed_type_impl::get_instance(utf8_type), column_kind::clustering_key)
                    .with_column("r1", int32_type)
                    .build();
            auto sst_gen = env.make_sst_factory(s, version);
            auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
            auto c_key = clustering_key_prefix::from_exploded(*s, {to_bytes("c1"), to_bytes("c2")});
            const column_definition& r1_col = *s->get_column_definition("r1");

            BOOST_TEST_MESSAGE(fmt::format("version {}", version));

            {
                mutation m(s, key);
                tombstone tomb(api::new_timestamp(), gc_clock::now());
                m.partition().apply_delete(*s, c_key, tomb);
                auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                check_min_max_column_names(sst, {"c1", "c2"}, {"c1", "c2"});
            }

            {
                mutation m(s, key);
                m.set_clustered_cell(c_key, r1_col, make_dead_atomic_cell(3600));
                auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                check_min_max_column_names(sst, {"c1", "c2"}, {"c1", "c2"});
            }

            {
                mutation m(s, key);
                m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
                auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                BOOST_REQUIRE(!sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                check_min_max_column_names(sst, {"c1", "c2"}, {"c1", "c2"});
            }

            {
                mutation m(s, key);
                tombstone tomb(api::new_timestamp(), gc_clock::now());
                m.partition().apply_delete(*s, c_key, tomb);

                auto key2 = partition_key::from_exploded(*s, {to_bytes("key2")});
                mutation m2(s, key2);
                m2.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));

                auto sst = make_sstable_containing(sst_gen, {std::move(m), std::move(m2)});
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                check_min_max_column_names(sst, {"c1", "c2"}, {"c1", "c2"});
            }

            {
                mutation m(s, key);
                tombstone tomb(api::new_timestamp(), gc_clock::now());
                m.partition().apply(tomb);
                auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                check_min_max_column_names(sst, {}, {});
            }

            {
                mutation m(s, key);
                tombstone tomb(api::new_timestamp(), gc_clock::now());
                range_tombstone rt(
                        clustering_key_prefix::from_exploded(*s, {to_bytes("a"), to_bytes("zz")}),
                        clustering_key_prefix::from_exploded(*s, {to_bytes("a"), to_bytes("aa")}),
                        tomb);
                m.partition().apply_delete(*s, std::move(rt));
                auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                if (version >= sstable_version_types::mc) {
                    check_min_max_column_names(sst, {"a", "zz"}, {"a", "aa"});
                }
            }

            {
                mutation m(s, key);
                m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
                tombstone tomb(api::new_timestamp(), gc_clock::now());
                range_tombstone rt(
                        clustering_key_prefix::from_exploded(*s, {to_bytes("a"), to_bytes("zz")}),
                        clustering_key_prefix::from_exploded(*s, {to_bytes("a")}),
                        tomb);
                m.partition().apply_delete(*s, std::move(rt));
                auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                if (version >= sstable_version_types::mc) {
                    check_min_max_column_names(sst, {"a", "zz"}, {"c1", "c2"});
                }
            }

            {
                mutation m(s, key);
                m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
                tombstone tomb(api::new_timestamp(), gc_clock::now());
                range_tombstone rt(
                        clustering_key_prefix::from_exploded(*s, {to_bytes("c1"), to_bytes("zz")}),
                        clustering_key_prefix::from_exploded(*s, {to_bytes("c1")}),
                        tomb);
                m.partition().apply_delete(*s, std::move(rt));
                auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                if (version >= sstable_version_types::mc) {
                    check_min_max_column_names(sst, {"c1", "zz"}, {"c1"});
                }
            }

            {
                mutation m(s, key);
                m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
                tombstone tomb(api::new_timestamp(), gc_clock::now());
                range_tombstone rt(
                        clustering_key_prefix::from_exploded(*s, {to_bytes("c1"), to_bytes("zz")}),
                        clustering_key_prefix::from_exploded(*s, {to_bytes("c1"), to_bytes("d")}),
                        tomb);
                m.partition().apply_delete(*s, std::move(rt));
                auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                if (version >= sstable_version_types::mc) {
                    check_min_max_column_names(sst, {"c1", "zz"}, {"c1", "c2"});
                }
            }

            if (version >= sstable_version_types::mc) {
                {
                    mutation m(s, key);
                    m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
                    tombstone tomb(api::new_timestamp(), gc_clock::now());
                    range_tombstone rt(
                            bound_view::bottom(),
                            bound_view(clustering_key_prefix::from_single_value(*s, bytes("z")), bound_kind::incl_end),
                            tomb);
                    m.partition().apply_delete(*s, std::move(rt));
                    auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                    BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                    check_min_max_column_names(sst, {}, {"z"});
                }

                {
                    mutation m(s, key);
                    m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
                    tombstone tomb(api::new_timestamp(), gc_clock::now());
                    range_tombstone rt(
                            bound_view(clustering_key_prefix::from_single_value(*s, bytes("a")), bound_kind::incl_start),
                            bound_view::top(),
                            tomb);
                    m.partition().apply_delete(*s, std::move(rt));
                    auto sst = make_sstable_containing(sst_gen, {std::move(m)});
                    BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
                    check_min_max_column_names(sst, {"a"}, {});
                }
            }
        }
    });
}

SEASTAR_TEST_CASE(test_partition_skipping) {
    return test_env::do_with_async([] (test_env& env) {
      for (const auto version : all_sstable_versions) {
        auto s = schema_builder("ks", "test_skipping_partitions")
                .with_column("pk", int32_type, column_kind::partition_key)
                .with_column("v", int32_type)
                .build();

        auto sst = env.reusable_sst(s, get_test_dir("partition_skipping",s), 1, version).get();

        std::vector<dht::decorated_key> keys;
        for (int i = 0; i < 10; i++) {
            auto pk = partition_key::from_single_value(*s, int32_type->decompose(i));
            keys.emplace_back(dht::decorate_key(*s, std::move(pk)));
        }
        dht::decorated_key::less_comparator cmp(s);
        std::sort(keys.begin(), keys.end(), cmp);

        assert_that(sstable_reader_v2(sst, s, env.make_reader_permit())).produces(keys);

        auto pr = dht::partition_range::make(dht::ring_position(keys[0]), dht::ring_position(keys[1]));
        assert_that(sstable_reader_v2(sst, s, env.make_reader_permit(), pr))
            .produces(keys[0])
            .produces(keys[1])
            .produces_end_of_stream()
            .fast_forward_to(dht::partition_range::make_starting_with(dht::ring_position(keys[8])))
            .produces(keys[8])
            .produces(keys[9])
            .produces_end_of_stream();

        pr = dht::partition_range::make(dht::ring_position(keys[1]), dht::ring_position(keys[1]));
        assert_that(sstable_reader_v2(sst, s, env.make_reader_permit(), pr))
            .produces(keys[1])
            .produces_end_of_stream()
            .fast_forward_to(dht::partition_range::make(dht::ring_position(keys[3]), dht::ring_position(keys[4])))
            .produces(keys[3])
            .produces(keys[4])
            .produces_end_of_stream()
            .fast_forward_to(dht::partition_range::make({ dht::ring_position(keys[4]), false }, dht::ring_position(keys[5])))
            .produces(keys[5])
            .produces_end_of_stream()
            .fast_forward_to(dht::partition_range::make(dht::ring_position(keys[6]), dht::ring_position(keys[6])))
            .produces(keys[6])
            .produces_end_of_stream()
            .fast_forward_to(dht::partition_range::make(dht::ring_position(keys[7]), dht::ring_position(keys[8])))
            .produces(keys[7])
            .fast_forward_to(dht::partition_range::make(dht::ring_position(keys[9]), dht::ring_position(keys[9])))
            .produces(keys[9])
            .produces_end_of_stream();

        pr = dht::partition_range::make({ dht::ring_position(keys[0]), false }, { dht::ring_position(keys[1]), false});
        assert_that(sstable_reader_v2(sst, s, env.make_reader_permit(), pr))
            .produces_end_of_stream()
            .fast_forward_to(dht::partition_range::make(dht::ring_position(keys[6]), dht::ring_position(keys[6])))
            .produces(keys[6])
            .produces_end_of_stream()
            .fast_forward_to(dht::partition_range::make({ dht::ring_position(keys[8]), false }, { dht::ring_position(keys[9]), false }))
            .produces_end_of_stream();

        pr = dht::partition_range::make(dht::ring_position(keys[0]), dht::ring_position(keys[1]));
        assert_that(sstable_reader_v2(sst, s, env.make_reader_permit(), pr))
            .fast_forward_to(dht::partition_range::make(dht::ring_position::starting_at(keys[0].token()), dht::ring_position::ending_at(keys[1].token())))
            .produces(keys[0])
            .produces(keys[1])
            .fast_forward_to(dht::partition_range::make(dht::ring_position::starting_at(keys[3].token()), dht::ring_position::ending_at(keys[4].token())))
            .produces(keys[3])
            .produces(keys[4])
            .fast_forward_to(dht::partition_range::make_starting_with(dht::ring_position::starting_at(keys[8].token())))
            .produces(keys[8])
            .produces(keys[9])
            .produces_end_of_stream();
      }
    });
}

SEASTAR_TEST_CASE(test_repeated_tombstone_skipping) {
    return test_env::do_with_async([] (test_env& env) {
      for (const auto version : writable_sstable_versions) {
        simple_schema table;

        auto permit = env.make_reader_permit();

        std::vector<mutation_fragment> fragments;

        uint32_t count = 1000; // large enough to cross index block several times

        auto rt = table.make_range_tombstone(query::clustering_range::make(
            query::clustering_range::bound(table.make_ckey(0), true),
            query::clustering_range::bound(table.make_ckey(count - 1), true)
        ));

        fragments.push_back(mutation_fragment(*table.schema(), permit, range_tombstone(rt)));

        std::vector<range_tombstone> rts;

        uint32_t seq = 1;
        while (seq < count) {
            rts.push_back(table.make_range_tombstone(query::clustering_range::make(
                query::clustering_range::bound(table.make_ckey(seq), true),
                query::clustering_range::bound(table.make_ckey(seq + 1), false)
            )));
            fragments.emplace_back(*table.schema(), permit, range_tombstone(rts.back()));
            ++seq;

            fragments.emplace_back(*table.schema(), permit, table.make_row(permit, table.make_ckey(seq), make_random_string(1)));
            ++seq;
        }

        sstable_writer_config cfg = env.manager().configure_writer();
        cfg.promoted_index_block_size = 100;
        auto mut = mutation(table.schema(), table.make_pkey("key"));
        for (auto&& mf : fragments) {
            mut.apply(mf);
        }
        auto ms = make_sstable_easy(env, make_mutation_reader_from_mutations_v2(table.schema(), std::move(permit), std::move(mut)), cfg, version)->as_mutation_source();

        for (uint32_t i = 3; i < seq; i++) {
            auto ck1 = table.make_ckey(1);
            auto ck2 = table.make_ckey((1 + i) / 2);
            auto ck3 = table.make_ckey(i);
            testlog.info("checking {} {}", ck2, ck3);
            auto slice = partition_slice_builder(*table.schema())
                .with_range(query::clustering_range::make_singular(ck1))
                .with_range(query::clustering_range::make_singular(ck2))
                .with_range(query::clustering_range::make_singular(ck3))
                .build();
            auto rd = ms.make_reader_v2(table.schema(), env.make_reader_permit(), query::full_partition_range, slice);
            assert_that(std::move(rd)).has_monotonic_positions();
        }
      }
    });
}

SEASTAR_TEST_CASE(test_skipping_using_index) {
    return test_env::do_with_async([] (test_env& env) {
      for (const auto version : writable_sstable_versions) {
        simple_schema table;

        const unsigned rows_per_part = 10;
        const unsigned partition_count = 10;

        std::vector<dht::decorated_key> keys;
        for (unsigned i = 0; i < partition_count; ++i) {
            keys.push_back(table.make_pkey(i));
        }
        std::sort(keys.begin(), keys.end(), dht::decorated_key::less_comparator(table.schema()));

        std::vector<mutation> partitions;
        uint32_t row_id = 0;
        for (auto&& key : keys) {
            mutation m(table.schema(), key);
            for (unsigned j = 0; j < rows_per_part; ++j) {
                table.add_row(m, table.make_ckey(row_id++), make_random_string(1));
            }
            partitions.emplace_back(std::move(m));
        }

        std::sort(partitions.begin(), partitions.end(), mutation_decorated_key_less_comparator());

        sstable_writer_config cfg = env.manager().configure_writer();
        cfg.promoted_index_block_size = 1; // So that every fragment is indexed
        cfg.promoted_index_auto_scale_threshold = 0; // disable auto-scaling
        auto ms = make_sstable_easy(env, make_mutation_reader_from_mutations_v2(table.schema(), env.make_reader_permit(), partitions), cfg, version)->as_mutation_source();
        auto rd = ms.make_reader_v2(table.schema(),
            env.make_reader_permit(),
            query::full_partition_range,
            table.schema()->full_slice(),
            nullptr,
            streamed_mutation::forwarding::yes,
            mutation_reader::forwarding::yes);

        auto assertions = assert_that(std::move(rd));
        // Consume first partition completely so that index is stale
        {
            assertions
                .produces_partition_start(keys[0])
                .fast_forward_to(position_range::all_clustered_rows());
            for (auto i = 0u; i < rows_per_part; i++) {
                assertions.produces_row_with_key(table.make_ckey(i));
            }
            assertions.produces_end_of_stream();
        }

        {
            auto base = rows_per_part;
            assertions
                .next_partition()
                .produces_partition_start(keys[1])
                .fast_forward_to(position_range(
                    position_in_partition::for_key(table.make_ckey(base)),
                    position_in_partition::for_key(table.make_ckey(base + 3))))
                .produces_row_with_key(table.make_ckey(base))
                .produces_row_with_key(table.make_ckey(base + 1))
                .produces_row_with_key(table.make_ckey(base + 2))
                .fast_forward_to(position_range(
                    position_in_partition::for_key(table.make_ckey(base + 5)),
                    position_in_partition::for_key(table.make_ckey(base + 6))))
                .produces_row_with_key(table.make_ckey(base + 5))
                .produces_end_of_stream()
                .fast_forward_to(position_range(
                    position_in_partition::for_key(table.make_ckey(base + rows_per_part)), // Skip all rows in current partition
                    position_in_partition::after_all_clustered_rows()))
                .produces_end_of_stream();
        }

        // Consume few fragments then skip
        {
            auto base = rows_per_part * 2;
            assertions
                .next_partition()
                .produces_partition_start(keys[2])
                .fast_forward_to(position_range(
                    position_in_partition::for_key(table.make_ckey(base)),
                    position_in_partition::for_key(table.make_ckey(base + 3))))
                .produces_row_with_key(table.make_ckey(base))
                .produces_row_with_key(table.make_ckey(base + 1))
                .produces_row_with_key(table.make_ckey(base + 2))
                .fast_forward_to(position_range(
                    position_in_partition::for_key(table.make_ckey(base + rows_per_part - 1)), // last row
                    position_in_partition::after_all_clustered_rows()))
                .produces_row_with_key(table.make_ckey(base + rows_per_part - 1))
                .produces_end_of_stream();
        }

        // Consume nothing from the next partition
        {
            assertions
                .next_partition()
                .produces_partition_start(keys[3])
                .next_partition();
        }

        {
            auto base = rows_per_part * 4;
            assertions
                .next_partition()
                .produces_partition_start(keys[4])
                .fast_forward_to(position_range(
                    position_in_partition::for_key(table.make_ckey(base + rows_per_part - 1)), // last row
                    position_in_partition::after_all_clustered_rows()))
                .produces_row_with_key(table.make_ckey(base + rows_per_part - 1))
                .produces_end_of_stream();
        }
      }
    });
}

static void copy_directory(fs::path src_dir, fs::path dst_dir) {
    fs::create_directory(dst_dir);
    auto src_dir_components = std::distance(src_dir.begin(), src_dir.end());
    using rdi = fs::recursive_directory_iterator;
    // Boost 1.55.0 doesn't support range for on recursive_directory_iterator
    // (even though previous and later versions do support it)
    for (auto&& dirent = rdi{src_dir}; dirent != rdi(); ++dirent) {
        auto&& path = dirent->path();
        auto new_path = dst_dir;
        for (auto i = std::next(path.begin(), src_dir_components); i != path.end(); ++i) {
            new_path /= *i;
        }
        fs::copy(path, new_path);
    }
}

SEASTAR_TEST_CASE(test_unknown_component) {
    return test_env::do_with_async([] (test_env& env) {
        copy_directory("test/resource/sstables/unknown_component", std::string(env.tempdir().path().string()) + "/unknown_component");
        auto sstp = env.reusable_sst(uncompressed_schema(), env.tempdir().path().string() + "/unknown_component").get();
        test::create_links(*sstp, env.tempdir().path().string()).get();
        // check that create_links() moved unknown component to new dir
        BOOST_REQUIRE(file_exists(env.tempdir().path().string() + "/la-1-big-UNKNOWN.txt").get());

        sstp = env.reusable_sst(uncompressed_schema(), generation_type{1}).get();
        env.manager().delete_atomically({sstp}).get();
        // assure unknown component is deleted
        BOOST_REQUIRE(!file_exists(env.tempdir().path().string() + "/la-1-big-UNKNOWN.txt").get());
    });
}

SEASTAR_TEST_CASE(sstable_set_incremental_selector) {
  return test_env::do_with([] (test_env& env) {
    auto s = make_shared_schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {}, {}, {}, utf8_type);
    auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::leveled, s->compaction_strategy_options());
    const auto decorated_keys = tests::generate_partition_keys(8, s);

    auto new_sstable = [&] (sstable_set& set, size_t k0, size_t k1, uint32_t level) {
        auto key0 = decorated_keys[k0];
        auto tok0 = key0.token();
        auto key1 = decorated_keys[k1];
        auto tok1 = key1.token();
        testlog.debug("creating sstable with k[{}] token={} k[{}] token={} level={}", k0, tok0, k1, tok1, level);
        auto sst = sstable_for_overlapping_test(env, s, key0.key(), key1.key(), level);
        set.insert(sst);
        return sst;
    };

    auto check = [&] (sstable_set::incremental_selector& selector, size_t k, std::unordered_set<shared_sstable> expected_ssts) {
        const dht::decorated_key& key = decorated_keys[k];
        auto sstables = selector.select(key).sstables;
        testlog.debug("checking sstables for key[{}] token={} found={} expected={}", k, decorated_keys[k].token(), sstables.size(), expected_ssts.size());
        BOOST_REQUIRE_EQUAL(sstables.size(), expected_ssts.size());
        for (auto& sst : sstables) {
            BOOST_REQUIRE(expected_ssts.contains(sst));
            expected_ssts.erase(sst);
        }
        BOOST_REQUIRE(expected_ssts.empty());
    };

    {
        sstable_set set = cs.make_sstable_set(s);
        std::vector<shared_sstable> ssts;
        ssts.push_back(new_sstable(set, 0, 1, 1));
        ssts.push_back(new_sstable(set, 0, 1, 1));
        ssts.push_back(new_sstable(set, 3, 4, 1));
        ssts.push_back(new_sstable(set, 4, 4, 1));
        ssts.push_back(new_sstable(set, 4, 5, 1));

        sstable_set::incremental_selector sel = set.make_incremental_selector();
        check(sel, 0, std::unordered_set<shared_sstable>{ssts[0], ssts[1]});
        check(sel, 1, std::unordered_set<shared_sstable>{ssts[0], ssts[1]});
        check(sel, 2, std::unordered_set<shared_sstable>{});
        check(sel, 3, std::unordered_set<shared_sstable>{ssts[2]});
        check(sel, 4, std::unordered_set<shared_sstable>{ssts[2], ssts[3], ssts[4]});
        check(sel, 5, std::unordered_set<shared_sstable>{ssts[4]});
        check(sel, 6, std::unordered_set<shared_sstable>{});
        check(sel, 7, std::unordered_set<shared_sstable>{});
    }

    {
        sstable_set set = cs.make_sstable_set(s);
        std::unordered_map<dht::token, std::unordered_set<shared_sstable>> map;
        std::vector<shared_sstable> ssts;
        ssts.push_back(new_sstable(set, 0, 1, 0));
        ssts.push_back(new_sstable(set, 0, 1, 1));
        ssts.push_back(new_sstable(set, 0, 1, 1));
        ssts.push_back(new_sstable(set, 3, 4, 1));
        ssts.push_back(new_sstable(set, 4, 4, 1));
        ssts.push_back(new_sstable(set, 4, 5, 1));

        sstable_set::incremental_selector sel = set.make_incremental_selector();
        check(sel, 0, std::unordered_set<shared_sstable>{ssts[0], ssts[1], ssts[2]});
        check(sel, 1, std::unordered_set<shared_sstable>{ssts[0], ssts[1], ssts[2]});
        check(sel, 2, std::unordered_set<shared_sstable>{ssts[0]});
        check(sel, 3, std::unordered_set<shared_sstable>{ssts[0], ssts[3]});
        check(sel, 4, std::unordered_set<shared_sstable>{ssts[0], ssts[3], ssts[4], ssts[5]});
        check(sel, 5, std::unordered_set<shared_sstable>{ssts[0], ssts[5]});
        check(sel, 6, std::unordered_set<shared_sstable>{ssts[0]});
        check(sel, 7, std::unordered_set<shared_sstable>{ssts[0]});
    }

    return make_ready_future<>();
  });
}

SEASTAR_TEST_CASE(sstable_set_erase) {
  return test_env::do_with([] (test_env& env) {
    auto s = make_shared_schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {}, {}, {}, utf8_type);
    const auto key = tests::generate_partition_key(s).key();

    // check that sstable_set::erase is capable of working properly when a non-existing element is given.
    {
        auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::leveled, s->compaction_strategy_options());
        sstable_set set = cs.make_sstable_set(s);

        auto sst = sstable_for_overlapping_test(env, s, key, key, 0);
        set.insert(sst);
        assert_sstable_set_size(set, 1);

        auto unleveled_sst = sstable_for_overlapping_test(env, s, key, key, 0);
        auto leveled_sst = sstable_for_overlapping_test(env, s, key, key, 1);
        set.erase(unleveled_sst);
        set.erase(leveled_sst);
        assert_sstable_set_size(set, 1);
        BOOST_REQUIRE(set.all()->contains(sst));
    }

    {
        auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::leveled, s->compaction_strategy_options());
        sstable_set set = cs.make_sstable_set(s);

        // triggers use-after-free, described in #4572, by operating on interval that relies on info of a destroyed sstable object.
        {
            auto sst = sstable_for_overlapping_test(env, s, key, key, 1);
            set.insert(sst);
            assert_sstable_set_size(set, 1);
        }

        auto sst2 = sstable_for_overlapping_test(env, s, key, key, 1);
        set.insert(sst2);
        assert_sstable_set_size(set, 2);

        set.erase(sst2);
    }

    {
        auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::size_tiered, s->compaction_strategy_options());
        sstable_set set = cs.make_sstable_set(s);

        auto sst = sstable_for_overlapping_test(env, s, key, key, 0);
        set.insert(sst);
        assert_sstable_set_size(set, 1);

        auto sst2 = sstable_for_overlapping_test(env, s, key, key, 0);
        set.erase(sst2);
        assert_sstable_set_size(set, 1);
        BOOST_REQUIRE(set.all()->contains(sst));
    }

    return make_ready_future<>();
  });
}

SEASTAR_TEST_CASE(sstable_tombstone_histogram_test) {
    return test_env::do_with_async([] (test_env& env) {
        for (auto version : writable_sstable_versions) {
            auto builder = schema_builder("tests", "tombstone_histogram_test")
                    .with_column("id", utf8_type, column_kind::partition_key)
                    .with_column("value", int32_type);
            auto s = builder.build();

            auto next_timestamp = [] {
                static thread_local api::timestamp_type next = 1;
                return next++;
            };

            auto make_delete = [&](partition_key key) {
                mutation m(s, key);
                tombstone tomb(next_timestamp(), gc_clock::now());
                m.partition().apply(tomb);
                return m;
            };

            std::vector<mutation> mutations;
            for (auto i = 0; i < sstables::TOMBSTONE_HISTOGRAM_BIN_SIZE * 2; i++) {
                auto key = partition_key::from_exploded(*s, {to_bytes("key" + to_sstring(i))});
                mutations.push_back(make_delete(key));
                forward_jump_clocks(std::chrono::seconds(1));
            }
            auto sst = make_sstable_containing(env.make_sstable(s, version), mutations);
            auto histogram = sst->get_stats_metadata().estimated_tombstone_drop_time;
            sst = env.reusable_sst(sst).get();
            auto histogram2 = sst->get_stats_metadata().estimated_tombstone_drop_time;

            // check that histogram respected limit
            BOOST_REQUIRE(histogram.bin.size() == TOMBSTONE_HISTOGRAM_BIN_SIZE);
            // check that load procedure will properly load histogram from statistics component
            BOOST_REQUIRE(histogram.bin == histogram2.bin);
        }
    });
}

SEASTAR_TEST_CASE(sstable_bad_tombstone_histogram_test) {
    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder("tests", "tombstone_histogram_test")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", int32_type);
        auto s = builder.build();
        auto sst = env.reusable_sst(s, "test/resource/sstables/bad_tombstone_histogram").get();
        auto histogram = sst->get_stats_metadata().estimated_tombstone_drop_time;
        BOOST_REQUIRE(histogram.max_bin_size == sstables::TOMBSTONE_HISTOGRAM_BIN_SIZE);
        // check that bad histogram was discarded
        BOOST_REQUIRE(histogram.bin.empty());
    });
}

SEASTAR_TEST_CASE(sstable_owner_shards) {
    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder("tests", "test")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", int32_type);
        auto s = builder.build();

        auto make_insert = [&] (const dht::decorated_key& key) {
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)), 1);
            return m;
        };
        auto make_shared_sstable = [&] (std::unordered_set<unsigned> shards, unsigned ignore_msb, unsigned smp_count) {
            auto key_schema = schema_builder(s).with_sharder(smp_count, ignore_msb).build();
            auto mut = [&] (auto shard) {
                return make_insert(tests::generate_partition_key(key_schema, shard));
            };
            auto muts = boost::copy_range<std::vector<mutation>>(shards
                | boost::adaptors::transformed([&] (auto shard) { return mut(shard); }));
            auto sst_gen = [&] () mutable {
                auto schema = schema_builder(s).with_sharder(1, ignore_msb).build();
                auto sst = env.make_sstable(std::move(schema));
                return sst;
            };
            auto sst = make_sstable_containing(sst_gen, std::move(muts));
            auto schema = schema_builder(s).with_sharder(smp_count, ignore_msb).build();
            sst = env.reusable_sst(std::move(schema), sst).get();
            return sst;
        };

        auto assert_sstable_owners = [&] (std::unordered_set<unsigned> expected_owners, unsigned ignore_msb, unsigned smp_count) {
            SCYLLA_ASSERT(expected_owners.size() <= smp_count);
            auto sst = make_shared_sstable(expected_owners, ignore_msb, smp_count);
            auto owners = boost::copy_range<std::unordered_set<unsigned>>(sst->get_shards_for_this_sstable());
            BOOST_REQUIRE(boost::algorithm::all_of(expected_owners, [&] (unsigned expected_owner) {
                return owners.contains(expected_owner);
            }));
        };

        assert_sstable_owners({ 0 }, 0, 1);
        assert_sstable_owners({ 0 }, 0, 1);

        assert_sstable_owners({ 0 }, 0, 4);
        assert_sstable_owners({ 0, 1 }, 0, 4);
        assert_sstable_owners({ 0, 2 }, 0, 4);
        assert_sstable_owners({ 0, 1, 2, 3 }, 0, 4);

        assert_sstable_owners({ 0 }, 12, 4);
        assert_sstable_owners({ 0, 1 }, 12, 4);
        assert_sstable_owners({ 0, 2 }, 12, 4);
        assert_sstable_owners({ 0, 1, 2, 3 }, 12, 4);

        assert_sstable_owners({ 10 }, 0, 63);
        assert_sstable_owners({ 10 }, 12, 63);
        assert_sstable_owners({ 10, 15 }, 0, 63);
        assert_sstable_owners({ 10, 15 }, 12, 63);
        assert_sstable_owners({ 0, 10, 15, 20, 30, 40, 50 }, 0, 63);
        assert_sstable_owners({ 0, 10, 15, 20, 30, 40, 50 }, 12, 63);
    });
}

SEASTAR_TEST_CASE(test_summary_entry_spanning_more_keys_than_min_interval) {
    return test_env::do_with_async([] (test_env& env) {
        auto s = make_shared_schema({}, some_keyspace, some_column_family,
            {{"p1", int32_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}}, {}, utf8_type);

        const column_definition& r1_col = *s->get_column_definition("r1");
        std::vector<mutation> mutations;
        auto keys_written = 0;
        for (auto i = 0; i < s->min_index_interval()*1.5; i++) {
            auto key = partition_key::from_exploded(*s, {int32_type->decompose(i)});
            auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});
            mutation m(s, key);
            m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
            mutations.push_back(std::move(m));
            keys_written++;
        }

        auto sst = make_sstable_containing(env.make_sstable(s), mutations);

        summary& sum = sstables::test(sst).get_summary();
        BOOST_REQUIRE(sum.entries.size() == 1);

        std::set<mutation, mutation_decorated_key_less_comparator> merged;
        merged.insert(mutations.begin(), mutations.end());
        auto rd = assert_that(sst->as_mutation_source().make_reader_v2(s, env.make_reader_permit(), query::full_partition_range));
        auto keys_read = 0;
        for (auto&& m : merged) {
            keys_read++;
            rd.produces(m);
        }
        rd.produces_end_of_stream();
        BOOST_REQUIRE(keys_read == keys_written);

        auto r = dht::partition_range::make({mutations.back().decorated_key(), true}, {mutations.back().decorated_key(), true});
        assert_that(sst->as_mutation_source().make_reader_v2(s, env.make_reader_permit(), r))
            .produces(slice(mutations, r))
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_wrong_counter_shard_order) {
        // CREATE TABLE IF NOT EXISTS scylla_bench.test_counters (
        //     pk bigint,
        //     ck bigint,
        //     c1 counter,
        //     c2 counter,
        //     c3 counter,
        //     c4 counter,
        //     c5 counter,
        //     PRIMARY KEY(pk, ck)
        // ) WITH compression = { }
        //
        // Populated with:
        // scylla-bench -mode counter_update -workload uniform -duration 15s
        //      -replication-factor 3 -partition-count 2 -clustering-row-count 4
        // on a three-node Scylla 1.7.4 cluster.
        return test_env::do_with_async([] (test_env& env) {
          for (const auto version : all_sstable_versions) {
            auto s = schema_builder("scylla_bench", "test_counters")
                    .with_column("pk", long_type, column_kind::partition_key)
                    .with_column("ck", long_type, column_kind::clustering_key)
                    .with_column("c1", counter_type)
                    .with_column("c2", counter_type)
                    .with_column("c3", counter_type)
                    .with_column("c4", counter_type)
                    .with_column("c5", counter_type)
                    .build();

            auto sst = env.reusable_sst(s, get_test_dir("wrong_counter_shard_order", s), 2, version).get();
            auto reader = sstable_reader_v2(sst, s, env.make_reader_permit());
            auto close_reader = deferred_close(reader);

            auto verify_row = [&s] (mutation_fragment_v2_opt mfopt, int64_t expected_value) {
                BOOST_REQUIRE(bool(mfopt));
                auto& mf = *mfopt;
                BOOST_REQUIRE(mf.is_clustering_row());
                auto& row = mf.as_clustering_row();
                size_t n = 0;
                row.cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& ac_o_c) {
                    auto acv = ac_o_c.as_atomic_cell(s->regular_column_at(id));
                    counter_cell_view ccv(acv);
                    counter_shard_view::less_compare_by_id cmp;
                    BOOST_REQUIRE_MESSAGE(boost::algorithm::is_sorted(ccv.shards(), cmp),
                                          fmt::format("{} is expected to be sorted", ccv));
                    BOOST_REQUIRE_EQUAL(ccv.total_value(), expected_value);
                    n++;
                });
                BOOST_REQUIRE_EQUAL(n, 5);
            };

            {
                auto mfopt = reader().get();
                BOOST_REQUIRE(mfopt);
                BOOST_REQUIRE(mfopt->is_partition_start());
                verify_row(reader().get(), 28545);
                verify_row(reader().get(), 27967);
                verify_row(reader().get(), 28342);
                verify_row(reader().get(), 28325);
                mfopt = reader().get();
                BOOST_REQUIRE(mfopt);
                BOOST_REQUIRE(mfopt->is_end_of_partition());
            }

            {
                auto mfopt = reader().get();
                BOOST_REQUIRE(mfopt);
                BOOST_REQUIRE(mfopt->is_partition_start());
                verify_row(reader().get(), 28386);
                verify_row(reader().get(), 28378);
                verify_row(reader().get(), 28129);
                verify_row(reader().get(), 28260);
                mfopt = reader().get();
                BOOST_REQUIRE(mfopt);
                BOOST_REQUIRE(mfopt->is_end_of_partition());
            }

            BOOST_REQUIRE(!reader().get());
        }
      });
}

static std::unique_ptr<index_reader> get_index_reader(shared_sstable sst, reader_permit permit) {
    return std::make_unique<index_reader>(sst, std::move(permit));
}

SEASTAR_TEST_CASE(test_broken_promoted_index_is_skipped) {
    // create table ks.test (pk int, ck int, v int, primary key(pk, ck)) with compact storage;
    //
    // Populated with:
    //
    // insert into ks.test (pk, ck, v) values (1, 1, 1);
    // insert into ks.test (pk, ck, v) values (1, 2, 1);
    // insert into ks.test (pk, ck, v) values (1, 3, 1);
    // delete from ks.test where pk = 1 and ck = 2;
    return test_env::do_with_async([] (test_env& env) {
      for (const auto version : all_sstable_versions) {
        auto s = schema_builder("ks", "test")
                .with_column("pk", int32_type, column_kind::partition_key)
                .with_column("ck", int32_type, column_kind::clustering_key)
                .with_column("v", int32_type)
                .build(schema_builder::compact_storage::yes);

        auto sst = env.make_sstable(s, get_test_dir("broken_non_compound_pi_and_range_tombstone", s), sstables::generation_type(1), version);
        try {
            sst->load(sst->get_schema()->get_sharder()).get();
        } catch (...) {
            BOOST_REQUIRE_EXCEPTION(current_exception_as_future().get(), sstables::malformed_sstable_exception, exception_predicate::message_contains(
                "Failed to read partition from SSTable "));
        }

        {
            assert_that(get_index_reader(sst, env.make_reader_permit())).is_empty(*s);
        }
      }
    });
}

SEASTAR_TEST_CASE(test_old_format_non_compound_range_tombstone_is_read) {
    // create table ks.test (pk int, ck int, v int, primary key(pk, ck)) with compact storage;
    //
    // Populated with:
    //
    // insert into ks.test (pk, ck, v) values (1, 1, 1);
    // insert into ks.test (pk, ck, v) values (1, 2, 1);
    // insert into ks.test (pk, ck, v) values (1, 3, 1);
    // delete from ks.test where pk = 1 and ck = 2;
    return test_env::do_with_async([] (test_env& env) {
        for (const auto version : all_sstable_versions) {
            if (version < sstable_version_types::mc) { // Applies only to formats older than 'm'
                auto s = schema_builder("ks", "test")
                    .with_column("pk", int32_type, column_kind::partition_key)
                    .with_column("ck", int32_type, column_kind::clustering_key)
                    .with_column("v", int32_type)
                    .build(schema_builder::compact_storage::yes);

                auto sst = env.reusable_sst(s, get_test_dir("broken_non_compound_pi_and_range_tombstone", s), 1, version).get();

                auto pk = partition_key::from_exploded(*s, { int32_type->decompose(1) });
                auto dk = dht::decorate_key(*s, pk);
                auto ck = clustering_key::from_exploded(*s, {int32_type->decompose(2)});
                mutation m(s, dk);
                m.set_clustered_cell(ck, *s->get_column_definition("v"), atomic_cell::make_live(*int32_type, 1511270919978349, int32_type->decompose(1), { }));
                m.partition().apply_delete(*s, ck, {1511270943827278, gc_clock::from_time_t(1511270943)});

                {
                    auto slice = partition_slice_builder(*s).with_range(query::clustering_range::make_singular({ck})).build();
                    assert_that(sst->as_mutation_source().make_reader_v2(s, env.make_reader_permit(), dht::partition_range::make_singular(dk), slice))
                            .produces(m)
                            .produces_end_of_stream();
                }
            }
        }
    });
}

SEASTAR_TEST_CASE(summary_rebuild_sanity) {
    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder("tests", "test")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", utf8_type);
        builder.set_compressor_params(compression_parameters::no_compression());
        auto s = builder.build(schema_builder::compact_storage::no);
        const column_definition& col = *s->get_column_definition("value");

        auto make_insert = [&] (partition_key key) {
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), col, make_atomic_cell(utf8_type, bytes(1024, 'a')));
            return m;
        };

        std::vector<mutation> mutations;
        for (auto i = 0; i < s->min_index_interval()*2; i++) {
            auto key = to_bytes("key" + to_sstring(i));
            mutations.push_back(make_insert(partition_key::from_exploded(*s, {std::move(key)})));
        }

        auto sst = make_sstable_containing(env.make_sstable(s), mutations);

        summary s1 = sstables::test(sst).move_summary();
        BOOST_REQUIRE(s1.entries.size() > 1);

        sstables::test(sst).remove_component(component_type::Summary).get();
        sst = env.reusable_sst(sst).get();
        summary& s2 = sstables::test(sst).get_summary();

        BOOST_REQUIRE(::memcmp(&s1.header, &s2.header, sizeof(summary::header)) == 0);
        BOOST_REQUIRE(s1.positions == s2.positions);
        BOOST_REQUIRE(s1.entries == s2.entries);
        BOOST_REQUIRE(s1.first_key.value == s2.first_key.value);
        BOOST_REQUIRE(s1.last_key.value == s2.last_key.value);
    });
}

SEASTAR_TEST_CASE(sstable_partition_estimation_sanity_test) {
    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder("tests", "test")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", utf8_type);
        builder.set_compressor_params(compression_parameters::no_compression());
        auto s = builder.build(schema_builder::compact_storage::no);
        const column_definition& col = *s->get_column_definition("value");

        auto summary_byte_cost = sstables::index_sampling_state::default_summary_byte_cost;

        auto make_large_partition = [&] (partition_key key) {
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), col, make_atomic_cell(utf8_type, bytes(20 * summary_byte_cost, 'a')));
            return m;
        };

        auto make_small_partition = [&] (partition_key key) {
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), col, make_atomic_cell(utf8_type, bytes(100, 'a')));
            return m;
        };

        {
            auto total_partitions = s->min_index_interval()*2;

            std::vector<mutation> mutations;
            for (auto i = 0; i < total_partitions; i++) {
                auto key = to_bytes("key" + to_sstring(i));
                mutations.push_back(make_large_partition(partition_key::from_exploded(*s, {std::move(key)})));
            }
            auto sst = make_sstable_containing(env.make_sstable(s), mutations);

            BOOST_REQUIRE(std::abs(int64_t(total_partitions) - int64_t(sst->get_estimated_key_count())) <= s->min_index_interval());
        }

        {
            auto total_partitions = s->min_index_interval()*2;

            std::vector<mutation> mutations;
            for (auto i = 0; i < total_partitions; i++) {
                auto key = to_bytes("key" + to_sstring(i));
                mutations.push_back(make_small_partition(partition_key::from_exploded(*s, {std::move(key)})));
            }
            auto sst = make_sstable_containing(env.make_sstable(s), mutations);

            BOOST_REQUIRE(std::abs(int64_t(total_partitions) - int64_t(sst->get_estimated_key_count())) <= s->min_index_interval());
        }
    });
}

SEASTAR_TEST_CASE(sstable_timestamp_metadata_correcness_with_negative) {
    BOOST_REQUIRE(smp::count == 1);
    return test_env::do_with_async([] (test_env& env) {
        for (auto version : writable_sstable_versions) {
            auto s = schema_builder("tests", "ts_correcness_test")
                    .with_column("id", utf8_type, column_kind::partition_key)
                    .with_column("value", int32_type).build();

            auto make_insert = [&](partition_key key, api::timestamp_type ts) {
                mutation m(s, key);
                m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)), ts);
                return m;
            };

            auto alpha = partition_key::from_exploded(*s, {to_bytes("alpha")});
            auto beta = partition_key::from_exploded(*s, {to_bytes("beta")});

            auto mut1 = make_insert(alpha, -50);
            auto mut2 = make_insert(beta, 5);

            auto sst = make_sstable_containing(env.make_sstable(s, version), {mut1, mut2});

            BOOST_REQUIRE(sst->get_stats_metadata().min_timestamp == -50);
            BOOST_REQUIRE(sst->get_stats_metadata().max_timestamp == 5);
        }
    });
}

SEASTAR_TEST_CASE(sstable_run_identifier_correctness) {
    BOOST_REQUIRE(smp::count == 1);
    return test_env::do_with_async([] (test_env& env) {
        auto s = schema_builder("tests", "ts_correcness_test")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", int32_type).build();

        mutation mut(s, partition_key::from_exploded(*s, {to_bytes("alpha")}));
        mut.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)), 0);

        sstable_writer_config cfg = env.manager().configure_writer();
        cfg.run_identifier = sstables::run_id::create_random_id();
        auto sst = make_sstable_easy(env, make_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), std::move(mut)), cfg);

        BOOST_REQUIRE(sst->run_identifier() == cfg.run_identifier);
    });
}

SEASTAR_TEST_CASE(sstable_run_disjoint_invariant_test) {
    return test_env::do_with([] (test_env& env) {
        simple_schema ss;
        auto s = ss.schema();

        const auto keys = tests::generate_partition_keys(6, s);

        sstables::sstable_run run;

        auto insert = [&] (int first_key_idx, int last_key_idx) {
            auto sst = sstable_for_overlapping_test(env, s, keys[first_key_idx].key(), keys[last_key_idx].key());
            return run.insert(sst);
        };

        // insert ranges [0, 0], [1, 1], [3, 4]
        BOOST_REQUIRE(insert(0, 0) == true);
        BOOST_REQUIRE(insert(1, 1) == true);
        BOOST_REQUIRE(insert(3, 4) == true);
        BOOST_REQUIRE(run.all().size() == 3);

        // check overlapping candidates won't be inserted
        BOOST_REQUIRE(insert(0, 4) == false);
        BOOST_REQUIRE(insert(4, 5) == false);
        BOOST_REQUIRE(run.all().size() == 3);

        // check non-overlapping candidates will be inserted
        BOOST_REQUIRE(insert(2, 2) == true);
        BOOST_REQUIRE(insert(5, 5) == true);
        BOOST_REQUIRE(run.all().size() == 5);

        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(sstable_run_clustering_disjoint_invariant_test) {
    return test_env::do_with_async([] (test_env& env) {
        simple_schema ss;
        auto s = ss.schema();
        auto pks = ss.make_pkeys(1);

        auto make_sstable = [&] (int first_ckey_idx, int last_ckey_idx) {
            std::vector<mutation> muts;
            auto mut = mutation(s, pks[0]);

            auto first_ckey_prefix = ss.make_ckey(first_ckey_idx);
            mut.partition().apply_insert(*s, first_ckey_prefix, ss.new_timestamp());
            auto last_ckey_prefix = ss.make_ckey(last_ckey_idx);
            if (first_ckey_prefix != last_ckey_prefix) {
                mut.partition().apply_insert(*s, last_ckey_prefix, ss.new_timestamp());
            }
            muts.push_back(std::move(mut));

            auto sst = make_sstable_containing(env.make_sstable(s), std::move(muts));

            BOOST_REQUIRE(sst->min_position().key() == first_ckey_prefix);
            BOOST_REQUIRE(sst->max_position().key() == last_ckey_prefix);
            testlog.info("sstable: {} {} -> {} {}", first_ckey_idx, last_ckey_idx, sst->first_partition_first_position(), sst->last_partition_last_position());

            return sst;
        };

        sstables::sstable_run run;

        auto insert = [&] (int first_ckey_idx, int last_ckey_idx) {
            auto sst = make_sstable(first_ckey_idx, last_ckey_idx);
            return run.insert(sst);
        };

        // insert sstables with disjoint clustering ranges [0, 1], [4, 5], [6, 7]
        BOOST_REQUIRE(insert(0, 1) == true);
        BOOST_REQUIRE(insert(4, 5) == true);
        BOOST_REQUIRE(insert(6, 7) == true);
        BOOST_REQUIRE(run.all().size() == 3);

        // check overlapping candidates won't be inserted
        BOOST_REQUIRE(insert(0, 4) == false);
        BOOST_REQUIRE(insert(1, 3) == false);
        BOOST_REQUIRE(insert(5, 6) == false);
        BOOST_REQUIRE(insert(7, 8) == false);
        BOOST_REQUIRE(run.all().size() == 3);

        // check non-overlapping candidates will be inserted
        BOOST_REQUIRE(insert(2, 3) == true);
        BOOST_REQUIRE(insert(8, 9) == true);
        BOOST_REQUIRE(run.all().size() == 5);
    });
}

SEASTAR_TEST_CASE(test_reads_cassandra_static_compact) {
    return test_env::do_with_async([] (test_env& env) {
        // CREATE COLUMNFAMILY cf (key varchar PRIMARY KEY, c2 text, c1 text) WITH COMPACT STORAGE ;
        auto s = schema_builder("ks", "cf")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("c1", utf8_type)
            .with_column("c2", utf8_type)
            .build(schema_builder::compact_storage::yes);

        // INSERT INTO ks.cf (key, c1, c2) VALUES ('a', 'abc', 'cde');
        auto sst = env.reusable_sst(s, get_test_dir("cassandra_static_compact", s), 1, sstables::sstable::version_types::mc).get();
        auto pkey = partition_key::from_exploded(*s, { utf8_type->decompose("a") });
        auto dkey = dht::decorate_key(*s, std::move(pkey));
        mutation m(s, dkey);
        m.set_clustered_cell(clustering_key::make_empty(), *s->get_column_definition("c1"),
                    atomic_cell::make_live(*utf8_type, 1551785032379079, utf8_type->decompose("abc"), {}));
        m.set_clustered_cell(clustering_key::make_empty(), *s->get_column_definition("c2"),
                    atomic_cell::make_live(*utf8_type, 1551785032379079, utf8_type->decompose("cde"), {}));

        assert_that(sst->as_mutation_source().make_reader_v2(s, env.make_reader_permit()))
            .produces(m)
            .produces_end_of_stream();
    });
}

static dht::token token_from_long(int64_t value) {
    return dht::token{ value };
}

SEASTAR_TEST_CASE(basic_interval_map_testing_for_sstable_set) {
    using value_set = std::unordered_set<int64_t>;
    using interval_map_type = boost::icl::interval_map<dht::compatible_ring_position_or_view, value_set>;
    using interval_type = interval_map_type::interval_type;

    interval_map_type map;

        auto builder = schema_builder("tests", "test")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", int32_type);
        auto s = builder.build();

    auto make_pos = [&] (int64_t token) {
        return dht::compatible_ring_position_or_view(s, dht::ring_position::starting_at(token_from_long(token)));
    };

    auto add = [&] (int64_t start, int64_t end, int gen) {
        map.insert({interval_type::closed(make_pos(start), make_pos(end)), value_set({gen})});
    };

    auto subtract = [&] (int64_t start, int64_t end, int gen) {
        map.subtract({interval_type::closed(make_pos(start), make_pos(end)), value_set({gen})});
    };

    add(6052159333454473039, 9223347124876901511, 0);
    add(957694089857623813, 6052133625299168475, 1);
    add(-9223359752074096060, -4134836824175349559, 2);
    add(-4134776408386727187, 957682147550689253, 3);
    add(6092345676202690928, 9223332435915649914, 4);
    add(-5395436281861775460, -1589168419922166021, 5);
    add(-1589165560271708558, 6092259415972553765, 6);
    add(-9223362900961284625, -5395452288575292639, 7);

    subtract(-9223359752074096060, -4134836824175349559, 2);
    subtract(-9223362900961284625, -5395452288575292639, 7);
    subtract(-4134776408386727187, 957682147550689253, 3);
    subtract(-5395436281861775460, -1589168419922166021, 5);
    subtract(957694089857623813, 6052133625299168475, 1);

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_zero_estimated_partitions) {
    return test_env::do_with_async([] (test_env& env) {
        simple_schema ss;
        auto s = ss.schema();

        const auto pk = tests::generate_partition_key(s);
        auto mut = mutation(s, pk);
        ss.add_row(mut, ss.make_ckey(0), "val");

        for (const auto version : writable_sstable_versions) {
            testlog.info("version={}", version);

            auto mr = make_mutation_reader_from_mutations_v2(ss.schema(), env.make_reader_permit(), mut);
            sstable_writer_config cfg = env.manager().configure_writer();
            auto sst = make_sstable_easy(env, std::move(mr), cfg, version, 0);

            auto sst_mr = sst->as_mutation_source().make_reader_v2(s, env.make_reader_permit(), query::full_partition_range, s->full_slice());
            auto close_mr = deferred_close(sst_mr);
            auto sst_mut = read_mutation_from_mutation_reader(sst_mr).get();

            // The real test here is that we don't SCYLLA_ASSERT() in
            // sstables::prepare_summary() with the write_components() call above,
            // this is only here as a sanity check.
            BOOST_REQUIRE(sst_mr.is_buffer_empty());
            BOOST_REQUIRE(sst_mr.is_end_of_stream());
            BOOST_REQUIRE(sst_mut);
            BOOST_REQUIRE_EQUAL(mut, *sst_mut);
        }
    });
}

SEASTAR_TEST_CASE(test_may_have_partition_tombstones) {
    return test_env::do_with_async([] (test_env& env) {
        simple_schema ss;
        auto s = ss.schema();
        auto pks = ss.make_pkeys(2);

        for (auto version : all_sstable_versions) {
            if (version < sstable_version_types::md) {
                continue;
            }

            auto mut1 = mutation(s, pks[0]);
            auto mut2 = mutation(s, pks[1]);
            mut1.partition().apply_insert(*s, ss.make_ckey(0), ss.new_timestamp());
            mut1.partition().apply_delete(*s, ss.make_ckey(1), ss.new_tombstone());
            ss.add_row(mut1, ss.make_ckey(2), "val");
            ss.delete_range(mut1, query::clustering_range::make({ss.make_ckey(3)}, {ss.make_ckey(5)}));
            ss.add_row(mut2, ss.make_ckey(6), "val");

            {
                auto sst = make_sstable_containing(env.make_sstable(s, version), {mut1, mut2});
                BOOST_REQUIRE(!sst->may_have_partition_tombstones());
            }

            mut2.partition().apply(ss.new_tombstone());
            auto sst = make_sstable_containing(env.make_sstable(s, version), {mut1, mut2});
            BOOST_REQUIRE(sst->may_have_partition_tombstones());
        }
    });
}

SEASTAR_TEST_CASE(test_missing_partition_end_fragment) {
    return test_env::do_with_async([] (test_env& env) {
        simple_schema ss;
        auto s = ss.schema();

        auto pkeys = ss.make_pkeys(2);

        set_abort_on_internal_error(false);
        auto enable_aborts = defer([] { set_abort_on_internal_error(true); }); // FIXME: restore to previous value

        for (const auto version : writable_sstable_versions) {
            testlog.info("version={}", version);

            std::deque<mutation_fragment_v2> frags;
            frags.push_back(mutation_fragment_v2(*s, env.make_reader_permit(), partition_start(pkeys[0], tombstone())));
            frags.push_back(mutation_fragment_v2(*s, env.make_reader_permit(), clustering_row(ss.make_ckey(0))));
            // partition_end is missing
            frags.push_back(mutation_fragment_v2(*s, env.make_reader_permit(), partition_start(pkeys[1], tombstone())));
            frags.push_back(mutation_fragment_v2(*s, env.make_reader_permit(), clustering_row(ss.make_ckey(0))));
            frags.push_back(mutation_fragment_v2(*s, env.make_reader_permit(), partition_end()));

            auto mr = make_mutation_reader_from_fragments(s, env.make_reader_permit(), std::move(frags));
            auto close_mr = deferred_close(mr);

            auto sst = env.make_sstable(s, version);
            sstable_writer_config cfg = env.manager().configure_writer();

            try {
                auto wr = sst->get_writer(*s, 1, cfg, encoding_stats{});
                mr.consume_in_thread(std::move(wr));
                BOOST_FAIL("write_components() should have failed");
            } catch (const std::runtime_error&) {
                testlog.info("failed as expected: {}", std::current_exception());
            }
        }
    });
}

SEASTAR_TEST_CASE(test_sstable_origin) {
    return test_env::do_with_async([] (test_env& env) {
        simple_schema ss;
        auto s = ss.schema();

        const auto pk = tests::generate_partition_key(s);
        auto mut = mutation(s, pk);
        ss.add_row(mut, ss.make_ckey(0), "val");

        for (const auto version : all_sstable_versions) {
            if (version < sstable_version_types::mc) {
                continue;
            }

            // Test empty sstable_origin.
            auto mr = make_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), mut);
            sstable_writer_config cfg = env.manager().configure_writer("");
            auto sst = make_sstable_easy(env, std::move(mr), cfg, version, 0);
            BOOST_REQUIRE_EQUAL(sst->get_origin(), "");

            // Test that a random sstable_origin is stored and retrieved properly.
            mr = make_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), mut);
            sstring origin = fmt::format("test-{}", tests::random::get_sstring());
            cfg = env.manager().configure_writer(origin);
            sst = make_sstable_easy(env, std::move(mr), cfg, version, 0);
            BOOST_REQUIRE_EQUAL(sst->get_origin(), origin);
        }
    });
}

SEASTAR_TEST_CASE(compound_sstable_set_basic_test) {
    return test_env::do_with([] (test_env& env) {
        auto s = make_shared_schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {}, {}, {}, utf8_type);
        auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::size_tiered, s->compaction_strategy_options());

        lw_shared_ptr<sstables::sstable_set> set1 = make_lw_shared(cs.make_sstable_set(s));
        lw_shared_ptr<sstables::sstable_set> set2 = make_lw_shared(cs.make_sstable_set(s));
        lw_shared_ptr<sstables::sstable_set> compound = make_lw_shared(sstables::make_compound_sstable_set(s, {set1, set2}));

        const auto keys = tests::generate_partition_keys(2, s);
        set1->insert(sstable_for_overlapping_test(env, s, keys[0].key(), keys[1].key(), 0));
        set2->insert(sstable_for_overlapping_test(env, s, keys[0].key(), keys[1].key(), 0));
        set2->insert(sstable_for_overlapping_test(env, s, keys[0].key(), keys[1].key(), 0));

        BOOST_REQUIRE(boost::accumulate(*compound->all() | boost::adaptors::transformed([] (const sstables::shared_sstable& sst) { return sst->generation().as_int(); }), unsigned(0)) == 6);
        {
            unsigned found = 0;
            for (auto sstables = compound->all(); [[maybe_unused]] auto& sst : *sstables) {
                found++;
            }
            size_t compound_size = compound->all()->size();
            BOOST_REQUIRE(compound_size == 3);
            BOOST_REQUIRE(compound_size == found);
        }

        {
            auto cloned_compound = *compound;
            assert_sstable_set_size(cloned_compound, 3);
        }

        set2 = make_lw_shared(cs.make_sstable_set(s));
        compound = make_lw_shared(sstables::make_compound_sstable_set(s, {set1, set2}));
        {
            unsigned found = 0;
            for (auto sstables = compound->all(); [[maybe_unused]] auto& sst : *sstables) {
                found++;
            }
            size_t compound_size = compound->all()->size();
            BOOST_REQUIRE(compound_size == 1);
            BOOST_REQUIRE(compound_size == found);
        }

        return make_ready_future<>();
    }, test_env_config{ .use_uuid = false });
}

SEASTAR_TEST_CASE(sstable_reader_with_timeout) {
    return test_env::do_with_async([] (test_env& env) {
            auto s = complex_schema();

            auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
            auto cp = clustering_key_prefix::from_exploded(*s, {to_bytes("c1")});

            mutation m(s, key);

            tombstone tomb(api::new_timestamp(), gc_clock::now());
            m.partition().apply_delete(*s, cp, tomb);

            auto sstp = make_sstable_containing(env.make_sstable(s), {std::move(m)});
            auto pr = dht::partition_range::make_singular(make_dkey(s, "key1"));
            auto timeout = db::timeout_clock::now();
            auto rd = sstp->make_reader(s, env.make_reader_permit(timeout), pr, s->full_slice());
            auto close_rd = deferred_close(rd);
            auto f = read_mutation_from_mutation_reader(rd);
            BOOST_REQUIRE_THROW(f.get(), timed_out_error);
    });
}

SEASTAR_TEST_CASE(test_validate_checksums) {
    return test_env::do_with_async([&] (test_env& env) {
        auto random_spec = tests::make_random_schema_specification(
                get_name(),
                std::uniform_int_distribution<size_t>(1, 4),
                std::uniform_int_distribution<size_t>(2, 4),
                std::uniform_int_distribution<size_t>(2, 8),
                std::uniform_int_distribution<size_t>(2, 8));
        auto random_schema = tests::random_schema{tests::random::get_int<uint32_t>(), *random_spec};
        auto schema = random_schema.schema();
        auto permit = env.make_reader_permit();

        testlog.info("Random schema:\n{}", random_schema.cql());

        const auto muts = tests::generate_random_mutations(random_schema).get();

        const std::map<sstring, sstring> no_compression_params = {};
        const std::map<sstring, sstring> lz4_compression_params = {{compression_parameters::SSTABLE_COMPRESSION, "LZ4Compressor"}};

        for (const auto version : writable_sstable_versions) {
            testlog.info("version={}", version);
            for (const auto& compression_params : {no_compression_params, lz4_compression_params}) {
                testlog.info("compression={}", compression_params);
                auto sst_schema = schema_builder(schema).set_compressor_params(compression_params).build();

                auto mr = make_mutation_reader_from_mutations_v2(schema, permit, muts);
                auto close_mr = deferred_close(mr);

                auto sst = env.make_sstable(sst_schema, version);
                sstable_writer_config cfg = env.manager().configure_writer();

                auto wr = sst->get_writer(*sst_schema, 1, cfg, encoding_stats{});
                mr.consume_in_thread(std::move(wr));

                sst->load(sst->get_schema()->get_sharder()).get();

                bool valid;

                testlog.info("Validating intact {}", sst->get_filename());

                valid = sstables::validate_checksums(sst, permit).get();
                BOOST_REQUIRE(valid);

                auto sst_file = open_file_dma(test(sst).filename(sstables::component_type::Data).native(), open_flags::wo).get();
                auto close_sst_file = defer([&sst_file] { sst_file.close().get(); });

                testlog.info("Validating corrupted {}", sst->get_filename());

                { // corrupt the sstable
                    const auto size = std::min(sst->ondisk_data_size() / 2, uint64_t(1024));
                    auto buf = temporary_buffer<char>::aligned(sst_file.disk_write_dma_alignment(), size);
                    std::fill(buf.get_write(), buf.get_write() + size, 0xba);
                    sst_file.dma_write(sst->ondisk_data_size() / 2, buf.begin(), buf.size()).get();
                }

                valid = sstables::validate_checksums(sst, permit).get();
                BOOST_REQUIRE(!valid);

                testlog.info("Validating truncated {}", sst->get_filename());

                { // truncate the sstable
                    sst_file.truncate(sst->ondisk_data_size() / 2).get();
                }

                valid = sstables::validate_checksums(sst, permit).get();
                BOOST_REQUIRE(!valid);
            }
        }
    });
}

SEASTAR_TEST_CASE(partial_sstable_deletion_test) {
    return test_env::do_with_async([] (test_env& env) {
        simple_schema ss;
        auto s = ss.schema();
        auto pks = ss.make_pkeys(1);

        auto mut1 = mutation(s, pks[0]);
        mut1.partition().apply_insert(*s, ss.make_ckey(0), ss.new_timestamp());
        auto sst = make_sstable_containing(env.make_sstable(s), {std::move(mut1)});

        // Rename TOC into TMP toc, to stress deletion path for partial files
        rename_file(test(sst).filename(sstables::component_type::TOC).native(), test(sst).filename(sstables::component_type::TemporaryTOC).native()).get();

        sst->unlink().get();
    });
}

SEASTAR_TEST_CASE(test_index_fast_forwarding_after_eof) {
    return test_env::do_with_async([&] (test_env& env) {
        auto random_spec = tests::make_random_schema_specification(
                get_name(),
                std::uniform_int_distribution<size_t>(1, 4),
                std::uniform_int_distribution<size_t>(2, 4),
                std::uniform_int_distribution<size_t>(2, 8),
                std::uniform_int_distribution<size_t>(2, 8));
        auto random_schema = tests::random_schema{tests::random::get_int<uint32_t>(), *random_spec};
        auto schema = random_schema.schema();
        auto permit = env.make_reader_permit();

        testlog.info("Random schema:\n{}", random_schema.cql());

        const auto muts = tests::generate_random_mutations(random_schema, 2).get();

        auto sst = env.make_sstable(schema, writable_sstable_versions.back());
        {
            auto mr = make_mutation_reader_from_mutations_v2(schema, permit, muts);
            auto close_mr = deferred_close(mr);

            sstable_writer_config cfg = env.manager().configure_writer();

            auto wr = sst->get_writer(*schema, 1, cfg, encoding_stats{});
            mr.consume_in_thread(std::move(wr));

            sst->load(sst->get_schema()->get_sharder()).get();
        }

        const auto t1 = muts.front().decorated_key()._token;
        const auto t2 = muts.back().decorated_key()._token;
        dht::partition_range_vector prs;

        prs.emplace_back(dht::ring_position::starting_at(dht::token{t1.raw() - 200}), dht::ring_position::ending_at(dht::token{t1.raw() - 100}));
        prs.emplace_back(dht::ring_position::starting_at(dht::token{t1.raw() + 2}), dht::ring_position::ending_at(dht::token{t2.raw() + 2}));
        // Should be at eof() after the above range is finished
        prs.emplace_back(dht::ring_position::starting_at(dht::token{t2.raw() + 100}), dht::ring_position::ending_at(dht::token{t2.raw() + 200}));
        prs.emplace_back(dht::ring_position::starting_at(dht::token{t2.raw() + 300}), dht::ring_position::ending_at(dht::token{t2.raw() + 400}));

        auto reader = sst->make_reader(schema, permit, prs.front(), schema->full_slice());
        auto close_reader = deferred_close(reader);
        while (reader().get());

        auto& region = env.manager().get_cache_tracker().region();

        for (auto it = std::next(prs.begin()); it != prs.end(); ++it) {
            testlog.info("fast_forward_to({})", *it);
            reader.fast_forward_to(*it).get();
            while (reader().get());
            // Make sure the index page linked into LRU after EOF is evicted.
            while (region.evict_some() == memory::reclaiming_result::reclaimed_something);
        }
    });
}

SEASTAR_TEST_CASE(test_crawling_reader_out_of_range_last_range_tombstone_change) {
    return test_env::do_with_async([] (test_env& env) {
        simple_schema table;

        auto mut = table.new_mutation("pk0");
        auto ckeys = table.make_ckeys(4);
        table.add_row(mut, ckeys[0], "v0");
        table.add_row(mut, ckeys[1], "v1");
        table.add_row(mut, ckeys[2], "v2");
        using bound = query::clustering_range::bound;
        table.delete_range(mut, query::clustering_range::make(bound{ckeys[3], true}, bound{clustering_key::make_empty(), true}), tombstone(1, gc_clock::now()));

        auto sst = make_sstable_containing(env.make_sstable(table.schema()), {mut});

        assert_that(sst->make_crawling_reader(table.schema(), env.make_reader_permit())).has_monotonic_positions();
    });
}

SEASTAR_TEST_CASE(test_crawling_reader_random_schema_random_mutations) {
    return test_env::do_with_async([] (test_env& env) {
        auto random_spec = tests::make_random_schema_specification(
                get_name(),
                std::uniform_int_distribution<size_t>(1, 4),
                std::uniform_int_distribution<size_t>(2, 4),
                std::uniform_int_distribution<size_t>(2, 8),
                std::uniform_int_distribution<size_t>(2, 8));
        auto random_schema = tests::random_schema{tests::random::get_int<uint32_t>(), *random_spec};
        auto schema = random_schema.schema();

        testlog.info("Random schema:\n{}", random_schema.cql());

        const auto muts = tests::generate_random_mutations(random_schema, 20).get();

        auto sst = make_sstable_containing(env.make_sstable(schema), muts);

        {
            auto rd = assert_that(sst->make_crawling_reader(schema, env.make_reader_permit()));

            for (const auto& mut : muts) {
                rd.produces(mut);
            }
        }

        assert_that(sst->make_crawling_reader(schema, env.make_reader_permit())).has_monotonic_positions();
    });
}

SEASTAR_TEST_CASE(find_first_position_in_partition_from_sstable_test) {
    return test_env::do_with_async([] (test_env& env) {
        class with_range_tombstone_tag;
        using with_range_tombstone = bool_class<with_range_tombstone_tag>;
        class with_static_row_tag;
        using with_static_row = bool_class<with_static_row_tag>;

        auto check_sstable_first_and_last_positions = [&] (size_t partitions, with_range_tombstone with_range_tombstone, with_static_row with_static_row) {
            testlog.info("check_sstable_first_and_last_positions: partitions={}, with_range_tombstone={}, with_static_row={}",
                partitions, bool(with_range_tombstone), bool(with_static_row));
            simple_schema ss;
            auto s = ss.schema();
            auto pks = ss.make_pkeys(partitions);
            auto tmp = env.tempdir().make_sweeper();

            std::vector<mutation> muts;
            std::optional<position_in_partition> first_position, last_position;

            static constexpr size_t ckeys_per_partition = 10;

            size_t ck_idx = 0;
            for (size_t pr = 0; pr < partitions; pr++) {
                auto mut1 = mutation(s, pks[pr]);

                if (with_static_row) {
                    ss.add_static_row(mut1, "svalue");
                    if (!first_position) {
                        first_position = position_in_partition::for_static_row();
                    }
                }

                for (size_t ck = 0; ck < ckeys_per_partition; ck++) {
                    auto ckey = ss.make_ckey(ck_idx + ck);
                    if (!first_position) {
                        first_position = position_in_partition::for_key(ckey);
                    }
                    last_position = position_in_partition::for_key(ckey);
                    if (with_range_tombstone && ck % 2 == 0) {
                        tombstone tomb(ss.new_timestamp(), gc_clock::now());
                        range_tombstone rt(
                            bound_view(ckey, bound_kind::incl_start),
                            bound_view(ckey, bound_kind::incl_end),
                            tomb);
                        mut1.partition().apply_delete(*s, std::move(rt));
                    } else {
                        mut1.partition().apply_insert(*s, ckey, ss.new_timestamp());
                    }
                }
                muts.push_back(std::move(mut1));
            }
            auto sst = make_sstable_containing(env.make_sstable(s), std::move(muts));
            position_in_partition::equal_compare eq(*s);
            if (!with_static_row) {
                BOOST_REQUIRE(sst->min_position().key() == first_position->key());
                BOOST_REQUIRE(sst->max_position().key() == last_position->key());
            }

            auto first_position_opt = sst->find_first_position_in_partition(env.make_reader_permit(), sst->get_first_decorated_key(), false).get();
            BOOST_REQUIRE(first_position_opt);

            auto last_position_opt = sst->find_first_position_in_partition(env.make_reader_permit(), sst->get_last_decorated_key(), true).get();
            BOOST_REQUIRE(last_position_opt);

            BOOST_REQUIRE(eq(*first_position_opt, *first_position));
            BOOST_REQUIRE(eq(*last_position_opt, *last_position));

            BOOST_REQUIRE(eq(sst->first_partition_first_position(), *first_position));
            BOOST_REQUIRE(eq(sst->last_partition_last_position(), *last_position));
        };

        std::array<size_t, 2> partitions_options = { 1, 5 };
        std::array<with_range_tombstone, 2> range_tombstone_options = { with_range_tombstone::no, with_range_tombstone::yes };
        std::array<with_static_row, 2> static_row_options = { with_static_row::no, with_static_row::yes };

        for (size_t partitions : partitions_options) {
            for (with_range_tombstone range_tombstone_opt : range_tombstone_options) {
                for (with_static_row static_row_opt : static_row_options) {
                    check_sstable_first_and_last_positions(partitions, range_tombstone_opt, static_row_opt);
                }
            }
        }
    });
}

future<> test_sstable_bytes_correctness(sstring tname, test_env_config cfg) {
    return test_env::do_with_async([tname] (test_env& env) {
        auto random_spec = tests::make_random_schema_specification(
                tname,
                std::uniform_int_distribution<size_t>(1, 4),
                std::uniform_int_distribution<size_t>(2, 4),
                std::uniform_int_distribution<size_t>(2, 8),
                std::uniform_int_distribution<size_t>(2, 8));
        auto random_schema = tests::random_schema{tests::random::get_int<uint32_t>(), *random_spec};
        auto schema = random_schema.schema();

        testlog.info("Random schema:\n{}", random_schema.cql());

        const auto muts = tests::generate_random_mutations(random_schema, 20).get();

        auto sst = make_sstable_containing(env.make_sstable(schema), muts);

        auto free_space = sst->get_storage().free_space().get();
        BOOST_REQUIRE(free_space > 0);
        testlog.info("prefix: {}, free space: {}", sst->get_storage().prefix(), free_space);

        auto get_bytes_on_disk_from_storage = [&] (const sstables::shared_sstable& sst) {
            uint64_t bytes_on_disk = 0;
            auto& underlying_storage = const_cast<sstables::storage&>(sst->get_storage());
            for (auto& component_type : sstables::test(sst).get_components()) {
                file f = underlying_storage.open_component(*sst, component_type, open_flags::ro, file_open_options{}, true).get();
                bytes_on_disk += f.size().get();
            }
            return bytes_on_disk;
        };

        auto expected_bytes_on_disk = get_bytes_on_disk_from_storage(sst);

        testlog.info("expected={}, actual={}", expected_bytes_on_disk, sst->bytes_on_disk());

        BOOST_REQUIRE(sst->bytes_on_disk() == expected_bytes_on_disk);
    }, std::move(cfg));
}

SEASTAR_TEST_CASE(test_sstable_bytes_on_disk_correctness) {
    return test_sstable_bytes_correctness(get_name() + "_disk", {});
}

SEASTAR_TEST_CASE(test_sstable_bytes_on_s3_correctness) {
    return test_sstable_bytes_correctness(get_name() + "_s3", test_env_config{ .storage = make_test_object_storage_options() });
}

SEASTAR_TEST_CASE(test_sstable_set_predicate) {
    return test_env::do_with_async([] (test_env& env) {
        auto random_spec = tests::make_random_schema_specification(
                get_name(),
                std::uniform_int_distribution<size_t>(1, 4),
                std::uniform_int_distribution<size_t>(2, 4),
                std::uniform_int_distribution<size_t>(2, 8),
                std::uniform_int_distribution<size_t>(2, 8));
        auto random_schema = tests::random_schema{tests::random::get_int<uint32_t>(), *random_spec};
        auto s = random_schema.schema();

        testlog.info("Random schema:\n{}", random_schema.cql());

        const auto muts = tests::generate_random_mutations(random_schema, 20).get();

        auto sst = make_sstable_containing(env.make_sstable(s), muts);

        auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::leveled, s->compaction_strategy_options());
        sstable_set set = cs.make_sstable_set(s);
        set.insert(sst);

        auto first_key_pr = dht::partition_range::make_singular(sst->get_first_decorated_key());

        auto make_point_query_reader = [&] (std::predicate<const sstable&> auto& pred) {
            auto t = env.make_table_for_tests(s);
            auto close_t = deferred_stop(t);
            utils::estimated_histogram eh;
            return set.create_single_key_sstable_reader(&*t, s, env.make_reader_permit(), eh,
                                                       first_key_pr,
                                                       s->full_slice(),
                                                       tracing::trace_state_ptr(),
                                                       ::streamed_mutation::forwarding::no,
                                                       ::mutation_reader::forwarding::no,
                                                       pred);
        };

        auto make_full_scan_reader = [&] (std::predicate<const sstable&> auto& pred) {
            return set.make_local_shard_sstable_reader(s, env.make_reader_permit(),
                                                       query::full_partition_range,
                                                       s->full_slice(),
                                                       tracing::trace_state_ptr(),
                                                       ::streamed_mutation::forwarding::no,
                                                       ::mutation_reader::forwarding::no,
                                                       default_read_monitor_generator(),
                                                       pred);
        };

        auto verify_reader_result = [&] (mutation_reader sst_mr, bool expect_eos) {
            auto close_mr = deferred_close(sst_mr);
            auto sst_mut = read_mutation_from_mutation_reader(sst_mr).get();

            if (expect_eos) {
                BOOST_REQUIRE(sst_mr.is_buffer_empty());
                BOOST_REQUIRE(sst_mr.is_end_of_stream());
                BOOST_REQUIRE(!sst_mut);
            } else {
                BOOST_REQUIRE(sst_mut);
            }
        };

        {
            static std::predicate<const sstable&> auto excluding_pred = [] (const sstable&) {
                return false;
            };

            testlog.info("excluding_pred: point query");
            verify_reader_result(make_point_query_reader(excluding_pred), true);
            testlog.info("excluding_pred: range query");
            verify_reader_result(make_full_scan_reader(excluding_pred), true);
        }

        {
            static std::predicate<const sstable&> auto inclusive_pred = [] (const sstable&) {
                return true;
            };

            testlog.info("inclusive_pred: point query");
            verify_reader_result(make_point_query_reader(inclusive_pred), false);
            testlog.info("inclusive_pred: range query");
            verify_reader_result(make_full_scan_reader(inclusive_pred), false);
        }
    });
}

