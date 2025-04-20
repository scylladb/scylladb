/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <algorithm>

#include <boost/test/unit_test.hpp>
#include <seastar/net/inet_address.hh>
#include "sstables/generation_type.hh"
#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/closeable.hh>

#include "test/boost/sstable_test.hh"
#include "sstables/key.hh"
#include <seastar/core/do_with.hh>
#include <seastar/core/thread.hh>
#include "sstables/sstables.hh"
#include "replica/database.hh"
#include "timestamp.hh"
#include "schema/schema_builder.hh"
#include "partition_slice_builder.hh"
#include "readers/combined.hh"
#include "replica/memtable-sstable.hh"
#include "test/lib/index_reader_assertions.hh"
#include "test/lib/mutation_reader_assertions.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/make_random_string.hh"
#include "test/lib/data_model.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/log.hh"

#include "readers/from_fragments_v2.hh"

using namespace sstables;
using namespace std::chrono_literals;

SEASTAR_TEST_CASE(nonexistent_key) {
  return test_env::do_with_async([] (test_env& env) {
      auto sstp = env.reusable_sst(uncompressed_schema(), uncompressed_dir()).get();
      auto pr = dht::partition_range::make_singular(make_dkey(uncompressed_schema(), "invalid_key"));
      auto s = uncompressed_schema();
      auto rd = sstp->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
      auto close_rd = deferred_close(rd);
      auto mutation = rd().get();
      BOOST_REQUIRE(!mutation);
  });
}

void test_no_clustered(sstables::test_env& env, bytes&& key, std::unordered_map<bytes, data_value> &&map) {
    auto sstp = env.reusable_sst(uncompressed_schema(), uncompressed_dir()).get();
    auto pr = dht::partition_range::make_singular(make_dkey(uncompressed_schema(), std::move(key)));
    auto s = uncompressed_schema();
    auto rd = sstp->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
    auto close_rd = deferred_close(rd);
    auto mutation = read_mutation_from_mutation_reader(rd).get();
    BOOST_REQUIRE(mutation);
    auto& mp = mutation->partition();
    for (auto&& e : mp.range(*s, interval<clustering_key_prefix>())) {
        BOOST_REQUIRE(to_bytes(e.key()) == to_bytes(""));
        BOOST_REQUIRE(e.row().cells().size() == map.size());

        auto &row = e.row().cells();
        for (auto&& c: map) {
            match_live_cell(row, *s, c.first, c.second);
        }
    }
}

SEASTAR_TEST_CASE(uncompressed_1) {
  return test_env::do_with_async([] (test_env& env) {
    test_no_clustered(env, "vinna", {{ "col1", to_sstring("daughter") }, { "col2", 3 }});
  });
}

SEASTAR_TEST_CASE(uncompressed_2) {
  return test_env::do_with_async([] (test_env& env) {
    test_no_clustered(env, "gustaf", {{ "col1", to_sstring("son") }, { "col2", 0 }});
  });
}

SEASTAR_TEST_CASE(uncompressed_3) {
  return test_env::do_with_async([] (test_env& env) {
    test_no_clustered(env, "isak", {{ "col1", to_sstring("son") }, { "col2", 1 }});
  });
}

SEASTAR_TEST_CASE(uncompressed_4) {
  return test_env::do_with_async([] (test_env& env) {
    test_no_clustered(env, "finna", {{ "col1", to_sstring("daughter") }, { "col2", 2 }});
  });
}

/*
 *
 * insert into todata.complex_schema (key, clust1, clust2, reg_set, reg, static_obj) values ('key1', 'cl1.1', 'cl2.1', { '1', '2' }, 'v1', 'static_value');
 * insert into todata.complex_schema (key, clust1, clust2, reg_list, reg, static_obj) values ('key1', 'cl1.2', 'cl2.2', [ '2', '1'], 'v2','static_value');
 * insert into todata.complex_schema (key, clust1, clust2, reg_map, reg, static_obj) values ('key2', 'kcl1.1', 'kcl2.1', { '3': '1', '4' : '2' }, 'v3', 'static_value');
 * insert into todata.complex_schema (key, clust1, clust2, reg_fset, reg, static_obj) values ('key2', 'kcl1.2', 'kcl2.2', { '3', '1', '4' , '2' }, 'v4', 'static_value');
 * insert into todata.complex_schema (key, static_collection) values ('key2', { '1', '2', '3' , '4' });
 * (flush)
 *
 * delete reg from todata.complex_schema where key = 'key2' and clust1 = 'kcl1.2' and clust2 = 'kcl2.2';
 * insert into todata.complex_schema (key, clust1, clust2, reg, static_obj) values ('key3', 'tcl1.1', 'tcl2.1', 'v5', 'static_value_3') using ttl 86400;
 * delete from todata.complex_schema where key = 'key1' and clust1='cl1.1';
 * delete static_obj from todata.complex_schema where key = 'key2';
 * delete reg_list[0] from todata.complex_schema where key = 'key1' and clust1='cl1.2' and clust2='cl2.2';
 * delete reg_fset from todata.complex_schema where key = 'key2' and clust1='kcl1.2' and clust2='kcl2.2';
 * delete reg_map['3'] from todata.complex_schema where key = 'key2' and clust1='kcl1.1' and clust2='kcl2.1';
 * delete static_collection['1'] from todata.complex_schema where key = 'key2';
 * (flush)
 *
 * insert into todata.complex_schema (key, static_obj) values('key2', 'final_static');
 * update todata.complex_schema set reg_map = reg_map + { '6': '1' } where key = 'key2' and clust1='kcl1.1' and clust2='kcl2.1';
 * update todata.complex_schema set reg_list = reg_list + [ '6' ] where key = 'key1' and clust1='cl1.2' and clust2='cl2.2';
 * update todata.complex_schema set reg_set = reg_set + { '6' } where key = 'key1' and clust1='cl1.2' and clust2='cl2.2';
 * (flush)
 */

// FIXME: we are lacking a full deletion test
static mutation generate_clustered(sstables::test_env& env, bytes&& key, generation_type gen) {
    auto sstp = env.reusable_sst(complex_schema(), "test/resource/sstables/complex", gen).get();
    auto pr = dht::partition_range::make_singular(make_dkey(complex_schema(), std::move(key)));
    auto s = complex_schema();
    auto rd = sstp->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
    auto close_rd = deferred_close(rd);
    auto mutation = read_mutation_from_mutation_reader(rd).get();
    BOOST_REQUIRE(mutation);
    return std::move(*mutation);
}

inline auto clustered_row(mutation& mutation, const schema& s, std::vector<bytes>&& v) {
    auto exploded = exploded_clustering_prefix(std::move(v));
    auto clustering_pair = clustering_key::from_clustering_prefix(s, exploded);
    return deletable_row(s, mutation.partition().clustered_row(s, clustering_pair));
}

SEASTAR_TEST_CASE(complex_sst1_k1) {
    return test_env::do_with_async([] (test_env& env) {
        auto mutation = generate_clustered(env, "key1", generation_type{1});
        auto s = complex_schema();

        auto& sr = mutation.partition().static_row().get();
        match_live_cell(sr, *s, "static_obj", data_value(to_bytes("static_value")));

        auto row1 = clustered_row(mutation, *s, {"cl1.1", "cl2.1"});
        match_live_cell(row1.cells(), *s, "reg", data_value(to_bytes("v1")));
        match_absent(row1.cells(), *s, "reg_list");
        match_absent(row1.cells(), *s, "reg_map");
        match_absent(row1.cells(), *s, "reg_fset");
        auto reg_set = match_collection(row1.cells(), *s, "reg_set", tombstone(deletion_time{1431451390, 1431451390209521l}));
        match_collection_element<status::live>(reg_set.cells[0], to_bytes("1"), bytes_opt{});
        match_collection_element<status::live>(reg_set.cells[1], to_bytes("2"), bytes_opt{});

        auto row2 = clustered_row(mutation, *s, {"cl1.2", "cl2.2"});
        match_live_cell(row2.cells(), *s, "reg", data_value(to_bytes("v2")));
        match_absent(row2.cells(), *s, "reg_set");
        match_absent(row2.cells(), *s, "reg_map");
        match_absent(row2.cells(), *s, "reg_fset");
        auto reg_list = match_collection(row2.cells(), *s, "reg_list", tombstone(deletion_time{1431451390, 1431451390213471l}));
        match_collection_element<status::live>(reg_list.cells[0], bytes_opt{}, to_bytes("2"));
        match_collection_element<status::live>(reg_list.cells[1], bytes_opt{}, to_bytes("1"));
    });
}

SEASTAR_TEST_CASE(complex_sst1_k2) {
    return test_env::do_with_async([] (test_env& env) {
        auto mutation = generate_clustered(env, "key2", generation_type{1});
        auto s = complex_schema();

        auto& sr = mutation.partition().static_row().get();
        match_live_cell(sr, *s, "static_obj", data_value(to_bytes("static_value")));
        auto static_set = match_collection(sr, *s, "static_collection", tombstone(deletion_time{1431451390, 1431451390225257l}));
        match_collection_element<status::live>(static_set.cells[0], to_bytes("1"), bytes_opt{});
        match_collection_element<status::live>(static_set.cells[1], to_bytes("2"), bytes_opt{});
        match_collection_element<status::live>(static_set.cells[2], to_bytes("3"), bytes_opt{});
        match_collection_element<status::live>(static_set.cells[3], to_bytes("4"), bytes_opt{});

        auto row1 = clustered_row(mutation, *s, {"kcl1.1", "kcl2.1"});
        match_live_cell(row1.cells(), *s, "reg", data_value(to_bytes("v3")));
        match_absent(row1.cells(), *s, "reg_list");
        match_absent(row1.cells(), *s, "reg_set");
        match_absent(row1.cells(), *s, "reg_fset");
        auto reg_map = match_collection(row1.cells(), *s, "reg_map", tombstone(deletion_time{1431451390, 1431451390217436l}));
        match_collection_element<status::live>(reg_map.cells[0], to_bytes("3"), to_bytes("1"));
        match_collection_element<status::live>(reg_map.cells[1], to_bytes("4"), to_bytes("2"));

        auto row2 = clustered_row(mutation, *s, {"kcl1.2", "kcl2.2"});
        match_live_cell(row2.cells(), *s, "reg", data_value(to_bytes("v4")));
        match_absent(row2.cells(), *s, "reg_set");
        match_absent(row2.cells(), *s, "reg_map");
        match_absent(row2.cells(), *s, "reg_list");
    });
}

SEASTAR_TEST_CASE(complex_sst2_k1) {
    return test_env::do_with_async([] (test_env& env) {
        auto mutation = generate_clustered(env, "key1", generation_type{2});
        auto s = complex_schema();

        auto exploded = exploded_clustering_prefix({"cl1.1", "cl2.1"});
        auto clustering = clustering_key::from_clustering_prefix(*s, exploded);

        auto t1 = mutation.partition().range_tombstone_for_row(*s, clustering);
        BOOST_REQUIRE(t1.timestamp == 1431451394600754l);
        BOOST_REQUIRE(t1.deletion_time == gc_clock::time_point(gc_clock::duration(1431451394)));

        auto row = clustered_row(mutation, *s, {"cl1.2", "cl2.2"});
        auto reg_list = match_collection(row.cells(), *s, "reg_list", tombstone(deletion_time{0, api::missing_timestamp}));
        match_collection_element<status::dead>(reg_list.cells[0], bytes_opt{}, bytes_opt{});
    });
}

SEASTAR_TEST_CASE(complex_sst2_k2) {
    return test_env::do_with_async([] (test_env& env) {
        auto mutation = generate_clustered(env, "key2", generation_type{2});
        auto s = complex_schema();

        auto& sr = mutation.partition().static_row().get();
        match_dead_cell(sr, *s, "static_obj");
        auto static_set = match_collection(sr, *s, "static_collection", tombstone(deletion_time{0, api::missing_timestamp}));
        match_collection_element<status::dead>(static_set.cells[0], to_bytes("1"), bytes_opt{});

        auto row1 = clustered_row(mutation, *s, {"kcl1.1", "kcl2.1"});
        // map dead
        match_absent(row1.cells(), *s, "reg_list");
        match_absent(row1.cells(), *s, "reg_set");
        match_absent(row1.cells(), *s, "reg_fset");
        match_absent(row1.cells(), *s, "reg");
        match_collection(row1.cells(), *s, "reg_map", tombstone(deletion_time{0, api::missing_timestamp}));

        auto row2 = clustered_row(mutation, *s, {"kcl1.2", "kcl2.2"});
        match_dead_cell(row2.cells(), *s, "reg");
        match_absent(row2.cells(), *s, "reg_map");
        match_absent(row2.cells(), *s, "reg_list");
        match_absent(row2.cells(), *s, "reg_set");
        match_dead_cell(row2.cells(), *s, "reg_fset");
        match_dead_cell(row2.cells(), *s, "reg");
    });
}

SEASTAR_TEST_CASE(complex_sst2_k3) {
    return test_env::do_with_async([] (test_env& env) {
        auto mutation = generate_clustered(env, "key3", generation_type{2});
        auto s = complex_schema();

        auto& sr = mutation.partition().static_row().get();
        match_expiring_cell(sr, *s, "static_obj", data_value(to_bytes("static_value_3")), 1431451394597062l, 1431537794);

        auto row1 = clustered_row(mutation, *s, {"tcl1.1", "tcl2.1"});
        BOOST_REQUIRE(row1.created_at() == 1431451394597062l);
        match_expiring_cell(row1.cells(), *s, "reg", data_value(to_bytes("v5")), 1431451394597062l, 1431537794);
        match_absent(row1.cells(), *s, "reg_list");
        match_absent(row1.cells(), *s, "reg_set");
        match_absent(row1.cells(), *s, "reg_map");
        match_absent(row1.cells(), *s, "reg_fset");
    });
}

SEASTAR_TEST_CASE(complex_sst3_k1) {
    return test_env::do_with_async([] (test_env& env) {
        auto mutation = generate_clustered(env, "key1", generation_type{3});
        auto s = complex_schema();

        auto row = clustered_row(mutation, *s, {"cl1.2", "cl2.2"});

        auto reg_set = match_collection(row.cells(), *s, "reg_set", tombstone(deletion_time{0, api::missing_timestamp}));
        match_collection_element<status::live>(reg_set.cells[0], to_bytes("6"), bytes_opt{});

        auto reg_list = match_collection(row.cells(), *s, "reg_list", tombstone(deletion_time{0, api::missing_timestamp}));
        match_collection_element<status::live>(reg_list.cells[0], bytes_opt{}, to_bytes("6"));

        match_absent(row.cells(), *s, "static_obj");
        match_absent(row.cells(), *s, "reg_map");
        match_absent(row.cells(), *s, "reg");
        match_absent(row.cells(), *s, "reg_fset");
    });
}

SEASTAR_TEST_CASE(complex_sst3_k2) {
    return test_env::do_with_async([] (test_env& env) {
        auto mutation = generate_clustered(env, "key2", generation_type{3});
        auto s = complex_schema();

        auto& sr = mutation.partition().static_row().get();
        match_live_cell(sr, *s, "static_obj", data_value(to_bytes("final_static")));

        auto row = clustered_row(mutation, *s, {"kcl1.1", "kcl2.1"});
        auto reg_map = match_collection(row.cells(), *s, "reg_map", tombstone(deletion_time{0, api::missing_timestamp}));
        match_collection_element<status::live>(reg_map.cells[0], to_bytes("6"), to_bytes("1"));
        match_absent(row.cells(), *s, "reg_list");
        match_absent(row.cells(), *s, "reg_set");
        match_absent(row.cells(), *s, "reg");
        match_absent(row.cells(), *s, "reg_fset");
    });
}

void test_range_reads(sstables::test_env& env, const dht::token& min, const dht::token& max, std::vector<bytes>& expected) {
    auto sstp = env.reusable_sst(uncompressed_schema(), uncompressed_dir()).get();
    auto s = uncompressed_schema();
    size_t count = 0;
    auto expected_size = expected.size();
    auto pr = dht::partition_range::make(dht::ring_position::starting_at(min), dht::ring_position::ending_at(max));
    auto mutations = sstp->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
    auto close_m = deferred_close(mutations);
    while (true) {
        // Note: The data in the following lambda, including
        // "mutations", continues to live until after the last
        // iteration's future completes, so its lifetime is safe.
        mutation_fragment_v2_opt mfopt = mutations().get();
        if (!mfopt) {
            break;
        }
        BOOST_REQUIRE(mfopt->is_partition_start());
        BOOST_REQUIRE(count < expected_size);
        BOOST_REQUIRE(std::vector<bytes>({expected.back()}) == mfopt->as_partition_start().key().key().explode());
        expected.pop_back();
        count++;
        mutations.next_partition().get();
    }
    BOOST_REQUIRE(count == expected_size);
}

SEASTAR_TEST_CASE(read_range) {
    return test_env::do_with_async([] (test_env& env) {
        std::vector<bytes> expected = { to_bytes("finna"), to_bytes("isak"), to_bytes("gustaf"), to_bytes("vinna") };
        test_range_reads(env, dht::minimum_token(), dht::maximum_token(), expected);
    });
}

SEASTAR_TEST_CASE(read_partial_range) {
    return test_env::do_with_async([] (test_env& env) {
        std::vector<bytes> expected = { to_bytes("finna"), to_bytes("isak") };
        test_range_reads(env, uncompressed_schema()->get_partitioner().get_token(key_view(bytes_view(expected.back()))), dht::maximum_token(), expected);
    });
}

SEASTAR_TEST_CASE(read_partial_range_2) {
    return test_env::do_with_async([] (test_env& env) {
        std::vector<bytes> expected = { to_bytes("gustaf"), to_bytes("vinna") };
        test_range_reads(env, dht::minimum_token(), uncompressed_schema()->get_partitioner().get_token(key_view(bytes_view(expected.front()))), expected);
    });
}

static
mutation_source make_sstable_mutation_source(sstables::test_env& env, schema_ptr s, std::vector<mutation> mutations,
        sstables::sstable::version_types version, gc_clock::time_point query_time = gc_clock::now()) {
    return make_sstable_easy(env, make_memtable(s, mutations), env.manager().configure_writer(), version, mutations.size(), query_time)->as_mutation_source();
}

SEASTAR_TEST_CASE(test_sstable_can_write_and_read_range_tombstone) {
    return test_env::do_with_async([] (test_env& env) {
        auto s = schema_builder("ks", "cf")
                    .with_column("p1", utf8_type, column_kind::partition_key)
                    .with_column("c1", int32_type, column_kind::clustering_key)
                    .with_column("r1", int32_type)
                    .build();
        auto sst_gen = env.make_sst_factory(s);

        auto key = tests::generate_partition_key(s);
        auto c_key_start = clustering_key::from_exploded(*s, {int32_type->decompose(1)});
        auto c_key_end = clustering_key::from_exploded(*s, {int32_type->decompose(2)});

        mutation m(s, key);
        auto ttl = gc_clock::now() + std::chrono::seconds(1);
        m.partition().apply_delete(*s, range_tombstone(c_key_start, bound_kind::excl_start, c_key_end, bound_kind::excl_end, tombstone(9, ttl)));

        auto mt = make_memtable(s, {std::move(m)});

        verify_mutation(env, env.make_sstable(s), mt, query::full_partition_range, [&] (mutation_opt& mut) {
            BOOST_REQUIRE(bool(mut));
            auto rts = mut->partition().row_tombstones();
            BOOST_REQUIRE(rts.size() == 1);
            auto it = rts.begin();
            BOOST_REQUIRE(it->tombstone().equal(*s, range_tombstone(
                          c_key_start,
                          bound_kind::excl_start,
                          c_key_end,
                          bound_kind::excl_end,
                          tombstone(9, ttl))));
            return stop_iteration::yes;
        }).get();
    });
}

SEASTAR_TEST_CASE(compact_storage_sparse_read) {
    return test_env::do_with_async([] (test_env& env) {
        auto sstp = env.reusable_sst(compact_sparse_schema(), "test/resource/sstables/compact_sparse").get();
        auto pr = dht::partition_range::make_singular(make_dkey(compact_sparse_schema(), "first_row"));
        auto s = compact_sparse_schema();
        auto rd = sstp->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
        auto close_rd = deferred_close(rd);
        auto mutation = read_mutation_from_mutation_reader(rd).get();
        BOOST_REQUIRE(mutation);
        auto& mp = mutation->partition();
        auto& row = mp.clustered_row(*s, clustering_key::make_empty());
        match_live_cell(row.cells(), *s, "cl1", data_value(to_bytes("cl1")));
        match_live_cell(row.cells(), *s, "cl2", data_value(to_bytes("cl2")));
    });
}

SEASTAR_TEST_CASE(compact_storage_simple_dense_read) {
    return test_env::do_with_async([] (test_env& env) {
        auto sstp = env.reusable_sst(compact_simple_dense_schema(), "test/resource/sstables/compact_simple_dense").get();
        auto pr = dht::partition_range::make_singular(make_dkey(compact_simple_dense_schema(), "first_row"));
        auto s = compact_simple_dense_schema();
        auto rd = sstp->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
        auto close_rd = deferred_close(rd);
        auto mutation = read_mutation_from_mutation_reader(rd).get();
        auto& mp = mutation->partition();

        auto exploded = exploded_clustering_prefix({"cl1"});
        auto clustering = clustering_key::from_clustering_prefix(*s, exploded);

        auto& row = mp.clustered_row(*s, clustering);
        match_live_cell(row.cells(), *s, "cl2", data_value(to_bytes("cl2")));
    });
}

SEASTAR_TEST_CASE(compact_storage_dense_read) {
    return test_env::do_with_async([] (test_env& env) {
        auto sstp = env.reusable_sst(compact_dense_schema(), "test/resource/sstables/compact_dense").get();
        auto pr = dht::partition_range::make_singular(make_dkey(compact_dense_schema(), "first_row"));
        auto s = compact_dense_schema();
        auto rd = sstp->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
        auto close_rd = deferred_close(rd);
        auto mutation = read_mutation_from_mutation_reader(rd).get();
        auto& mp = mutation->partition();

        auto exploded = exploded_clustering_prefix({"cl1", "cl2"});
        auto clustering = clustering_key::from_clustering_prefix(*s, exploded);

        auto& row = mp.clustered_row(*s, clustering);
        match_live_cell(row.cells(), *s, "cl3", data_value(to_bytes("cl3")));
    });
}

// We recently had an issue, documented at #188, where range-reading from an
// sstable would break if collections were used.
//
// Make sure we don't regress on that.
SEASTAR_TEST_CASE(broken_ranges_collection) {
    return test_env::do_with_async([] (test_env& env) {
        auto sstp = env.reusable_sst(peers_schema(), "test/resource/sstables/broken_ranges", generation_type{2}).get();
        auto s = peers_schema();
        auto reader = sstp->as_mutation_source().make_reader_v2(s, env.make_reader_permit(), query::full_partition_range);
        auto close_r = deferred_close(reader);
        while (true) {
            mutation_opt mut = read_mutation_from_mutation_reader(reader).get();
            auto key_equal = [s, &mut] (sstring ip) {
                return mut->key().equal(*s, partition_key::from_deeply_exploded(*s, { net::inet_address(ip) }));
            };

            if (!mut) {
                break;
            } else if (key_equal("127.0.0.1")) {
                auto& row = mut->partition().clustered_row(*s, clustering_key::make_empty());
                match_absent(row.cells(), *s, "tokens");
            } else if (key_equal("127.0.0.3")) {
                auto& row = mut->partition().clustered_row(*s, clustering_key::make_empty());
                auto tokens = match_collection(row.cells(), *s, "tokens", tombstone(deletion_time{0x55E5F2D5, 0x051EB3FC99715Dl }));
                match_collection_element<status::live>(tokens.cells[0], to_bytes("-8180144272884242102"), bytes_opt{});
            } else {
                BOOST_REQUIRE(key_equal("127.0.0.2"));
                auto t = mut->partition().partition_tombstone();
                BOOST_REQUIRE(t.timestamp == 0x051EB3FB016850l);
            }
        }
    });
}

static schema_ptr tombstone_overlap_schema() {
    static thread_local auto s = [] {
        schema_builder builder("try1", "tab", generate_legacy_id("try1", "tab"));
        builder.with_column("pk", utf8_type, column_kind::partition_key);
        builder.with_column("ck1", utf8_type, column_kind::clustering_key);
        builder.with_column("ck2", utf8_type, column_kind::clustering_key);
        builder.with_column("data", utf8_type);
        return builder.build(schema_builder::compact_storage::no);
    }();
    return s;
}


static future<sstable_ptr> ka_sst(sstables::test_env& env, schema_ptr schema, sstring dir, sstables::generation_type::int_t generation) {
    return env.reusable_sst(std::move(schema), std::move(dir), sstables::generation_from_value(generation), sstables::sstable::version_types::ka, big, sstable_open_config{});
}

//  Considering the schema above, the sstable looks like:
//     {"key": "pk",
//      "cells": [["aaa:_","aaa:bbb:_",1459334681228103,"t",1459334681],
//                ["aaa:bbb:_","aaa:bbb:!",1459334681244989,"t",1459334681],
//                ["aaa:bbb:!","aaa:!",1459334681228103,"t",1459334681]]}
//               ]
SEASTAR_TEST_CASE(tombstone_in_tombstone) {
    return test_env::do_with_async([] (test_env& env) {
        auto sstp = ka_sst(env, tombstone_overlap_schema(), "test/resource/sstables/tombstone_overlap", 1).get();
        auto s = tombstone_overlap_schema();
        auto reader = sstp->make_reader(s, env.make_reader_permit(), query::full_partition_range, s->full_slice());
        auto close_r = deferred_close(reader);
        while (true) {
            mutation_opt mut = read_mutation_from_mutation_reader(reader).get();
            if (!mut) {
                break;
            }
            auto make_pkey = [s] (sstring b) {
                return partition_key::from_deeply_exploded(*s, { data_value(b) });
            };
            auto make_ckey = [s] (sstring c1, sstring c2 = {}) {
                std::vector<data_value> v;
                v.push_back(data_value(c1));
                if (!c2.empty()) {
                    v.push_back(data_value(c2));
                }
                return clustering_key::from_deeply_exploded(*s, std::move(v));
            };
            BOOST_REQUIRE(mut->key().equal(*s, make_pkey("pk")));
            // Somewhat counterintuitively, scylla represents
            // deleting a small row with all clustering keys set - not
            // as a "row tombstone" but rather as a deleted clustering row.
            auto rts = mut->partition().row_tombstones();
            BOOST_REQUIRE(rts.size() == 2);
            auto it = rts.begin();
            BOOST_REQUIRE(it->tombstone().equal(*s, range_tombstone(
                            make_ckey("aaa"),
                            bound_kind::incl_start,
                            make_ckey("aaa", "bbb"),
                            bound_kind::excl_end,
                            tombstone(1459334681228103LL, it->tombstone().tomb.deletion_time))));
            ++it;
            BOOST_REQUIRE(it->tombstone().equal(*s, range_tombstone(
                            make_ckey("aaa", "bbb"),
                            bound_kind::excl_start,
                            make_ckey("aaa"),
                            bound_kind::incl_end,
                            tombstone(1459334681228103LL, it->tombstone().tomb.deletion_time))));
            auto rows = mut->partition().clustered_rows();
            BOOST_REQUIRE(rows.calculate_size() == 1);
            for (auto& e : rows) {
                BOOST_REQUIRE(e.key().equal(*s, make_ckey("aaa", "bbb")));
                BOOST_REQUIRE(e.row().deleted_at().tomb().timestamp == 1459334681244989LL);
            }
        }
    });
}

//  Same schema as above, the sstable looks like:
//    {"key": "pk",
//     "cells": [["aaa:_","aaa:bbb:_",1459334681228103,"t",1459334681],
//               ["aaa:bbb:_","aaa:ccc:!",1459334681228103,"t",1459334681],
//               ["aaa:ccc:!","aaa:ddd:!",1459334681228103,"t",1459334681],
//               ["aaa:ddd:!","aaa:!",1459334681228103,"t",1459334681]]}
//
// We're not sure how this sort of sstable can be generated with Cassandra 2's
// CQL, but we saw a similar thing is a real use case.
SEASTAR_TEST_CASE(range_tombstone_reading) {
    return test_env::do_with_async([] (test_env& env) {
        auto sstp = ka_sst(env, tombstone_overlap_schema(), "test/resource/sstables/tombstone_overlap", 4).get();
        auto s = tombstone_overlap_schema();
        auto reader = sstp->make_reader(s, env.make_reader_permit(), query::full_partition_range, s->full_slice());
        auto close_r = deferred_close(reader);
        while (true) {
            mutation_opt mut = read_mutation_from_mutation_reader(reader).get();
            if (!mut) {
                break;
            }
            auto make_pkey = [s] (sstring b) {
                return partition_key::from_deeply_exploded(*s, { data_value(b) });
            };
            auto make_ckey = [s] (sstring c1, sstring c2 = {}) {
                std::vector<data_value> v;
                v.push_back(data_value(c1));
                if (!c2.empty()) {
                    v.push_back(data_value(c2));
                }
                return clustering_key::from_deeply_exploded(*s, std::move(v));
            };
            BOOST_REQUIRE(mut->key().equal(*s, make_pkey("pk")));
            auto rts = mut->partition().row_tombstones();
            BOOST_REQUIRE(rts.size() == 1);
            auto it = rts.begin();
            BOOST_REQUIRE(it->tombstone().equal(*s, range_tombstone(
                            make_ckey("aaa"),
                            bound_kind::incl_start,
                            make_ckey("aaa"),
                            bound_kind::incl_end,
                            tombstone(1459334681228103LL, it->tombstone().tomb.deletion_time))));
            auto rows = mut->partition().clustered_rows();
            BOOST_REQUIRE(rows.calculate_size() == 0);
        }
    });
}

// In this test case we have *three* levels of of tombstones:
//     create COLUMNFAMILY tab2 (pk text, ck1 text, ck2 text, ck3 text, data text, primary key(pk, ck1, ck2, ck3));
//     delete from tab2 where pk = 'pk' and ck1 = 'aaa';
//     delete from tab2 where pk = 'pk' and ck1 = 'aaa' and ck2 = 'bbb';
//     delete from tab2 where pk = 'pk' and ck1 = 'aaa' and ck2 = 'bbb' and ck3 = 'ccc';
// And then, to have more fun, I edited the resulting sstable manually (using
// Cassandra's json2sstable and sstable2json tools) to further split the
// resulting tombstones into even more tombstones:
//  {"key": "pk",
//   "cells":
//       [["aaa:_","aaa:bba:_",1459438519943668,"t",1459438519],
//        ["aaa:bba:_","aaa:bbb:_",1459438519943668,"t",1459438519],
//        ["aaa:bbb:_","aaa:bbb:ccb:_",1459438519950348,"t",1459438519],
//        ["aaa:bbb:ccb:_","aaa:bbb:ccc:_",1459438519950348,"t",1459438519],
//        ["aaa:bbb:ccc:_","aaa:bbb:ccc:!",1459438519958850,"t",1459438519],
//        ["aaa:bbb:ccc:!","aaa:bbb:ddd:!",1459438519950348,"t",1459438519],
//        ["aaa:bbb:ddd:!","aaa:bbb:!",1459438519950348,"t",1459438519],
//        ["aaa:bbb:!","aaa:!",1459438519943668,"t",1459438519]]}
static schema_ptr tombstone_overlap_schema2() {
    static thread_local auto s = [] {
        schema_builder builder("try1", "tab2", generate_legacy_id("try1", "tab2"));
        builder.with_column("pk", utf8_type, column_kind::partition_key);
        builder.with_column("ck1", utf8_type, column_kind::clustering_key);
        builder.with_column("ck2", utf8_type, column_kind::clustering_key);
        builder.with_column("ck3", utf8_type, column_kind::clustering_key);
        builder.with_column("data", utf8_type);
        return builder.build(schema_builder::compact_storage::no);
    }();
    return s;
}
SEASTAR_TEST_CASE(tombstone_in_tombstone2) {
    return test_env::do_with_async([] (test_env& env) {
        auto sstp = ka_sst(env, tombstone_overlap_schema2(), "test/resource/sstables/tombstone_overlap", 3).get();
        auto s = tombstone_overlap_schema2();
        auto reader = sstp->make_reader(s, env.make_reader_permit(), query::full_partition_range, s->full_slice());
        auto close_r = deferred_close(reader);
        while (true) {
            mutation_opt mut = read_mutation_from_mutation_reader(reader).get();
            if (!mut) {
                break;
            }
            auto make_pkey = [s] (sstring b) {
                return partition_key::from_deeply_exploded(*s, { data_value(b) });
            };
            auto make_ckey = [s] (sstring c1, sstring c2 = {}, sstring c3 = {}) {
                std::vector<data_value> v;
                v.push_back(data_value(c1));
                if (!c2.empty()) {
                    v.push_back(data_value(c2));
                }
                if (!c3.empty()) {
                    v.push_back(data_value(c3));
                }
                return clustering_key::from_deeply_exploded(*s, std::move(v));
            };
            BOOST_REQUIRE(mut->key().equal(*s, make_pkey("pk")));
            auto rows = mut->partition().clustered_rows();
            auto rts = mut->partition().row_tombstones();

            auto it = rts.begin();
            BOOST_REQUIRE(it->start_bound().equal(*s, bound_view(make_ckey("aaa"), bound_kind::incl_start)));
            BOOST_REQUIRE(it->end_bound().equal(*s, bound_view(make_ckey("aaa", "bbb"), bound_kind::excl_end)));
            BOOST_REQUIRE(it->tombstone().tomb.timestamp == 1459438519943668L);
            ++it;
            BOOST_REQUIRE(it->start_bound().equal(*s, bound_view(make_ckey("aaa", "bbb"), bound_kind::incl_start)));
            BOOST_REQUIRE(it->end_bound().equal(*s, bound_view(make_ckey("aaa", "bbb", "ccc"), bound_kind::excl_end)));
            BOOST_REQUIRE(it->tombstone().tomb.timestamp == 1459438519950348L);
            ++it;
            BOOST_REQUIRE(it->start_bound().equal(*s, bound_view(make_ckey("aaa", "bbb", "ccc"), bound_kind::excl_start)));
            BOOST_REQUIRE(it->end_bound().equal(*s, bound_view(make_ckey("aaa", "bbb"), bound_kind::incl_end)));
            BOOST_REQUIRE(it->tombstone().tomb.timestamp == 1459438519950348L);
            ++it;
            BOOST_REQUIRE(it->start_bound().equal(*s, bound_view(make_ckey("aaa", "bbb"), bound_kind::excl_start)));
            BOOST_REQUIRE(it->end_bound().equal(*s, bound_view(make_ckey("aaa"), bound_kind::incl_end)));
            BOOST_REQUIRE(it->tombstone().tomb.timestamp == 1459438519943668L);
            ++it;
            BOOST_REQUIRE(it == rts.end());

            BOOST_REQUIRE(rows.calculate_size() == 1);
            for (auto& e : rows) {
                BOOST_REQUIRE(e.key().equal(*s, make_ckey("aaa", "bbb", "ccc")));
                BOOST_REQUIRE(e.row().deleted_at().tomb().timestamp == 1459438519958850LL);
            }
        }
    });
}

// Reproducer for #4783
static schema_ptr buffer_overflow_schema() {
    static thread_local auto s = [] {
        schema_builder builder("test_ks", "test_tab", generate_legacy_id("test_ks", "test_tab"));
        builder.with_column("pk", int32_type, column_kind::partition_key);
        builder.with_column("ck1", int32_type, column_kind::clustering_key);
        builder.with_column("ck2", int32_type, column_kind::clustering_key);
        builder.with_column("data", utf8_type);
        return builder.build(schema_builder::compact_storage::no);
    }();
    return s;
}
SEASTAR_TEST_CASE(buffer_overflow) {
  return test_env::do_with_async([] (test_env& env) {
    auto s = buffer_overflow_schema();
    auto sstp = ka_sst(env, s, "test/resource/sstables/buffer_overflow", 5).get();
    auto r = sstp->make_reader(s, env.make_reader_permit(), query::full_partition_range, s->full_slice());
    auto pk1 = partition_key::from_exploded(*s, { int32_type->decompose(4) });
    auto dk1 = dht::decorate_key(*s, pk1);
    auto pk2 = partition_key::from_exploded(*s, { int32_type->decompose(3) });
    auto dk2 = dht::decorate_key(*s, pk2);
    auto ck1 = clustering_key::from_exploded(*s, {int32_type->decompose(2), int32_type->decompose(2)});
    auto ck2 = clustering_key::from_exploded(*s, {int32_type->decompose(1), int32_type->decompose(2)});
    auto tk1 = clustering_key::from_exploded(*s, {int32_type->decompose(2)});
    auto tk2 = clustering_key::from_exploded(*s, {int32_type->decompose(1)});
    range_tombstone_change rt1_before(position_in_partition_view(tk1, bound_weight::before_all_prefixed),
                                      tombstone{1564482368866800, gc_clock::from_time_t(1564482368)});
    range_tombstone_change rt1_after(position_in_partition_view(tk1, bound_weight::after_all_prefixed), tombstone());
    range_tombstone_change rt2_before(position_in_partition_view(tk2, bound_weight::before_all_prefixed),
                                      tombstone{1564482374781792, gc_clock::from_time_t(1564482374)});
    range_tombstone_change rt2_after(position_in_partition_view(tk2, bound_weight::after_all_prefixed), tombstone());
    r.set_max_buffer_size(std::max(
                mutation_fragment_v2(*s, env.make_reader_permit(), partition_start(dk1, rt1_before.tombstone())).memory_usage()
                    + mutation_fragment_v2(*s, env.make_reader_permit(), range_tombstone_change(rt1_before)).memory_usage(),
                mutation_fragment_v2(*s, env.make_reader_permit(), clustering_row(ck1)).memory_usage()));
    flat_reader_assertions_v2 rd(std::move(r));
    rd.produces_partition_start(dk1)
        .produces_range_tombstone_change(rt1_before)
        .produces_row_with_key(ck1)
        .produces_range_tombstone_change(rt1_after)
        .produces_partition_end()
        .produces_partition_start(dk2)
        .produces_range_tombstone_change(rt2_before)
        .produces_row_with_key(ck2)
        .produces_range_tombstone_change(rt2_after)
        .produces_partition_end()
        .produces_end_of_stream();
  });
}

SEASTAR_TEST_CASE(test_non_compound_table_row_is_not_marked_as_static) {
   return test_env::do_with_async([] (test_env& env) {
      for (const auto version : writable_sstable_versions) {
        schema_builder builder("ks", "cf");
        builder.with_column("p", utf8_type, column_kind::partition_key);
        builder.with_column("c", int32_type, column_kind::clustering_key);
        builder.with_column("v", int32_type);
        auto s = builder.build(schema_builder::compact_storage::yes);

        auto k = tests::generate_partition_key(s);
        auto ck = clustering_key::from_exploded(*s, {int32_type->decompose(static_cast<int32_t>(0xffff0000))});

        mutation m(s, k);
        auto cell = atomic_cell::make_live(*int32_type, 1, int32_type->decompose(17), { });
        m.set_clustered_cell(ck, *s->get_column_definition("v"), std::move(cell));

        auto sst = make_sstable_containing(env.make_sstable(s, version), {std::move(m)});
        auto mut = with_closeable(sst->make_reader(s, env.make_reader_permit(), query::full_partition_range, s->full_slice()), [] (auto& mr) {
            return read_mutation_from_mutation_reader(mr);
        }).get();
        BOOST_REQUIRE(bool(mut));
      }
    });
}

SEASTAR_TEST_CASE(test_has_partition_key) {
    return test_env::do_with_async([] (test_env& env) {
        for (const auto version : writable_sstable_versions) {
            schema_builder builder("ks", "cf");
            builder.with_column("p", utf8_type, column_kind::partition_key);
            builder.with_column("c", int32_type, column_kind::clustering_key);
            builder.with_column("v", int32_type);
            auto s = builder.build(schema_builder::compact_storage::yes);

            auto dk = tests::generate_partition_key(s);
            auto ck = clustering_key::from_exploded(*s, {int32_type->decompose(static_cast<int32_t>(0xffff0000))});

            mutation m(s, dk);
            auto cell = atomic_cell::make_live(*int32_type, 1, int32_type->decompose(17), { });
            m.set_clustered_cell(ck, *s->get_column_definition("v"), std::move(cell));

            auto sst = make_sstable_containing(env.make_sstable(s, version), {std::move(m)});
            auto hk = sstables::sstable::make_hashed_key(*s, dk.key());
            auto mr = sst->make_reader(s, env.make_reader_permit(), query::full_partition_range, s->full_slice());
            auto close_mr = deferred_close(mr);
            auto res =  sst->has_partition_key(hk, dk).get();
            BOOST_REQUIRE(bool(res));

            auto dk2 = dht::decorate_key(*s, partition_key::from_nodetool_style_string(s, "xx"));
            auto hk2 = sstables::sstable::make_hashed_key(*s, dk2.key());
            res =  sst->has_partition_key(hk2, dk2).get();
            BOOST_REQUIRE(! bool(res));
        }
    });
}

static std::unique_ptr<index_reader> get_index_reader(shared_sstable sst, reader_permit permit) {
    return std::make_unique<index_reader>(sst, std::move(permit));
}

SEASTAR_TEST_CASE(test_promoted_index_blocks_are_monotonic) {
    return test_env::do_with_async([] (test_env& env) {
        schema_builder builder("ks", "cf");
        builder.with_column("p", utf8_type, column_kind::partition_key);
        builder.with_column("c1", int32_type, column_kind::clustering_key);
        builder.with_column("c2", int32_type, column_kind::clustering_key);
        builder.with_column("v", int32_type);
        auto s = builder.build();

        auto k = tests::generate_partition_key(s);
        auto cell = atomic_cell::make_live(*int32_type, 1, int32_type->decompose(88), { });
        mutation m(s, k);

        auto ck = clustering_key::from_exploded(*s, {int32_type->decompose(1), int32_type->decompose(2)});
        m.set_clustered_cell(ck, *s->get_column_definition("v"), atomic_cell(*int32_type, cell));

        ck = clustering_key::from_exploded(*s, {int32_type->decompose(1), int32_type->decompose(4)});
        m.set_clustered_cell(ck, *s->get_column_definition("v"), atomic_cell(*int32_type, cell));

        ck = clustering_key::from_exploded(*s, {int32_type->decompose(1), int32_type->decompose(6)});
        m.set_clustered_cell(ck, *s->get_column_definition("v"), atomic_cell(*int32_type, cell));

        ck = clustering_key::from_exploded(*s, {int32_type->decompose(3), int32_type->decompose(9)});
        m.set_clustered_cell(ck, *s->get_column_definition("v"), atomic_cell(*int32_type, cell));

        m.partition().apply_row_tombstone(*s, range_tombstone(
                clustering_key_prefix::from_exploded(*s, {int32_type->decompose(1)}),
                bound_kind::excl_start,
                clustering_key_prefix::from_exploded(*s, {int32_type->decompose(2)}),
                bound_kind::incl_end,
                {1, gc_clock::now()}));

        auto mt = make_memtable(s, {std::move(m)});
        sstable_writer_config cfg = env.manager().configure_writer();
        cfg.promoted_index_block_size = 1;
        cfg.promoted_index_auto_scale_threshold = 0; // disable auto-scaling

        auto sst = make_sstable_easy(env, mt, cfg);
        assert_that(get_index_reader(sst, env.make_reader_permit())).has_monotonic_positions(*s);
    });
}

SEASTAR_TEST_CASE(test_promoted_index_blocks_are_monotonic_with_auto_scaling) {
    return test_env::do_with_async([] (test_env& env) {
        schema_builder builder("ks", "cf");
        builder.with_column("p", utf8_type, column_kind::partition_key);
        builder.with_column("c1", int32_type, column_kind::clustering_key);
        builder.with_column("c2", int32_type, column_kind::clustering_key);
        builder.with_column("v", int32_type);
        auto s = builder.build();

        auto k = tests::generate_partition_key(s);
        auto cell = atomic_cell::make_live(*int32_type, 1, int32_type->decompose(88), { });
        mutation m(s, k);

        for (int i = 1; i <= 1024; i++) {
          auto ck = clustering_key::from_exploded(*s, {int32_type->decompose(i), int32_type->decompose(i*2)});
          m.set_clustered_cell(ck, *s->get_column_definition("v"), atomic_cell(*int32_type, cell));
        }

        m.partition().apply_row_tombstone(*s, range_tombstone(
                clustering_key_prefix::from_exploded(*s, {int32_type->decompose(1)}),
                bound_kind::excl_start,
                clustering_key_prefix::from_exploded(*s, {int32_type->decompose(2)}),
                bound_kind::incl_end,
                {1, gc_clock::now()}));

        auto mt = make_memtable(s, {std::move(m)});
        sstable_writer_config cfg = env.manager().configure_writer();
        cfg.promoted_index_block_size = 1;
        cfg.promoted_index_auto_scale_threshold = 100;  // set to a low value to trigger auto-scaling

        auto sst = make_sstable_easy(env, mt, cfg);
        assert_that(get_index_reader(sst, env.make_reader_permit())).has_monotonic_positions(*s);
    });
}

SEASTAR_TEST_CASE(test_promoted_index_blocks_are_monotonic_compound_dense) {
   return test_env::do_with_async([] (test_env& env) {
      for (const auto version : writable_sstable_versions) {
        schema_builder builder("ks", "cf");
        builder.with_column("p", utf8_type, column_kind::partition_key);
        builder.with_column("c1", int32_type, column_kind::clustering_key);
        builder.with_column("c2", int32_type, column_kind::clustering_key);
        builder.with_column("v", int32_type);
        auto s = builder.build(schema_builder::compact_storage::yes);

        auto dk = tests::generate_partition_key(s);
        auto cell = atomic_cell::make_live(*int32_type, 1, int32_type->decompose(88), { });
        mutation m(s, dk);

        auto ck1 = clustering_key::from_exploded(*s, {int32_type->decompose(1), int32_type->decompose(2)});
        m.set_clustered_cell(ck1, *s->get_column_definition("v"), atomic_cell(*int32_type, cell));

        auto ck2 = clustering_key::from_exploded(*s, {int32_type->decompose(1), int32_type->decompose(4)});
        m.set_clustered_cell(ck2, *s->get_column_definition("v"), atomic_cell(*int32_type, cell));

        auto ck3 = clustering_key::from_exploded(*s, {int32_type->decompose(1), int32_type->decompose(6)});
        m.set_clustered_cell(ck3, *s->get_column_definition("v"), atomic_cell(*int32_type, cell));

        auto ck4 = clustering_key::from_exploded(*s, {int32_type->decompose(3), int32_type->decompose(9)});
        m.set_clustered_cell(ck4, *s->get_column_definition("v"), atomic_cell(*int32_type, cell));

        m.partition().apply_row_tombstone(*s, range_tombstone(
                clustering_key_prefix::from_exploded(*s, {int32_type->decompose(1)}),
                bound_kind::incl_start,
                clustering_key_prefix::from_exploded(*s, {int32_type->decompose(2)}),
                bound_kind::incl_end,
                {1, gc_clock::now()}));

        auto mt = make_memtable(s, {m});
        sstable_writer_config cfg = env.manager().configure_writer();
        cfg.promoted_index_block_size = 1;

        auto sst = make_sstable_easy(env, mt, cfg, version);

        {
            assert_that(get_index_reader(sst, env.make_reader_permit())).has_monotonic_positions(*s);
        }

        {
            auto slice = partition_slice_builder(*s).with_range(query::clustering_range::make_starting_with({ck1})).build();
            assert_that(sst->as_mutation_source().make_reader_v2(s, env.make_reader_permit(), dht::partition_range::make_singular(dk), slice))
                    .produces(m, slice.get_all_ranges())
                    .produces_end_of_stream();
        }
      }
    });
}

SEASTAR_TEST_CASE(test_promoted_index_blocks_are_monotonic_non_compound_dense) {
   return test_env::do_with_async([] (test_env& env) {
      for (const auto version : writable_sstable_versions) {
        schema_builder builder("ks", "cf");
        builder.with_column("p", utf8_type, column_kind::partition_key);
        builder.with_column("c1", int32_type, column_kind::clustering_key);
        builder.with_column("v", int32_type);
        auto s = builder.build(schema_builder::compact_storage::yes);

        auto dk = tests::generate_partition_key(s);
        auto cell = atomic_cell::make_live(*int32_type, 1, int32_type->decompose(88), { });
        mutation m(s, dk);

        auto ck1 = clustering_key::from_exploded(*s, {int32_type->decompose(1)});
        m.set_clustered_cell(ck1, *s->get_column_definition("v"), atomic_cell(*int32_type, cell));

        auto ck2 = clustering_key::from_exploded(*s, {int32_type->decompose(2)});
        m.set_clustered_cell(ck2, *s->get_column_definition("v"), atomic_cell(*int32_type, cell));

        auto ck3 = clustering_key::from_exploded(*s, {int32_type->decompose(3)});
        m.set_clustered_cell(ck3, *s->get_column_definition("v"), atomic_cell(*int32_type, cell));

        m.partition().apply_row_tombstone(*s, range_tombstone(
                clustering_key_prefix::from_exploded(*s, {int32_type->decompose(1)}),
                bound_kind::incl_start,
                clustering_key_prefix::from_exploded(*s, {int32_type->decompose(2)}),
                bound_kind::incl_end,
                {1, gc_clock::now()}));

        auto mt = make_memtable(s, {m});
        sstable_writer_config cfg = env.manager().configure_writer();
        cfg.promoted_index_block_size = 1;

        auto sst = make_sstable_easy(env, mt, cfg, version);

        {
            assert_that(get_index_reader(sst, env.make_reader_permit())).has_monotonic_positions(*s);
        }

        {
            auto slice = partition_slice_builder(*s).with_range(query::clustering_range::make_starting_with({ck1})).build();
            assert_that(sst->as_mutation_source().make_reader_v2(s, env.make_reader_permit(), dht::partition_range::make_singular(dk), slice))
                    .produces(m)
                    .produces_end_of_stream();
        }
      }
    });
}

SEASTAR_TEST_CASE(test_promoted_index_repeats_open_tombstones) {
   return test_env::do_with_async([] (test_env& env) {
      for (const auto version : writable_sstable_versions) {
        int id = 0;
        for (auto& compact : { schema_builder::compact_storage::no, schema_builder::compact_storage::yes }) {
            const auto generation = id++;
            schema_builder builder("ks", format("cf{:d}", generation));
            builder.with_column("p", utf8_type, column_kind::partition_key);
            builder.with_column("c1", bytes_type, column_kind::clustering_key);
            builder.with_column("v", int32_type);
            auto s = builder.build(compact);

            auto dk = tests::generate_partition_key(s);
            auto cell = atomic_cell::make_live(*int32_type, 1, int32_type->decompose(88), { });
            mutation m(s, dk);

            m.partition().apply_row_tombstone(*s, range_tombstone(
                    clustering_key_prefix::from_exploded(*s, {bytes_type->decompose(data_value(to_bytes("ck1")))}),
                    bound_kind::incl_start,
                    clustering_key_prefix::from_exploded(*s, {bytes_type->decompose(data_value(to_bytes("ck5")))}),
                    bound_kind::incl_end,
                    {1, gc_clock::now()}));

            auto ck = clustering_key::from_exploded(*s, {bytes_type->decompose(data_value(to_bytes("ck3")))});
            m.set_clustered_cell(ck, *s->get_column_definition("v"), atomic_cell(*int32_type, cell));

            auto mt = make_memtable(s, {m});
            sstable_writer_config cfg = env.manager().configure_writer();
            cfg.promoted_index_block_size = 1;

            auto sst = make_sstable_easy(env, mt, cfg, version);

            {
                auto slice = partition_slice_builder(*s).with_range(query::clustering_range::make_starting_with({ck})).build();
                assert_that(sst->as_mutation_source().make_reader_v2(s, env.make_reader_permit(), dht::partition_range::make_singular(dk), slice))
                        .produces(m, slice.get_all_ranges())
                        .produces_end_of_stream();
            }
        }
      }
    });
}

SEASTAR_TEST_CASE(test_range_tombstones_are_correctly_seralized_for_non_compound_dense_schemas) {
   return test_env::do_with_async([] (test_env& env) {
      for (const auto version : writable_sstable_versions) {
        schema_builder builder("ks", "cf");
        builder.with_column("p", utf8_type, column_kind::partition_key);
        builder.with_column("c", int32_type, column_kind::clustering_key);
        builder.with_column("v", int32_type);
        auto s = builder.build(schema_builder::compact_storage::yes);

        auto dk = tests::generate_partition_key(s);
        mutation m(s, dk);

        m.partition().apply_row_tombstone(*s, range_tombstone(
                clustering_key_prefix::from_exploded(*s, {int32_type->decompose(1)}),
                bound_kind::incl_start,
                clustering_key_prefix::from_exploded(*s, {int32_type->decompose(2)}),
                bound_kind::incl_end,
                {1, gc_clock::now()}));

        auto mt = make_memtable(s, {m});
        sstable_writer_config cfg = env.manager().configure_writer();

        auto sst = make_sstable_easy(env, mt, cfg, version);

        {
            auto slice = partition_slice_builder(*s).build();
            assert_that(sst->as_mutation_source().make_reader_v2(s, env.make_reader_permit(), dht::partition_range::make_singular(dk), slice))
                    .produces(m)
                    .produces_end_of_stream();
        }
      }
    });
}

SEASTAR_TEST_CASE(test_promoted_index_is_absent_for_schemas_without_clustering_key) {
   return test_env::do_with_async([] (test_env& env) {
      for (const auto version : writable_sstable_versions) {
        schema_builder builder("ks", "cf");
        builder.with_column("p", utf8_type, column_kind::partition_key);
        builder.with_column("v", int32_type);
        auto s = builder.build(schema_builder::compact_storage::yes);

        auto dk = tests::generate_partition_key(s);
        mutation m(s, dk);
        for (auto&& v : { 1, 2, 3, 4 }) {
            auto cell = atomic_cell::make_live(*int32_type, 1, int32_type->decompose(v), { });
            m.set_clustered_cell(clustering_key_prefix::make_empty(), *s->get_column_definition("v"), atomic_cell(*int32_type, cell));
        }
        auto mt = make_memtable(s, {m});
        sstable_writer_config cfg = env.manager().configure_writer();
        cfg.promoted_index_block_size = 1;

        auto sst = make_sstable_easy(env, mt, cfg, version);
        assert_that(get_index_reader(sst, env.make_reader_permit())).is_empty(*s);
      }
    });
}

SEASTAR_TEST_CASE(test_writing_combined_stream_with_tombstones_at_the_same_position) {
   return test_env::do_with_async([] (test_env& env) {
      for (const auto version : writable_sstable_versions) {
        simple_schema ss;
        auto s = ss.schema();

        auto rt1 = ss.make_range_tombstone(ss.make_ckey_range(1, 10));
        auto rt2 = ss.make_range_tombstone(ss.make_ckey_range(1, 5)); // rt1 + rt2 = {[1, 5], (5, 10]}

        auto local_k = tests::generate_partition_key(s);

        mutation m1(s, local_k);
        ss.add_row(m1, ss.make_ckey(0), "v0"); // So that we don't hit workaround for #1203, which would cover up bugs
        m1.partition().apply_delete(*s, rt1);
        m1.partition().apply_delete(*s, ss.make_ckey(4), ss.new_tombstone());

        auto rt3 = ss.make_range_tombstone(ss.make_ckey_range(20, 21));
        m1.partition().apply_delete(*s, ss.make_ckey(20), ss.new_tombstone());
        m1.partition().apply_delete(*s, rt3);

        mutation m2(s, local_k);
        m2.partition().apply_delete(*s, rt2);
        ss.add_row(m2, ss.make_ckey(4), "v2"); // position inside rt2

        auto mt1 = make_memtable(s, {m1});
        auto mt2 = make_memtable(s, {m2});
        auto combined_permit = env.make_reader_permit();
        auto mr = make_combined_reader(s, combined_permit,
            mt1->make_mutation_reader(s, combined_permit), mt2->make_mutation_reader(s, combined_permit));
        auto sst = make_sstable_easy(env, std::move(mr), env.manager().configure_writer(), version);

        assert_that(sst->as_mutation_source().make_reader_v2(s, env.make_reader_permit()))
            .produces(m1 + m2)
            .produces_end_of_stream();
      }
    });
}

SEASTAR_TEST_CASE(test_no_index_reads_when_rows_fall_into_range_boundaries) {
    return test_env::do_with_async([] (test_env& env) {
        for (const auto version : writable_sstable_versions) {
            simple_schema ss(simple_schema::with_static::yes);
            auto s = ss.schema();

            auto pks = tests::generate_partition_keys(2, s);

            mutation m1(s, pks[0]);
            ss.add_row(m1, ss.make_ckey(1), "v");
            ss.add_row(m1, ss.make_ckey(2), "v");
            ss.add_row(m1, ss.make_ckey(5), "v");
            ss.add_row(m1, ss.make_ckey(6), "v");

            mutation m2(s, pks[1]);
            ss.add_static_row(m2, "svalue");
            ss.add_row(m2, ss.make_ckey(2), "v");
            ss.add_row(m2, ss.make_ckey(5), "v");
            ss.add_row(m2, ss.make_ckey(6), "v");

            auto ms = make_sstable_mutation_source(env, s, {m1, m2}, version);

            auto index_accesses = [&env] {
                auto&& stats = env.manager().get_cache_tracker().get_partition_index_cache_stats();
                return stats.hits + stats.misses + stats.blocks;
            };

            auto before = index_accesses();

            {
                assert_that(ms.make_reader_v2(s, env.make_reader_permit()))
                    .produces(m1)
                    .produces(m2)
                    .produces_end_of_stream();

                BOOST_REQUIRE_EQUAL(index_accesses(), before);
            }
      }
    });
}

SEASTAR_TEST_CASE(test_key_count_estimation) {
    return test_env::do_with_async([] (test_env& env) {
        for (const auto version : writable_sstable_versions) {
            auto s = schema_builder("ks", "cf")
                .with_column("pk", bytes_type, column_kind::partition_key)
                .with_column("v", int32_type)
                .build();

            int count = 10'000;
            std::vector<dht::decorated_key> all_pks = tests::generate_partition_keys(count + 2, s, local_shard_only::yes, tests::key_size{8, 8});
            std::vector<dht::decorated_key> pks(all_pks.begin() + 1, all_pks.end() - 1);
            auto mt = make_lw_shared<replica::memtable>(s);
            for (auto pk : pks) {
                mutation m(s, pk);
                m.set_clustered_cell(clustering_key::make_empty(), bytes("v"), data_value(int32_t(1)), 1 /* ts */);
                mt->apply(m);
            }

            auto _ = env.tempdir().make_sweeper();
            shared_sstable sst = make_sstable_easy(env, mt, env.manager().configure_writer(), version, pks.size());

            auto max_est = sst->get_estimated_key_count();
            testlog.trace("count = {}", count);
            testlog.trace("est = {}", max_est);

            {
                auto est = sst->estimated_keys_for_range(dht::token_range::make_open_ended_both_sides());
                testlog.trace("est([-inf; +inf]) = {}", est);
                BOOST_REQUIRE_EQUAL(est, sst->get_estimated_key_count());
            }

            for (int size : {1, 64, 256, 512, 1024, 4096, count}) {
                auto r = dht::token_range::make(pks[0].token(), pks[size - 1].token());
                auto est = sst->estimated_keys_for_range(r);
                testlog.trace("est([0; {}] = {}", size - 1, est);
                BOOST_REQUIRE_GE(est, size);
                BOOST_REQUIRE_LE(est, max_est);
            }

            for (int size : {1, 64, 256, 512, 1024, 4096, count}) {
                auto lower = 5000;
                auto upper = std::min(count - 1, lower + size - 1);
                auto r = dht::token_range::make(pks[lower].token(), pks[upper].token());
                auto est = sst->estimated_keys_for_range(r);
                testlog.trace("est([{}; {}]) = {}", lower, upper, est);
                BOOST_REQUIRE_GE(est, upper - lower + 1);
                BOOST_REQUIRE_LE(est, max_est);
            }

            {
                auto r = dht::token_range::make(all_pks[0].token(), all_pks[0].token());
                auto est = sst->estimated_keys_for_range(r);
                testlog.trace("est(non-overlapping to the left) = {}", est);
                BOOST_REQUIRE_EQUAL(est, 0);
            }

            {
                auto r = dht::token_range::make(all_pks[all_pks.size() - 1].token(), all_pks[all_pks.size() - 1].token());
                auto est = sst->estimated_keys_for_range(r);
                testlog.trace("est(non-overlapping to the right) = {}", est);
                BOOST_REQUIRE_EQUAL(est, 0);
            }
        }
    });
}

SEASTAR_TEST_CASE(test_large_index_pages_do_not_cause_large_allocations) {
  return test_env::do_with_async([] (test_env& env) {
    // We create a sequence of partitions such that first we have a partition with a very long key, then
    // series of partitions with small keys. This should result in large index page.

    simple_schema ss;
    auto s = ss.schema();

    const size_t large_key_pad_size = 9000;
    const size_t small_key_pad_size = 16;
    const size_t n_small_keys = 100000;

    auto make_pkey_text = [] (size_t pad_size) -> sstring {
        static int i = 0;
        return format("pkey_0x{:x}_{}", i++, make_random_string(pad_size));
    };

    // Choose min from several random keys
    std::optional<dht::decorated_key> large_key;
    for (int i = 0; i < 10; ++i) {
        auto pk = ss.make_pkey(make_pkey_text(large_key_pad_size));
        if (!large_key || pk.less_compare(*s, *large_key)) {
            large_key = std::move(pk);
        }
    }

    std::vector<dht::decorated_key> small_keys; // only larger than *large_key
    while (small_keys.size() < n_small_keys) {
        auto pk = ss.make_pkey(make_pkey_text(small_key_pad_size));
        if (large_key->less_compare(*s, pk)) {
            small_keys.emplace_back(std::move(pk));
        }
    }

    std::sort(small_keys.begin(), small_keys.end(), dht::decorated_key::less_comparator(s));

    seastar::memory::scoped_large_allocation_warning_threshold mtg(logalloc::segment_size);

    auto mt = make_lw_shared<replica::memtable>(s);
    {
        mutation m(s, *large_key);
        ss.add_row(m, ss.make_ckey(0), "v");
        mt->apply(m);
    }

    for (auto&& key : small_keys) {
        mutation m(s, key);
        auto value = make_random_string(128);
        ss.add_row(m, ss.make_ckey(0), value);
        mt->apply(m);
    }

    sstable_writer_config cfg = env.manager().configure_writer();
    auto sst = make_sstable_easy(env, mt, cfg);

    auto pr = dht::partition_range::make_singular(small_keys[0]);

    mutation expected = *with_closeable(mt->make_mutation_reader(s, env.make_reader_permit(), pr), [] (auto& mt_reader) {
        return read_mutation_from_mutation_reader(mt_reader);
    }).get();

    auto t0 = std::chrono::steady_clock::now();
    auto large_allocs_before = memory::stats().large_allocations();
    mutation actual = *with_closeable(sst->as_mutation_source().make_reader_v2(s, env.make_reader_permit(), pr), [] (auto& sst_reader) {
        return read_mutation_from_mutation_reader(sst_reader);
    }).get();
    auto large_allocs_after = memory::stats().large_allocations();
    auto duration = std::chrono::steady_clock::now() - t0;

    testlog.trace("Read took {:d} us", duration / 1us);

    assert_that(actual).is_equal_to(expected);
    BOOST_REQUIRE_EQUAL(large_allocs_after - large_allocs_before, 0);
  });
}

SEASTAR_TEST_CASE(test_reading_serialization_header) {
  return test_env::do_with_async([] (test_env& env) {
    auto random_int32_value = [] {
        return int32_type->decompose(tests::random::get_int<int32_t>());
    };

    auto td = tests::data_model::table_description({ { "pk", int32_type } }, { { "ck", utf8_type } });

    auto td1 = td;
    td1.add_static_column("s1", int32_type);
    td1.add_regular_column("v1", int32_type);
    td1.add_regular_column("v2", int32_type);
    auto built_schema = td1.build();
    auto s = built_schema.schema;

    auto md1 = tests::data_model::mutation_description({ to_bytes("pk1") });
    md1.add_clustered_row_marker({ to_bytes("ck1") }, -10);
    md1.add_clustered_cell({ to_bytes("ck1") }, "v1", random_int32_value());
    auto m1 = md1.build(s);

    auto now = gc_clock::now();
    auto ttl = gc_clock::duration(std::chrono::hours(1));
    auto expiry_time = now + ttl;

    auto md2 = tests::data_model::mutation_description({ to_bytes("pk2") });
    md2.add_static_cell("s1",
            tests::data_model::mutation_description::atomic_value(random_int32_value(), tests::data_model::data_timestamp, ttl, expiry_time));
    auto m2 = md2.build(s);

    auto mt = make_memtable(s, {m1, m2});
    auto md1_overwrite = tests::data_model::mutation_description({ to_bytes("pk1") });
    md1_overwrite.add_clustered_row_marker({ to_bytes("ck1") }, 10);
    auto m1ow = md1_overwrite.build(s);
    mt->apply(m1ow);

    std::optional<sstables::generation_type> gen;
    {
        // SSTable class has way too many responsibilities. In particular, it mixes the reading and
        // writing parts. Let's use a separate objects for writing and reading to ensure that nothing
        // carries over that wouldn't normally be read from disk.
        auto sst = env.make_sstable(s);
        gen.emplace(sst->generation());
        sst->write_components(mt->make_mutation_reader(s, env.make_reader_permit()), 2, s, env.manager().configure_writer(), mt->get_encoding_stats()).get();
    }

    auto sst = env.reusable_sst(s, *gen).get();

    auto hdr = sst->get_serialization_header();
    BOOST_CHECK_EQUAL(hdr.static_columns.elements.size(), 1);
    BOOST_CHECK_EQUAL(hdr.static_columns.elements[0].name.value, to_bytes("s1"));
    BOOST_CHECK_EQUAL(hdr.regular_columns.elements.size(), 2);
    BOOST_CHECK(hdr.regular_columns.elements[0].name.value == to_bytes("v1"));
    BOOST_CHECK(hdr.regular_columns.elements[1].name.value == to_bytes("v2"));

    BOOST_CHECK_EQUAL(hdr.get_min_timestamp(), -10);
    BOOST_CHECK_EQUAL(hdr.get_min_local_deletion_time(), expiry_time.time_since_epoch().count());
    BOOST_CHECK_EQUAL(hdr.get_min_ttl(), ttl.count());

    auto stats = sst->get_encoding_stats_for_compaction();
    BOOST_CHECK(stats.min_local_deletion_time == expiry_time);
    BOOST_CHECK_EQUAL(stats.min_timestamp, 10);
    // Like Cassandra even if a row marker is not expiring we update the metadata with NO_TTL value
    // which is 0.
    BOOST_CHECK(stats.min_ttl == gc_clock::duration(0));
  });
}

SEASTAR_THREAD_TEST_CASE(test_merging_encoding_stats) {
    auto ecc = encoding_stats_collector{};
    auto ec1 = encoding_stats{};

    ecc.update(ec1);
    auto ec = ecc.get();
    BOOST_CHECK_EQUAL(ec.min_timestamp, ec1.min_timestamp);
    BOOST_CHECK(ec.min_local_deletion_time == ec1.min_local_deletion_time);
    BOOST_CHECK(ec.min_ttl == ec1.min_ttl);

    ec1.min_timestamp = -10;
    ec1.min_local_deletion_time = gc_clock::now();
    ec1.min_ttl = gc_clock::duration(std::chrono::hours(1));

    ecc = encoding_stats_collector{};
    ecc.update(ec1);
    ec = ecc.get();
    BOOST_CHECK_EQUAL(ec.min_timestamp, ec1.min_timestamp);
    BOOST_CHECK(ec.min_local_deletion_time == ec1.min_local_deletion_time);
    BOOST_CHECK(ec.min_ttl == ec1.min_ttl);

    auto ec2 = encoding_stats{};
    ec2.min_timestamp = -20;
    ec2.min_local_deletion_time = ec1.min_local_deletion_time - std::chrono::seconds(1);
    ec2.min_ttl = gc_clock::duration(std::chrono::hours(2));
    ecc.update(ec2);

    ec = ecc.get();
    BOOST_CHECK_EQUAL(ec.min_timestamp, -20);
    BOOST_CHECK(ec.min_local_deletion_time == ec2.min_local_deletion_time);
    BOOST_CHECK(ec.min_ttl == ec1.min_ttl);
}


// Reproducer for #4206
SEASTAR_TEST_CASE(test_counter_header_size) {
  return test_env::do_with_async([] (test_env& env) {
    auto s = schema_builder("ks", "counter_test")
        .with_column("pk", int32_type, column_kind::partition_key)
        .with_column("ck", int32_type, column_kind::clustering_key)
        .with_column("c1", counter_type)
        .build();

    auto pk = partition_key::from_single_value(*s, int32_type->decompose(0));
    auto ck = clustering_key::from_single_value(*s, int32_type->decompose(0));

    auto& col = *s->get_column_definition(utf8_type->decompose(sstring("c1")));

    auto ids = std::vector<counter_id>();
    for (auto i = 0; i < 128; i++) {
        ids.emplace_back(counter_id(utils::make_random_uuid()));
    }
    std::sort(ids.begin(), ids.end());

    mutation m(s, pk);
    counter_cell_builder ccb;
    for (auto id : ids) {
        ccb.add_shard(counter_shard(id, 1, 1));
    }
    m.set_clustered_cell(ck, col, ccb.build(api::new_timestamp()));

    auto mt = make_memtable(s, {m});
    for (const auto version : writable_sstable_versions) {
        auto sst = make_sstable_easy(env, mt, env.manager().configure_writer(), version);
        assert_that(sst->as_mutation_source().make_reader_v2(s, env.make_reader_permit()))
            .produces(m)
            .produces_end_of_stream();
    }
  });
}

SEASTAR_TEST_CASE(test_static_compact_tables_are_read) {
    return test_env::do_with_async([] (test_env& env) {
        for (const auto version : writable_sstable_versions) {
            auto s = schema_builder("ks", "test")
                .with_column("pk", int32_type, column_kind::partition_key)
                .with_column("v1", int32_type)
                .with_column("v2", int32_type)
                .build(schema_builder::compact_storage::yes);

            auto pk1 = partition_key::from_exploded(*s, {int32_type->decompose(1)});
            auto dk1 = dht::decorate_key(*s, pk1);
            mutation m1(s, dk1);
            m1.set_clustered_cell(clustering_key::make_empty(), *s->get_column_definition("v1"),
                atomic_cell::make_live(*int32_type, 1511270919978349, int32_type->decompose(4), {}));

            auto pk2 = partition_key::from_exploded(*s, {int32_type->decompose(2)});
            auto dk2 = dht::decorate_key(*s, pk2);
            mutation m2(s, dk2);
            m2.set_clustered_cell(clustering_key::make_empty(), *s->get_column_definition("v1"),
                atomic_cell::make_live(*int32_type, 1511270919978348, int32_type->decompose(5), {}));
            m2.set_clustered_cell(clustering_key::make_empty(), *s->get_column_definition("v2"),
                atomic_cell::make_live(*int32_type, 1511270919978347, int32_type->decompose(6), {}));

            std::vector<mutation> muts = {m1, m2};
            std::ranges::sort(muts, mutation_decorated_key_less_comparator{});

            auto ms = make_sstable_mutation_source(env, s, muts, version);

            assert_that(ms.make_reader_v2(s, env.make_reader_permit()))
                .produces(muts[0])
                .produces(muts[1])
                .produces_end_of_stream();
        }
    });
}

SEASTAR_TEST_CASE(writer_handles_subsequent_range_tombstone_changes_without_tombstones) {
    // This test exposes a problem of a peculiar setup of tombstones that trigger
    // a mutation fragment stream validation exception if stream is compacted.
    //
    // Applying tombstones in the order:
    //
    // range_tombstone_change pos(ck1), after_all_prefixed, tombstone_timestamp=1
    // range_tombstone_change pos(ck2), before_all_prefixed, tombstone=NONE
    // range_tombstone_change pos(NONE), after_all_prefixed, tombstone=NONE
    //
    // Can lead to swapping the order of mutations when written and read from
    // disk via sstable writer. This is caused by conversion of
    // range_tombstone_change (in memory representation) to range tombstone
    // marker (on disk representation) and back.
    //
    // When this mutation stream is written to disk, the range tombstone
    // markers type is calculated based on the relationship between
    // range_tombstone_changes. The RTC series as above produces markers
    // (start, end, start). When the last marker is loaded from disk, it's kind
    // gets incorrectly loaded as before_all_prefixed instead of
    // after_all_prefixed. This leads to incorrect order of mutations.
    return test_env::do_with_async([] (test_env& env) {
        for ([[maybe_unused]] const auto version : writable_sstable_versions) {
            schema_ptr s = schema_builder("ks", "cf")
                .with_column("pk", bytes_type, column_kind::partition_key)
                .with_column("ck1", bytes_type, column_kind::clustering_key)
                .with_column("ck2", bytes_type, column_kind::clustering_key)
                .build();
            tests::reader_concurrency_semaphore_wrapper sem;
            auto pk = tests::generate_partition_key(s);
            reader_permit p = sem.make_permit();
            auto make_rtc = [s, p](bound_weight bw, std::vector<bytes>&& ck, tombstone t) {
                return mutation_fragment_v2(*s, p, range_tombstone_change(
                    position_in_partition(partition_region::clustered, bw, clustering_key_prefix{std::move(ck)}),
                    std::move(t)));
            };
            std::deque<mutation_fragment_v2> fragments;
            fragments.emplace_back(*s, p, partition_start(pk, {}));
            fragments.push_back(make_rtc(bound_weight::after_all_prefixed, {serialized(1l)}, {api::min_timestamp + 1, {}}));
            fragments.push_back(make_rtc(bound_weight::before_all_prefixed, {serialized(2l)}, {}));
            fragments.push_back(make_rtc(bound_weight::after_all_prefixed, {}, {}));
            fragments.emplace_back(*s, p, partition_end{});

            mutation_reader input_reader = make_mutation_reader_from_fragments(s, p, std::move(fragments));
            deferred_close dc1{input_reader};
            sstable_writer_config cfg = env.manager().configure_writer();
            auto _ = env.tempdir().make_sweeper();
            auto sstable = make_sstable_easy(env, std::move(input_reader), cfg);

            mutation_fragment_stream_validating_filter f{"mutation_order_violation_test", *s, mutation_fragment_stream_validation_level::clustering_key};
            auto sstable_reader = sstable->make_reader(s, sem.make_permit(), query::full_partition_range, s->full_slice());
            deferred_close dc{sstable_reader};
            sstable_reader.consume_pausable([&f](mutation_fragment_v2 mf) {
                f(mf);
                return stop_iteration::no;
            }).get();
        }
    });
}

