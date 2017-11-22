/*
 * Copyright (C) 2015 ScyllaDB
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
#include "tests/test-utils.hh"
#include "sstable_test.hh"
#include "sstables/key.hh"
#include "core/do_with.hh"
#include "core/thread.hh"
#include "sstables/sstables.hh"
#include "database.hh"
#include "timestamp.hh"
#include "schema_builder.hh"
#include "mutation_reader.hh"
#include "mutation_reader_assertions.hh"
#include "mutation_source_test.hh"
#include "tmpdir.hh"
#include "memtable-sstable.hh"
#include "disk-error-handler.hh"
#include "tests/sstable_assertions.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

using namespace sstables;

SEASTAR_TEST_CASE(nonexistent_key) {
    return reusable_sst(uncompressed_schema(), "tests/sstables/uncompressed", 1).then([] (auto sstp) {
        return do_with(key::from_bytes(to_bytes("invalid_key")), [sstp] (auto& key) {
            auto s = uncompressed_schema();
            return sstp->read_row(s, key).then([sstp, s, &key] (auto mutation) {
                BOOST_REQUIRE(!mutation);
                return make_ready_future<>();
            });
        });
    });
}

future<> test_no_clustered(bytes&& key, std::unordered_map<bytes, data_value> &&map) {
    return reusable_sst(uncompressed_schema(), "tests/sstables/uncompressed", 1).then([k = std::move(key), map = std::move(map)] (auto sstp) mutable {
        return do_with(sstables::key(std::move(k)), [sstp, map = std::move(map)] (auto& key) {
            auto s = uncompressed_schema();
            return sstp->read_row(s, key).then([] (auto sm) {
                return mutation_from_streamed_mutation(std::move(sm));
            }).then([sstp, s, &key, map = std::move(map)] (auto mutation) {
                BOOST_REQUIRE(mutation);
                auto& mp = mutation->partition();
                for (auto&& e : mp.range(*s, nonwrapping_range<clustering_key_prefix>())) {
                    BOOST_REQUIRE(to_bytes(e.key()) == to_bytes(""));
                    BOOST_REQUIRE(e.row().cells().size() == map.size());

                    auto &row = e.row().cells();
                    for (auto&& c: map) {
                        match_live_cell(row, *s, c.first, c.second);
                    }
                }
                return make_ready_future<>();
            });
        });
    });
}

SEASTAR_TEST_CASE(uncompressed_1) {
    return test_no_clustered("vinna", {{ "col1", to_sstring("daughter") }, { "col2", 3 }});
}

SEASTAR_TEST_CASE(uncompressed_2) {
    return test_no_clustered("gustaf", {{ "col1", to_sstring("son") }, { "col2", 0 }});
}

SEASTAR_TEST_CASE(uncompressed_3) {
    return test_no_clustered("isak", {{ "col1", to_sstring("son") }, { "col2", 1 }});
}

SEASTAR_TEST_CASE(uncompressed_4) {
    return test_no_clustered("finna", {{ "col1", to_sstring("daughter") }, { "col2", 2 }});
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
template <int Generation>
future<mutation> generate_clustered(bytes&& key) {
    return reusable_sst(complex_schema(), "tests/sstables/complex", Generation).then([k = std::move(key)] (auto sstp) mutable {
        return do_with(sstables::key(std::move(k)), [sstp] (auto& key) {
            auto s = complex_schema();
            return sstp->read_row(s, key).then([] (auto sm) {
                return mutation_from_streamed_mutation(std::move(sm));
            }).then([sstp, s, &key] (auto mutation) {
                BOOST_REQUIRE(mutation);
                return std::move(*mutation);
            });
        });
    });
}

inline auto clustered_row(mutation& mutation, const schema& s, std::vector<bytes>&& v) {
    auto exploded = exploded_clustering_prefix(std::move(v));
    auto clustering_pair = clustering_key::from_clustering_prefix(s, exploded);
    return mutation.partition().clustered_row(s, clustering_pair);
}

SEASTAR_TEST_CASE(complex_sst1_k1) {
    return generate_clustered<1>("key1").then([] (auto&& mutation) {
        auto s = complex_schema();

        auto sr = mutation.partition().static_row();
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

        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(complex_sst1_k2) {
    return generate_clustered<1>("key2").then([] (auto&& mutation) {
        auto s = complex_schema();

        auto sr = mutation.partition().static_row();
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

        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(complex_sst2_k1) {
    return generate_clustered<2>("key1").then([] (auto&& mutation) {
        auto s = complex_schema();

        auto exploded = exploded_clustering_prefix({"cl1.1", "cl2.1"});
        auto clustering = clustering_key::from_clustering_prefix(*s, exploded);

        auto t1 = mutation.partition().range_tombstone_for_row(*s, clustering);
        BOOST_REQUIRE(t1.timestamp == 1431451394600754l);
        BOOST_REQUIRE(t1.deletion_time == gc_clock::time_point(gc_clock::duration(1431451394)));

        auto row = clustered_row(mutation, *s, {"cl1.2", "cl2.2"});
        auto reg_list = match_collection(row.cells(), *s, "reg_list", tombstone(deletion_time{0, api::missing_timestamp}));
        match_collection_element<status::dead>(reg_list.cells[0], bytes_opt{}, bytes_opt{});
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(complex_sst2_k2) {
    return generate_clustered<2>("key2").then([] (auto&& mutation) {
        auto s = complex_schema();

        auto sr = mutation.partition().static_row();
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

        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(complex_sst2_k3) {
    return generate_clustered<2>("key3").then([] (auto&& mutation) {
        auto s = complex_schema();

        auto sr = mutation.partition().static_row();
        match_expiring_cell(sr, *s, "static_obj", data_value(to_bytes("static_value_3")), 1431451394597062l, 1431537794);

        auto row1 = clustered_row(mutation, *s, {"tcl1.1", "tcl2.1"});
        BOOST_REQUIRE(row1.created_at() == 1431451394597062l);
        match_expiring_cell(row1.cells(), *s, "reg", data_value(to_bytes("v5")), 1431451394597062l, 1431537794);
        match_absent(row1.cells(), *s, "reg_list");
        match_absent(row1.cells(), *s, "reg_set");
        match_absent(row1.cells(), *s, "reg_map");
        match_absent(row1.cells(), *s, "reg_fset");
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(complex_sst3_k1) {
    return generate_clustered<3>("key1").then([] (auto&& mutation) {
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
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(complex_sst3_k2) {
    return generate_clustered<3>("key2").then([] (auto&& mutation) {
        auto s = complex_schema();

        auto sr = mutation.partition().static_row();
        match_live_cell(sr, *s, "static_obj", data_value(to_bytes("final_static")));

        auto row = clustered_row(mutation, *s, {"kcl1.1", "kcl2.1"});
        auto reg_map = match_collection(row.cells(), *s, "reg_map", tombstone(deletion_time{0, api::missing_timestamp}));
        match_collection_element<status::live>(reg_map.cells[0], to_bytes("6"), to_bytes("1"));
        match_absent(row.cells(), *s, "reg_list");
        match_absent(row.cells(), *s, "reg_set");
        match_absent(row.cells(), *s, "reg");
        match_absent(row.cells(), *s, "reg_fset");
        return make_ready_future<>();
    });
}

future<> test_range_reads(const dht::token& min, const dht::token& max, std::vector<bytes>& expected) {
    return reusable_sst(uncompressed_schema(), "tests/sstables/uncompressed", 1).then([min, max, &expected] (auto sstp) mutable {
        auto s = uncompressed_schema();
        auto count = make_lw_shared<size_t>(0);
        auto expected_size = expected.size();
        auto stop = make_lw_shared<bool>(false);
        return do_with(dht::partition_range::make(dht::ring_position::starting_at(min),
                                                              dht::ring_position::ending_at(max)), [&, sstp, s] (auto& pr) {
            auto mutations = sstp->read_range_rows(s, pr);
            return do_until([stop] { return *stop; },
                // Note: The data in the following lambda, including
                // "mutations", continues to live until after the last
                // iteration's future completes, so its lifetime is safe.
                [sstp, mutations = std::move(mutations), &expected, expected_size, count, stop] () mutable {
                    return mutations().then([&expected, expected_size, count, stop] (streamed_mutation_opt mutation) mutable {
                        if (mutation) {
                            BOOST_REQUIRE(*count < expected_size);
                            BOOST_REQUIRE(std::vector<bytes>({expected.back()}) == mutation->key().explode());
                            expected.pop_back();
                            (*count)++;
                        } else {
                            *stop = true;
                        }
                    });
            }).then([count, expected_size] {
                BOOST_REQUIRE(*count == expected_size);
            });
        });
    });
}

SEASTAR_TEST_CASE(read_range) {
    std::vector<bytes> expected = { to_bytes("finna"), to_bytes("isak"), to_bytes("gustaf"), to_bytes("vinna") };
    return do_with(std::move(expected), [] (auto& expected) {
        return test_range_reads(dht::minimum_token(), dht::maximum_token(), expected);
    });
}

SEASTAR_TEST_CASE(read_partial_range) {
    std::vector<bytes> expected = { to_bytes("finna"), to_bytes("isak") };
    return do_with(std::move(expected), [] (auto& expected) {
        return test_range_reads(dht::global_partitioner().get_token(key_view(bytes_view(expected.back()))), dht::maximum_token(), expected);
    });
}

SEASTAR_TEST_CASE(read_partial_range_2) {
    std::vector<bytes> expected = { to_bytes("gustaf"), to_bytes("vinna") };
    return do_with(std::move(expected), [] (auto& expected) {
        return test_range_reads(dht::minimum_token(), dht::global_partitioner().get_token(key_view(bytes_view(expected.front()))), expected);
    });
}

// Must be run in a seastar thread
static
void test_mutation_source(sstable_writer_config cfg, sstables::sstable::version_types version) {
    std::vector<tmpdir> dirs;
    run_mutation_source_tests([&dirs, &cfg, version] (schema_ptr s, const std::vector<mutation>& partitions) -> mutation_source {
        tmpdir sstable_dir;
        auto sst = sstables::make_sstable(s,
            sstable_dir.path,
            1 /* generation */,
            version,
            sstables::sstable::format_types::big);
        dirs.emplace_back(std::move(sstable_dir));

        auto mt = make_lw_shared<memtable>(s);

        for (auto&& m : partitions) {
            mt->apply(m);
        }

        sst->write_components(mt->make_reader(s), partitions.size(), s, cfg).get();
        sst->load().get();

        return as_mutation_source(sst);
    });
}


SEASTAR_TEST_CASE(test_sstable_conforms_to_mutation_source) {
    return seastar::async([] {
        for (auto version : {sstables::sstable::version_types::ka, sstables::sstable::version_types::la}) {
            for (auto index_block_size : {1, 128, 64*1024}) {
                sstable_writer_config cfg;
                cfg.promoted_index_block_size = index_block_size;
                test_mutation_source(cfg, version);
            }
        }
    });
}

SEASTAR_TEST_CASE(test_sstable_can_write_and_read_range_tombstone) {
    return seastar::async([] {
        auto dir = make_lw_shared<tmpdir>();
        auto s = make_lw_shared(schema({}, "ks", "cf",
            {{"p1", utf8_type}}, {{"c1", int32_type}}, {{"r1", int32_type}}, {}, utf8_type));

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key_start = clustering_key::from_exploded(*s, {int32_type->decompose(1)});
        auto c_key_end = clustering_key::from_exploded(*s, {int32_type->decompose(2)});

        mutation m(key, s);
        auto ttl = gc_clock::now() + std::chrono::seconds(1);
        m.partition().apply_delete(*s, range_tombstone(c_key_start, bound_kind::excl_start, c_key_end, bound_kind::excl_end, tombstone(9, ttl)));

        auto mt = make_lw_shared<memtable>(s);
        mt->apply(std::move(m));

        auto sst = sstables::make_sstable(s,
                dir->path,
                1 /* generation */,
                sstables::sstable::version_types::la,
                sstables::sstable::format_types::big);
        write_memtable_to_sstable(*mt, sst).get();
        sst->load().get();
        auto mr = sst->read_rows_flat(s);
        auto mut = read_mutation_from_flat_mutation_reader(s, mr).get0();
        BOOST_REQUIRE(bool(mut));
        auto& rts = mut->partition().row_tombstones();
        BOOST_REQUIRE(rts.size() == 1);
        auto it = rts.begin();
        BOOST_REQUIRE(it->equal(*s, range_tombstone(
                        c_key_start,
                        bound_kind::excl_start,
                        c_key_end,
                        bound_kind::excl_end,
                        tombstone(9, ttl))));
    });
}

SEASTAR_TEST_CASE(compact_storage_sparse_read) {
    return reusable_sst(compact_sparse_schema(), "tests/sstables/compact_sparse", 1).then([] (auto sstp) {
        return do_with(sstables::key("first_row"), [sstp] (auto& key) {
            auto s = compact_sparse_schema();
            return sstp->read_row(s, key).then([] (auto sm) {
                return mutation_from_streamed_mutation(std::move(sm));
            }).then([sstp, s, &key] (auto mutation) {
                auto& mp = mutation->partition();
                auto row = mp.clustered_row(*s, clustering_key::make_empty());
                match_live_cell(row.cells(), *s, "cl1", data_value(to_bytes("cl1")));
                match_live_cell(row.cells(), *s, "cl2", data_value(to_bytes("cl2")));
                return make_ready_future<>();
            });
        });
    });
}

SEASTAR_TEST_CASE(compact_storage_simple_dense_read) {
    return reusable_sst(compact_simple_dense_schema(), "tests/sstables/compact_simple_dense", 1).then([] (auto sstp) {
        return do_with(sstables::key("first_row"), [sstp] (auto& key) {
            auto s = compact_simple_dense_schema();
            return sstp->read_row(s, key).then([] (auto sm) {
                return mutation_from_streamed_mutation(std::move(sm));
            }).then([sstp, s, &key] (auto mutation) {
                auto& mp = mutation->partition();

                auto exploded = exploded_clustering_prefix({"cl1"});
                auto clustering = clustering_key::from_clustering_prefix(*s, exploded);

                auto row = mp.clustered_row(*s, clustering);
                match_live_cell(row.cells(), *s, "cl2", data_value(to_bytes("cl2")));
                return make_ready_future<>();
            });
        });
    });
}

SEASTAR_TEST_CASE(compact_storage_dense_read) {
    return reusable_sst(compact_dense_schema(), "tests/sstables/compact_dense", 1).then([] (auto sstp) {
        return do_with(sstables::key("first_row"), [sstp] (auto& key) {
            auto s = compact_dense_schema();
            return sstp->read_row(s, key).then([] (auto sm) {
                return mutation_from_streamed_mutation(std::move(sm));
            }).then([sstp, s, &key] (auto mutation) {
                auto& mp = mutation->partition();

                auto exploded = exploded_clustering_prefix({"cl1", "cl2"});
                auto clustering = clustering_key::from_clustering_prefix(*s, exploded);

                auto row = mp.clustered_row(*s, clustering);
                match_live_cell(row.cells(), *s, "cl3", data_value(to_bytes("cl3")));
                return make_ready_future<>();
            });
        });
    });
}

// We recently had an issue, documented at #188, where range-reading from an
// sstable would break if collections were used.
//
// Make sure we don't regress on that.
SEASTAR_TEST_CASE(broken_ranges_collection) {
    return reusable_sst(peers_schema(), "tests/sstables/broken_ranges", 2).then([] (auto sstp) {
        auto s = peers_schema();
        auto reader = make_lw_shared<mutation_reader>(sstp->as_mutation_source()(s));
        return repeat([s, reader] {
            return (*reader)().then([] (auto sm) {
                return mutation_from_streamed_mutation(std::move(sm));
            }).then([s, reader] (mutation_opt mut) {
                auto key_equal = [s, &mut] (sstring ip) {
                    return mut->key().equal(*s, partition_key::from_deeply_exploded(*s, { net::ipv4_address(ip) }));
                };

                if (!mut) {
                    return stop_iteration::yes;
                } else if (key_equal("127.0.0.1")) {
                    auto row = mut->partition().clustered_row(*s, clustering_key::make_empty());
                    match_absent(row.cells(), *s, "tokens");
                } else if (key_equal("127.0.0.3")) {
                    auto row = mut->partition().clustered_row(*s, clustering_key::make_empty());
                    auto tokens = match_collection(row.cells(), *s, "tokens", tombstone(deletion_time{0x55E5F2D5, 0x051EB3FC99715Dl }));
                    match_collection_element<status::live>(tokens.cells[0], to_bytes("-8180144272884242102"), bytes_opt{});
                } else {
                    BOOST_REQUIRE(key_equal("127.0.0.2"));
                    auto t = mut->partition().partition_tombstone();
                    BOOST_REQUIRE(t.timestamp == 0x051EB3FB016850l);
                }
                return stop_iteration::no;
            });
        });
    });
}

static schema_ptr tombstone_overlap_schema() {
    static thread_local auto s = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id("try1", "tab"), "try1", "tab",
        // partition key
        {{"pk", utf8_type}},
        // clustering key
        {{"ck1", utf8_type}, {"ck2", utf8_type}},
        // regular columns
        {{"data", utf8_type}},
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


static future<sstable_ptr> ka_sst(schema_ptr schema, sstring dir, unsigned long generation) {
    auto sst = make_sstable(std::move(schema), dir, generation, sstables::sstable::version_types::ka, big);
    auto fut = sst->load();
    return std::move(fut).then([sst = std::move(sst)] {
        return make_ready_future<sstable_ptr>(std::move(sst));
    });
}

//  Considering the schema above, the sstable looks like:
//     {"key": "pk",
//      "cells": [["aaa:_","aaa:bbb:_",1459334681228103,"t",1459334681],
//                ["aaa:bbb:_","aaa:bbb:!",1459334681244989,"t",1459334681],
//                ["aaa:bbb:!","aaa:!",1459334681228103,"t",1459334681]]}
//               ]
SEASTAR_TEST_CASE(tombstone_in_tombstone) {
    return ka_sst(tombstone_overlap_schema(), "tests/sstables/tombstone_overlap", 1).then([] (auto sstp) {
        auto s = tombstone_overlap_schema();
        return do_with(sstp->read_rows_flat(s), [sstp, s] (auto& reader) {
            return repeat([sstp, s, &reader] {
                return read_mutation_from_flat_mutation_reader(s, reader).then([s] (mutation_opt mut) {
                    if (!mut) {
                        return stop_iteration::yes;
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
                    auto& rts = mut->partition().row_tombstones();
                    BOOST_REQUIRE(rts.size() == 2);
                    auto it = rts.begin();
                    BOOST_REQUIRE(it->equal(*s, range_tombstone(
                                    make_ckey("aaa"),
                                    bound_kind::incl_start,
                                    make_ckey("aaa", "bbb"),
                                    bound_kind::excl_end,
                                    tombstone(1459334681228103LL, it->tomb.deletion_time))));
                    ++it;
                    BOOST_REQUIRE(it->equal(*s, range_tombstone(
                                    make_ckey("aaa", "bbb"),
                                    bound_kind::excl_start,
                                    make_ckey("aaa"),
                                    bound_kind::incl_end,
                                    tombstone(1459334681228103LL, it->tomb.deletion_time))));
                    auto& rows = mut->partition().clustered_rows();
                    BOOST_REQUIRE(rows.calculate_size() == 1);
                    for (auto e : rows) {
                        BOOST_REQUIRE(e.key().equal(*s, make_ckey("aaa", "bbb")));
                        BOOST_REQUIRE(e.row().deleted_at().tomb().timestamp == 1459334681244989LL);
                    }

                    return stop_iteration::no;
                });
            });
        });
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
    return ka_sst(tombstone_overlap_schema(), "tests/sstables/tombstone_overlap", 4).then([] (auto sstp) {
        auto s = tombstone_overlap_schema();
        return do_with(sstp->read_rows_flat(s), [sstp, s] (auto& reader) {
            return repeat([sstp, s, &reader] {
                return read_mutation_from_flat_mutation_reader(s, reader).then([s] (mutation_opt mut) {
                    if (!mut) {
                        return stop_iteration::yes;
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
                    auto& rts = mut->partition().row_tombstones();
                    BOOST_REQUIRE(rts.size() == 1);
                    auto it = rts.begin();
                    BOOST_REQUIRE(it->equal(*s, range_tombstone(
                                    make_ckey("aaa"),
                                    bound_kind::incl_start,
                                    make_ckey("aaa"),
                                    bound_kind::incl_end,
                                    tombstone(1459334681228103LL, it->tomb.deletion_time))));
                    auto& rows = mut->partition().clustered_rows();
                    BOOST_REQUIRE(rows.calculate_size() == 0);
                    return stop_iteration::no;
                });
            });
        });
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
        schema_builder builder(make_lw_shared(schema(generate_legacy_id("try1", "tab2"), "try1", "tab2",
        // partition key
        {{"pk", utf8_type}},
        // clustering key
        {{"ck1", utf8_type}, {"ck2", utf8_type}, {"ck3", utf8_type}},
        // regular columns
        {{"data", utf8_type}},
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
SEASTAR_TEST_CASE(tombstone_in_tombstone2) {
    return ka_sst(tombstone_overlap_schema2(), "tests/sstables/tombstone_overlap", 3).then([] (auto sstp) {
        auto s = tombstone_overlap_schema2();
        return do_with(sstp->read_rows_flat(s), [sstp, s] (auto& reader) {
            return repeat([sstp, s, &reader] {
                return read_mutation_from_flat_mutation_reader(s, reader).then([s] (mutation_opt mut) {
                    if (!mut) {
                        return stop_iteration::yes;
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
                    auto& rows = mut->partition().clustered_rows();
                    auto& rts = mut->partition().row_tombstones();

                    auto it = rts.begin();
                    BOOST_REQUIRE(it->start_bound().equal(*s, bound_view(make_ckey("aaa"), bound_kind::incl_start)));
                    BOOST_REQUIRE(it->end_bound().equal(*s, bound_view(make_ckey("aaa", "bbb"), bound_kind::excl_end)));
                    BOOST_REQUIRE(it->tomb.timestamp == 1459438519943668L);
                    ++it;
                    BOOST_REQUIRE(it->start_bound().equal(*s, bound_view(make_ckey("aaa", "bbb"), bound_kind::incl_start)));
                    BOOST_REQUIRE(it->end_bound().equal(*s, bound_view(make_ckey("aaa", "bbb", "ccc"), bound_kind::excl_end)));
                    BOOST_REQUIRE(it->tomb.timestamp == 1459438519950348L);
                    ++it;
                    BOOST_REQUIRE(it->start_bound().equal(*s, bound_view(make_ckey("aaa", "bbb", "ccc"), bound_kind::excl_start)));
                    BOOST_REQUIRE(it->end_bound().equal(*s, bound_view(make_ckey("aaa", "bbb"), bound_kind::incl_end)));
                    BOOST_REQUIRE(it->tomb.timestamp == 1459438519950348L);
                    ++it;
                    BOOST_REQUIRE(it->start_bound().equal(*s, bound_view(make_ckey("aaa", "bbb"), bound_kind::excl_start)));
                    BOOST_REQUIRE(it->end_bound().equal(*s, bound_view(make_ckey("aaa"), bound_kind::incl_end)));
                    BOOST_REQUIRE(it->tomb.timestamp == 1459438519943668L);
                    ++it;
                    BOOST_REQUIRE(it == rts.end());

                    BOOST_REQUIRE(rows.calculate_size() == 1);
                    for (auto e : rows) {
                        BOOST_REQUIRE(e.key().equal(*s, make_ckey("aaa", "bbb", "ccc")));
                        BOOST_REQUIRE(e.row().deleted_at().tomb().timestamp == 1459438519958850LL);
                    }

                    return stop_iteration::no;
                });
            });
        });
    });
}

SEASTAR_TEST_CASE(test_non_compound_table_row_is_not_marked_as_static) {
    return seastar::async([] {
        auto dir = make_lw_shared<tmpdir>();
        schema_builder builder("ks", "cf");
        builder.with_column("p", utf8_type, column_kind::partition_key);
        builder.with_column("c", int32_type, column_kind::clustering_key);
        builder.with_column("v", int32_type);
        auto s = builder.build(schema_builder::compact_storage::yes);

        auto k = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto ck = clustering_key::from_exploded(*s, {int32_type->decompose(static_cast<int32_t>(0xffff0000))});

        mutation m(k, s);
        auto cell = atomic_cell::make_live(1, int32_type->decompose(17), { });
        m.set_clustered_cell(ck, *s->get_column_definition("v"), std::move(cell));

        auto mt = make_lw_shared<memtable>(s);
        mt->apply(std::move(m));

        auto sst = sstables::make_sstable(s,
                                dir->path,
                                1 /* generation */,
                                sstables::sstable::version_types::ka,
                                sstables::sstable::format_types::big);
        write_memtable_to_sstable(*mt, sst).get();
        sst->load().get();
        auto mr = sst->read_rows_flat(s);
        auto mut = read_mutation_from_flat_mutation_reader(s, mr).get0();
        BOOST_REQUIRE(bool(mut));
    });
}

SEASTAR_TEST_CASE(test_promoted_index_blocks_are_monotonic) {
    return seastar::async([] {
        auto dir = make_lw_shared<tmpdir>();
        schema_builder builder("ks", "cf");
        builder.with_column("p", utf8_type, column_kind::partition_key);
        builder.with_column("c1", int32_type, column_kind::clustering_key);
        builder.with_column("c2", int32_type, column_kind::clustering_key);
        builder.with_column("v", int32_type);
        auto s = builder.build();

        auto k = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto cell = atomic_cell::make_live(1, int32_type->decompose(88), { });
        mutation m(k, s);

        auto ck = clustering_key::from_exploded(*s, {int32_type->decompose(1), int32_type->decompose(2)});
        m.set_clustered_cell(ck, *s->get_column_definition("v"), cell);

        ck = clustering_key::from_exploded(*s, {int32_type->decompose(1), int32_type->decompose(4)});
        m.set_clustered_cell(ck, *s->get_column_definition("v"), cell);

        ck = clustering_key::from_exploded(*s, {int32_type->decompose(1), int32_type->decompose(6)});
        m.set_clustered_cell(ck, *s->get_column_definition("v"), cell);

        ck = clustering_key::from_exploded(*s, {int32_type->decompose(3), int32_type->decompose(9)});
        m.set_clustered_cell(ck, *s->get_column_definition("v"), cell);

        m.partition().apply_row_tombstone(*s, range_tombstone(
                clustering_key_prefix::from_exploded(*s, {int32_type->decompose(1)}),
                bound_kind::excl_start,
                clustering_key_prefix::from_exploded(*s, {int32_type->decompose(2)}),
                bound_kind::incl_end,
                {1, gc_clock::now()}));

        auto mt = make_lw_shared<memtable>(s);
        mt->apply(std::move(m));

        auto sst = sstables::make_sstable(s,
                                dir->path,
                                1 /* generation */,
                                sstables::sstable::version_types::ka,
                                sstables::sstable::format_types::big);
        sstable_writer_config cfg;
        cfg.promoted_index_block_size = 1;
        sst->write_components(mt->make_reader(s), 1, s, cfg).get();
        sst->load().get();
        assert_that(sst->get_index_reader(default_priority_class())).has_monotonic_positions(*s);
    });
}
