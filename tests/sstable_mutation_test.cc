/*
 * Copyright 2015 Cloudius Systems
 */

#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include "tests/test-utils.hh"
#include "sstable_test.hh"
#include "sstables/key.hh"
#include "core/do_with.hh"
#include "core/thread.hh"
#include "database.hh"
#include "timestamp.hh"
#include "schema_builder.hh"
#include "mutation_reader.hh"
#include "mutation_reader_assertions.hh"
#include "mutation_source_test.hh"
#include "tmpdir.hh"

using namespace sstables;

SEASTAR_TEST_CASE(nonexistent_key) {
    return reusable_sst("tests/sstables/uncompressed", 1).then([] (auto sstp) {
        return do_with(key::from_bytes(to_bytes("invalid_key")), [sstp] (auto& key) {
            auto s = uncompressed_schema();
            return sstp->read_row(s, key).then([sstp, s, &key] (auto mutation) {
                BOOST_REQUIRE(!mutation);
                return make_ready_future<>();
            });
        });
    });
}

future<> test_no_clustered(bytes&& key, std::unordered_map<bytes, boost::any> &&map) {
    return reusable_sst("tests/sstables/uncompressed", 1).then([k = std::move(key), map = std::move(map)] (auto sstp) mutable {
        return do_with(sstables::key(std::move(k)), [sstp, map = std::move(map)] (auto& key) {
            auto s = uncompressed_schema();
            return sstp->read_row(s, key).then([sstp, s, &key, map = std::move(map)] (auto mutation) {
                BOOST_REQUIRE(mutation);
                auto& mp = mutation->partition();
                for (auto&& e : mp.range(*s, query::range<clustering_key_prefix>())) {
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
    return test_no_clustered("vinna", {{ "col1", boost::any(to_sstring("daughter")) }, { "col2", boost::any(3) }});
}

SEASTAR_TEST_CASE(uncompressed_2) {
    return test_no_clustered("gustaf", {{ "col1", boost::any(to_sstring("son")) }, { "col2", boost::any(0) }});
}

SEASTAR_TEST_CASE(uncompressed_3) {
    return test_no_clustered("isak", {{ "col1", boost::any(to_sstring("son")) }, { "col2", boost::any(1) }});
}

SEASTAR_TEST_CASE(uncompressed_4) {
    return test_no_clustered("finna", {{ "col1", boost::any(to_sstring("daughter")) }, { "col2", boost::any(2) }});
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
    return reusable_sst("tests/sstables/complex", Generation).then([k = std::move(key)] (auto sstp) mutable {
        return do_with(sstables::key(std::move(k)), [sstp] (auto& key) {
            auto s = complex_schema();
            return sstp->read_row(s, key).then([sstp, s, &key] (auto mutation) {
                BOOST_REQUIRE(mutation);
                return std::move(*mutation);
            });
        });
    });
}

inline auto clustered_row(mutation& mutation, const schema& s, std::vector<bytes>&& v) {
    auto exploded = exploded_clustering_prefix(std::move(v));
    auto clustering_pair = clustering_key::from_clustering_prefix(s, exploded);
    return mutation.partition().clustered_row(clustering_pair);
}

SEASTAR_TEST_CASE(complex_sst1_k1) {
    return generate_clustered<1>("key1").then([] (auto&& mutation) {
        auto s = complex_schema();

        auto sr = mutation.partition().static_row();
        match_live_cell(sr, *s, "static_obj", boost::any(to_bytes("static_value")));

        auto row1 = clustered_row(mutation, *s, {"cl1.1", "cl2.1"});
        match_live_cell(row1.cells(), *s, "reg", boost::any(to_bytes("v1")));
        match_absent(row1.cells(), *s, "reg_list");
        match_absent(row1.cells(), *s, "reg_map");
        match_absent(row1.cells(), *s, "reg_fset");
        auto reg_set = match_collection(row1.cells(), *s, "reg_set", tombstone(deletion_time{1431451390, 1431451390209521l}));
        match_collection_element<status::live>(reg_set.cells[0], to_bytes("1"), bytes_opt{});
        match_collection_element<status::live>(reg_set.cells[1], to_bytes("2"), bytes_opt{});

        auto row2 = clustered_row(mutation, *s, {"cl1.2", "cl2.2"});
        match_live_cell(row2.cells(), *s, "reg", boost::any(to_bytes("v2")));
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
        match_live_cell(sr, *s, "static_obj", boost::any(to_bytes("static_value")));
        auto static_set = match_collection(sr, *s, "static_collection", tombstone(deletion_time{1431451390, 1431451390225257l}));
        match_collection_element<status::live>(static_set.cells[0], to_bytes("1"), bytes_opt{});
        match_collection_element<status::live>(static_set.cells[1], to_bytes("2"), bytes_opt{});
        match_collection_element<status::live>(static_set.cells[2], to_bytes("3"), bytes_opt{});
        match_collection_element<status::live>(static_set.cells[3], to_bytes("4"), bytes_opt{});

        auto row1 = clustered_row(mutation, *s, {"kcl1.1", "kcl2.1"});
        match_live_cell(row1.cells(), *s, "reg", boost::any(to_bytes("v3")));
        match_absent(row1.cells(), *s, "reg_list");
        match_absent(row1.cells(), *s, "reg_set");
        match_absent(row1.cells(), *s, "reg_fset");
        auto reg_map = match_collection(row1.cells(), *s, "reg_map", tombstone(deletion_time{1431451390, 1431451390217436l}));
        match_collection_element<status::live>(reg_map.cells[0], to_bytes("3"), to_bytes("1"));
        match_collection_element<status::live>(reg_map.cells[1], to_bytes("4"), to_bytes("2"));

        auto row2 = clustered_row(mutation, *s, {"kcl1.2", "kcl2.2"});
        match_live_cell(row2.cells(), *s, "reg", boost::any(to_bytes("v4")));
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
        match_expiring_cell(sr, *s, "static_obj", boost::any(to_bytes("static_value_3")), 1431451394597062l, 1431537794);

        auto row1 = clustered_row(mutation, *s, {"tcl1.1", "tcl2.1"});
        BOOST_REQUIRE(row1.created_at() == 1431451394597062l);
        match_expiring_cell(row1.cells(), *s, "reg", boost::any(to_bytes("v5")), 1431451394597062l, 1431537794);
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
        match_live_cell(sr, *s, "static_obj", boost::any(to_bytes("final_static")));

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
    return reusable_sst("tests/sstables/uncompressed", 1).then([min, max, &expected] (auto sstp) mutable {
        auto s = uncompressed_schema();
        auto count = make_lw_shared<size_t>(0);
        auto expected_size = expected.size();
        auto mutations = sstp->read_range_rows(s, min, max);
        auto stop = make_lw_shared<bool>(false);
        return do_until([stop] { return *stop; },
                // Note: The data in the following lambda, including
                // "mutations", continues to live until after the last
                // iteration's future completes, so its lifetime is safe.
                [sstp, mutations = std::move(mutations), &expected, expected_size, count, stop] () mutable {
                    return mutations.read().then([&expected, expected_size, count, stop] (mutation_opt mutation) mutable {
                        if (mutation) {
                            BOOST_REQUIRE(*count < expected_size);
                            BOOST_REQUIRE(bytes_view(expected.back()) == bytes_view(mutation->key()));
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

::mutation_source as_mutation_source(schema_ptr s, lw_shared_ptr<sstables::sstable> sst) {
    return [s, sst] (const query::partition_range& range) mutable {
        return as_mutation_reader(sst, sst->read_range_rows(s, range));
    };
}

SEASTAR_TEST_CASE(test_sstable_conforms_to_mutation_source) {
    return seastar::async([] {
        std::vector<tmpdir> dirs;

        run_mutation_source_tests([&dirs] (schema_ptr s, const std::vector<mutation>& partitions) -> mutation_source {
            tmpdir sstable_dir;
            auto sst = make_lw_shared<sstables::sstable>("ks", "cf",
                sstable_dir.path,
                1 /* generation */,
                sstables::sstable::version_types::la,
                sstables::sstable::format_types::big);
            dirs.emplace_back(std::move(sstable_dir));

            auto mt = make_lw_shared<memtable>(s);

            for (auto&& m : partitions) {
                mt->apply(m);
            }

            sst->write_components(*mt).get();
            sst->load().get();

            return as_mutation_source(s, sst);
        });
    });
}

SEASTAR_TEST_CASE(compact_storage_sparse_read) {
    return reusable_sst("tests/sstables/compact_sparse", 1).then([] (auto sstp) {
        return do_with(sstables::key("first_row"), [sstp] (auto& key) {
            auto s = compact_sparse_schema();
            return sstp->read_row(s, key).then([sstp, s, &key] (auto mutation) {
                auto& mp = mutation->partition();
                auto row = mp.clustered_row(clustering_key::make_empty(*s));
                match_live_cell(row.cells(), *s, "cl1", boost::any(to_bytes("cl1")));
                match_live_cell(row.cells(), *s, "cl2", boost::any(to_bytes("cl2")));
                return make_ready_future<>();
            });
        });
    });
}

SEASTAR_TEST_CASE(compact_storage_simple_dense_read) {
    return reusable_sst("tests/sstables/compact_simple_dense", 1).then([] (auto sstp) {
        return do_with(sstables::key("first_row"), [sstp] (auto& key) {
            auto s = compact_simple_dense_schema();
            return sstp->read_row(s, key).then([sstp, s, &key] (auto mutation) {
                auto& mp = mutation->partition();

                auto exploded = exploded_clustering_prefix({"cl1"});
                auto clustering = clustering_key::from_clustering_prefix(*s, exploded);

                auto row = mp.clustered_row(clustering);
                match_live_cell(row.cells(), *s, "cl2", boost::any(to_bytes("cl2")));
                return make_ready_future<>();
            });
        });
    });
}

SEASTAR_TEST_CASE(compact_storage_dense_read) {
    return reusable_sst("tests/sstables/compact_dense", 1).then([] (auto sstp) {
        return do_with(sstables::key("first_row"), [sstp] (auto& key) {
            auto s = compact_dense_schema();
            return sstp->read_row(s, key).then([sstp, s, &key] (auto mutation) {
                auto& mp = mutation->partition();

                auto exploded = exploded_clustering_prefix({"cl1", "cl2"});
                auto clustering = clustering_key::from_clustering_prefix(*s, exploded);

                auto row = mp.clustered_row(clustering);
                match_live_cell(row.cells(), *s, "cl3", boost::any(to_bytes("cl3")));
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
    class sstable_range_wrapping_reader final : public ::mutation_reader::impl {
        sstable_ptr _sst;
        sstables::mutation_reader _smr;
    public:
        sstable_range_wrapping_reader(sstable_ptr sst, schema_ptr s)
                : _sst(sst)
                , _smr(sst->read_rows(std::move(s))) {}
        virtual future<mutation_opt> operator()() override {
            return _smr.read();
        }
    };

    return reusable_sst("tests/sstables/broken_ranges", 2).then([] (auto sstp) {
        auto s = peers_schema();
        auto reader = make_lw_shared<::mutation_reader>(make_mutation_reader<sstable_range_wrapping_reader>(sstp, s));
        return repeat([s, reader] {
            return (*reader)().then([s, reader] (mutation_opt mut) {
                auto key_equal = [s, &mut] (sstring ip) {
                    return mut->key().representation() == partition_key::from_deeply_exploded(*s, { boost::any(net::ipv4_address(ip)) }).representation();
                };

                if (!mut) {
                    return stop_iteration::yes;
                } else if (key_equal("127.0.0.1")) {
                    auto row = mut->partition().clustered_row(clustering_key::make_empty(*s));
                    match_absent(row.cells(), *s, "tokens");
                } else if (key_equal("127.0.0.3")) {
                    auto row = mut->partition().clustered_row(clustering_key::make_empty(*s));
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
