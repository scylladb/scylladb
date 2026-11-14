/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests of the reader of sstables with a Parquet Data file (version `aa`,
// sstables/parquet/sstable_reader.hh), and of the read side of the mapping
// (sstables/parquet/mapping_reader.hh).
//
// The mutation source conformance suite, and the other tests that loop over
// the sstable versions, cover the reader's contract. The tests here cover
// what they do not:
//
// - Random schemas and mutations, written with row defaults picked at
//   random, read back and compared with the input and with an `mt` sstable
//   written from the same mutations (PLAN.md, "Testing").
// - Damage to the Data file, which the reader reports as a malformed sstable.
// - Rows that break the decoding rules of mapping.hh, written leaf by leaf
//   with the codec writer, which the row decoder reports as corruption.

#include <fstream>

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/closeable.hh>

#include "readers/from_mutations.hh"
#include "schema/schema_builder.hh"
#include "sstables/file_writer.hh"
#include "sstables/parquet/file_summary.hh"
#include "sstables/parquet/mapping_reader.hh"
#include "sstables/parquet/reader.hh"
#include "sstables/parquet/writer.hh"
#include "sstables/serialization_header_builder.hh"
#include "sstables/sstables.hh"
// The serialization header is a Statistics.db component, whose virtual
// write() needs the serialization templates.
#include "sstables/writer.hh"
#include "test/lib/log.hh"
#include "test/lib/memory_file.hh"
#include "test/lib/mutation_reader_assertions.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/random_schema.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/sstable_test_env.hh"
#include "test/lib/sstable_utils.hh"
#include "types/set.hh"
#include "utils/memory_data_sink.hh"

using namespace sstables;

namespace {

constexpr auto aa = sstable_version_types::aa;
constexpr auto mt = sstable_version_types::mt;

using tweak_config = std::function<void (sstable_writer_config&)>;

// The writer tunables that the tests vary: the defaults, and page sets and
// row groups so small that partitions have many index blocks and the file
// has many row groups and late columns.
const std::vector<std::pair<const char*, tweak_config>> configs = {
    {"default", [] (sstable_writer_config&) {}},
    {"tiny", [] (sstable_writer_config& cfg) {
        cfg.parquet_page_set_size = 1;
        cfg.parquet_min_page_size = 1;
        cfg.parquet_row_group_size = 1;
    }},
    {"small", [] (sstable_writer_config& cfg) {
        cfg.parquet_page_set_size = 512;
        cfg.parquet_min_page_size = 128;
        cfg.parquet_row_group_size = 4096;
    }},
};

shared_sstable write(test_env& env, schema_ptr s, const utils::chunked_vector<mutation>& muts, sstable_version_types v, tweak_config tweak = {}) {
    auto sst = env.make_sstable(s, v);
    auto cfg = env.manager().configure_writer("test");
    if (tweak) {
        tweak(cfg);
    }
    sst->write_components(make_mutation_reader_from_mutations(s, env.make_reader_permit(), muts), muts.size(), s, cfg, encoding_stats{}).get();
    sst->load(s->get_sharder()).get();
    return sst;
}

// Writes `muts` as `aa` with the row defaults picked at random, and checks
// what the readers produce against the input and against `mt`.
void check_round_trip(test_env& env, schema_ptr s, const utils::chunked_vector<mutation>& muts, uint64_t seed) {
    auto ref = write(env, s, muts, mt);
    for (const auto& [name, tweak] : configs) {
        testlog.info("config: {}, row defaults seed: {}", name, seed);
        auto sst = write(env, s, muts, aa, [&, &tweak = tweak] (sstable_writer_config& cfg) {
            tweak(cfg);
            cfg.parquet_random_row_defaults_seed = seed;
        });

        {
            auto rd = assert_that(sst->make_reader(s, env.make_reader_permit(), query::full_partition_range, s->full_slice()));
            for (const auto& m : muts) {
                rd.produces(m);
            }
            rd.produces_end_of_stream();
        }
        {
            auto rd = assert_that(sst->make_full_scan_reader(s, env.make_reader_permit()));
            for (const auto& m : muts) {
                rd.produces(m);
            }
            rd.produces_end_of_stream();
        }

        compare_readers(*s,
                ref->make_reader(s, env.make_reader_permit(), query::full_partition_range, s->full_slice()),
                sst->make_reader(s, env.make_reader_permit(), query::full_partition_range, s->full_slice()),
                true);

        // Single-partition reads, which go through the index.
        for (const auto& m : muts) {
            const auto pr = dht::partition_range::make_singular(m.decorated_key());
            compare_readers(*s,
                    ref->make_reader(s, env.make_reader_permit(), pr, s->full_slice()),
                    sst->make_reader(s, env.make_reader_permit(), pr, s->full_slice()),
                    true);
        }
    }
}

// Expires a third of the cells. The expiry times fit the 32 bits of the
// `mx` format, so that the `mt` sstable keeps them.
tests::expiry_generator random_expiry_generator() {
    return [] (std::mt19937& engine, tests::timestamp_destination) -> std::optional<tests::expiry_info> {
        if (tests::random::get_int<int>(0, 2, engine)) {
            return std::nullopt;
        }
        return tests::expiry_info{
            .ttl = gc_clock::duration(tests::random::get_int<int32_t>(1, 1'000'000, engine)),
            .expiry_point = gc_clock::time_point(gc_clock::duration(tests::random::get_int<int64_t>(1, std::numeric_limits<int32_t>::max() - 1, engine))),
        };
    };
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_random_schemas_round_trip) {
    test_env::do_with_async([] (test_env& env) {
        for (int i = 0; i < 4; ++i) {
            const auto seed = tests::random::get_int<uint32_t>();
            testlog.info("seed: {}", seed);
            auto spec = tests::make_random_schema_specification(fmt::format("ks{}", i));
            auto random_schema = tests::random_schema{seed, *spec};
            testlog.debug("schema:\n{}", random_schema.cql());
            const auto all = tests::generate_random_mutations(seed, random_schema, tests::default_timestamp_generator(),
                    random_expiry_generator(), std::uniform_int_distribution<size_t>(1, 8),
                    std::uniform_int_distribution<size_t>(0, 32), std::uniform_int_distribution<size_t>(0, 8)).get();
            // The writer refuses an sstable whose partitions all belong to
            // another shard, so only the partitions of this shard are kept.
            const auto s = random_schema.schema();
            utils::chunked_vector<mutation> muts;
            for (const auto& m : all) {
                if (s->get_sharder().shard_for_reads(m.token()) == this_shard_id()) {
                    muts.push_back(m);
                }
            }
            if (muts.empty()) {
                testlog.info("no partition of this shard");
                continue;
            }
            check_round_trip(env, s, muts, seed);
        }
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_random_mutations_round_trip) {
    test_env::do_with_async([] (test_env& env) {
        for (auto counters : {random_mutation_generator::generate_counters::no, random_mutation_generator::generate_counters::yes}) {
            const auto seed = tests::random::get_int<uint32_t>();
            testlog.info("counters: {}, seed: {}", bool(counters), seed);
            random_mutation_generator gen(counters, local_shard_only::yes, random_mutation_generator::generate_uncompactable::no, seed);
            check_round_trip(env, gen.schema(), gen(6), seed);
        }
    }).get();
}

namespace {

// Writes an sstable with several row groups, flips one byte of its Data
// file, and checks that a full read raises the malformed sstable error.
// `offset` picks the byte from the summary of the file.
void check_damage_is_detected(std::function<uint64_t (const parquet::file_summary&)> offset) {
    test_env::do_with_async([&] (test_env& env) {
        simple_schema ss(simple_schema::with_static::yes);
        auto s = ss.schema();
        utils::chunked_vector<mutation> muts;
        for (const auto& pk : ss.make_pkeys(4)) {
            mutation m(s, pk);
            ss.add_static_row(m, "static");
            for (int i = 0; i < 200; ++i) {
                ss.add_row(m, ss.make_ckey(i), fmt::format("value {} {}", i, tests::random::get_sstring(16)));
            }
            muts.push_back(std::move(m));
        }
        auto sst = write(env, s, muts, aa, [] (sstable_writer_config& cfg) {
            cfg.parquet_page_set_size = 1024;
            cfg.parquet_min_page_size = 256;
            cfg.parquet_row_group_size = 4096;
        });
        BOOST_REQUIRE_GT(sst->get_parquet_file_summary().row_groups(), 1);
        const uint64_t at = offset(sst->get_parquet_file_summary());
        testlog.info("flipping the byte at offset {}", at);

        {
            std::fstream f(sstables::test(sst).filename(component_type::Data).native(), std::ios::in | std::ios::out | std::ios::binary);
            BOOST_REQUIRE(f);
            f.seekg(at);
            char c;
            f.read(&c, 1);
            c ^= 0x5a;
            f.seekp(at);
            f.write(&c, 1);
            BOOST_REQUIRE(f);
        }

        // Opening the sstable may read the first and the last rows, so the
        // error may come from the open already. A fresh sstable object has
        // empty caches.
        auto read_all = [&] (bool full_scan) {
            auto damaged = env.reusable_sst(s, sst).get();
            auto rd = full_scan
                    ? damaged->make_full_scan_reader(s, env.make_reader_permit())
                    : damaged->make_reader(s, env.make_reader_permit(), query::full_partition_range, s->full_slice());
            auto close = deferred_close(rd);
            while (rd().get()) {}
        };
        BOOST_REQUIRE_THROW(read_all(true), malformed_sstable_exception);
        BOOST_REQUIRE_THROW(read_all(false), malformed_sstable_exception);
    }).get();
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_damaged_page) {
    // Bytes spread over the chunks of the first and the last row group,
    // which hit page headers and page bodies.
    for (int i = 1; i < 8; ++i) {
        check_damage_is_detected([i] (const parquet::file_summary& summary) {
            return summary.region_start(0) + summary.region_size(0) * i / 8;
        });
        check_damage_is_detected([i] (const parquet::file_summary& summary) {
            const uint32_t g = summary.row_groups() - 1;
            return summary.region_start(g) + summary.region_size(g) * i / 8;
        });
    }
}

SEASTAR_THREAD_TEST_CASE(test_damaged_offset_index) {
    for (int i = 0; i < 8; ++i) {
        check_damage_is_detected([i] (const parquet::file_summary& summary) {
            const auto& g = summary.row_group(0);
            return g.offset_index_offset + g.offset_index_length * i / 8;
        });
    }
}

namespace {

uint64_t count_rows(test_env& env, shared_sstable sst) {
    auto s = sst->get_schema();
    auto rd = sst->make_reader(s, env.make_reader_permit(), query::full_partition_range, s->full_slice());
    auto close = deferred_close(rd);
    uint64_t rows = 0;
    while (auto mf = rd().get()) {
        rows += mf->is_clustering_row();
    }
    return rows;
}

} // anonymous namespace

// The footer summary is read once, and a load from foreign_sstable_open_info
// reuses it: on the same shard, as resharding does, and on another shard.
SEASTAR_THREAD_TEST_CASE(test_load_from_open_info) {
    test_env::do_with_async([] (test_env& env) {
        simple_schema ss;
        auto s = ss.schema();
        utils::chunked_vector<mutation> muts;
        for (const auto& pk : ss.make_pkeys(4)) {
            mutation m(s, pk);
            for (int i = 0; i < 10; ++i) {
                ss.add_row(m, ss.make_ckey(i), "value");
            }
            muts.push_back(std::move(m));
        }
        auto src = write(env, s, muts, aa);
        const auto* summary = &src->get_parquet_file_summary();

        auto dst = env.make_sstable(s, src->generation(), aa);
        dst->load(src->get_open_info().get()).get();
        BOOST_REQUIRE_EQUAL(&dst->get_parquet_file_summary(), summary);
        BOOST_REQUIRE_EQUAL(count_rows(env, dst), 40);

        if (smp::count < 2) {
            testlog.warn("The load on another shard is not tested, because there is only one shard");
            return;
        }
        const sstring dir = env.tempdir().path().native();
        smp::submit_to(1, [dir, info = src->get_open_info().get(), generation = src->generation(), summary] () mutable {
            return test_env::do_with_async([&] (test_env& env) {
                simple_schema ss;
                auto sst = env.make_sstable(ss.schema(), dir, generation, aa);
                sst->load(std::move(info)).get();
                BOOST_REQUIRE_EQUAL(&sst->get_parquet_file_summary(), summary);
                BOOST_REQUIRE_EQUAL(count_rows(env, sst), 40);
            });
        }).get();
    }).get();
}

namespace {

namespace pq = sstables::parquet;

// A file with one row, written leaf by leaf with the codec writer, so that
// the row can break the rules that the mapping writer follows. The row
// starts a partition. Then the row decoder decodes it as the sstable reader
// does.
class raw_row {
    schema_ptr _s;
    serialization_header _header;
    pq::table_mapping _mapping;
    memory_data_sink_buffers _bufs;
    file_writer _fw;
    pq::writer _w;
public:
    raw_row()
        : _s(schema_builder(this_smp_shard_count(), "ks", "raw")
                .with_column("pk", int32_type, column_kind::partition_key)
                .with_column("c1", int32_type, column_kind::clustering_key)
                .with_column("c2", int32_type, column_kind::clustering_key)
                .with_column("s", int32_type, column_kind::static_column)
                .with_column("v", int32_type)
                .with_column("st", set_type_impl::get_instance(int32_type, true))
                .build())
        , _header(make_serialization_header(*_s, encoding_stats{}))
        , _mapping(pq::table_mapping::from_header(_header, pq::key_parquet_names(*_s)))
        , _fw(data_sink(std::make_unique<memory_data_sink>(_bufs)))
        , _w(_fw, _mapping.get_schema(), pq::writer_config{})
    {
        _w.append(_mapping.partition_key()[0].value, int32_t(1));
    }

    const pq::table_mapping& mapping() const { return _mapping; }
    const pq::row_columns& row() const { return _mapping.row(); }
    const pq::cell_columns& cell(std::string_view name) const {
        for (const auto* columns : {&_mapping.static_columns(), &_mapping.regular_columns()}) {
            for (const auto& c : *columns) {
                if (c.parquet_name == name) {
                    return c;
                }
            }
        }
        throw std::invalid_argument(fmt::format("no column {}", name));
    }
    uint16_t max_def(pq::column_id c) const { return _mapping.get_schema().get_column(c).max_def; }

    template <typename T>
    void set(pq::column_id c, T v) {
        _w.append(c, v);
    }
    template <typename T>
    void set_element(pq::column_id c, uint16_t rep, T v) {
        _w.append(c, pq::levels{.rep = rep, .def = max_def(c)}, v);
    }
    void row_kind(pq::fragment_kind k) {
        set(row().kind, int32_t(k));
    }
    void clustering_key() {
        set(_mapping.clustering_key()[0].value, int32_t(1));
        set(_mapping.clustering_key()[1].value, int32_t(2));
    }

    // Ends the row and the file, and decodes the row.
    void decode() {
        _w.end_row();
        _w.finish(pq::file_key_value_metadata());
        _fw.close();
        std::string data;
        for (const auto& b : _bufs.buffers()) {
            data.append(b.get(), b.size());
        }
        const uint64_t size = data.size();
        auto [f, impl] = tests::make_memory_file(std::move(data));
        const auto summary = pq::read_file_summary(f, size, std::nullopt).get();
        const auto opened = pq::open_mapping(_header, summary);
        column_translation ct;
        const pq::read_plan plan(opened, summary, *_s, ct.get_for_schema(*_s, _header, sstable_enabled_features{}));
        logalloc::region region;
        ::lru lru_list;
        pq::metadata_cache_stats cache_stats;
        pq::metadata_cache cache(region, lru_list, cache_stats);
        pq::reader r(f, summary, cache, plan.projection());
        auto close = deferred_close(r);
        pq::row_decoder d(*_s, plan, r, pq::read_direction::forward);
        d.position(0).get();
        BOOST_REQUIRE(d.starts_partition());
        d.decode_partition_key();
        d.decode_partition_tombstone();
        switch (d.kind()) {
        case pq::fragment_kind::none:
            d.check_none_row();
            break;
        case pq::fragment_kind::static_row:
            d.decode_static_row();
            break;
        case pq::fragment_kind::clustering_row:
        case pq::fragment_kind::clustering_row_with_marker: {
            clustering_row cr(d.decode_clustering_key());
            d.decode_clustering_row(cr);
            break;
        }
        default:
            d.decode_bound();
            break;
        }
    }
};

void check_corrupt(std::string_view what, std::function<void (raw_row&)> write) {
    testlog.info("{}", what);
    raw_row r;
    write(r);
    BOOST_REQUIRE_EXCEPTION(r.decode(), malformed_sstable_exception, [] (const malformed_sstable_exception& e) {
        testlog.info("error: {}", e.what());
        return true;
    });
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_valid_raw_rows) {
    // The harness decodes rows that follow the rules.
    raw_row none;
    none.row_kind(pq::fragment_kind::none);
    none.decode();

    raw_row cr;
    cr.row_kind(pq::fragment_kind::clustering_row);
    cr.set(cr.row().ts, int64_t(10));
    cr.clustering_key();
    cr.set(cr.cell("v").value, int32_t(5));
    cr.decode();

    raw_row bound;
    bound.row_kind(pq::fragment_kind::incl_start);
    bound.set(bound.row().ts, int64_t(10));
    bound.set(bound.row().tomb_deletion_time, int64_t(100));
    bound.decode();
}

SEASTAR_THREAD_TEST_CASE(test_corrupt_rows) {
    using k = pq::fragment_kind;
    check_corrupt("a row without a kind", [] (raw_row& r) {
        r.set(r.row().ts, int64_t(10));
    });
    check_corrupt("a kind out of range", [] (raw_row& r) {
        r.set(r.row().kind, int32_t(8));
    });
    check_corrupt("a clustering row without a timestamp", [] (raw_row& r) {
        r.row_kind(k::clustering_row);
        r.clustering_key();
    });
    check_corrupt("a row of kind none with a timestamp", [] (raw_row& r) {
        r.row_kind(k::none);
        r.set(r.row().ts, int64_t(10));
    });
    check_corrupt("a row of kind none with a cell", [] (raw_row& r) {
        r.row_kind(k::none);
        r.set(r.cell("v").value, int32_t(5));
    });
    check_corrupt("a TTL without an expiry", [] (raw_row& r) {
        r.row_kind(k::clustering_row);
        r.set(r.row().ts, int64_t(10));
        r.clustering_key();
        r.set(r.row().ttl, int32_t(10));
    });
    check_corrupt("a TTL of zero", [] (raw_row& r) {
        r.row_kind(k::clustering_row);
        r.set(r.row().ts, int64_t(10));
        r.clustering_key();
        r.set(r.row().ttl, int32_t(0));
        r.set(r.row().expiry, int64_t(1000));
    });
    check_corrupt("a row tombstone timestamp without a deletion time", [] (raw_row& r) {
        r.row_kind(k::clustering_row);
        r.set(r.row().ts, int64_t(10));
        r.clustering_key();
        r.set(r.row().tomb_ts, int64_t(1));
    });
    check_corrupt("a partition tombstone without a deletion time", [] (raw_row& r) {
        r.row_kind(k::none);
        r.set(r.row().partition_tomb_ts, int64_t(1));
    });
    check_corrupt("a marker deletion time without a marker", [] (raw_row& r) {
        r.row_kind(k::clustering_row);
        r.set(r.row().ts, int64_t(10));
        r.clustering_key();
        r.set(r.row().marker_deletion_time, int64_t(100));
    });
    check_corrupt("a clustering key component after a null one", [] (raw_row& r) {
        r.row_kind(k::incl_start);
        r.set(r.row().ts, int64_t(10));
        r.set(r.row().tomb_deletion_time, int64_t(100));
        r.set(r.mapping().clustering_key()[1].value, int32_t(2));
    });
    check_corrupt("a clustering row with a partial key", [] (raw_row& r) {
        r.row_kind(k::clustering_row);
        r.set(r.row().ts, int64_t(10));
        r.set(r.mapping().clustering_key()[0].value, int32_t(1));
    });
    check_corrupt("a static row with a clustering key", [] (raw_row& r) {
        r.row_kind(k::static_row);
        r.set(r.row().ts, int64_t(10));
        r.clustering_key();
        r.set(r.cell("s").value, int32_t(5));
    });
    check_corrupt("a static row with a regular cell", [] (raw_row& r) {
        r.row_kind(k::static_row);
        r.set(r.row().ts, int64_t(10));
        r.set(r.cell("v").value, int32_t(5));
    });
    check_corrupt("a range tombstone bound without a deletion time", [] (raw_row& r) {
        r.row_kind(k::excl_end);
        r.set(r.row().ts, int64_t(10));
    });
    check_corrupt("a range tombstone bound with a cell", [] (raw_row& r) {
        r.row_kind(k::incl_start);
        r.set(r.row().ts, int64_t(10));
        r.set(r.row().tomb_deletion_time, int64_t(100));
        r.set(r.cell("v").value, int32_t(5));
    });
    check_corrupt("a dead cell with a value", [] (raw_row& r) {
        r.row_kind(k::clustering_row);
        r.set(r.row().ts, int64_t(10));
        r.clustering_key();
        r.set(r.cell("v").value, int32_t(5));
        r.set(r.cell("v").meta.deletion_time, int64_t(100));
    });
    check_corrupt("cell metadata without a cell", [] (raw_row& r) {
        r.row_kind(k::clustering_row);
        r.set(r.row().ts, int64_t(10));
        r.clustering_key();
        r.set(r.cell("v").meta.ts, int64_t(1));
    });
    check_corrupt("an expiry on a cell that does not expire", [] (raw_row& r) {
        r.row_kind(k::clustering_row);
        r.set(r.row().ts, int64_t(10));
        r.clustering_key();
        r.set(r.cell("v").value, int32_t(5));
        r.set(*r.cell("v").meta.expiry, int64_t(1000));
    });
    check_corrupt("an expiring cell without an expiry", [] (raw_row& r) {
        r.row_kind(k::clustering_row);
        r.set(r.row().ts, int64_t(10));
        r.clustering_key();
        r.set(r.cell("v").value, int32_t(5));
        r.set(*r.cell("v").meta.ttl, int32_t(10));
    });
    check_corrupt("a metadata list longer than the elements", [] (raw_row& r) {
        r.row_kind(k::clustering_row);
        r.set(r.row().ts, int64_t(10));
        r.clustering_key();
        const auto& st = r.cell("st");
        r.set_element(st.value, 0, int32_t(1));
        r.set_element(st.meta.ts, 0, int64_t(1));
        r.set_element(st.meta.ts, 1, int64_t(2));
    });
    check_corrupt("a collection tombstone timestamp without a deletion time", [] (raw_row& r) {
        r.row_kind(k::clustering_row);
        r.set(r.row().ts, int64_t(10));
        r.clustering_key();
        r.set(*r.cell("st").tomb_ts, int64_t(1));
    });
}
