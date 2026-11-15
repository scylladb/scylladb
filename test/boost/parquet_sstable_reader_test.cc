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
//   written from the same mutations.
// - Reverse reads with random clustering slices, of random mutations and of
//   wide partitions with many range tombstones, compared with the in-memory
//   reversing reader over a forward read and with an `mt` sstable.
// - The I/O of a reverse read of a few rows of a wide partition, which is
//   bounded by those rows and not by the partition.
// - Damage to the Data file, which the reader reports as a malformed sstable.
// - Rows that break the decoding rules of mapping.hh, written leaf by leaf
//   with the codec writer, which the row decoder reports as corruption.
// - The size measures that the reader and the sstable report:
//   data_size(), read progress, and the key estimate.

#include <filesystem>
#include <fstream>

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/closeable.hh>

#include "partition_slice_builder.hh"
#include "readers/from_mutations.hh"
#include "readers/reversing.hh"
#include "schema/schema_builder.hh"
#include "sstables/file_writer.hh"
#include "sstables/metadata_collector.hh"
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

// Ranges of positions in the order of `s`, with bounds at random prefixes
// of `keys`, as the mutation source suite makes them.
std::vector<position_range> random_position_ranges(const schema& s, const std::vector<clustering_key_prefix>& keys, std::mt19937& engine) {
    std::vector<position_in_partition> positions;
    if (tests::random::get_bool(engine)) {
        positions.push_back(position_in_partition::before_all_clustered_rows());
    }
    for (const auto& k : keys) {
        const auto exploded = k.explode(s);
        if (exploded.empty()) {
            continue;
        }
        const auto tag = tests::random::get_int<int>(1, 3, engine);
        const auto prefix_size = tag == 2 ? exploded.size() : tests::random::get_int<size_t>(1, exploded.size(), engine);
        auto prefix = clustering_key_prefix::from_exploded(s, std::vector<bytes>(exploded.begin(), exploded.begin() + prefix_size));
        switch (tag) {
        case 1:
            positions.emplace_back(position_in_partition::before_clustering_row_tag_t{}, std::move(prefix));
            break;
        case 2:
            positions.emplace_back(position_in_partition::clustering_row_tag_t{}, std::move(prefix));
            break;
        default:
            positions.emplace_back(position_in_partition_view::after_all_prefixed(prefix));
            break;
        }
    }
    const auto subset_size = tests::random::get_int<size_t>(0, positions.size(), engine);
    positions = tests::random::random_subset<position_in_partition>(std::move(positions), subset_size, engine);
    std::ranges::sort(positions, position_in_partition::less_compare(s));
    positions.erase(std::unique(positions.begin(), positions.end(), position_in_partition::equal_compare(s)), positions.end());
    if (tests::random::get_bool(engine)) {
        positions.push_back(position_in_partition::after_all_clustered_rows());
    }
    std::vector<position_range> ranges;
    for (size_t i = 0; i + 1 < positions.size(); i += 2) {
        ranges.emplace_back(std::move(positions[i]), std::move(positions[i + 1]));
    }
    if (ranges.empty()) {
        ranges.emplace_back(position_in_partition::before_all_clustered_rows(), position_in_partition::after_all_clustered_rows());
    }
    return ranges;
}

// Clustering ranges in the order of the table, of a shape picked at random:
// the full partition, several ranges with bounds at prefixes, single rows,
// or ranges that start and end inside range tombstones.
query::clustering_row_ranges random_clustering_ranges(const schema& s, const utils::chunked_vector<mutation>& muts, std::mt19937& engine) {
    std::vector<clustering_key_prefix> row_keys;
    std::vector<range_tombstone> rts;
    for (const auto& m : muts) {
        for (const auto& r : m.partition().clustered_rows()) {
            row_keys.push_back(r.key());
        }
        for (const auto& rt : m.partition().row_tombstones()) {
            rts.push_back(rt.tombstone());
        }
    }
    const position_in_partition::less_compare less(s);
    std::vector<position_range> ranges;
    switch (tests::random::get_int<int>(0, 3, engine)) {
    case 0:
        break;
    case 1: {
        auto keys = row_keys;
        for (const auto& rt : rts) {
            keys.push_back(rt.start);
            keys.push_back(rt.end);
        }
        ranges = random_position_ranges(s, keys, engine);
        break;
    }
    case 2: {
        auto keys = tests::random::random_subset<clustering_key_prefix>(row_keys, std::min<size_t>(row_keys.size(), 4), engine);
        for (const auto& k : keys) {
            ranges.emplace_back(position_in_partition::before_key(k), position_in_partition(position_in_partition_view::after_all_prefixed(k)));
        }
        break;
    }
    default: {
        auto picked = tests::random::random_subset<range_tombstone>(rts, std::min<size_t>(rts.size(), 3), engine);
        for (const auto& rt : picked) {
            // Strictly inside the tombstone if possible.
            position_in_partition start(position_in_partition_view::after_all_prefixed(rt.start));
            position_in_partition end(rt.end_position());
            if (!less(start, end)) {
                start = position_in_partition(rt.position());
            }
            if (less(start, end)) {
                ranges.emplace_back(std::move(start), std::move(end));
            }
        }
        break;
    }
    }
    std::ranges::sort(ranges, less, [] (const position_range& r) -> position_in_partition_view { return r.start(); });
    query::clustering_row_ranges result;
    std::optional<position_in_partition> last_end;
    for (const auto& r : ranges) {
        if (last_end && less(r.start(), *last_end)) {
            continue;
        }
        if (auto cr = position_range_to_clustering_range(r, s)) {
            result.push_back(std::move(*cr));
            last_end = position_in_partition(r.end());
        }
    }
    if (result.empty()) {
        result.push_back(query::clustering_range::make_open_ended_both_sides());
    }
    return result;
}

std::vector<mutation> read_partitions(mutation_reader& rd) {
    std::vector<mutation> r;
    while (auto m = read_mutation_from_mutation_reader(rd).get()) {
        r.push_back(std::move(*m));
    }
    return r;
}

std::vector<mutation> read_all(mutation_reader rd) {
    auto close = deferred_close(rd);
    return read_partitions(rd);
}

void check_equal(const std::vector<mutation>& actual, const std::vector<mutation>& expected) {
    BOOST_REQUIRE_EQUAL(actual.size(), expected.size());
    for (size_t i = 0; i < actual.size(); ++i) {
        assert_that(actual[i]).is_equal_to(expected[i]);
    }
}

// Reads `sst` in reverse with random clustering slices, and compares the
// result with the in-memory reversing reader over a forward read of `sst`,
// and with a reverse read of `ref`, an `mt` sstable of the same mutations.
// Covers single-partition reads, multi-partition reads, and both kinds of
// fast-forwarding.
void check_reverse_reads(test_env& env, schema_ptr s, const utils::chunked_vector<mutation>& muts, shared_sstable ref, shared_sstable sst,
        std::mt19937& engine) {
    const auto rs = s->make_reversed();
    for (int i = 0; i < 4; ++i) {
        const auto fwd_slice = partition_slice_builder(*s).with_ranges(random_clustering_ranges(*s, muts, engine)).build();
        const auto rev_slice = query::reverse_slice(*s, fwd_slice);
        testlog.debug("reversed slice: {}", rev_slice);
        // The reversing reader may emit redundant range tombstone changes,
        // so it is compared by the mutations that it produces. So is a
        // multi-partition reverse read of `mt`, which goes through it too.
        // A single-partition reverse read of `mt` is native, and the
        // fragments must match exactly.
        auto check = [&] (const dht::partition_range& pr) {
            testlog.debug("partition range: {}", pr);
            auto permit = env.make_reader_permit();
            const auto actual = read_all(sst->make_reader(rs, permit, pr, rev_slice));
            check_equal(actual, read_all(make_reversing_reader(sst->make_reader(s, permit, pr, fwd_slice), permit.max_result_size())));
            if (pr.is_singular()) {
                compare_readers(*rs, ref->make_reader(rs, permit, pr, rev_slice), sst->make_reader(rs, permit, pr, rev_slice), true);
            } else {
                check_equal(actual, read_all(ref->make_reader(rs, permit, pr, rev_slice)));
            }
        };
        check(query::full_partition_range);
        for (const auto& m : muts) {
            check(dht::partition_range::make_singular(m.decorated_key()));
        }

        // fast_forward_to(partition_range): every other partition, one at a
        // time. The `mx` reader does not support it in reverse, so the
        // reference is the reversing reader over a forward read of `mt`.
        {
            // The ranges are not singular, because the `mx` reader of a
            // singular range does not support fast_forward_to() either.
            std::vector<dht::partition_range> ranges;
            for (size_t k = 0; k < muts.size(); k += 2) {
                std::optional<dht::partition_range::bound> end;
                if (k + 1 < muts.size()) {
                    end = dht::partition_range::bound(muts[k + 1].decorated_key(), false);
                }
                ranges.push_back(dht::partition_range(dht::partition_range::bound(muts[k].decorated_key(), true), std::move(end)));
            }
            auto permit = env.make_reader_permit();
            auto expected = make_reversing_reader(ref->make_reader(s, permit, ranges.front(), fwd_slice, nullptr,
                    streamed_mutation::forwarding::no, mutation_reader::forwarding::yes), permit.max_result_size());
            auto close_expected = deferred_close(expected);
            auto actual = sst->make_reader(rs, permit, ranges.front(), rev_slice, nullptr, streamed_mutation::forwarding::no, mutation_reader::forwarding::yes);
            auto close_actual = deferred_close(actual);
            for (size_t k = 0; k < ranges.size(); ++k) {
                if (k) {
                    expected.fast_forward_to(ranges[k]).get();
                    actual.fast_forward_to(ranges[k]).get();
                }
                check_equal(read_partitions(actual), read_partitions(expected));
            }
        }
    }

    // fast_forward_to(position_range), with ranges in the reversed order.
    std::vector<clustering_key_prefix> keys;
    for (const auto& m : muts) {
        for (const auto& r : m.partition().clustered_rows()) {
            keys.push_back(r.key());
        }
    }
    const auto rev_full_slice = query::reverse_slice(*s, s->full_slice());
    for (const auto& m : muts) {
        const auto fwd_ranges = random_position_ranges(*rs, keys, engine);
        const auto pr = dht::partition_range::make_singular(m.decorated_key());
        auto permit = env.make_reader_permit();
        // Forwarding may split range tombstones differently in equivalent
        // streams (see #9472), so the results are compared as mutations.
        const auto expected = forwardable_reader_to_mutation(
                ref->make_reader(rs, permit, pr, rev_full_slice, nullptr, streamed_mutation::forwarding::yes, mutation_reader::forwarding::no),
                fwd_ranges);
        const auto actual = forwardable_reader_to_mutation(
                sst->make_reader(rs, permit, pr, rev_full_slice, nullptr, streamed_mutation::forwarding::yes, mutation_reader::forwarding::no),
                fwd_ranges);
        assert_that(actual).is_equal_to(expected);
    }
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

        std::mt19937 engine(seed);
        check_reverse_reads(env, s, muts, ref, sst, engine);
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

namespace {

// Wide partitions with many overlapping range deletions, which the writer
// turns into many range tombstone boundaries, and with a static row in some.
utils::chunked_vector<mutation> wide_partitions(simple_schema& ss, std::mt19937& engine) {
    auto s = ss.schema();
    utils::chunked_vector<mutation> muts;
    auto pkeys = ss.make_pkeys(3);
    for (size_t p = 0; p < pkeys.size(); ++p) {
        mutation m(s, pkeys[p]);
        if (p != 1) {
            ss.add_static_row(m, "static");
        }
        for (uint32_t i = 0; i < 600; ++i) {
            if (tests::random::get_int<int>(0, 4, engine)) {
                ss.add_row(m, ss.make_ckey(i), fmt::format("value {}", i));
            }
        }
        for (int i = 0; i < 80; ++i) {
            const auto a = tests::random::get_int<uint32_t>(0, 620, engine);
            const auto b = tests::random::get_int<uint32_t>(a, std::min<uint32_t>(a + 40, 620), engine);
            const bool a_incl = tests::random::get_bool(engine);
            const bool b_incl = tests::random::get_bool(engine) || a == b;
            ss.delete_range(m, query::clustering_range(
                    query::clustering_range::bound(ss.make_ckey(a), a_incl || a == b),
                    query::clustering_range::bound(ss.make_ckey(b), b_incl)));
        }
        muts.push_back(std::move(m));
    }
    std::ranges::sort(muts, mutation_decorated_key_less_comparator());
    return muts;
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_reverse_reads_of_wide_partitions) {
    test_env::do_with_async([] (test_env& env) {
        const auto seed = tests::random::get_int<uint32_t>();
        testlog.info("seed: {}", seed);
        std::mt19937 engine(seed);
        simple_schema ss;
        const auto muts = wide_partitions(ss, engine);
        auto s = ss.schema();
        auto ref = write(env, s, muts, mt);
        for (const auto& [name, tweak] : configs) {
            testlog.info("config: {}", name);
            auto sst = write(env, s, muts, aa, tweak);
            check_reverse_reads(env, s, muts, ref, sst, engine);
        }
    }).get();
}

namespace {

// Counts the reads from a file and their bytes.
class counting_file_impl : public file_impl {
    file _file;
public:
    uint64_t reads = 0;
    uint64_t bytes = 0;
    explicit counting_file_impl(file f) : _file(std::move(f)) {}
    future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, io_intent* intent) override {
        return get_file_impl(_file)->write_dma(pos, buffer, len, intent);
    }
    future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, io_intent* intent) override {
        return get_file_impl(_file)->write_dma(pos, std::move(iov), intent);
    }
    future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, io_intent* intent) override {
        ++reads;
        bytes += len;
        return get_file_impl(_file)->read_dma(pos, buffer, len, intent);
    }
    future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, io_intent* intent) override {
        ++reads;
        for (const auto& v : iov) {
            bytes += v.iov_len;
        }
        return get_file_impl(_file)->read_dma(pos, std::move(iov), intent);
    }
    future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, io_intent* intent) override {
        ++reads;
        bytes += range_size;
        return get_file_impl(_file)->dma_read_bulk(offset, range_size, intent);
    }
    future<> flush() override { return get_file_impl(_file)->flush(); }
    future<struct stat> stat() override { return get_file_impl(_file)->stat(); }
    future<> truncate(uint64_t length) override { return get_file_impl(_file)->truncate(length); }
    future<> discard(uint64_t offset, uint64_t length) override { return get_file_impl(_file)->discard(offset, length); }
    future<> allocate(uint64_t position, uint64_t length) override { return get_file_impl(_file)->allocate(position, length); }
    future<uint64_t> size() override { return get_file_impl(_file)->size(); }
    future<> close() override { return get_file_impl(_file)->close(); }
    std::unique_ptr<seastar::file_handle_impl> dup() override { return get_file_impl(_file)->dup(); }
    subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override {
        return get_file_impl(_file)->list_directory(std::move(next));
    }
};

// Checks that read progress never goes back, and records where it ended.
class progress_checker : public read_monitor {
    const reader_position_tracker* _tracker = nullptr;
    uint64_t _last = 0;
public:
    std::optional<uint64_t> final_position;
    uint64_t total = 0;
    void on_read_started(const reader_position_tracker& t) override {
        _tracker = &t;
        _last = t.position;
        total = t.total_read_size;
    }
    void on_read_completed() override {
        check();
        final_position = _tracker->position;
        _tracker = nullptr;
    }
    void check() {
        if (_tracker) {
            BOOST_REQUIRE_GE(_tracker->position, _last);
            _last = _tracker->position;
        }
    }
};

} // anonymous namespace

// The trie index stores a deletion time in 32 bits, capped at
// max_deletion_time, while the Data file stores it uncapped. For tombstones
// past 2038, reads that skip with the index see the same tombstones as a
// sequential read of the Data file, in both directions.
SEASTAR_THREAD_TEST_CASE(test_deletion_times_beyond_the_index_cap) {
    test_env::do_with_async([] (test_env& env) {
        simple_schema ss;
        auto s = ss.schema();
        const auto rs = s->make_reversed();
        const auto far = gc_clock::time_point(gc_clock::duration(int64_t(1) << 33));
        BOOST_REQUIRE_GT(far.time_since_epoch().count(), sstables::max_deletion_time);
        auto pk = ss.make_pkey();
        mutation m(s, pk);
        m.partition().apply(tombstone(api::new_timestamp(), far));
        const uint32_t rows = 3000;
        for (uint32_t i = 0; i < rows; ++i) {
            ss.add_row(m, ss.make_ckey(i), fmt::format("value {}", i));
        }
        // Range tombstones that span many index blocks, newer than the rows.
        std::vector<uint32_t> starts;
        for (uint32_t a = 100; a + 500 < rows; a += 700) {
            starts.push_back(a);
            ss.delete_range(m, query::clustering_range::make({ss.make_ckey(a), true}, {ss.make_ckey(a + 500), false}),
                    tombstone(api::new_timestamp(), far + std::chrono::seconds(a)));
        }
        auto sst = write(env, s, {m}, aa, [] (sstable_writer_config& cfg) {
            cfg.parquet_page_set_size = 1024;
            cfg.parquet_min_page_size = 256;
            cfg.parquet_row_group_size = 16 * 1024;
        });
        auto permit = env.make_reader_permit();
        // A full scan reads the Data file in order, without the index.
        check_equal(read_all(sst->make_full_scan_reader(s, permit)), {m});

        const auto pr = dht::partition_range::make_singular(pk);
        auto check = [&] (std::vector<query::clustering_range> ranges) {
            const auto fwd_slice = partition_slice_builder(*s).with_ranges(std::move(ranges)).build();
            const auto rev_slice = query::reverse_slice(*s, fwd_slice);
            testlog.debug("slice: {}", fwd_slice);
            check_equal(read_all(sst->make_reader(s, permit, pr, fwd_slice)),
                    read_all(make_mutation_reader_from_mutations(s, permit, m, fwd_slice)));
            check_equal(read_all(sst->make_reader(rs, permit, pr, rev_slice)),
                    read_all(make_reversing_reader(make_mutation_reader_from_mutations(s, permit, m, fwd_slice), permit.max_result_size())));
        };
        for (const auto a : starts) {
            // Ranges that start and end inside a range tombstone, far from
            // its bounds, so that the reader skips into it with the index.
            check({ss.make_ckey_range(a + 200, a + 300)});
            check({ss.make_ckey_range(a + 50, a + 60), ss.make_ckey_range(a + 400, a + 450)});
        }
        check({query::clustering_range::make_starting_with({ss.make_ckey(starts.back() + 250), true})});
        check({query::clustering_range::make_ending_with({ss.make_ckey(starts.front() + 250), true})});
    }).get();
}

// A reverse read of a few rows of a wide partition reads the pages of those
// rows, of the partition's head, and some read-ahead, but not the whole
// partition. Checked by counting the bytes read from the Data file, at the
// end of the partition (where the walk starts), at its start (reached with
// an index skip before the walk reads anything), and in its middle.
// A reverse read of the whole partition reads ahead backward: it makes about
// as many requests as a forward read. Read progress grows in both directions.
SEASTAR_THREAD_TEST_CASE(test_reverse_read_io_is_bounded) {
    test_env::do_with_async([] (test_env& env) {
        simple_schema ss;
        auto s = ss.schema();
        const auto rs = s->make_reversed();
        const uint32_t rows = 4000;
        auto pk = ss.make_pkey();
        mutation m(s, pk);
        ss.add_static_row(m, "static");
        for (uint32_t i = 0; i < rows; ++i) {
            ss.add_row(m, ss.make_ckey(i), fmt::format("value {} {}", i, tests::random::get_sstring(64)));
        }
        auto sst = write(env, s, {m}, aa, [] (sstable_writer_config& cfg) {
            cfg.parquet_page_set_size = 4096;
            cfg.parquet_min_page_size = 1024;
            cfg.parquet_row_group_size = 64 * 1024;
        });
        BOOST_REQUIRE_GT(sst->get_parquet_file_summary().row_groups(), 4);
        const auto pr = dht::partition_range::make_singular(pk);

        struct result {
            uint64_t reads;
            uint64_t bytes;
            uint64_t rows;
        };
        // Reads `range` in the given direction. A fresh sstable object has
        // empty caches of OffsetIndexes and dictionaries.
        auto read = [&] (query::clustering_range range, bool reverse) {
            auto fresh = env.reusable_sst(s, sst).get();
            auto counting = make_shared<counting_file_impl>(sstables::test(fresh).data_file());
            sstables::test(fresh).data_file() = file(counting);
            auto slice = partition_slice_builder(*s).with_range(std::move(range)).build();
            if (reverse) {
                slice = query::reverse_slice(*s, std::move(slice));
            }
            const auto& qs = reverse ? rs : s;
            progress_checker progress;
            auto rd = fresh->make_reader(qs, env.make_reader_permit(), pr, slice, nullptr,
                    streamed_mutation::forwarding::no, mutation_reader::forwarding::no, progress);
            uint64_t produced = 0;
            std::optional<clustering_key> last;
            while (auto mf = rd().get()) {
                progress.check();
                if (mf->is_clustering_row()) {
                    if (last) {
                        BOOST_REQUIRE(clustering_key::less_compare(*qs)(*last, mf->as_clustering_row().key()));
                    }
                    last = mf->as_clustering_row().key();
                    ++produced;
                }
            }
            rd.close().get();
            BOOST_REQUIRE(progress.final_position);
            BOOST_REQUIRE_LE(*progress.final_position, sst->data_size());
            return result{counting->reads, counting->bytes, produced};
        };

        const auto all = query::clustering_range::make_open_ended_both_sides();
        const auto forward = read(all, false);
        const auto full = read(all, true);
        testlog.info("the full partition: {} reads and {} bytes forward, {} reads and {} bytes in reverse",
                forward.reads, forward.bytes, full.reads, full.bytes);
        BOOST_REQUIRE_EQUAL(forward.rows, rows);
        BOOST_REQUIRE_EQUAL(full.rows, rows);
        BOOST_REQUIRE_LE(full.reads, 2 * forward.reads);
        for (const uint32_t first : {rows - 5, 0u, rows / 2}) {
            const auto r = read(ss.make_ckey_range(first, first + 4), true);
            testlog.info("rows [{}, {}]: {} reads and {} bytes", first, first + 4, r.reads, r.bytes);
            BOOST_REQUIRE_EQUAL(r.rows, 5);
            BOOST_REQUIRE_LT(r.bytes * 10, full.bytes);
        }
    }).get();
}

namespace {

// A table whose raw size per row is known: a clustering row with one cell
// of `v` has the values of `_scylla.kind` (4 bytes), `_scylla.ts` (8 bytes),
// `ck` (4 bytes), and `v`. The first row of a partition also has `pk`
// (4 bytes). The cell has the row's default timestamp, so its own
// timestamp column is null.
schema_ptr sized_table(compression_parameters cp) {
    return schema_builder(this_smp_shard_count(), "ks", "sized")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("v", utf8_type)
            .set_compressor_params(cp)
            .build();
}

constexpr uint64_t sized_row_overhead = 4 + 8 + 4;
constexpr uint64_t sized_partition_overhead = 4;

// A partition of `rows` rows, whose values have sizes from `value_size`.
mutation sized_partition(schema_ptr s, int32_t pk, int rows, std::function<size_t (int)> value_size) {
    mutation m(s, partition_key::from_single_value(*s, int32_type->decompose(pk)));
    const auto& v = *s->get_column_definition("v");
    for (int i = 0; i < rows; ++i) {
        auto ck = clustering_key::from_single_value(*s, int32_type->decompose(i));
        sstring value(value_size(i), 'x');
        m.set_clustered_cell(ck, v, atomic_cell::make_live(*utf8_type, 1000 + i, utf8_type->decompose(value)));
    }
    return m;
}

uint64_t raw_size_of(const mutation& m) {
    const auto& v = *m.schema()->get_column_definition("v");
    uint64_t size = sized_partition_overhead;
    for (const auto& r : m.partition().clustered_rows()) {
        size += sized_row_overhead + r.row().cells().cell_at(v.id).as_atomic_cell(v).value().size();
    }
    return size;
}

// Mutations of partitions on this shard, sorted.
utils::chunked_vector<mutation> local_sorted(utils::chunked_vector<mutation> muts) {
    utils::chunked_vector<mutation> local;
    for (auto& m : muts) {
        if (m.schema()->get_sharder().shard_for_reads(m.token()) == this_shard_id()) {
            local.push_back(std::move(m));
        }
    }
    std::ranges::sort(local, mutation_decorated_key_less_comparator());
    return local;
}

} // anonymous namespace

// data_size() is the total raw size of the rows, which Scylla.db stores, and
// ondisk_data_size() is the size of the file. The compression ratio is
// their quotient for a table with compression.
SEASTAR_THREAD_TEST_CASE(test_data_size) {
    test_env::do_with_async([] (test_env& env) {
        using algorithm = compression_parameters::algorithm;
        for (const auto alg : {algorithm::none, algorithm::zstd}) {
            testlog.info("compression {}", compression_parameters::algorithm_to_name(alg));
            auto s = sized_table(compression_parameters(alg));
            utils::chunked_vector<mutation> muts;
            for (int pk = 0; pk < 50; ++pk) {
                muts.push_back(sized_partition(s, pk, pk % 7 + 1, [pk] (int i) { return (pk * 37 + i * 101) % 500; }));
            }
            muts = local_sorted(std::move(muts));
            BOOST_REQUIRE(!muts.empty());
            const uint64_t raw = std::ranges::fold_left(muts | std::views::transform(raw_size_of), uint64_t(0), std::plus());
            auto sst = write(env, s, muts, aa, [] (sstable_writer_config& cfg) {
                cfg.parquet_row_group_size = 4096;
            });
            const auto* entry = sst->get_scylla_metadata()->get_parquet_data_file();
            BOOST_REQUIRE(entry);
            const uint64_t file_size = std::filesystem::file_size(sstables::test(sst).filename(component_type::Data).native());
            BOOST_REQUIRE_EQUAL(sst->data_size(), raw);
            BOOST_REQUIRE_EQUAL(entry->raw_size, raw);
            BOOST_REQUIRE_EQUAL(sst->ondisk_data_size(), file_size);
            BOOST_REQUIRE_EQUAL(entry->file_size, file_size);
            const auto stats = sst->get_file_size_stats();
            BOOST_REQUIRE_EQUAL(stats.on_disk - stats.before_compression, file_size - raw);
            if (alg == algorithm::none) {
                BOOST_REQUIRE_EQUAL(sst->get_compression_ratio(), metadata_collector::NO_COMPRESSION_RATIO);
            } else {
                BOOST_REQUIRE_LT(file_size, raw);
                BOOST_REQUIRE_EQUAL(sst->get_compression_ratio(), double(file_size) / raw);
            }
        }
    }).get();
}
