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
// - Checksum validation: the checksums of the pages and the footer, and
//   the digest, and the digest check of whole-file streams.

#include <filesystem>
#include <fstream>

#include <seastar/testing/on_internal_error.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/closeable.hh>

#include "mutation/mutation_rebuilder.hh"
#include "partition_slice_builder.hh"
#include "readers/from_fragments.hh"
#include "readers/from_mutations.hh"
#include "readers/reversing.hh"
#include "schema/schema_builder.hh"
#include "sstables/file_writer.hh"
#include "sstables/metadata_collector.hh"
#include "sstables/parquet/file_summary.hh"
#include "sstables/parquet/file_verifier.hh"
#include "sstables/parquet/mapping_reader.hh"
#include "sstables/parquet/page_decoder.hh"
#include "sstables/parquet/reader.hh"
#include "sstables/parquet/sstable_reader.hh"
#include "sstables/parquet/stats.hh"
#include "sstables/parquet/sstable_writer.hh"
#include "sstables/compressor.hh"
#include "sstables/parquet/writer.hh"
#include "sstables/serialization_header_builder.hh"
#include "sstables/sstables.hh"
// The serialization header is a Statistics.db component, whose virtual
// write() needs the serialization templates.
#include "sstables/writer.hh"
#include "test/lib/eventually.hh"
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

// Validates `sst` as the `validate` mode of scrub does, and returns the
// number of errors.
uint64_t validation_errors(test_env& env, shared_sstable sst) {
    abort_source abort;
    uint64_t reported = 0;
    const auto errors = sst->validate(env.make_reader_permit(), abort, [&] (sstring what) {
        ++reported;
        testlog.info("validation error: {}", what);
    }).get();
    BOOST_REQUIRE_EQUAL(errors, reported);
    return errors;
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

        // Validation finds no error in a valid sstable, also with many
        // row index blocks.
        BOOST_REQUIRE_EQUAL(validation_errors(env, sst), 0);

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
// sequential read of the Data file, in both directions, and validate()
// accepts the sstable.
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
        BOOST_REQUIRE_EQUAL(validation_errors(env, sst), 0);

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

// Read progress is the total raw size times the rows read, divided by the
// total rows. It never goes back, and a full scan
// ends at data_size(), which compaction and view building expect.
SEASTAR_THREAD_TEST_CASE(test_read_progress) {
    test_env::do_with_async([] (test_env& env) {
        auto s = sized_table(compression_parameters(compression_parameters::algorithm::lz4));
        utils::chunked_vector<mutation> muts;
        // Partitions of different sizes, so that progress by raw size and
        // progress by rows differ.
        for (int pk = 0; pk < 40; ++pk) {
            muts.push_back(sized_partition(s, pk, pk % 5 + 1, [pk] (int) { return pk % 2 ? 10 : 1000; }));
        }
        muts = local_sorted(std::move(muts));
        BOOST_REQUIRE_GE(muts.size(), 4u);
        auto sst = write(env, s, muts, aa, [] (sstable_writer_config& cfg) {
            cfg.parquet_page_set_size = 512;
            cfg.parquet_row_group_size = 4096;
        });
        const uint64_t data_size = sst->data_size();
        const uint64_t total_rows = sst->get_parquet_file_summary().rows();
        auto progress_at = [&] (uint64_t row) {
            return uint64_t((unsigned __int128)data_size * row / total_rows);
        };

        {
            progress_checker progress;
            auto rd = sst->make_full_scan_reader(s, env.make_reader_permit(), nullptr, progress);
            while (rd().get()) {
                progress.check();
            }
            rd.close().get();
            BOOST_REQUIRE_EQUAL(progress.total, data_size);
            BOOST_REQUIRE_EQUAL(progress.final_position.value(), data_size);
        }

        // Each partition alone. Its first row is the sum of the rows of the
        // partitions before it.
        uint64_t first = 0;
        for (const auto& m : muts) {
            const uint64_t end = first + std::ranges::distance(m.partition().clustered_rows());
            // The reader keeps a reference to the range.
            const auto pr = dht::partition_range::make_singular(m.decorated_key());
            progress_checker progress;
            auto rd = sst->make_reader(s, env.make_reader_permit(), pr, s->full_slice(),
                    nullptr, streamed_mutation::forwarding::no, mutation_reader::forwarding::no, progress);
            uint64_t rows = 0;
            while (auto mf = rd().get()) {
                rows += mf->is_clustering_row();
                progress.check();
            }
            rd.close().get();
            BOOST_REQUIRE_EQUAL(rows, end - first);
            BOOST_REQUIRE_EQUAL(progress.total, progress_at(end) - progress_at(first));
            BOOST_REQUIRE_EQUAL(progress.final_position.value(), progress_at(end));
            first = end;
        }
        BOOST_REQUIRE_EQUAL(first, total_rows);
    }).get();
}

// The key estimate for a token range is the range of row ordinals that the
// index gives, divided by the total rows, times the key count.
// With one row per partition, it is exact up to the
// inexactness of the trie index at each end, whatever the sizes of the
// partitions.
SEASTAR_THREAD_TEST_CASE(test_key_estimate_uses_ordinals) {
    test_env::do_with_async([] (test_env& env) {
        auto s = sized_table(compression_parameters::no_compression());
        utils::chunked_vector<mutation> muts;
        for (int pk = 0; pk < 2000; ++pk) {
            // Some partitions are a hundred times larger than others.
            muts.push_back(sized_partition(s, pk, 1, [pk] (int) { return pk % 3 ? 10 : 1000; }));
        }
        muts = local_sorted(std::move(muts));
        const auto n = muts.size();
        BOOST_REQUIRE_GE(n, 100u);
        auto sst = write(env, s, muts, aa);
        BOOST_REQUIRE_EQUAL(sst->get_estimated_key_count(), n);
        BOOST_REQUIRE_EQUAL(sst->estimated_keys_for_range(dht::token_range::make_open_ended_both_sides()).get(), n);
        for (const auto& [lower, upper] : {std::pair<size_t, size_t>{0, n / 10}, {n / 4, n / 2}, {n / 3, n / 3}, {n / 2, n - 1}}) {
            const auto r = dht::token_range::make(muts[lower].token(), muts[upper].token());
            const auto est = sst->estimated_keys_for_range(r).get();
            testlog.info("[{}, {}]: {}", lower, upper, est);
            BOOST_REQUIRE_GE(est, upper - lower + 1);
            BOOST_REQUIRE_LE(est, upper - lower + 3);
        }
    }).get();
}

namespace {

void flip_byte(shared_sstable sst, uint64_t at) {
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

// A table whose column `w` gets its first value in the last partitions, so
// that the Data file has late chunks (see sstables/parquet/writer.hh).
schema_ptr late_column_table() {
    return schema_builder(this_smp_shard_count(), "ks", "late")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("v", utf8_type)
            .with_column("w", int32_type)
            .set_compressor_params(compression_parameters(compression_parameters::algorithm::lz4))
            .build();
}

utils::chunked_vector<mutation> late_column_mutations(schema_ptr s) {
    const auto& v = *s->get_column_definition("v");
    const auto& w = *s->get_column_definition("w");
    utils::chunked_vector<mutation> muts;
    for (int pk = 0; pk < 40; ++pk) {
        mutation m(s, partition_key::from_single_value(*s, int32_type->decompose(pk)));
        for (int ck = 0; ck < 20; ++ck) {
            auto key = clustering_key::from_single_value(*s, int32_type->decompose(ck));
            m.set_clustered_cell(key, v, atomic_cell::make_live(*utf8_type, 1000, utf8_type->decompose(tests::random::get_sstring(16))));
        }
        muts.push_back(std::move(m));
    }
    muts = local_sorted(std::move(muts));
    // Only the last partitions in the file have `w`.
    for (size_t i = muts.size() - std::min<size_t>(muts.size(), 3); i < muts.size(); ++i) {
        for (int ck = 0; ck < 20; ++ck) {
            auto key = clustering_key::from_single_value(*s, int32_type->decompose(ck));
            muts[i].set_clustered_cell(key, w, atomic_cell::make_live(*int32_type, 1000, int32_type->decompose(ck)));
        }
    }
    return muts;
}

// Runs the verifier of the Data file directly, and returns the number of
// problems. Without the digest, only the checksums of the pages and the
// footer can detect damage.
// `wrong_digest` replaces the digest by a wrong one.
uint64_t count_problems(test_env& env, shared_sstable sst, bool with_digest, bool wrong_digest = false) {
    auto permit = env.make_reader_permit();
    auto in = sst->data_stream(0, sst->ondisk_data_size(), permit, nullptr, nullptr).get();
    auto close = deferred_close(in);
    auto digest = with_digest ? sst->read_digest().get() : std::nullopt;
    if (digest && wrong_digest) {
        *digest += 1;
    }
    return parquet::verify_file(in, sst->make_data_file_for_read(permit, nullptr), sst->get_parquet_file_summary(), parquet::verify_options{
        .file_size = sst->ondisk_data_size(),
        .footer_crc32 = sst->get_scylla_metadata()->get_parquet_data_file()->footer_crc32,
        .digest = digest,
        .permit = permit,
    }, [] (sstring what) {
        testlog.info("problem: {}", what);
    }).get();
}

} // anonymous namespace

static const tweak_config small_pages = [] (sstable_writer_config& cfg) {
    cfg.parquet_page_set_size = 512;
    cfg.parquet_min_page_size = 128;
    cfg.parquet_row_group_size = 4096;
};

// Checksum validation of a Parquet Data file checks the checksum of every
// page, the footer checksum, and the digest of the whole file. Damage
// anywhere fails validation. A damaged page body or footer is reported
// without the digest too. test_validate_page_damage covers pages byte by
// byte.
SEASTAR_THREAD_TEST_CASE(test_validate_checksums) {
    struct damage {
        const char* what;
        std::function<uint64_t (const parquet::file_summary&)> offset;
        // The exact number of problems that the verifier reports, without
        // the digest. Unset where the byte may lie in a page header or an
        // OffsetIndex, which a damaged byte may change to another valid
        // value, or make contradict the pages.
        std::optional<uint64_t> problems;
    };
    const std::vector<damage> damages = {
        {"a page of the first row group", [] (const parquet::file_summary& s) { return s.region_start(0) + s.region_size(0) / 2; }, std::nullopt},
        {"a page of the last row group", [] (const parquet::file_summary& s) {
            const uint32_t g = s.row_groups() - 1;
            return s.region_start(g) + s.region_size(g) / 3;
        }, std::nullopt},
        {"an OffsetIndex block", [] (const parquet::file_summary& s) {
            return s.row_group(0).offset_index_offset + s.row_group(0).offset_index_length / 2;
        }, std::nullopt},
        {"a late chunk", [] (const parquet::file_summary& s) {
            const uint64_t start = s.region_start(s.row_groups());
            return start + (s.footer_offset() - start) / 2;
        }, std::nullopt},
        {"the footer", [] (const parquet::file_summary& s) { return s.footer_offset() + 10; }, 1},
    };
    for (const auto& d : damages) {
        testlog.info("damage: {}", d.what);
        test_env::do_with_async([&] (test_env& env) {
            auto s = late_column_table();
            auto sst = write(env, s, late_column_mutations(s), aa, small_pages);
            const auto& summary = sst->get_parquet_file_summary();
            BOOST_REQUIRE_GT(summary.row_groups(), 2);
            BOOST_REQUIRE_LT(summary.region_start(summary.row_groups()), summary.footer_offset());
            auto permit = env.make_reader_permit();

            auto res = validate_checksums_and_digests(sst, permit).get();
            BOOST_REQUIRE(res.status == validate_checksums_status::valid);
            BOOST_REQUIRE(res.has_digest);
            BOOST_REQUIRE(res.has_checksum);
            BOOST_REQUIRE_EQUAL(count_problems(env, sst, true), 0);

            flip_byte(sst, d.offset(summary));
            res = validate_checksums_and_digests(sst, permit).get();
            BOOST_REQUIRE(res.status == validate_checksums_status::invalid);
            BOOST_REQUIRE_GE(count_problems(env, sst, true), 1);
            if (d.problems) {
                BOOST_REQUIRE_EQUAL(count_problems(env, sst, true), *d.problems + 1);
                BOOST_REQUIRE_EQUAL(count_problems(env, sst, false), *d.problems);
            }
        }).get();
    }

    // The digest alone: the Data file is intact and the expected digest is
    // wrong. Scylla.db holds the digest that the sstable uses, so this is
    // tested with the verifier directly.
    test_env::do_with_async([] (test_env& env) {
        auto s = late_column_table();
        auto sst = write(env, s, late_column_mutations(s), aa);
        BOOST_REQUIRE_EQUAL(count_problems(env, sst, true, true), 1);
    }).get();
}

// Damages each byte of the page headers of the first row group, and a few
// bytes of each page body, one at a time. Validation must fail every time.
// Without the digest, the verifier must report a damaged body exactly once,
// and it reports most damaged header bytes too. file_verifier.hh lists the
// fields that only the digest guards. In this file, that is the length of
// the definition levels.
SEASTAR_THREAD_TEST_CASE(test_validate_page_damage) {
    test_env::do_with_async([] (test_env& env) {
        auto s = late_column_table();
        auto sst = write(env, s, late_column_mutations(s), aa, small_pages);
        const auto& summary = sst->get_parquet_file_summary();
        auto permit = env.make_reader_permit();
        std::string data;
        {
            std::ifstream f(sstables::test(sst).filename(component_type::Data).native(), std::ios::binary);
            data.assign(std::istreambuf_iterator<char>(f), {});
        }
        BOOST_REQUIRE_EQUAL(data.size(), sst->ondisk_data_size());

        // The extents of the pages of row group 0: start, header size, size.
        struct extent {
            uint64_t start;
            uint64_t header;
            uint64_t size;
        };
        std::vector<extent> pages;
        const auto& rg = summary.row_group(0);
        temporary_buffer<char> block(data.data() + rg.offset_index_offset, rg.offset_index_length);
        std::vector<temporary_buffer<char>> pieces;
        pieces.push_back(std::move(block));
        auto idx = parquet::decode_row_group_index(summary, 0, fragmented_temporary_buffer(std::move(pieces), rg.offset_index_length)).get();
        auto add_page = [&] (uint64_t start, uint64_t size) {
            parquet::thrift::decoder d(std::span<const char>(data.data() + start, size));
            parquet::parse_page_header(d);
            pages.push_back({start, d.position(), size});
        };
        for (parquet::leaf_index c = 0; c < summary.columns().size(); ++c) {
            if (!summary.in_place(0, c)) {
                continue;
            }
            if (summary.has_dictionary(0, c)) {
                add_page(idx.chunk_start(c), idx.dictionary_size(c));
            }
            for (uint32_t j = 0; j < idx.pages(c); ++j) {
                add_page(idx.page_at(c, j).offset, idx.page_at(c, j).size);
            }
        }
        BOOST_REQUIRE_GT(pages.size(), 4);

        uint64_t header_bytes = 0;
        uint64_t header_bytes_detected = 0;
        auto damage = [&] (uint64_t at, bool in_header) {
            flip_byte(sst, at);
            BOOST_REQUIRE(validate_checksums_and_digests(sst, permit).get().status == validate_checksums_status::invalid);
            BOOST_REQUIRE_GE(count_problems(env, sst, true), 1);
            const auto problems = count_problems(env, sst, false);
            if (in_header) {
                ++header_bytes;
                header_bytes_detected += problems > 0;
            } else {
                BOOST_REQUIRE_EQUAL(problems, 1);
            }
            flip_byte(sst, at);
        };
        for (const auto& p : pages) {
            for (uint64_t i = 0; i < p.header; ++i) {
                damage(p.start + i, true);
            }
            const uint64_t body = p.size - p.header;
            for (uint64_t i : {uint64_t(0), body / 2, body - 1}) {
                damage(p.start + p.header + i, false);
            }
        }
        BOOST_REQUIRE_EQUAL(count_problems(env, sst, true), 0);
        testlog.info("damaged header bytes detected without the digest: {} of {}", header_bytes_detected, header_bytes);
        // The writer's headers take about 30 bytes, and the definition
        // level length takes one of them.
        BOOST_REQUIRE_GE(header_bytes_detected * 10, header_bytes * 9);
    }).get();
}

// The verifier charges the OffsetIndex block that it decodes, and the
// totals of the row groups, to the permit of its options, and releases
// them when it returns. The stream and the file use another permit, so
// that their buffers do not count.
SEASTAR_THREAD_TEST_CASE(test_verifier_charges_permit) {
    test_env::do_with_async([] (test_env& env) {
        auto s = late_column_table();
        auto sst = write(env, s, late_column_mutations(s), aa, small_pages);
        const auto& summary = sst->get_parquet_file_summary();
        // The last byte of the region of row group 0 is in the body of its
        // last page. The verifier reports it while it holds the block.
        flip_byte(sst, summary.row_group(0).offset_index_offset - 1);

        auto io_permit = env.make_reader_permit();
        auto permit = env.make_reader_permit();
        const auto baseline = permit.consumed_resources().memory;
        auto in = sst->data_stream(0, sst->ondisk_data_size(), io_permit, nullptr, nullptr).get();
        auto close = deferred_close(in);
        std::optional<ssize_t> charged;
        const auto problems = parquet::verify_file(in, sst->make_data_file_for_read(io_permit, nullptr), summary, parquet::verify_options{
            .file_size = sst->ondisk_data_size(),
            .footer_crc32 = sst->get_scylla_metadata()->get_parquet_data_file()->footer_crc32,
            .permit = permit,
        }, [&] (sstring what) {
            testlog.info("problem: {}", what);
            if (!charged) {
                charged = permit.consumed_resources().memory - baseline;
            }
        }).get();
        BOOST_REQUIRE_EQUAL(problems, 1);
        BOOST_REQUIRE(charged);
        BOOST_REQUIRE_GE(*charged, ssize_t(parquet::row_group_index_decode_memory(summary, 0) + summary.row_groups() * 32));
        BOOST_REQUIRE_EQUAL(permit.consumed_resources().memory, baseline);
    }).get();
}

// A whole-file stream with the integrity check checks the digest, once the
// sstable has read it.
SEASTAR_THREAD_TEST_CASE(test_whole_file_stream_checks_digest) {
    test_env::do_with_async([] (test_env& env) {
        auto s = sized_table(compression_parameters(compression_parameters::algorithm::lz4));
        utils::chunked_vector<mutation> muts;
        for (int pk = 0; pk < 20; ++pk) {
            muts.push_back(sized_partition(s, pk, 10, [] (int i) { return 100 + i; }));
        }
        muts = local_sorted(std::move(muts));
        auto sst = write(env, s, muts, aa);
        auto permit = env.make_reader_permit();
        BOOST_REQUIRE(sst->read_digest().get());

        auto read_whole_file = [&] {
            auto in = sst->data_stream(0, sst->ondisk_data_size(), permit, nullptr, nullptr,
                    sstable::raw_stream::compressed_chunks, integrity_check::yes).get();
            auto close = deferred_close(in);
            uint64_t size = 0;
            while (auto buf = in.read().get()) {
                size += buf.size();
            }
            return size;
        };
        BOOST_REQUIRE_EQUAL(read_whole_file(), sst->ondisk_data_size());
        flip_byte(sst, sst->get_parquet_file_summary().region_start(0) + 10);
        BOOST_REQUIRE_THROW(read_whole_file(), malformed_sstable_exception);
    }).get();
}

// A stream of a part of a Parquet Data file cannot check its integrity, so
// asking for one is an internal error, and not a stream that checks nothing.
SEASTAR_THREAD_TEST_CASE(test_partial_stream_with_integrity_check) {
    test_env::do_with_async([] (test_env& env) {
        auto s = sized_table(compression_parameters(compression_parameters::algorithm::lz4));
        auto sst = write(env, s, local_sorted({sized_partition(s, 0, 10, [] (int i) { return 100 + i; })}), aa);
        auto permit = env.make_reader_permit();
        BOOST_REQUIRE(sst->read_digest().get());
        seastar::testing::scoped_no_abort_on_internal_error no_abort;
        for (const auto& [pos, len] : {std::pair<uint64_t, uint64_t>{0, sst->ondisk_data_size() - 1}, {1, sst->ondisk_data_size() - 1}}) {
            BOOST_REQUIRE_THROW(sst->data_stream(pos, len, permit, nullptr, nullptr, sstable::raw_stream::yes, integrity_check::yes).get(),
                    std::runtime_error);
            auto in = sst->data_stream(pos, len, permit, nullptr, nullptr, sstable::raw_stream::yes, integrity_check::no).get();
            in.close().get();
        }
    }).get();
}

namespace {

// Writes fragments as they come, with validation of their order off, as the
// scrub tests do to make an sstable with rows out of order.
shared_sstable write_fragments(test_env& env, schema_ptr s, std::deque<mutation_fragment_v2> frags) {
    auto sst = env.make_sstable(s, aa);
    auto cfg = env.manager().configure_writer("test");
    cfg.validation_level = mutation_fragment_stream_validation_level::partition_region;
    auto rd = make_mutation_reader_from_fragments(s, env.make_reader_permit(), std::move(frags));
    sst->write_components(std::move(rd), 1, s, cfg, encoding_stats{}).get();
    sst->load(s->get_sharder()).get();
    return sst;
}

} // anonymous namespace

// The validate mode of scrub reports
// errors and counts them, as for `mx`: rows out of order, a damaged page,
// and an index that does not match the Data file. It stops when asked to.
SEASTAR_THREAD_TEST_CASE(test_validate) {
    test_env::do_with_async([] (test_env& env) {
        simple_schema ss(simple_schema::with_static::yes);
        auto s = ss.schema();
        auto permit = env.make_reader_permit();
        const auto keys = ss.make_pkeys(6);

        testlog.info("rows out of order");
        {
            std::deque<mutation_fragment_v2> frags;
            frags.emplace_back(*s, permit, partition_start(keys[0], {}));
            for (int ck : {0, 2, 1, 3}) {
                frags.push_back(ss.make_row_v2(permit, ss.make_ckey(ck), "v"));
            }
            frags.emplace_back(*s, permit, partition_end());
            auto sst = write_fragments(env, s, std::move(frags));
            BOOST_REQUIRE_GT(validation_errors(env, sst), 0);
        }

        // Partitions with many index blocks.
        auto make_muts = [&] (int rows) {
            utils::chunked_vector<mutation> muts;
            for (const auto& pk : keys) {
                mutation m(s, pk);
                ss.add_static_row(m, "static");
                for (int i = 0; i < rows; ++i) {
                    ss.add_row(m, ss.make_ckey(i), fmt::format("value {}", i));
                }
                muts.push_back(std::move(m));
            }
            return muts;
        };
        const auto tweak = [] (sstable_writer_config& cfg) {
            cfg.parquet_page_set_size = 256;
            cfg.parquet_min_page_size = 64;
            cfg.parquet_row_group_size = 4096;
        };

        testlog.info("a damaged page");
        {
            auto sst = write(env, s, make_muts(100), aa, tweak);
            BOOST_REQUIRE_EQUAL(validation_errors(env, sst), 0);
            const auto& summary = sst->get_parquet_file_summary();
            flip_byte(sst, summary.region_start(1) + summary.region_size(1) / 2);
            sst = env.reusable_sst(s, sst).get();
            BOOST_REQUIRE_GT(validation_errors(env, sst), 0);

            testlog.info("abort");
            abort_source abort;
            abort.request_abort();
            BOOST_REQUIRE_EQUAL(sst->validate(permit, abort, [] (sstring) {}).get(), 0);
        }

        testlog.info("an index of another Data file");
        {
            // The same partitions with another number of rows: the
            // partitions, and the row index blocks, are at other rows.
            auto sst = write(env, s, make_muts(100), aa, tweak);
            auto other = write(env, s, make_muts(90), aa, tweak);
            for (auto c : {component_type::Partitions, component_type::Rows}) {
                std::filesystem::copy_file(sstables::test(other).filename(c).native(), sstables::test(sst).filename(c).native(),
                        std::filesystem::copy_options::overwrite_existing);
            }
            // A fresh object, which reads the copied index. Validation of
            // the component digests would stop at the copied files, so the
            // Parquet validation runs directly.
            // The open must not read the first and the last rows with the index.
            sst = env.reusable_sst(s, sstring(sst->get_storage().prefix()), sst->generation(), aa,
                    sstable_open_config{.load_first_and_last_position_metadata = false}).get();
            abort_source abort;
            uint64_t index_errors = 0;
            const auto errors = parquet::validate(sst, permit, abort, [&] (sstring what) {
                testlog.info("validation error: {}", what);
                index_errors += what.starts_with("mismatching index/data");
            }, default_read_monitor()).get();
            BOOST_REQUIRE_GT(index_errors, 0);
            BOOST_REQUIRE_EQUAL(errors, index_errors);
        }
    }).get();
}

// The shard counters behind the Parquet metrics (sstables/parquet/stats.hh)
// count the pages and row groups of a file when its writer finishes, the
// reads of a reader when it closes, and the memory of the footer summary
// and of the metadata cache while the sstable is open.
SEASTAR_THREAD_TEST_CASE(test_shard_stats) {
    test_env::do_with_async([] (test_env& env) {
        const auto& stats = parquet::local_shard_stats;
        const auto before = stats;
        simple_schema ss;
        auto s = ss.schema();
        utils::chunked_vector<mutation> muts;
        for (const auto& pk : ss.make_pkeys(4)) {
            mutation m(s, pk);
            for (int i = 0; i < 200; ++i) {
                ss.add_row(m, ss.make_ckey(i), fmt::format("value {}", i % 7));
            }
            muts.push_back(std::move(m));
        }
        auto sst = write(env, s, muts, aa, [] (sstable_writer_config& cfg) {
            cfg.parquet_page_set_size = 1024;
            cfg.parquet_min_page_size = 256;
            cfg.parquet_row_group_size = 4096;
        });
        const auto& summary = sst->get_parquet_file_summary();
        BOOST_REQUIRE_GT(summary.row_groups(), 1);
        BOOST_REQUIRE_EQUAL(stats.row_groups_written - before.row_groups_written, summary.row_groups());
        BOOST_REQUIRE_GE(stats.data_pages_written - before.data_pages_written, summary.row_groups() * summary.columns().size());
        BOOST_REQUIRE_GT(stats.dictionary_pages_written, before.dictionary_pages_written);
        BOOST_REQUIRE_EQUAL(stats.footer_summary_bytes - before.footer_summary_bytes, summary.memory_usage());

        const auto before_read = stats;
        {
            auto rd = sst->make_full_scan_reader(s, env.make_reader_permit());
            auto close = deferred_close(rd);
            while (rd().get()) {}
        }
        BOOST_REQUIRE_GT(stats.read_requests, before_read.read_requests);
        BOOST_REQUIRE_GT(stats.bytes_read, before_read.bytes_read);
        BOOST_REQUIRE_GE(stats.data_pages_read - before_read.data_pages_read, summary.row_groups());
        BOOST_REQUIRE_GT(stats.dictionary_pages_read, before_read.dictionary_pages_read);
        BOOST_REQUIRE_GE(stats.offset_index_reads - before_read.offset_index_reads, summary.row_groups());
        BOOST_REQUIRE_GE(stats.offset_index_cache_misses - before_read.offset_index_cache_misses, summary.row_groups());
        BOOST_REQUIRE_GT(stats.dictionary_cache_misses, before_read.dictionary_cache_misses);
        const auto& cache = sst->get_parquet_read_state().cache();
        BOOST_REQUIRE_GT(cache.memory_usage(), 0);
        BOOST_REQUIRE_EQUAL(stats.metadata_cache_bytes - before.metadata_cache_bytes, cache.memory_usage());

        // A second read finds the OffsetIndexes in the cache.
        const auto before_second_read = stats;
        {
            auto rd = sst->make_full_scan_reader(s, env.make_reader_permit());
            auto close = deferred_close(rd);
            while (rd().get()) {}
        }
        BOOST_REQUIRE_EQUAL(stats.offset_index_reads, before_second_read.offset_index_reads);
        BOOST_REQUIRE_GT(stats.offset_index_cache_hits, before_second_read.offset_index_cache_hits);

        // The memory is released with the sstable.
        sst = {};
        REQUIRE_EVENTUALLY_EQUAL<uint64_t>([&] { return stats.footer_summary_bytes; }, before.footer_summary_bytes);
        REQUIRE_EVENTUALLY_EQUAL<uint64_t>([&] { return stats.metadata_cache_bytes; }, before.metadata_cache_bytes);
    }).get();
}

// compression_parameters_of() inverts the writer's choice of compression
// codec, which the tool's schema loader and the REST API use to report the
// compression of a Parquet sstable.
SEASTAR_THREAD_TEST_CASE(test_compression_parameters_of_codec) {
    test_env::do_with_async([] (test_env& env) {
        using algorithm = compression_parameters::algorithm;
        const std::vector<std::pair<algorithm, algorithm>> cases = {
            {algorithm::none, algorithm::none},
            {algorithm::lz4, algorithm::lz4},
            {algorithm::lz4_with_dicts, algorithm::lz4},
            {algorithm::snappy, algorithm::snappy},
            {algorithm::deflate, algorithm::deflate},
            {algorithm::zstd, algorithm::zstd},
            {algorithm::zstd_with_dicts, algorithm::zstd},
        };
        const auto cfg = env.manager().configure_writer("test");
        for (const auto& [table, expected] : cases) {
            const auto codec = parquet::make_writer_config(cfg, compression_parameters(table)).codec;
            BOOST_REQUIRE(parquet::compression_parameters_of(codec).get_algorithm() == expected);
            BOOST_REQUIRE(parquet::make_writer_config(cfg, parquet::compression_parameters_of(codec)).codec == codec);
        }
    }).get();
}

// The OffsetIndexes and dictionaries of an sstable are cached in the LSA
// region and in the LRU of the cache_tracker, as index entries.
// Memory reclaim evicts them through the cache_tracker, and a read fetches
// them again. A reader keeps working when the entries that it uses are
// evicted under it.
SEASTAR_THREAD_TEST_CASE(test_metadata_cache_eviction) {
    test_env::do_with_async([] (test_env& env) {
        simple_schema ss;
        auto s = ss.schema();
        auto pk = ss.make_pkey();
        mutation m(s, pk);
        for (uint32_t i = 0; i < 2000; ++i) {
            // Few distinct values, so that the chunks have dictionaries.
            ss.add_row(m, ss.make_ckey(i), fmt::format("value {}", i % 7));
        }
        auto sst = write(env, s, {m}, aa, [] (sstable_writer_config& cfg) {
            cfg.parquet_page_set_size = 1024;
            cfg.parquet_min_page_size = 256;
            cfg.parquet_row_group_size = 4096;
        });
        const auto row_groups = sst->get_parquet_file_summary().row_groups();
        BOOST_REQUIRE_GT(row_groups, 4);
        auto& tracker = env.manager().get_cache_tracker();
        const auto& tracker_stats = tracker.get_parquet_metadata_cache_stats();
        const auto& cache = sst->get_parquet_read_state().cache();
        const auto& stats = parquet::local_shard_stats;
        const std::vector<mutation> expected{m};
        auto read = [&] {
            check_equal(read_all(sst->make_reader(s, env.make_reader_permit(), query::full_partition_range, s->full_slice())), expected);
        };

        const auto misses = stats.offset_index_cache_misses;
        read();
        BOOST_REQUIRE_GT(cache.size(), row_groups);
        BOOST_REQUIRE_GT(cache.memory_usage(), 0);
        BOOST_REQUIRE_EQUAL(tracker_stats.used_bytes, cache.memory_usage());
        BOOST_REQUIRE_GE(tracker.region().occupancy().used_space(), cache.memory_usage());
        BOOST_REQUIRE_GE(stats.offset_index_cache_misses - misses, row_groups);
        // A second read finds every OffsetIndex block in the cache.
        const auto index_reads = stats.offset_index_reads;
        read();
        BOOST_REQUIRE_EQUAL(stats.offset_index_reads, index_reads);

        // The evictor of the region evicts the entries in LRU order.
        const auto evictions = stats.metadata_cache_evictions;
        const auto entries = cache.size();
        while (cache.size() && tracker.region().evict_some() == memory::reclaiming_result::reclaimed_something) {}
        BOOST_REQUIRE_EQUAL(cache.size(), 0);
        BOOST_REQUIRE_EQUAL(tracker_stats.used_bytes, 0);
        BOOST_REQUIRE_EQUAL(stats.metadata_cache_evictions - evictions, entries);
        read();
        BOOST_REQUIRE_GT(stats.offset_index_reads, index_reads);
        BOOST_REQUIRE_EQUAL(cache.size(), entries);

        // Memory reclaim evicts from the LRU, here all of it, while a reader
        // is in the middle of the partition.
        auto rd = sst->make_reader(s, env.make_reader_permit(), query::full_partition_range, s->full_slice());
        auto close = deferred_close(rd);
        mutation_rebuilder_v2 rebuilder(s);
        for (int i = 0; i < 1000; ++i) {
            auto mf = rd().get();
            BOOST_REQUIRE(mf);
            rebuilder.consume(std::move(*mf));
        }
        tracker.clear();
        BOOST_REQUIRE_EQUAL(cache.size(), 0);
        BOOST_REQUIRE_EQUAL(tracker_stats.used_bytes, 0);
        while (auto mf = rd().get()) {
            rebuilder.consume(std::move(*mf));
        }
        auto result = rebuilder.consume_end_of_stream();
        BOOST_REQUIRE(result);
        assert_that(*result).is_equal_to(m);
        BOOST_REQUIRE_GT(cache.size(), 0);

        // Dropping the caches of the sstable evicts its entries.
        sst->drop_caches().get();
        BOOST_REQUIRE_EQUAL(cache.size(), 0);
        BOOST_REQUIRE_EQUAL(tracker_stats.used_bytes, 0);
    }).get();
}

// A read of an uncompressed sstable with dictionaries leaves no memory
// charged to its permit once the reader is closed, while the cache holds
// its OffsetIndexes and dictionary pages. Concurrent readers populate the
// cache once.
SEASTAR_THREAD_TEST_CASE(test_metadata_cache_holds_no_permit) {
    test_env::do_with_async([] (test_env& env) {
        auto s = sized_table(compression_parameters::no_compression());
        utils::chunked_vector<mutation> muts;
        for (int32_t pk = 0; pk < 4; ++pk) {
            mutation m(s, partition_key::from_single_value(*s, int32_type->decompose(pk)));
            for (int32_t i = 0; i < 2000; ++i) {
                // Few distinct values, so that the chunks have dictionaries.
                m.set_clustered_cell(clustering_key::from_single_value(*s, int32_type->decompose(i)), "v",
                        data_value(fmt::format("value {}", i % 7)), api::timestamp_type(1));
            }
            muts.push_back(std::move(m));
        }
        std::ranges::sort(muts, mutation_decorated_key_less_comparator());
        auto sst = write(env, s, muts, aa, [] (sstable_writer_config& cfg) {
            cfg.parquet_page_set_size = 1024;
            cfg.parquet_min_page_size = 256;
            cfg.parquet_row_group_size = 4096;
        });
        const auto& summary = sst->get_parquet_file_summary();
        BOOST_REQUIRE_GT(summary.row_groups(), 2);
        const auto& cache = sst->get_parquet_read_state().cache();
        const auto& stats = parquet::local_shard_stats;

        reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, "test", 100, 100 << 20, 100,
                utils::updateable_value<uint32_t>(2));
        auto stop = deferred_stop(semaphore);
        const auto initial = semaphore.available_resources();
        auto read = [&] {
            auto rd = sst->make_reader(s, semaphore.obtain_permit(s, "read", 0, db::no_timeout, {}).get(),
                    query::full_partition_range, s->full_slice());
            auto close = deferred_close(rd);
            while (rd().get()) {}
        };
        const auto dictionary_misses = stats.dictionary_cache_misses;
        read();
        BOOST_REQUIRE_GT(stats.dictionary_cache_misses, dictionary_misses);
        BOOST_REQUIRE_GT(cache.size(), summary.row_groups());
        BOOST_REQUIRE(semaphore.available_resources() == initial);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().current_permits, 0);

        // The entries stay, and readers that run at once find them or add
        // them once.
        const auto entries = cache.size();
        const auto bytes = cache.memory_usage();
        when_all_succeed(seastar::async(read), seastar::async(read)).get();
        BOOST_REQUIRE_EQUAL(cache.size(), entries);
        BOOST_REQUIRE_EQUAL(cache.memory_usage(), bytes);
        env.manager().get_cache_tracker().clear();
        when_all_succeed(seastar::async(read), seastar::async(read), seastar::async(read)).get();
        BOOST_REQUIRE_EQUAL(cache.size(), entries);
        BOOST_REQUIRE_EQUAL(cache.memory_usage(), bytes);
        BOOST_REQUIRE(semaphore.available_resources() == initial);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().current_permits, 0);
    }).get();
}
