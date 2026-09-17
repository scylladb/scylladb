/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

/**
 * Commitlog stress tests.
 *
 * The tests in commitlog_test.cc are almost exclusively *sequential* and
 * *homogeneous*: one writer, one table, entries of a single fixed size
 * (either a 13-byte string, max_record_size(), or 2 x max_record_size()).
 * The oversized ("fragmented") path is only exercised on its own, never
 * while normal writes are in flight, never in BATCH mode, never with
 * force_sync, never under disk pressure with a flush handler, and never
 * against a damaged or interleaved set of segments on replay.
 *
 * This file fills those gaps:
 *
 *  - many concurrent writers, several tables, heavy-tailed random entry
 *    sizes from 1 byte to 10 MB (plus exact boundary sizes) in the same
 *    log, in PERIODIC and BATCH mode, with and without O_DSYNC.
 *    Contents are verified byte for byte both live and after a simulated
 *    crash + restart, using a shared replay_state exactly as the real
 *    replayer does;
 *  - a single add_entries() batch that mixes dozens of tiny mutations with
 *    10 MB cells over three different schemas, checking that column
 *    mappings are placed such that the replayer can resolve every schema;
 *  - replay of a 10 MB fragmented entry whose middle segment was deleted
 *    or truncated (the complete entries around it must still replay);
 *  - fragment-id reuse across process lifetimes (a stale, incomplete
 *    fragmented entry from a previous run must not shadow an intact one);
 *  - an oversized write timing out under disk pressure, with rollback and
 *    continued use of the log;
 *  - small writers with short timeouts queued behind an oversized write
 *    that holds all request-controller memory;
 *  - mixed sizes under a tight disk limit with a "memtable flush"
 *    handler (stall detector);
 *  - end-to-end replay through commitlog_replayer into memtables of three
 *    CQL tables with mixed cell sizes.
 */

#include <boost/test/unit_test.hpp>

#include <deque>
#include <random>
#include <ranges>
#include <set>
#include <unordered_map>
#include <unordered_set>

#include <fmt/ranges.h>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/util/closeable.hh>

#include "utils/assert.hh"
#include "utils/UUID_gen.hh"
#include "test/lib/tmpdir.hh"
#include "test/lib/log.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/data_model.hh"
#include "test/lib/key_utils.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/test_utils.hh"
#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_replayer.hh"
#include "db/commitlog/commitlog_entry.hh"
#include "db/commitlog/rp_set.hh"
#include "readers/combined.hh"
#include "replica/database.hh"
#include "schema/schema_builder.hh"
#include "types/types.hh"

BOOST_AUTO_TEST_SUITE(commitlog_stress_test)

using namespace db;
using namespace std::chrono_literals;

namespace {

constexpr size_t MB = 1024 * 1024;
constexpr size_t KB = 1024;

table_id make_table_id() {
    return table_id(utils::UUID_gen::get_time_UUID());
}

// Deterministic byte stream. The content of every entry is a pure function
// of its seed, so we can verify 10 MB entries without keeping copies.
struct pattern {
    uint64_t state;
    explicit pattern(uint64_t seed) : state(seed * 0x9E3779B97F4A7C15ULL + 0x632BE59BD9B4E019ULL) {}
    uint8_t next() {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        return uint8_t(state >> 56);
    }
};

void write_pattern(commitlog::output& out, uint64_t seed, size_t size) {
    pattern p(seed);
    char buf[4096];
    while (size) {
        auto n = std::min(size, sizeof(buf));
        for (size_t i = 0; i < n; ++i) {
            buf[i] = char(p.next());
        }
        out.write(buf, n);
        size -= n;
    }
}

bool verify_pattern(const fragmented_temporary_buffer& buf, uint64_t seed, size_t size) {
    if (buf.size_bytes() != size) {
        return false;
    }
    pattern p(seed);
    for (auto& frag : buf) {
        for (auto c : frag) {
            if (uint8_t(c) != p.next()) {
                return false;
            }
        }
    }
    return true;
}

struct entry_info {
    size_t size;
    uint64_t seed;
};
using entry_map = std::unordered_map<replay_position, entry_info>;

future<rp_handle> add_pattern(commitlog& log, const table_id& id, size_t size, uint64_t seed, commitlog::force_sync sync,
        db::timeout_clock::time_point timeout = db::timeout_clock::time_point::max()) {
    return log.add_mutation(id, size, timeout, sync, [seed, size](commitlog::output& out) {
        write_pattern(out, seed, size);
    });
}

std::vector<sstring> sorted_by_id(std::vector<sstring> names) {
    std::ranges::sort(names, std::less(), [](const sstring& n) { return commitlog::descriptor(n).id; });
    return names;
}

struct replay_result {
    size_t found = 0;
    size_t verified = 0;
    size_t unexpected = 0;
    size_t truncations = 0;
    size_t corruptions = 0;
};

// Read all segments in id order through a single replay_state - this is how
// commitlog_replayer works and is what lets fragmented entries that span
// segments be reassembled. Every delivered entry is checked byte for byte.
future<replay_result> replay_and_verify(std::vector<sstring> names, const entry_map& expected, std::unordered_set<replay_position>* seen = nullptr) {
    replay_result res;
    commitlog::replay_state state;
    for (auto& name : sorted_by_id(std::move(names))) {
        try {
            co_await commitlog::read_log_file(state, name, commitlog::descriptor::FILENAME_PREFIX, [&](commitlog::buffer_and_replay_position buf_rp) -> future<> {
                ++res.found;
                auto i = expected.find(buf_rp.position);
                if (i == expected.end()) {
                    BOOST_ERROR(fmt::format("Unexpected entry at {} ({} bytes)", buf_rp.position, buf_rp.buffer.size_bytes()));
                    ++res.unexpected;
                    co_return;
                }
                if (verify_pattern(buf_rp.buffer, i->second.seed, i->second.size)) {
                    ++res.verified;
                } else {
                    BOOST_ERROR(fmt::format("Content mismatch for entry at {}: expected {} bytes, got {}", buf_rp.position, i->second.size, buf_rp.buffer.size_bytes()));
                }
                if (seen && !seen->emplace(buf_rp.position).second) {
                    BOOST_ERROR(fmt::format("Entry at {} delivered twice", buf_rp.position));
                }
                co_return;
            });
        } catch (commitlog::segment_truncation& e) {
            testlog.info("{}: truncation: {}", name, e.what());
            ++res.truncations;
        } catch (commitlog::segment_data_corruption_error& e) {
            testlog.info("{}: corruption: {}", name, e.what());
            ++res.corruptions;
        }
    }
    co_return res;
}

// Heavy-tailed size distribution: mostly tiny, some medium, some large
// (up to one full record), a few that must be fragmented, and the exact
// boundaries around max_record_size().
size_t random_entry_size(std::mt19937_64& rng, size_t max_record, size_t max_oversized) {
    auto r = rng() % 100;
    if (r < 40) {
        return 1 + rng() % 64;
    }
    if (r < 70) {
        return 64 + rng() % (64 * KB);
    }
    if (r < 88) {
        return 64 * KB + rng() % (max_record - 64 * KB);
    }
    if (r == 88) {
        return max_record;
    }
    if (r == 89) {
        return max_record - 1;
    }
    if (r == 90) {
        return max_record + 1;
    }
    return max_record + 1 + rng() % (max_oversized - max_record);
}

struct planned_write {
    table_id table;
    size_t size;
    uint64_t seed;
    commitlog::force_sync sync;
};

commitlog::config make_stress_config(const tmpdir& tmp, size_t segment_mb, size_t total_mb_per_shard) {
    commitlog::config cfg;
    cfg.commit_log_location = tmp.path().string();
    cfg.commitlog_segment_size_in_mb = segment_mb;
    cfg.commitlog_total_space_in_mb = total_mb_per_shard * this_smp_shard_count();
    cfg.allow_going_over_size_limit = false;
    cfg.allow_fragmented_entries = true;
    // NB: leave warn_about_segments_left_on_disk_after_shutdown at its
    // default (true). It is load-bearing, not just cosmetic: on a dirty
    // shutdown ~segment only disengages a fragmented entry's _extended_segments
    // handles (so the trailing fragment segments are *kept* on disk) when this
    // is true. Forcing it false makes release()/shutdown() delete the trailing
    // fragment segments, which does NOT model a real crash (kill -9 runs no
    // destructors and preserves every segment).
    return cfg;
}

// Drop all handles without discarding (as if the memtables were never
// flushed), orphan the segments and shut down. The files are left on disk
// for a subsequent commitlog instance to find via get_segments_to_replay().
future<> simulate_crash(commitlog& log, std::vector<rp_handle>& handles) {
    for (auto& h : handles) {
        h.release();
    }
    handles.clear();
    co_await log.release();
    co_await log.shutdown();
}

} // anonymous namespace

/**
 * Concurrent writers, several tables, sizes from 1 byte to 10 MB in the same
 * log. Verifies every byte, live and after a simulated crash + restart.
 */
static future<> do_test_mixed_size_concurrent_writers(commitlog::sync_mode mode, bool o_dsync, size_t n_fibers, size_t per_fiber) {
    tmpdir tmp;
    // 4 MB segments -> max_record ~2 MB, so a 10 MB entry fragments over 3+ segments.
    auto cfg = make_stress_config(tmp, 4, 2048);
    cfg.mode = mode;
    cfg.use_o_dsync = o_dsync;

    auto seed = tests::random::get_int<uint64_t>();
    testlog.info("mode={} o_dsync={} seed={}", mode == commitlog::sync_mode::BATCH ? "batch" : "periodic", o_dsync, seed);
    std::mt19937_64 rng(seed);

    std::vector<table_id> tables;
    for (int i = 0; i < 6; ++i) {
        tables.push_back(make_table_id());
    }

    entry_map expected;
    std::vector<rp_handle> handles;
    constexpr size_t max_oversized = 10 * MB + 4096;

    {
        auto log = co_await commitlog::create_commitlog(cfg);
        auto max_record = log.max_record_size();

        // Draw the whole plan up front so the random sequence does not depend on scheduling.
        std::vector<std::vector<planned_write>> plan(n_fibers);
        for (size_t f = 0; f < n_fibers; ++f) {
            for (size_t i = 0; i < per_fiber; ++i) {
                plan[f].push_back(planned_write{
                    tables[rng() % tables.size()],
                    random_entry_size(rng, max_record, max_oversized),
                    rng(),
                    commitlog::force_sync(rng() % 10 == 0),
                });
            }
        }
        // Fiber 0 additionally hits exact boundaries: chunk size, sector size, record size.
        for (auto base : { size_t(128 * KB), size_t(512), size_t(4096), max_record }) {
            for (int d = -12; d <= 12; d += 4) {
                auto sz = size_t(std::max<ssize_t>(1, ssize_t(base) + d));
                plan[0].push_back(planned_write{tables[0], sz, rng(), commitlog::force_sync::no});
            }
        }

        size_t total_bytes = 0;
        for (auto& p : plan) {
            for (auto& w : p) {
                total_bytes += w.size;
            }
        }
        testlog.info("writing {} entries, {} MB total", n_fibers * per_fiber, total_bytes / MB);

        co_await coroutine::parallel_for_each(std::views::iota(size_t(0), n_fibers), [&](size_t f) -> future<> {
            for (auto& w : plan[f]) {
                auto h = co_await with_timeout(db::timeout_clock::now() + 120s, add_pattern(log, w.table, w.size, w.seed, w.sync));
                BOOST_REQUIRE(expected.emplace(h.rp(), entry_info{w.size, w.seed}).second);
                handles.emplace_back(std::move(h));
            }
        });

        co_await log.sync_all_segments();

        // Reading the live, still-active log is best-effort: the last
        // (still-allocating) segment can withhold the tail of a fragmented
        // entry whose final fragment has not been finalized on disk yet.
        // What must hold live is that everything *delivered* is byte-correct
        // and expected. The durable contract - every synced entry survives a
        // crash - is checked by the crash + restart replay below, which must
        // reproduce every single entry.
        auto live = co_await replay_and_verify(log.get_active_segment_names(), expected);
        BOOST_CHECK_EQUAL(live.unexpected, 0);
        BOOST_CHECK_LE(live.verified, expected.size());

        co_await simulate_crash(log, handles);
    }

    auto log = co_await commitlog::create_commitlog(cfg);
    auto to_replay = co_await log.get_segments_to_replay();
    BOOST_REQUIRE(!to_replay.empty());
    std::unordered_set<replay_position> seen;
    auto res = co_await replay_and_verify(to_replay, expected, &seen);
    if (res.verified != expected.size()) {
        size_t n_missing_big = 0, n_missing_small = 0;
        for (auto& [rp, info] : expected) {
            if (!seen.contains(rp)) {
                testlog.error("MISSING after replay: rp={} size={}", rp, info.size);
                (info.size > 2 * MB ? n_missing_big : n_missing_small)++;
            }
        }
        testlog.error("missing {} entries: {} large (>2MB), {} small", expected.size() - res.verified, n_missing_big, n_missing_small);
    }
    BOOST_CHECK_EQUAL(res.verified, expected.size());
    BOOST_CHECK_EQUAL(res.unexpected, 0);
    BOOST_CHECK_EQUAL(res.truncations, 0);
    BOOST_CHECK_EQUAL(res.corruptions, 0);
    co_await log.shutdown();
    co_await log.clear();
}

SEASTAR_TEST_CASE(test_mixed_size_concurrent_writers_periodic) {
    co_await do_test_mixed_size_concurrent_writers(commitlog::sync_mode::PERIODIC, false, 8, 24);
}

SEASTAR_TEST_CASE(test_mixed_size_concurrent_writers_periodic_odsync) {
    co_await do_test_mixed_size_concurrent_writers(commitlog::sync_mode::PERIODIC, true, 8, 24);
}

SEASTAR_TEST_CASE(test_mixed_size_concurrent_writers_batch) {
    co_await do_test_mixed_size_concurrent_writers(commitlog::sync_mode::BATCH, true, 6, 12);
}

/**
 * One add_entries() batch mixing dozens of tiny mutations with 10 MB cells
 * over three schemas. The batch is far too large for one entry, so it takes
 * the oversized path where small sub-entries use the normal writer and
 * huge ones are fragmented across segments. Every mutation must round-trip
 * and the column mapping must be resolvable in replay order, i.e. the first
 * time a schema version is seen in the stream it must carry its mapping.
 */
namespace {

schema_ptr make_test_schema(const char* name, int variant) {
    schema_builder b(this_smp_shard_count(), "ks", name);
    switch (variant) {
    case 0:
        b.with_column("pk", bytes_type, column_kind::partition_key);
        b.with_column("ck", int32_type, column_kind::clustering_key);
        b.with_column("v", bytes_type);
        break;
    case 1:
        b.with_column("pk", utf8_type, column_kind::partition_key);
        b.with_column("ck", utf8_type, column_kind::clustering_key);
        b.with_column("s", bytes_type, column_kind::static_column);
        b.with_column("v1", bytes_type);
        b.with_column("v2", bytes_type);
        break;
    default:
        b.with_column("pk", bytes_type, column_kind::partition_key);
        b.with_column("v", bytes_type);
        break;
    }
    return b.build();
}

mutation make_test_mutation(schema_ptr s, int variant, size_t cell_size, std::mt19937_64& rng) {
    auto dk = tests::generate_partition_key(s);
    tests::data_model::mutation_description md(dk.key().explode(*s));
    auto rows = 1 + rng() % 3;
    switch (variant) {
    case 0:
        for (size_t r = 0; r < rows; ++r) {
            md.add_clustered_cell({int32_type->decompose(int32_t(rng() % 100000))}, "v", tests::random::get_bytes(cell_size));
        }
        break;
    case 1:
        md.add_static_cell("s", tests::random::get_bytes(std::min<size_t>(cell_size, 777)));
        for (size_t r = 0; r < rows; ++r) {
            tests::data_model::mutation_description::key ck = {utf8_type->decompose(data_value(format("ck{}", rng() % 100000)))};
            md.add_clustered_cell(ck, "v1", tests::random::get_bytes(cell_size));
            md.add_clustered_cell(ck, "v2", tests::random::get_bytes(rng() % 128));
        }
        break;
    default:
        md.add_clustered_cell({}, "v", tests::random::get_bytes(cell_size));
        break;
    }
    return md.build(s);
}

}

SEASTAR_TEST_CASE(test_add_entries_batch_mixing_tiny_and_huge_mutations_across_schemas) {
    tmpdir tmp;
    // 8 MB segments -> 4 MB max entry. Two 10 MB cells force fragmentation.
    auto cfg = make_stress_config(tmp, 8, 1024);

    auto seed = tests::random::get_int<uint64_t>();
    testlog.info("seed={}", seed);
    std::mt19937_64 rng(seed);

    auto log = co_await commitlog::create_commitlog(cfg);

    std::vector<schema_ptr> schemas = { make_test_schema("t_ck_blob", 0), make_test_schema("t_text_static", 1), make_test_schema("t_pk_only", 2) };

    // Cell sizes: two 10 MB, three ~700 KB, the rest tiny. Shuffle them.
    std::vector<size_t> cell_sizes = { 10 * MB, 10 * MB, 700 * KB, 700 * KB, 700 * KB };
    while (cell_sizes.size() < 40) {
        cell_sizes.push_back(1 + rng() % 64);
    }
    std::shuffle(cell_sizes.begin(), cell_sizes.end(), rng);

    const auto n = cell_sizes.size();
    std::vector<mutation> muts;
    utils::chunked_vector<frozen_mutation> fms;
    utils::chunked_vector<commitlog_mutation_entry_writer> writers;
    muts.reserve(n);
    fms.reserve(n);
    writers.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        auto variant = int(i % schemas.size());
        muts.push_back(make_test_mutation(schemas[variant], variant, cell_sizes[i], rng));
        fms.emplace_back(freeze(muts.back()));
        writers.emplace_back(schemas[variant], fms.back(), commitlog::force_sync(rng() % 5 == 0));
    }

    auto handles = co_await log.add_entries(std::move(writers), db::timeout_clock::now() + 300s);
    BOOST_REQUIRE_EQUAL(handles.size(), n);

    std::unordered_map<replay_position, size_t> rp2idx;
    std::set<segment_id_type> segment_ids;
    for (size_t i = 0; i < n; ++i) {
        BOOST_REQUIRE(rp2idx.emplace(handles[i].rp(), i).second);
        segment_ids.insert(handles[i].rp().id);
    }
    // With two 10 MB cells and 8 MB segments the batch must have spilled into more than one segment.
    BOOST_CHECK_GE(segment_ids.size(), 2);

    co_await log.sync_all_segments();

    std::unordered_set<table_schema_version> known_versions;
    size_t found = 0;
    commitlog::replay_state state;
    for (auto& name : sorted_by_id(log.get_active_segment_names())) {
        co_await commitlog::read_log_file(state, name, commitlog::descriptor::FILENAME_PREFIX, [&](commitlog::buffer_and_replay_position buf_rp) -> future<> {
            auto i = rp2idx.find(buf_rp.position);
            if (i == rp2idx.end()) {
                BOOST_ERROR(fmt::format("Unexpected entry at {}", buf_rp.position));
                co_return;
            }
            ++found;
            commitlog_entry_reader r(buf_rp.buffer);
            BOOST_REQUIRE(std::holds_alternative<mutation_entry>(r.entry().item));
            const auto& me = std::get<mutation_entry>(r.entry().item);
            const auto& fm = me.mutation();
            // This is exactly the rule commitlog_replayer applies: the first
            // entry of a schema version must carry the column mapping.
            if (!known_versions.contains(fm.schema_version())) {
                BOOST_CHECK_MESSAGE(me.mapping().has_value(), fmt::format("Entry {} at {} is the first of schema version {} but has no column mapping", i->second, buf_rp.position, fm.schema_version()));
                known_versions.insert(fm.schema_version());
            }
            auto& m = muts[i->second];
            BOOST_CHECK_EQUAL(fm.column_family_id(), m.schema()->id());
            BOOST_CHECK_EQUAL(fm.unfreeze(m.schema()), m);
            co_return;
        });
    }
    BOOST_CHECK_EQUAL(found, n);

    for (auto& h : handles) {
        h.release();
    }
    co_await log.shutdown();
    co_await log.clear();
}

/**
 * A 10 MB entry spread over ~11 1 MB segments, with small entries before and
 * after it. One segment in the middle of the fragmented entry is deleted or
 * truncated ("lost in a crash"). On replay the incomplete entry must not be
 * delivered, but all complete entries around it must - and nothing may crash.
 */
static future<> do_test_fragmented_entry_with_damaged_middle_segment(bool truncate) {
    tmpdir tmp;
    auto cfg = make_stress_config(tmp, 1, 128);

    auto uuid = make_table_id();
    entry_map smalls;
    replay_position big_rp;
    constexpr size_t big_size = 10 * MB;
    constexpr uint64_t big_seed = 0xb16;
    sstring victim;

    {
        auto log = co_await commitlog::create_commitlog(cfg);
        for (int i = 0; i < 10; ++i) {
            auto h = co_await add_pattern(log, uuid, 64 + i, 100 + i, commitlog::force_sync::no);
            smalls.emplace(h.rp(), entry_info{size_t(64 + i), uint64_t(100 + i)});
            h.release();
        }
        co_await log.force_new_active_segment();

        auto hb = co_await add_pattern(log, uuid, big_size, big_seed, commitlog::force_sync::no);
        big_rp = hb.release();

        std::optional<segment_id_type> first_trailing_id;
        for (int i = 0; i < 10; ++i) {
            auto h = co_await add_pattern(log, uuid, 64 + i, 200 + i, commitlog::force_sync::no);
            if (!first_trailing_id) {
                first_trailing_id = h.rp().id;
            }
            smalls.emplace(h.rp(), entry_info{size_t(64 + i), uint64_t(200 + i)});
            h.release();
        }
        co_await log.sync_all_segments();

        // Segments strictly inside the fragmented entry (they hold nothing else).
        std::vector<sstring> inner;
        for (auto& name : sorted_by_id(log.get_active_segment_names())) {
            auto id = commitlog::descriptor(name).id;
            if (id > big_rp.id && id < *first_trailing_id) {
                inner.push_back(name);
            }
        }
        testlog.info("big entry at {}, spans up to segment {}, {} inner segments", big_rp, *first_trailing_id, inner.size());
        BOOST_REQUIRE_GE(inner.size(), 3);
        victim = inner[inner.size() / 2];

        co_await log.release();
        co_await log.shutdown();
    }

    if (truncate) {
        auto f = co_await open_file_dma(victim, open_flags::rw);
        auto size = co_await f.size();
        co_await f.truncate(align_down<uint64_t>(size / 2, 4096));
        co_await f.close();
    } else {
        co_await remove_file(victim);
    }

    auto log = co_await commitlog::create_commitlog(cfg);
    auto to_replay = co_await log.get_segments_to_replay();
    entry_map expected = smalls;
    expected.emplace(big_rp, entry_info{big_size, big_seed});
    std::unordered_set<replay_position> seen;
    auto res = co_await replay_and_verify(to_replay, expected, &seen);

    BOOST_CHECK_EQUAL(res.unexpected, 0);
    BOOST_CHECK_MESSAGE(!seen.contains(big_rp), "incomplete fragmented entry must not be replayed");
    BOOST_CHECK_EQUAL(res.verified, smalls.size());
    if (truncate) {
        BOOST_CHECK_GE(res.truncations + res.corruptions, 1);
    }

    co_await log.shutdown();
    co_await log.clear();
}

SEASTAR_TEST_CASE(test_fragmented_entry_with_deleted_middle_segment) {
    co_await do_test_fragmented_entry_with_damaged_middle_segment(false);
}

SEASTAR_TEST_CASE(test_fragmented_entry_with_truncated_middle_segment) {
    co_await do_test_fragmented_entry_with_damaged_middle_segment(true);
}

/**
 * Minimal, deterministic probe: a single fiber, single table, writing a
 * strict alternation of small entries and oversized (multi-segment,
 * fragmented) entries, then a clean sync + crash + restart replay. No
 * concurrency, no randomness. Every synced entry - small and large - must
 * come back. This isolates "do interleaved small entries and fragmented
 * entries survive replay together" from any scheduling effects.
 */
SEASTAR_TEST_CASE(test_interleaved_small_and_fragmented_entries_replay) {
    tmpdir tmp;
    auto cfg = make_stress_config(tmp, 1, 256); // 1 MB segments -> ~0.5 MB max record
    auto uuid = make_table_id();

    entry_map expected;
    std::vector<rp_handle> handles;
    constexpr int n_big = 6;
    const size_t big_size = 4 * MB + 12345; // spans ~9 segments, fragmented

    {
        auto log = co_await commitlog::create_commitlog(cfg);
        uint64_t seed = 1;
        for (int i = 0; i < n_big; ++i) {
            // small before
            auto hs = co_await add_pattern(log, uuid, 40 + i, seed++, commitlog::force_sync::no);
            BOOST_REQUIRE(expected.emplace(hs.rp(), entry_info{size_t(40 + i), seed - 1}).second);
            handles.emplace_back(std::move(hs));
            // big (fragmented)
            auto hb = co_await add_pattern(log, uuid, big_size, seed++, commitlog::force_sync::no);
            BOOST_REQUIRE(expected.emplace(hb.rp(), entry_info{big_size, seed - 1}).second);
            handles.emplace_back(std::move(hb));
            // small after
            auto ha = co_await add_pattern(log, uuid, 50 + i, seed++, commitlog::force_sync::no);
            BOOST_REQUIRE(expected.emplace(ha.rp(), entry_info{size_t(50 + i), seed - 1}).second);
            handles.emplace_back(std::move(ha));
        }
        co_await log.sync_all_segments();
        co_await simulate_crash(log, handles);
    }

    auto log = co_await commitlog::create_commitlog(cfg);
    auto to_replay = co_await log.get_segments_to_replay();
    std::unordered_set<replay_position> seen;
    auto res = co_await replay_and_verify(to_replay, expected, &seen);
    for (auto& [rp, info] : expected) {
        if (!seen.contains(rp)) {
            testlog.error("MISSING after replay: rp={} size={}", rp, info.size);
        }
    }
    BOOST_CHECK_EQUAL(res.verified, expected.size());
    BOOST_CHECK_EQUAL(res.unexpected, 0);
    BOOST_CHECK_EQUAL(res.truncations, 0);
    BOOST_CHECK_EQUAL(res.corruptions, 0);
    co_await log.shutdown();
    co_await log.clear();
}

/**
 * Fragment ids are a per-shard counter that restarts from 1 in every
 * process (segment_manager::_frag_id_counter) and is written raw into each
 * fragment. commitlog::read_log_file reassembles a fragmented entry by
 * grouping fragments in replay_state::fragment_state keyed *only* on that
 * id, and one replay_state is shared across every segment of a shard,
 * regardless of which process wrote it.
 *
 * Segments from two process lifetimes can legitimately coexist on disk:
 * lifetime 1 writes a huge (multi-segment) mutation and crashes before its
 * last fragment reaches disk and before its segments are flushed; lifetime 2
 * boots, writes another huge mutation to new (higher-id) segments, and
 * crashes before the old segments are deleted; lifetime 3 then replays both
 * generations together.
 *
 * Because the fragment counter reset, the incomplete entry A (lifetime 1)
 * and the intact entry B (lifetime 2) both carry fragment id 1. Without the
 * fix their fragments land in the same fragment_state[1] bucket with
 * overlapping offsets, the offset-join can never collapse the bucket to a
 * single complete entry, and B - a fully synced, client-acknowledged write -
 * is silently dropped on replay.
 *
 * The fix (in commitlog::read_log_file) discards any stale accumulation for a
 * fragment id when a fresh offset-0 fragment for that id arrives, since
 * segments are replayed in ascending id order and an offset-0 fragment is
 * always the first fragment of its entry. This test is the regression guard:
 * B must survive.
 */
SEASTAR_TEST_CASE(test_fragment_id_reuse_across_restarts_does_not_lose_entries) {
    tmpdir tmp;
    auto cfg = make_stress_config(tmp, 1, 128);
    auto uuid = make_table_id();

    // Lifetime 1: fragmented entry A, whose last segment is lost.
    replay_position a_rp;
    constexpr size_t a_size = 2 * MB + 512 * KB;
    {
        auto log = co_await commitlog::create_commitlog(cfg);
        auto h = co_await add_pattern(log, uuid, a_size, 0xa, commitlog::force_sync::no);
        a_rp = h.release();
        co_await log.sync_all_segments();
        auto names = sorted_by_id(log.get_active_segment_names());
        BOOST_REQUIRE_GE(names.size(), 3);
        auto last = names.back();
        co_await log.release();
        co_await log.shutdown();
        co_await remove_file(last);
    }

    // Lifetime 2: intact fragmented entry B. Its fragment id restarts at 1, same as A.
    replay_position b_rp;
    constexpr size_t b_size = 3 * MB;
    {
        auto log = co_await commitlog::create_commitlog(cfg);
        auto h = co_await add_pattern(log, uuid, b_size, 0xb, commitlog::force_sync::no);
        b_rp = h.release();
        co_await log.sync_all_segments();
        co_await log.release();
        co_await log.shutdown();
    }
    BOOST_REQUIRE_GT(b_rp.id, a_rp.id);

    // Lifetime 3: replay everything in id order with one replay_state, as the replayer does.
    auto log = co_await commitlog::create_commitlog(cfg);
    auto to_replay = co_await log.get_segments_to_replay();
    entry_map expected = { { a_rp, entry_info{a_size, 0xa} }, { b_rp, entry_info{b_size, 0xb} } };

    std::unordered_set<replay_position> seen;
    auto res = co_await replay_and_verify(to_replay, expected, &seen);
    BOOST_CHECK_EQUAL(res.unexpected, 0);
    BOOST_CHECK_MESSAGE(!seen.contains(a_rp), "incomplete entry A must not be replayed");
    BOOST_CHECK_MESSAGE(seen.contains(b_rp), "intact entry B was not replayed: its fragment id collides with the stale fragments of A from the previous lifetime");

    // Control: B alone replays fine.
    std::vector<sstring> only_b;
    for (auto& name : to_replay) {
        if (commitlog::descriptor(name).id >= b_rp.id) {
            only_b.push_back(name);
        }
    }
    std::unordered_set<replay_position> seen_b;
    auto res_b = co_await replay_and_verify(only_b, expected, &seen_b);
    BOOST_CHECK(seen_b.contains(b_rp));
    BOOST_CHECK_EQUAL(res_b.verified, 1);

    co_await log.shutdown();
    co_await log.clear();
}

/**
 * An oversized write that cannot get enough segments must honour its
 * timeout (not stall), roll back cleanly, and leave the log usable: after
 * the "memtable flush" releases space the same write succeeds and replay
 * shows exactly the successful entries.
 */
SEASTAR_TEST_CASE(test_oversized_write_timeout_rolls_back_and_log_stays_usable) {
    tmpdir tmp;
    // 4 x 1 MB segments per shard.
    auto cfg = make_stress_config(tmp, 1, 4);
    cfg.commitlog_sync_period_in_ms = 10;

    auto log = co_await commitlog::create_commitlog(cfg);
    auto uuid = make_table_id();

    // Pin ~2 full segments plus the start of a third, as unflushed memtable data would.
    rp_set pinned;
    entry_map smalls;
    uint64_t seed = 1000;
    while (pinned.size() < 3) {
        auto h = co_await add_pattern(log, uuid, 32 * KB, seed, commitlog::force_sync::no);
        smalls.emplace(h.rp(), entry_info{32 * KB, seed});
        ++seed;
        pinned.put(std::move(h));
    }

    // ~2 MB free, 2.5 MB needed: passes the up-front disk-space check but must block on a new segment.
    constexpr size_t big_size = 2 * MB + 512 * KB;
    auto start = std::chrono::steady_clock::now();
    bool timed_out = false;
    try {
        auto h = co_await add_pattern(log, uuid, big_size, 0xbad, commitlog::force_sync::no, db::timeout_clock::now() + 2s);
        h.release();
        BOOST_FAIL("oversized write succeeded although the disk limit should have blocked it");
    } catch (timed_out_error&) {
        timed_out = true;
    } catch (std::invalid_argument& e) {
        BOOST_FAIL(fmt::format("oversized write was rejected up front instead of timing out: {}", e.what()));
    }
    auto elapsed = std::chrono::steady_clock::now() - start;
    BOOST_CHECK(timed_out);
    BOOST_CHECK_LT(std::chrono::duration_cast<std::chrono::seconds>(elapsed).count(), 30);
    BOOST_CHECK_EQUAL(log.get_num_active_allocations(), 0);

    // The log must still accept normal writes after the rollback.
    auto hs = co_await add_pattern(log, uuid, 100, 0x5, commitlog::force_sync::yes, db::timeout_clock::now() + 30s);
    smalls.emplace(hs.rp(), entry_info{100, 0x5});
    pinned.put(std::move(hs));

    // "Memtable flush": release the pinned data, then retry.
    log.discard_completed_segments(uuid, pinned);
    auto hb = co_await add_pattern(log, uuid, big_size, 0x600d, commitlog::force_sync::no, db::timeout_clock::now() + 60s);
    auto big_rp = hb.release();
    co_await log.sync_all_segments();

    entry_map expected = smalls;
    expected.emplace(big_rp, entry_info{big_size, 0x600d});
    std::unordered_set<replay_position> seen;
    // Read the live log (best-effort at the active tail): the retried big
    // entry must be present, and crucially nothing from the rolled-back
    // attempt may surface as an unexpected or mismatched entry.
    auto res = co_await replay_and_verify(log.get_active_segment_names(), expected, &seen);
    BOOST_CHECK(seen.contains(big_rp));
    BOOST_CHECK_EQUAL(res.unexpected, 0);

    co_await log.shutdown();
    co_await log.clear();
}

/**
 * While an oversized write is queued for (or holds) all request-controller
 * memory, small writes with short timeouts must time out rather than wedge,
 * and the log must be healthy afterwards.
 */
SEASTAR_TEST_CASE(test_small_writers_time_out_behind_oversized_write) {
    tmpdir tmp;
    auto cfg = make_stress_config(tmp, 1, 128);

    auto log = co_await commitlog::create_commitlog(cfg);
    auto uuid = make_table_id();

    size_t timed_out = 0, succeeded = 0, other = 0;
    log.set_oversized_pre_wait_memory_func([&]() -> future<> {
        // We are now queued for every unit of the request controller.
        std::vector<future<>> fs;
        for (int i = 0; i < 16; ++i) {
            fs.push_back(add_pattern(log, uuid, 100, 1000 + i, commitlog::force_sync::no, db::timeout_clock::now() + 100ms).then_wrapped([&](future<rp_handle> f) {
                try {
                    auto h = f.get();
                    h.release();
                    ++succeeded;
                } catch (timed_out_error&) {
                    ++timed_out;
                } catch (...) {
                    testlog.error("unexpected exception: {}", std::current_exception());
                    ++other;
                }
            }));
        }
        auto results = co_await when_all(fs.begin(), fs.end());
        for (auto& f : results) {
            f.get();
        }
    });

    constexpr size_t big_size = 2 * MB + 512 * KB;
    auto hb = co_await with_timeout(db::timeout_clock::now() + 60s, add_pattern(log, uuid, big_size, 0xb16, commitlog::force_sync::no));
    log.set_oversized_pre_wait_memory_func({});
    auto big_rp = hb.release();

    testlog.info("small writers: {} timed out, {} succeeded, {} other", timed_out, succeeded, other);
    BOOST_CHECK_EQUAL(other, 0);
    BOOST_CHECK_EQUAL(timed_out + succeeded, 16);
    BOOST_CHECK_GT(timed_out, 0);

    // Log must be healthy: a normal write, then everything is readable.
    auto hs = co_await with_timeout(db::timeout_clock::now() + 30s, add_pattern(log, uuid, 4000, 0x4000, commitlog::force_sync::yes));
    auto small_rp = hs.release();
    co_await log.sync_all_segments();
    BOOST_CHECK_EQUAL(log.get_num_active_allocations(), 0);

    entry_map expected = { { big_rp, entry_info{big_size, 0xb16} }, { small_rp, entry_info{4000, 0x4000} } };
    std::unordered_set<replay_position> seen;
    auto res = co_await replay_and_verify(log.get_active_segment_names(), expected, &seen);
    BOOST_CHECK(seen.contains(big_rp));
    BOOST_CHECK(seen.contains(small_rp));
    // Timed-out small writes never got a replay position; if any of them was
    // still written we would see it here as unexpected.
    BOOST_CHECK_EQUAL(res.unexpected, 0);

    co_await log.shutdown();
    co_await log.clear();
}

/**
 * Mixed sizes under a tight disk limit with a flush handler that behaves
 * like memtable flushing (asynchronous, slightly delayed). Detects stalls
 * and footprint accounting drift.
 */
SEASTAR_TEST_CASE(test_mixed_sizes_under_disk_limit_with_flush_handler) {
    tmpdir tmp;
    // 16 x 1 MB per shard. Oversized entries up to 6 MB need a big chunk of it.
    auto cfg = make_stress_config(tmp, 1, 16);
    cfg.commitlog_sync_period_in_ms = 10;

    auto seed = tests::random::get_int<uint64_t>();
    testlog.info("seed={}", seed);
    std::mt19937_64 rng(seed);

    auto log = co_await commitlog::create_commitlog(cfg);
    auto max_record = log.max_record_size();
    const auto segment_size = cfg.commitlog_segment_size_in_mb * MB;

    struct table_state {
        rp_set current;
        std::deque<rp_set> flushing;
    };
    std::vector<table_id> tables;
    std::unordered_map<table_id, table_state> state;
    for (int i = 0; i < 4; ++i) {
        tables.push_back(make_table_id());
        state[tables.back()];
    }

    size_t flush_requests = 0;
    size_t discards = 0;
    timer<> flush_timer;
    flush_timer.set_callback([&] {
        for (auto& [id, ts] : state) {
            while (!ts.flushing.empty()) {
                log.discard_completed_segments(id, ts.flushing.front());
                ts.flushing.pop_front();
                ++discards;
            }
        }
    });
    auto anchor = log.add_flush_handler([&](cf_id_type id, replay_position pos) {
        ++flush_requests;
        auto& ts = state[id];
        if (!ts.current.empty()) {
            ts.flushing.emplace_back(std::exchange(ts.current, rp_set{}));
        }
        if (!flush_timer.armed()) {
            flush_timer.arm(10ms);
        }
    });

    constexpr size_t n_fibers = 6, per_fiber = 30;
    std::vector<std::vector<planned_write>> plan(n_fibers);
    for (size_t f = 0; f < n_fibers; ++f) {
        for (size_t i = 0; i < per_fiber; ++i) {
            plan[f].push_back(planned_write{
                tables[rng() % tables.size()],
                random_entry_size(rng, max_record, 6 * MB),
                rng(),
                commitlog::force_sync(rng() % 8 == 0),
            });
        }
    }

    size_t writes = 0;
    bool stalled = false;
    uint64_t max_footprint = 0;
    co_await coroutine::parallel_for_each(std::views::iota(size_t(0), n_fibers), [&](size_t f) -> future<> {
        for (auto& w : plan[f]) {
            if (stalled) {
                co_return;
            }
            try {
                auto h = co_await with_timeout(db::timeout_clock::now() + 60s, add_pattern(log, w.table, w.size, w.seed, w.sync));
                state[w.table].current.put(std::move(h));
                ++writes;
            } catch (timed_out_error&) {
                stalled = true;
                BOOST_ERROR(fmt::format("write of {} bytes stalled for 60s: footprint={} limit={} active_allocs={} blocked_on_new_segment={} flush_requests={} discards={}",
                        w.size, log.disk_footprint(), log.disk_limit(), log.get_num_active_allocations(), log.get_num_blocked_on_new_segment(), flush_requests, discards));
                co_return;
            }
            max_footprint = std::max(max_footprint, log.disk_footprint());
        }
    });

    BOOST_REQUIRE(!stalled);
    BOOST_CHECK_EQUAL(writes, n_fibers * per_fiber);
    BOOST_CHECK_GT(flush_requests, 0);
    testlog.info("max footprint {} MB, limit {} MB, flush requests {}, discards {}", max_footprint / MB, log.disk_limit() / MB, flush_requests, discards);
    // May exceed the limit by at most one segment (the one being allocated).
    BOOST_CHECK_LE(max_footprint, log.disk_limit() + segment_size);

    // Drain: flush everything and make sure the log ends up (nearly) empty.
    flush_timer.cancel();
    for (auto& [id, ts] : state) {
        if (!ts.current.empty()) {
            ts.flushing.emplace_back(std::exchange(ts.current, rp_set{}));
        }
        while (!ts.flushing.empty()) {
            log.discard_completed_segments(id, ts.flushing.front());
            ts.flushing.pop_front();
        }
    }
    co_await log.sync_all_segments();
    co_await log.wait_for_pending_deletes();
    BOOST_CHECK_EQUAL(log.get_num_active_allocations(), 0);
    BOOST_CHECK_EQUAL(log.get_num_dirty_segments(), 0);
    BOOST_CHECK_LE(log.get_active_segment_names().size(), 1);

    anchor.unregister();
    co_await log.shutdown();
    co_await log.clear();
}

/**
 * End to end: three CQL tables with different schemas, mutations with
 * cells from 8 bytes to 10 MB written straight to the table commitlog
 * (bypassing memtables), then recovered by commitlog_replayer. Memtables
 * must end up holding exactly the merged mutations.
 */
SEASTAR_TEST_CASE(test_replayer_restores_mixed_size_mutations_across_tables) {
    cql_test_config cfg;
    // 4 MB segments: 10 MB cells must be fragmented.
    cfg.db_config->commitlog_segment_size_in_mb(4);
    cfg.db_config->commitlog_use_fragmented_entries(true);

    return do_with_cql_env_thread([](cql_test_env& env) {
        env.execute_cql("create table t1 (pk text primary key, v blob)").get();
        env.execute_cql("create table t2 (pk int, ck int, v blob, primary key (pk, ck))").get();
        env.execute_cql("create table t3 (pk blob primary key, a int, b text, c blob)").get();

        auto& db = env.local_db();
        struct tbl {
            replica::table* t;
            schema_ptr s;
            std::vector<mutation> expected;
        };
        std::vector<tbl> tables;
        for (auto name : { "t1", "t2", "t3" }) {
            auto& t = db.find_column_family("ks", name);
            tables.push_back(tbl{&t, t.schema(), {}});
        }
        auto& cl = *tables[0].t->commitlog();
        BOOST_REQUIRE(cl.active_config().allow_fragmented_entries);
        auto max_record = cl.max_record_size();

        auto seed = tests::random::get_int<uint64_t>();
        testlog.info("seed={} max_record={}", seed, max_record);
        std::mt19937_64 rng(seed);

        const size_t sizes[] = { 8, 4 * KB, 300 * KB, max_record + 1, 10 * MB };
        constexpr int n = 25;
        for (int i = 0; i < n; ++i) {
            auto& tb = tables[i % tables.size()];
            auto cell_size = sizes[(i / tables.size()) % std::size(sizes)];
            auto dk = tests::generate_partition_key(tb.s);
            tests::data_model::mutation_description md(dk.key().explode(*tb.s));
            auto cell = tests::random::get_bytes(cell_size);
            switch (i % tables.size()) {
            case 0:
                md.add_clustered_cell({}, "v", cell);
                break;
            case 1:
                md.add_clustered_cell({int32_type->decompose(int32_t(rng() % 4))}, "v", cell);
                break;
            default:
                md.add_clustered_cell({}, "a", int32_type->decompose(int32_t(i)));
                md.add_clustered_cell({}, "b", utf8_type->decompose(data_value(format("row{}", i))));
                md.add_clustered_cell({}, "c", cell);
                break;
            }
            auto m = md.build(tb.s);
            auto fm = freeze(m);
            commitlog_mutation_entry_writer w(tb.s, fm, commitlog::force_sync(i % 4 == 0));
            auto h = cl.add_entry(tb.s->id(), w, db::no_timeout).get();
            h.release();
            tb.expected.push_back(std::move(m));
        }

        for (auto& tb : tables) {
            auto memtables = active_memtables(*tb.t);
            BOOST_REQUIRE(std::ranges::all_of(memtables, std::mem_fn(&replica::memtable::empty)));
        }

        cl.sync_all_segments().get();
        auto paths = cl.get_active_segment_names();
        BOOST_REQUIRE_GE(paths.size(), 2);

        auto rp = db::commitlog_replayer::create_replayer(env.db(), env.get_system_keyspace()).get();
        rp.recover(paths, db::commitlog::descriptor::FILENAME_PREFIX).get();

        for (auto& tb : tables) {
            auto& s = tb.s;
            // Merge expected mutations per partition.
            std::vector<mutation> merged;
            for (auto& m : tb.expected) {
                auto i = std::ranges::find_if(merged, [&](const mutation& x) { return x.decorated_key().equal(*s, m.decorated_key()); });
                if (i == merged.end()) {
                    merged.push_back(m);
                } else {
                    i->apply(m);
                }
            }
            std::ranges::sort(merged, [&](const mutation& a, const mutation& b) { return a.decorated_key().less_compare(*s, b.decorated_key()); });

            std::vector<mutation> actual;
            auto memtables = active_memtables(*tb.t);
            auto permit = db.get_reader_concurrency_semaphore().make_tracking_only_permit(s, "test", db::no_timeout, {});
            std::vector<mutation_reader> readers;
            for (auto mt : memtables) {
                readers.push_back(mt->make_mutation_reader(s, permit));
            }
            auto rd = make_combined_reader(s, permit, std::move(readers));
            auto close_rd = deferred_close(rd);
            while (auto mopt = read_mutation_from_mutation_reader(rd).get()) {
                actual.push_back(std::move(*mopt));
            }
            std::ranges::sort(actual, [&](const mutation& a, const mutation& b) { return a.decorated_key().less_compare(*s, b.decorated_key()); });

            BOOST_REQUIRE_EQUAL(actual.size(), merged.size());
            for (size_t i = 0; i < merged.size(); ++i) {
                BOOST_CHECK_EQUAL(actual[i], merged[i]);
            }
        }
    }, cfg);
}

BOOST_AUTO_TEST_SUITE_END()
