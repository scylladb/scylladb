/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Side-by-side timings for `pq` against the current default format, on the same
// mutations, in the same process. Covers the performance requirements in
// docs/dev/parquet-storage-format.md section 4 that were previously blocked on
// Parquet sstables existing on disk at all:
//
//   R-9   write throughput must not regress materially
//   R-10  full-scan reads should be faster
//   R-11  point reads may be slower, but bounded
//   R-13  memory must stay bounded regardless of sstable size
//
// This is a measurement harness, not an assertion suite: it prints a table and
// only fails if something is functionally wrong. Absolute numbers depend on the
// machine; the ratios are the point. Run it directly:
//
//   ./build/dev/test/boost/sstable_parquet_perf_test -- -c1 -m4G

#include <seastar/testing/thread_test_case.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/format.hh>

#include <algorithm>
#include <cstdlib>
#include "test/lib/sstable_test_env.hh"
#include "test/lib/sstable_utils.hh"

#include <map>
#include "sstables/parquet/reader.hh"
#include "sstables/parquet/format/parquet_reader.hh"
#include "sstables/parquet/batch_reader.hh"
#include "partition_slice_builder.hh"
#include "sstables/parquet/format/parquet_metadata.hh"
#include "schema/schema_builder.hh"
#include "sstables/sstables.hh"
#include "mutation/mutation.hh"
#include "readers/from_mutations.hh"

#include <chrono>
#include <random>
#include <string_view>

using namespace sstables;
using namespace std::chrono;

namespace {

using clk = steady_clock;

// Extra int columns appended to the schema, via PQ_PERF_EXTRA_COLS. Exists because the
// size cost of a small row group scales with *leaf count* -- every row group writes a
// column-chunk header plus statistics per leaf -- so a 5-column table cannot tell you what
// the row-group default costs a 197-column one (design doc 10.1f-prod, open question 15).
// Default 0 keeps the historical schema, so previously published numbers stay comparable.
int extra_cols() {
    if (const char* e = std::getenv("PQ_PERF_EXTRA_COLS")) {
        return std::max(0, std::atoi(e));
    }
    return 0;
}

sstring extra_col_name(int i) { return sstring(format("x_{:03d}", i)); }

schema_ptr perf_schema() {
    auto b = schema_builder(1, "ks", "perf");
    b.with_column("pk", utf8_type, column_kind::partition_key)
     .with_column("ck", int32_type, column_kind::clustering_key)
     .with_column("v_int", int32_type)
     .with_column("v_big", long_type)
     .with_column("v_dbl", double_type)
     .with_column("v_txt", utf8_type)
     .with_column("v_txt2", utf8_type);
    for (int i = 0, n = extra_cols(); i < n; ++i) {
        b.with_column(to_bytes(extra_col_name(i)), int32_type);
    }
    // Row-group size, via the same per-table property an operator would set. Needed
    // because the size cost of a small row group scales with leaf count while the
    // latency benefit does not obviously do so, and open question 15 turns on whether
    // one default can serve both shapes.
    std::map<sstring, sstring> opts;
    if (const char* e = std::getenv("PQ_PERF_RG_ROWS")) {
        opts["rows_per_row_group"] = sstring(e);
    }
    // Page size. Worth a knob because it stopped being reachable: the writer uses
    // min(page_values, row group size), and with row groups cut at 5 000 rows the 8 192
    // default never binds -- so every point read decodes a whole row group's page.
    if (const char* e = std::getenv("PQ_PERF_PAGE_ROWS")) {
        opts["page_rows"] = sstring(e);
    }
    // Anything else, spelled the way CQL spells it: PQ_PERF_PQ_OPTS="compression=lz4,dictionary=none".
    // The two knobs above predate this and stay, being the two that get swept most; this exists so
    // that measuring a *new* sub-option does not need a new env var and a rebuild of the harness.
    // Notably `compression`: the parquet arm is zstd while the row format's default is
    // LZ4WithDictsCompressor, so any CPU comparison between them that does not vary the codec is
    // measuring the codec as much as the format (design doc 10.28).
    if (const char* e = std::getenv("PQ_PERF_PQ_OPTS")) {
        std::string_view all(e);
        while (!all.empty()) {
            const auto comma = all.find(',');
            auto kv = all.substr(0, comma);
            const auto eq = kv.find('=');
            if (eq != std::string_view::npos) {
                opts[sstring(kv.substr(0, eq))] = sstring(kv.substr(eq + 1));
            }
            if (comma == std::string_view::npos) { break; }
            all = all.substr(comma + 1);
        }
    }
    if (!opts.empty()) {
        b.set_parquet_options(std::move(opts));
    }
    return b.build();
}

// Values with realistic redundancy: a small vocabulary for the text columns and
// correlated numerics. Random bytes would make every format look identical
// because nothing would compress -- the same trap section 9 calls out.
// Bytes of *incompressible* payload to put in v_txt2, replacing its low-cardinality string.
//
// Default 0, which leaves the generated data byte-for-byte what it has always been, because every
// figure in section 10.4 was measured against it.
//
// It exists because the local corpus is deliberately compressible -- eight words and small
// integers, 3.9 bytes/row for the row format -- while the cluster corpus of section 10.30 is
// 24.9 bytes/row. That matters for a decode-bound format: a scan that is at parity locally was
// 2.87x slower there, and per-row the *row format* agrees across the two (2.5 us against 3.0 us)
// while pq does not (2.4 us against 8.7 us). Compressibility is the largest difference between the
// two corpora, and this knob is what makes it a variable instead of a confound.
int text_bytes() {
    static const int n = [] {
        if (const char* e = std::getenv("PQ_PERF_TEXT_BYTES")) {
            return int(std::max(0L, std::atol(e)));
        }
        return 0;
    }();
    return n;
}

// Give every row a marker, i.e. write it the way INSERT does rather than the way UPDATE does.
//
// Default off, so every figure measured against this corpus before today still stands. It exists
// because projection is only applied to a row group whose rows all carry a live marker (see
// pq_reader::projection_is_safe), and this generator writes cells without one -- so a projection
// benchmark against the default corpus measures the gate declining, not the optimisation.
bool with_markers() {
    static const bool on = [] {
        const char* e = std::getenv("PQ_PERF_MARKERS");
        return e && *e && *e != '0';
    }();
    return on;
}

utils::chunked_vector<mutation> gen(schema_ptr s, int n_part, int n_rows) {
    static const char* WORDS[] = {"active", "pending", "closed", "archived", "error",
                                  "retry", "queued", "done"};
    std::mt19937_64 rng(42);
    utils::chunked_vector<mutation> muts;
    muts.reserve(n_part);
    for (int p = 0; p < n_part; ++p) {
        auto pk = partition_key::from_single_value(
                *s, utf8_type->decompose(sstring(format("user{:07d}", p))));
        mutation m(s, pk);
        const api::timestamp_type row_ts = 1'700'000'000'000'000LL + p;
        for (int r = 0; r < n_rows; ++r) {
            auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
            if (with_markers()) {
                m.partition().clustered_row(*s, ck).apply(row_marker(row_ts));
            }
            auto put = [&] (const char* name, bytes val) {
                const auto& cd = *s->get_column_definition(to_bytes(name));
                m.set_clustered_cell(ck, cd, atomic_cell::make_live(*cd.type, row_ts, val));
            };
            put("v_int", int32_type->decompose(int32_t(rng() % 1000)));
            put("v_big", long_type->decompose(int64_t(p) * 1000 + r));
            put("v_dbl", double_type->decompose(double(rng() % 100000) / 100.0));
            put("v_txt", utf8_type->decompose(sstring(WORDS[rng() % 8])));
            if (const int tb = text_bytes(); tb > 0) {
                // Uniform over the printable ASCII range, so neither the zstd dictionary nor the
                // Parquet dictionary encoding has anything to find. Drawn from the same `rng` the
                // rest of the row uses, so the corpus stays deterministic.
                sstring blob(tb, '\0');
                for (int i = 0; i < tb; ++i) { blob[i] = char(33 + rng() % 94); }
                put("v_txt2", utf8_type->decompose(blob));
            } else {
                put("v_txt2", utf8_type->decompose(
                        sstring(format("{}/{}/{}", WORDS[rng() % 8], p % 997, r))));
            }
            // Low-cardinality, like the SMART counters that make real wide tables wide:
            // the point of these columns is their number, not their content.
            for (int i = 0, n = extra_cols(); i < n; ++i) {
                put(extra_col_name(i).c_str(),
                    int32_type->decompose(int32_t((p + r + i) % 64)));
            }
        }
        muts.push_back(std::move(m));
    }
    std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
        return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
    });
    return muts;
}

// How many times to repeat each measured scan. Default 1: this test is enrolled in the standard
// boost suite, so the default has to stay a smoke test.
int scan_repeats() {
    static const int n = [] {
        if (const char* e = std::getenv("PQ_PERF_SCAN_REPEATS")) {
            return int(std::max(1L, std::atol(e)));
        }
        return 1;
    }();
    return n;
}

// Which formats to measure. "both" (default), "pq", or "default". A single-arm run exists for
// profiling: `perf record` cannot separate two readers running in one process, and the two paths
// share enough of the mutation-building machinery that a mixed profile cannot be split by symbol
// either. It reports no ratios, because with one arm there is nothing to divide by.
bool arm_enabled(const char* which) {
    static const sstring sel = [] {
        const char* e = std::getenv("PQ_PERF_ARM");
        return sstring(e && *e ? e : "both");
    }();
    return sel == "both" || sel == which;
}

struct result {
    sstring   label;
    double    write_ms = 0;
    double    scan_ms = 0;
    // Same rows as scan_ms, read through a partition range that carries explicit bounds
    // instead of query::full_partition_range. Nothing else differs -- same schema, same
    // slice, same sstable, same process -- so the gap between the two is the cost of the
    // *path* the bound selects, which is what design doc 10.26 exists to measure.
    double    bscan_ms = 0;
    size_t    bscan_rows = 0;
    double    point_us = 0;      // mean per point read
    double    point_p50 = 0, point_p95 = 0, point_p99 = 0;
    size_t    point_n = 0;
    uint64_t  bytes = 0;
    int64_t   scan_peak_kb = 0;  // live-memory high-water during the scan
    size_t    rows = 0;
    // One profile per operation, when PQ_READER_PROFILE is set.
    //
    // The counters behind reader_profile_report() are process-global and additive, so a single
    // report taken at the end of this function covers the write, both scans and every point read
    // at once. That is unreadable for anything but a point read: at the sizes a measurement uses,
    // 1 000 point reads and one full scan land in the same table and the larger simply buries the
    // smaller. Worse, the two are *differently shaped* -- an unbounded scan streams whole row
    // groups (rg_fetch, rg_decode) while a point read fetches pages (page_fetch, decode_cpu) --
    // so a merged report shows all four phases at once and attributes neither operation.
    //
    // Reset before each operation and snapshot after, so each block describes exactly one thing.
    sstring   prof_scan, prof_bscan, prof_point;
};

result measure(sstables::test_env& env, schema_ptr s,
               const utils::chunked_vector<mutation>& muts,
               sstable_version_types ver, const sstring& label,
               const std::vector<size_t>& point_idx) {
    result r;
    r.label = label;

    auto copy = muts;
    auto t0 = clk::now();
    auto sst = make_sstable_containing(env.make_sstable(s, ver), std::move(copy)).get();
    r.write_ms = duration<double, std::milli>(clk::now() - t0).count();
    r.bytes = sst->ondisk_data_size();

    // Full scan. Sample live memory as we go so R-13 is measured, not assumed.
    //
    // Repeated PQ_PERF_SCAN_REPEATS times, mean reported. One scan is both noisy and, for a
    // symbol-level profile, unusable: the two writes dominate the process and a `perf record` of
    // the whole run attributes most of its samples to the writer. Repeating the scan is what makes
    // the scan the thing being profiled. The rows and the memory high-water are taken from the
    // first pass, so neither figure changes with the repeat count.
    const auto before = int64_t(memory::stats().allocated_memory());
    int64_t peak = 0;
    sstables::parquet::reader_profile_reset();
    t0 = clk::now();
    for (int pass = 0; pass < scan_repeats(); ++pass) {
        size_t seen = 0;
        auto rd = sst->make_reader(s, env.make_reader_permit(), query::full_partition_range,
                                   s->full_slice());
        auto close = deferred_close(rd);
        while (auto m = read_mutation_from_mutation_reader(rd).get()) {
            ++seen;
            peak = std::max(peak, int64_t(memory::stats().allocated_memory()) - before);
        }
        if (pass == 0) { r.rows = seen; }
    }
    r.scan_ms = duration<double, std::milli>(clk::now() - t0).count() / double(scan_repeats());
    r.scan_peak_kb = peak / 1024;
    r.prof_scan = sstables::parquet::reader_profile_report();

    // The same scan again, over a bounded range that spans every partition in the file.
    // `muts` is sorted by decorated key, so [front, back] is the whole ring segment the
    // sstable holds and this reads exactly the rows the unbounded scan just read.
    //
    // It exists because a token-restricted CQL scan -- `WHERE token(pk) >= a AND token(pk) < b`,
    // and every page of a paged range query after the first -- arrives at the sstable reader as
    // a *bounded* range, and pq_reader::next_window() forks on precisely that: a bounded read
    // takes the point-read machinery (512-row windows, per-window page fetch and decode) while
    // an unbounded one streams whole row groups in 16 384-row windows. Any scan figure that does
    // not say which of the two it measured is unreadable, which is how 10.4c and 10.25 ended up
    // contradicting each other.
    {
        auto pr = dht::partition_range::make({muts.front().decorated_key(), true},
                                             {muts.back().decorated_key(), true});
        sstables::parquet::reader_profile_reset();
        t0 = clk::now();
        auto rd = sst->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
        auto close = deferred_close(rd);
        while (auto m = read_mutation_from_mutation_reader(rd).get()) {
            ++r.bscan_rows;
        }
        r.bscan_ms = duration<double, std::milli>(clk::now() - t0).count();
        r.prof_bscan = sstables::parquet::reader_profile_report();
    }

    // Point reads, each on a fresh reader so nothing is carried over.
    //
    // Timed individually rather than as one bulk average. A mean over a handful of reads
    // hides the distribution, and the distribution is the interesting part of a point-read
    // number: the cost here is dominated by per-read setup -- opening a row group and
    // decompressing a dictionary page -- so a few cheap reads can flatter the mean badly.
    std::vector<double> samples;
    samples.reserve(point_idx.size());
    sstables::parquet::reader_profile_reset();
    for (size_t i : point_idx) {
        auto pr = dht::partition_range::make_singular(muts[i].decorated_key());
        auto t1 = clk::now();
        auto rd = sst->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
        auto close = deferred_close(rd);
        auto m = read_mutation_from_mutation_reader(rd).get();
        samples.push_back(duration<double, std::micro>(clk::now() - t1).count());
        if (!m) { BOOST_FAIL("point read returned nothing"); }
    }
    double sum = 0;
    for (double v : samples) { sum += v; }
    r.point_us = sum / double(samples.size());
    r.point_n = samples.size();
    r.prof_point = sstables::parquet::reader_profile_report();
    std::sort(samples.begin(), samples.end());
    auto pct = [&] (double q) {
        return samples[std::min(samples.size() - 1,
                                size_t(q * double(samples.size())))];
    };
    r.point_p50 = pct(0.50);
    r.point_p95 = pct(0.95);
    r.point_p99 = pct(0.99);

    return r;
}

} // namespace

SEASTAR_THREAD_TEST_CASE(perf_pq_vs_default) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = perf_schema();

        // Sized for CI by default, and scaled up by env var for measurement.
        //
        // This test is enrolled in the standard boost suite (configure.py), so it runs on every
        // CI invocation -- and at the sizes every figure in section 10.4 was measured with
        // (20 000 partitions, 10 000 point reads) it is minutes of runtime for numbers nobody
        // reads on a shared runner. It asserts nothing about latency, so it cannot fail on a
        // loaded machine; it can only be slow. Small default, big when asked:
        //
        //   PQ_PERF_PARTITIONS=20000 PQ_PERF_POINTS=10000 ./sstable_parquet_perf_test -- -c1 -m2G
        //
        // The `--` is required and was missing here: Boost.Test parses the whole command line
        // first and rejects `-c1` as one of its own parameters, so the documented form exited 200
        // without running anything.
        //
        // which is what width_curve.sh and any figure quoted in section 10.4 must use. The
        // small default is a smoke test of both writers and the point-read path; it is not a
        // measurement, and its numbers should not be quoted.
        const int n_part = [] {
            if (const char* e = std::getenv("PQ_PERF_PARTITIONS")) {
                return int(std::max(1L, std::atol(e)));
            }
            return 2'000;
        }();
        const int n_rows = 5;
        auto muts = gen(s, n_part, n_rows);

        // A fixed spread of partitions, same set for both formats.
        // Uniformly random distinct partitions, not a stride.
        //
        // This used to be 50 evenly-spaced indices, which is both too small a sample to
        // say anything about a latency distribution and too friendly an access pattern:
        // a stride walks the partition index and summary in order, so it gets locality
        // that a real point-read workload does not have. Both formats get the identical
        // key list, so the comparison stays fair either way -- but the absolute numbers
        // were optimistic for both.
        const size_t n_points = [] {
            if (const char* e = std::getenv("PQ_PERF_POINTS")) {
                return size_t(std::max(1L, std::atol(e)));
            }
            return size_t(1000);   // see PQ_PERF_PARTITIONS above
        }();
        std::vector<size_t> point_idx(muts.size());
        for (size_t i = 0; i < muts.size(); ++i) { point_idx[i] = i; }
        std::shuffle(point_idx.begin(), point_idx.end(), std::mt19937_64(12345));
        if (point_idx.size() > n_points) { point_idx.resize(n_points); }

        const bool want_def = arm_enabled("default"), want_pq = arm_enabled("pq");
        result def, pq;
        if (want_def) {
            def = measure(env, s, muts, sstables::get_highest_sstable_version(), "default (me)", point_idx);
            BOOST_REQUIRE_EQUAL(def.rows, size_t(n_part));
        }
        if (want_pq) {
            pq = measure(env, s, muts, sstable_version_types::pq, "pq (parquet)", point_idx);
            BOOST_REQUIRE_EQUAL(pq.rows, size_t(n_part));
        }

        auto line = [] (const result& r) {
            std::printf("  %-14s  %9.0f  %9.0f  %9.0f  %11.1f  %12llu  %10lld\n",
                        r.label.c_str(), r.write_ms, r.scan_ms, r.bscan_ms, r.point_us,
                        (unsigned long long)r.bytes, (long long)r.scan_peak_kb);
        };
        std::printf("\n=== pq vs default: %d partitions x %d rows = %d rows"
                    " (scan mean of %d) ===\n",
                    n_part, n_rows, n_part * n_rows, scan_repeats());
        std::printf("  %-14s  %9s  %9s  %9s  %11s  %12s  %10s\n",
                    "format", "write ms", "scan ms", "bscan ms", "point us", "data bytes",
                    "scan kB");
        if (want_def) { line(def); }
        if (want_pq) { line(pq); }
        std::printf("\n  point-read distribution over %zu uniformly random partitions:\n",
                    want_def ? def.point_n : pq.point_n);
        std::printf("  %-14s  %10s  %10s  %10s  %10s\n",
                    "format", "mean us", "p50 us", "p95 us", "p99 us");
        for (const auto& r : {def, pq}) {
            if (r.label.empty()) { continue; }
            std::printf("  %-14s  %10.1f  %10.1f  %10.1f  %10.1f\n",
                        r.label.c_str(), r.point_us, r.point_p50, r.point_p95, r.point_p99);
        }
        // Where each pq operation went, when PQ_READER_PROFILE is set. Printed next to the
        // ratios so the attribution and the number being attributed are read together. One block
        // per operation -- see the note on result::prof_scan for why a merged one says nothing.
        if (!pq.prof_scan.empty()) {
            std::printf("\n  --- pq unbounded scan (%zu rows in %.0f ms) ---\n",
                        pq.rows, pq.scan_ms);
            std::printf("%s", pq.prof_scan.c_str());
            std::printf("\n  --- pq bounded scan (%zu rows in %.0f ms) ---\n",
                        pq.bscan_rows, pq.bscan_ms);
            std::printf("%s", pq.prof_bscan.c_str());
            std::printf("\n  --- pq point reads (%zu reads, %.1f us mean) ---\n",
                        pq.point_n, pq.point_us);
            std::printf("%s", pq.prof_point.c_str());
            std::printf("\n");
        }
        if (!(want_def && want_pq)) {
            std::printf("  single-arm run (PQ_PERF_ARM); no ratios\n\n");
            return;
        }
        std::printf("  point ratios: mean %.1fx  p50 %.1fx  p95 %.1fx  p99 %.1fx\n",
                    pq.point_us / def.point_us, pq.point_p50 / def.point_p50,
                    pq.point_p95 / def.point_p95, pq.point_p99 / def.point_p99);
        std::printf("  ratios (pq/default): write %.2fx  scan %.2fx  bscan %.2fx  point %.2fx"
                    "  size %.3fx\n",
                    pq.write_ms / def.write_ms, pq.scan_ms / def.scan_ms,
                    pq.bscan_ms / def.bscan_ms,
                    pq.point_us / def.point_us, double(pq.bytes) / double(def.bytes));
        // What the bound costs each format on its own terms. The row format's index makes a
        // bounded range no harder than an unbounded one; pq's fork makes it a different reader.
        std::printf("  bounded/unbounded, same rows: default %.2fx  pq %.2fx"
                    "  (rows %zu vs %zu)\n\n",
                    def.bscan_ms / def.scan_ms, pq.bscan_ms / pq.scan_ms,
                    pq.rows, pq.bscan_rows);
    }).get();
}

// R-13 specifically: memory during a scan must not grow with the sstable. Two
// sizes, same schema -- if peak scan memory tracks the file, the reader is
// materialising it.
SEASTAR_THREAD_TEST_CASE(perf_pq_scan_memory_scaling) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = perf_schema();
        std::printf("\n=== R-13: scan memory vs sstable size (pq) ===\n");
        std::printf("  %10s  %12s  %12s  %10s\n", "rows", "data bytes", "scan peak kB", "point us");

        int64_t small_peak = 0, large_peak = 0;
        for (int n_part : {4'000, 32'000}) {
            auto muts = gen(s, n_part, 5);
            auto sst = make_sstable_containing(
                    env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

            const auto before = int64_t(memory::stats().allocated_memory());
            int64_t peak = 0;
            size_t n = 0;
            {
                auto rd = sst->make_reader(s, env.make_reader_permit(),
                                           query::full_partition_range, s->full_slice());
                auto close = deferred_close(rd);
                while (auto m = read_mutation_from_mutation_reader(rd).get()) {
                    ++n;
                    peak = std::max(peak, int64_t(memory::stats().allocated_memory()) - before);
                }
            }
            BOOST_REQUIRE_EQUAL(n, size_t(n_part));

            // Point-read cost at each size. If it tracks the file rather than the
            // partition, the reader is I/O bound on the row-group read and the
            // OffsetIndex-driven per-page read is the fix; if it is flat, it is not.
            auto muts2 = gen(s, n_part, 5);
            double pt = 0;
            {
                auto t0 = clk::now();
                const int probes = 20;
                for (int i = 0; i < probes; ++i) {
                    auto pr = dht::partition_range::make_singular(
                            muts2[size_t(i) * (muts2.size() / probes)].decorated_key());
                    auto rd = sst->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
                    auto close = deferred_close(rd);
                    auto m = read_mutation_from_mutation_reader(rd).get();
                    if (!m) { BOOST_FAIL("point read missed"); }
                }
                pt = duration<double, std::micro>(clk::now() - t0).count() / probes;
            }
            std::printf("  %10d  %12llu  %12lld  %10.1f\n", n_part * 5,
                        (unsigned long long)sst->ondisk_data_size(), (long long)(peak / 1024), pt);
            (n_part == 4'000 ? small_peak : large_peak) = peak;
        }
        if (small_peak > 0) {
            const double growth = double(large_peak) / double(small_peak);
            std::printf("  8x the rows cost %.2fx the peak scan memory "
                        "(bounded would be ~1x)\n\n", growth);
            // R-13 is a requirement, so assert it rather than print it. This block measured the
            // ratio and then discarded it, which means a reader that started materialising the
            // whole file would have gone green and simply printed 8.0x -- exactly the regression
            // the test exists to catch.
            //
            // The bound is 3x, not 1.5x. A bounded reader is ~1x, but "peak allocated during the
            // scan" also picks up per-partition churn and allocator granularity at two quite
            // different file sizes, so the floor is noisy at these sizes. 3x is comfortably above
            // that noise and far below the ~8x that tracking the file would produce, which is the
            // only failure mode worth catching here.
            BOOST_REQUIRE_MESSAGE(growth < 3.0,
                    seastar::format("R-13: peak scan memory grew {:.2f}x for 8x the rows "
                                    "({} -> {} bytes); a bounded reader is ~1x, so the reader is "
                                    "materialising the sstable", growth, small_peak, large_peak));
        }
    }).get();
}

// What a scan would cost if it did not build a mutation per row.
//
// This is the number the vectorised-scan question turns on, and until now it had only been
// inferred: `perf record` says ~two thirds of a scan on *both* formats is shared
// mutation-building machinery, and that the parquet-specific half is 16.0 % against the row
// format's 17.8 % (design doc 10.38). From that the ceiling on optimising parquet decode is
// 1.19x -- but the ceiling on *not building mutations* was never measured, only bounded from
// the other side.
//
// So: decode every row group of the same sstable straight through the format layer, touch every
// value, and build nothing. The gap to the reader's own full scan is what a vectorised path is
// competing for. Two honest caveats, both stated in the output. The file is read into memory
// first, so this excludes read I/O -- fair for a CPU comparison and flattering by however much
// the scan's I/O costs. And "touch every value" is not "deliver every value to a client": a real
// vectorised path still has to produce output in some form, so this is a floor and not a target.
SEASTAR_THREAD_TEST_CASE(perf_pq_columnar_scan_floor) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = perf_schema();
        const int n_part = [] {
            if (const char* e = std::getenv("PQ_PERF_PARTITIONS")) {
                return int(std::max(1L, std::atol(e)));
            }
            return 2'000;
        }();
        const int n_rows = 5;
        auto muts = gen(s, n_part, n_rows);

        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), muts).get();

        // The reader's own scan, for the comparison.
        size_t rows_via_reader = 0;
        auto t0 = clk::now();
        {
            auto rd = sst->make_reader(s, env.make_reader_permit(), query::full_partition_range,
                                       s->full_slice());
            auto close = deferred_close(rd);
            while (auto m = read_mutation_from_mutation_reader(rd).get()) { ++rows_via_reader; }
        }
        const double reader_ms = duration<double, std::milli>(clk::now() - t0).count();

        // The same bytes, decoded columnar, building nothing.
        const uint64_t len = sst->ondisk_data_size();
        auto buf = sst->data_read(0, size_t(len), env.make_reader_permit()).get();
        auto image = std::span<const uint8_t>(
                reinterpret_cast<const uint8_t*>(buf.get()), buf.size());

        size_t values = 0, rows_via_columnar = 0;
        double checksum = 0;
        t0 = clk::now();
        auto md = sstables::parquet::format::parse_footer(image);
        for (size_t rg = 0; rg < md.row_groups.size(); ++rg) {
            auto cols = sstables::parquet::format::read_row_group(image, md, rg);
            rows_via_columnar += size_t(md.row_groups[rg].num_rows);
            for (const auto& cd : cols) {
                // Touched, so that none of this is optimised away, and cheaply -- summing is not
                // the point, decoding is.
                for (auto v : cd.i32) { checksum += double(v); }
                for (auto v : cd.i64) { checksum += double(v); }
                for (auto v : cd.f64) { checksum += v; }
                for (const auto& v : cd.str) { checksum += double(v.size()); }
                values += cd.num_values();
            }
        }
        const double columnar_ms = duration<double, std::milli>(clk::now() - t0).count();

        // And the same scan through the batch reader -- which, unlike the floor above, reads from
        // disk a row group at a time exactly as a real consumer would. This is the number a
        // vectorised consumer would actually see; the floor says how much of the gap is the
        // interface and how much is still I/O and bookkeeping.
        size_t batch_rows = 0, batches = 0;
        double bchecksum = 0;
        t0 = clk::now();
        {
            auto br = sstables::parquet::make_batch_reader(sst, s, env.make_reader_permit());
            while (auto b = br->next().get()) {
                ++batches;
                batch_rows += size_t(b->rows);
                for (const auto& cd : b->columns) {
                    for (auto v : cd.i32) { bchecksum += double(v); }
                    for (auto v : cd.i64) { bchecksum += double(v); }
                    for (auto v : cd.f64) { bchecksum += v; }
                    for (const auto& v : cd.str) { bchecksum += double(v.size()); }
                }
            }
            br->close().get();
        }
        const double batch_ms = duration<double, std::milli>(clk::now() - t0).count();

        // And the same scan projected down to one regular column, which is what a columnar format
        // is supposed to make cheap and what 10.30 measured as not working on either path.
        uint64_t full_bytes = 0, narrow_bytes = 0;
        double narrow_ms = 0;
        size_t n_regular = 0;
        {
            auto br = sstables::parquet::make_batch_reader(sst, s, env.make_reader_permit());
            br->init().get();
            n_regular = br->schema_mapping().n_regular;
            while (auto b = br->next().get()) { }
            full_bytes = br->bytes_read();
            br->close().get();

            sstables::parquet::projection proj;
            proj.want_regular.assign(n_regular, false);
            if (n_regular) { proj.want_regular[0] = true; }
            auto t1 = clk::now();
            auto nr = sstables::parquet::make_batch_reader(sst, s, env.make_reader_permit(), proj);
            while (auto b = nr->next().get()) { }
            narrow_ms = duration<double, std::milli>(clk::now() - t1).count();
            narrow_bytes = nr->bytes_read();
            nr->close().get();
        }

        BOOST_REQUIRE_GT(values, 0u);
        BOOST_REQUIRE_NE(checksum, 0.0);
        BOOST_REQUIRE_EQUAL(bchecksum, checksum);

        const double total_rows = double(n_part) * double(n_rows);
        std::printf("\n=== columnar scan floor: %d partitions x %d rows ===\n", n_part, n_rows);
        std::printf("  %-34s %10.1f ms  %10.2f M rows/s\n", "reader full scan (mutations)",
                    reader_ms, total_rows / reader_ms / 1e3);
        std::printf("  %-34s %10.1f ms  %10.2f M rows/s\n", "columnar decode only (no mutations)",
                    columnar_ms, total_rows / columnar_ms / 1e3);
        std::printf("  %-34s %10.1f ms  %10.2f M rows/s\n", "batch reader (reads from disk)",
                    batch_ms, total_rows / batch_ms / 1e3);
        std::printf("  headroom from not building mutations: %.2fx floor, %.2fx via batch reader\n",
                    reader_ms / columnar_ms, reader_ms / batch_ms);
        std::printf("  (batches %zu, batch rows %zu)\n", batches, batch_rows);
        // And through the *mutation* reader with the slice permitting projection -- which is what
        // a client SELECT ... BYPASS CACHE now gets. Unlike the batch rows above, this still builds
        // mutations, so it is the number a client would actually see rather than a reader-interface
        // number.
        double slice_all_ms = 0, slice_one_ms = 0;
        {
            auto one = partition_slice_builder(*s)
                    .with_regular_column(to_bytes("v_int"))
                    .build();
            auto one_proj = one;
            one_proj.options.set<query::partition_slice::option::may_project_columns>();

            auto timed = [&] (const query::partition_slice& sl) {
                auto t1 = clk::now();
                auto rd = sst->make_reader(s, env.make_reader_permit(),
                                           query::full_partition_range, sl);
                auto cl = deferred_close(rd);
                while (auto m = read_mutation_from_mutation_reader(rd).get()) { }
                return duration<double, std::milli>(clk::now() - t1).count();
            };
            slice_all_ms = timed(one);          // one column asked for, every column read
            slice_one_ms = timed(one_proj);     // one column asked for, one column read
        }
        std::printf("  SELECT one column through the mutation reader:"
                    " %.1f ms without projection, %.1f ms with (%.2fx)\n",
                    slice_all_ms, slice_one_ms,
                    slice_one_ms > 0 ? slice_all_ms / slice_one_ms : 0.0);
        std::printf("  projection to 1 of %zu regular columns: %llu -> %llu bytes (%.1f%%),"
                    " %.1f ms\n",
                    n_regular, (unsigned long long)full_bytes, (unsigned long long)narrow_bytes,
                    full_bytes ? 100.0 * double(narrow_bytes) / double(full_bytes) : 0.0,
                    narrow_ms);
        std::printf("  (partitions via reader %zu, rows via columnar %zu, values %zu)\n",
                    rows_via_reader, rows_via_columnar, values);
        std::printf("  caveats: the floor row excludes read I/O (file pre-read into memory); the\n"
                    "           batch row does not, and costs %.0f%% more, which is what the I/O is\n"
                    "           worth here. Touching values is not delivering them, so neither row\n"
                    "           is a target -- a real consumer still has to produce output.\n\n",
                    (batch_ms / columnar_ms - 1.0) * 100.0);
    }).get();
}
