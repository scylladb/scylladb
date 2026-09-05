/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Phase 2 correctness and cost test for metadata folding.
//
// Two things are asserted here:
//
//   1. LOSSLESSNESS. shred() -> reassemble() must return exactly the input, for
//      every folding level, across null patterns, TTLs, deletions, and -- most
//      importantly -- rows whose cells carry *different* timestamps. Folding L1
//      is only sound if divergent cells survive via the exception column.
//
//   2. THE DIVERGENCE COST CURVE. The design doc's threat-to-validity #1 was
//      that L1 was only ever measured on data where every cell in a row shares
//      one timestamp (INSERT-written rows), and that UPDATE-assembled rows were
//      unmeasured. This sweeps the divergence rate from 0% to 100% and reports
//      what folding actually costs.
//
// This used to live at `sstables/parquet/test_shred.cc`, where it was built and run
// only by the hand-run `sstables/parquet/run_tests.sh`. Nothing in the project's own
// test wiring executed it, so a regression in folding losslessness -- the property
// that the whole format rests on -- could not fail any CI job. It now lives here, as
// a `test/unit` target, because that suite is exactly the shape this program already
// had: a standalone `main()` judged by its exit code, with one CI case per entry in
// the `custom_args` list for it in `test/unit/test_config.yaml`. `run_tests.sh` still
// builds and runs this same file, so there is one copy of the matrix, not two.
//
// The seven subcommands, and which of them CI runs:
//
//   roundtrip     -- losslessness across folding levels        RUN IN CI
//   filetrip      -- rows -> parquet -> rows, through a file   RUN IN CI
//   logical       -- L3 logical export is lossy, and says so   RUN IN CI
//   recovery      -- mapped_schema recovered from the file     RUN IN CI
//   legacy        -- pre-fold files still read (per-column __ldt)  RUN IN CI
//   collections   -- non-frozen collections                    RUN IN CI
//   cost          -- divergence sweep, CSV on stdout           NOT IN CI
//
// `cost` is deliberately excluded. It is a measurement, not an assertion: it prints a
// CSV and returns 0 unconditionally, so wiring it would add a CI case that cannot
// fail -- green whatever folding does to the data. A case that cannot go red is worse
// than no case, because it reads as coverage. It stays in `run_tests.sh` (suite 7),
// where a human reads the numbers it emits, which is what it is for. The other six
// were each checked for the same defect before being wired: all six count failures
// from real comparisons against expected values and return non-zero when any trips.

#include "sstables/parquet/schema_mapping.hh"
#include "sstables/parquet/format/parquet_metadata.hh"
#include "sstables/parquet/format/parquet_reader.hh"

#include <cstdio>
#include <limits>
#include <random>
#include <string>
#include <vector>

using namespace sstables::parquet;

namespace {

std::vector<cql_column> make_schema(size_t n_regular) {
    std::vector<cql_column> c;
    c.push_back({"pk", cql_type::bigint, column_kind::partition_key});
    c.push_back({"ck", cql_type::timestamp, column_kind::clustering_key});
    static const cql_type cycle[] = {cql_type::bigint, cql_type::int32,
                                     cql_type::dbl, cql_type::text};
    for (size_t i = 0; i < n_regular; ++i) {
        c.push_back({"c" + std::to_string(i), cycle[i % 4], column_kind::regular});
    }
    return c;
}

struct gen_opts {
    size_t rows = 5000;
    size_t n_regular = 12;
    double null_rate = 0.25;
    double divergence_rate = 0.0;   // fraction of rows with per-cell timestamps
    double ttl_rate = 0.0;
    double delete_rate = 0.0;
    // Fraction of rows whose dead cells get *per-cell* deletion times rather than one
    // shared by the row. 0 is the common case and the one the fold is for: a bind-NULL
    // INSERT is a single statement, so every tombstone it writes shares one deletion time.
    // 1 is the adversarial case, where every dead cell needs an exception entry.
    //
    // This dimension did not exist before, and its absence was not neutral: the generator
    // drew every deletion time independently, so the corpus was 100 % divergent and the
    // *shared* case -- the one the fold actually optimises -- was never generated at all.
    double del_divergence_rate = 1.0;
    // Row-level metadata. Markers are the common case -- almost every CQL INSERT
    // makes one -- so the default is "most rows have one".
    double marker_rate = 0.0;
    double marker_ttl_rate = 0.0;
    double row_del_rate = 0.0;
    double part_del_rate = 0.0;
    // Draw timestamps from the extremes of int64 rather than a narrow band. The
    // folding scheme stores per-cell and marker timestamps as deltas against the
    // row's, and those subtractions overflow once the span approaches 2^63 -- which
    // the conformance corpus does deliberately.
    double extreme_ts_rate = 0.0;
    uint64_t seed = 7;
};

std::vector<row> generate(const std::vector<cql_column>& cols, const gen_opts& o) {
    std::mt19937_64 rng(o.seed);
    auto unit = [&] { return double(rng() % 1000000) / 1000000.0; };
    const size_t nreg = cols.size() - 2;
    std::vector<row> rows;
    rows.reserve(o.rows);
    int64_t base_ts = 1700000000000000LL;
    static const char* WORDS[] = {"alpha","beta","gamma","delta","epsilon","zeta","eta"};

    for (size_t i = 0; i < o.rows; ++i) {
        row r;
        r.key.push_back(int64_t(i) * 13 + 1);
        r.key.push_back(base_ts + int64_t(i) * 1000);
        int64_t row_ts = base_ts + int64_t(rng() % 2000000000);
        if (unit() < o.extreme_ts_rate) {
            static const int64_t extremes[] = {
                std::numeric_limits<int64_t>::min() + 64,
                std::numeric_limits<int64_t>::max() - 64,
                -9223372036854775737LL,
            };
            row_ts = extremes[rng() % 3];
        }
        const bool diverge = unit() < o.divergence_rate;
        // The row's own deletion time, shared by all of its dead cells unless this row is
        // drawn as divergent. Extremes included because the exception channel stores a
        // *delta* between two of these, and int32's ends are where that arithmetic is
        // easiest to get wrong.
        const bool del_diverge = unit() < o.del_divergence_rate;
        auto draw_ldt = [&] {
            switch (rng() % 8) {
            case 0:  return std::numeric_limits<int32_t>::max();
            case 1:  return std::numeric_limits<int32_t>::min() + 1;
            case 2:  return int32_t(0);
            default: return int32_t(rng() % 100000);
            }
        };
        const int32_t row_ldt = draw_ldt();

        for (size_t k = 0; k < nreg; ++k) {
            if (unit() < o.null_rate) { continue; }   // column absent for this row
            cell c;
            c.live = true;
            // A diverging row gives each cell its own write time, as repeated
            // UPDATEs to different columns would.
            if (diverge && unit() < o.extreme_ts_rate) {
                static const int64_t far[] = {
                    std::numeric_limits<int64_t>::max() - 32,
                    std::numeric_limits<int64_t>::min() + 32,
                };
                c.timestamp = far[rng() % 2];
            } else {
                // Saturating, so the generator itself does not overflow when the
                // row timestamp is already near the top of the range.
                const int64_t bump = int64_t(rng() % 100000);
                c.timestamp = diverge
                        ? (row_ts > std::numeric_limits<int64_t>::max() - bump
                                ? std::numeric_limits<int64_t>::max()
                                : row_ts + bump)
                        : row_ts;
            }
            switch (cols[2 + k].type) {
            case cql_type::bigint: c.v = int64_t(rng() % 1000000); break;
            case cql_type::int32:  c.v = int32_t(rng() % 1000); break;
            case cql_type::dbl:    c.v = double(rng() % 100000) / 100.0; break;
            default:               c.v = std::string(WORDS[rng() % 7]); break;
            }
            if (unit() < o.ttl_rate) {
                c.ttl = int32_t(rng() % 86400);
                // A live cell with a TTL also carries its expiry, because that is what the
                // write path produces: writer_impl sets local_deletion_time = av.expiry()
                // whenever is_live_and_has_ttl(). The generator never did, so the
                // live-expiry half of `__ldt_<col>` was never exercised -- which matters
                // now that the leaf carries *only* that case and dead cells go to the
                // folded channel instead.
                c.local_deletion_time = draw_ldt();
            }
            if (unit() < o.delete_rate) {
                // Now a tombstone: no value, and the ldt means deletion time rather than
                // expiry. Any TTL drawn above is deliberately left in place -- the write
                // path cannot produce that combination, but the mapping must not silently
                // lose a field just because the writer happens not to set it.
                c.live = false;
                c.v.reset();
                c.local_deletion_time = del_diverge ? draw_ldt() : row_ldt;
            }
            r.cells.emplace(k, std::move(c));
        }
        if (unit() < o.marker_rate) {
            marker_info m;
            // Usually the row's own timestamp, occasionally not -- the delta
            // encoding has to survive both.
            const int64_t mb = int64_t(rng() % 5000);
            m.timestamp = (unit() < 0.8 || row_ts > std::numeric_limits<int64_t>::max() - mb)
                    ? row_ts : row_ts + mb;
            if (unit() < o.marker_ttl_rate) {
                m.ttl = int32_t(rng() % 86400);
                m.expiry = int32_t(rng() % 100000);
            }
            r.marker = m;
        }
        if (unit() < o.row_del_rate) {
            const int64_t rb = int64_t(rng() % 1000);
            r.row_del = deletion_info{
                    row_ts < std::numeric_limits<int64_t>::min() + rb ? row_ts : row_ts - rb,
                    int32_t(rng() % 100000)};
        }
        if (unit() < o.part_del_rate) {
            // Constant within a run of rows, as a real partition tombstone is.
            r.part_del = deletion_info{base_ts + int64_t(i / 50), int32_t(i / 50)};
        }
        rows.push_back(std::move(r));
    }
    return rows;
}

bool same_cell(const cell& a, const cell& b) {
    return a.live == b.live && a.v == b.v && a.timestamp == b.timestamp &&
           a.ttl == b.ttl && a.local_deletion_time == b.local_deletion_time;
}

// L1/L2 do not carry a dead-cell representation, so a tombstoned cell simply is
// not present after a round trip. Compare only what the level claims to keep.
bool compare(const std::vector<row>& in, const std::vector<row>& out,
             folding_level lvl, std::string& why) {
    if (in.size() != out.size()) { why = "row count"; return false; }
    for (size_t i = 0; i < in.size(); ++i) {
        if (in[i].key != out[i].key) { why = "key at row " + std::to_string(i); return false; }
        // Row markers and tombstones must survive every lossless level. Losing a
        // marker deletes a row that exists; losing a tombstone resurrects one
        // that does not.
        if (lvl != folding_level::verbatim) {
            if (in[i].marker != out[i].marker) {
                why = "row " + std::to_string(i) + ": marker differs"; return false;
            }
            if (in[i].row_del != out[i].row_del) {
                why = "row " + std::to_string(i) + ": row tombstone differs"; return false;
            }
            if (in[i].part_del != out[i].part_del) {
                why = "row " + std::to_string(i) + ": partition tombstone differs"; return false;
            }
        }
        // Every cell must survive every lossless level, dead ones included.
        //
        // This check used to require the opposite: outside L0 a dead cell was
        // expected to be *absent* from the output, and preserving one was reported as
        // "dead cell resurrected". That had the concept backwards. Dropping a dead
        // cell is what resurrects data -- the deletion stops shadowing whatever it was
        // hiding, so the old value reappears on the next merge. The expectation was
        // written to match what the reassembler did (it bailed on a missing value
        // before consulting `__ldt_`), which made a real data-loss bug look correct
        // for 540 cases.
        //
        // Only L3 may discard cell metadata, and it is export-only --
        // to_parquet_for_storage() refuses it outright.
        for (const auto& [k, c] : in[i].cells) {
            auto it = out[i].cells.find(k);
            if (it == out[i].cells.end()) {
                why = "row " + std::to_string(i) + " col " + std::to_string(k) + ": cell lost";
                return false;
            }
            if (lvl == folding_level::verbatim) {
                if (!same_cell(c, it->second)) {
                    why = "row " + std::to_string(i) + " col " + std::to_string(k) + ": cell differs";
                    return false;
                }
            } else {
                if (c.v != it->second.v) {
                    why = "row " + std::to_string(i) + " col " + std::to_string(k) + ": value differs";
                    return false;
                }
                if (c.timestamp != it->second.timestamp) {
                    why = "row " + std::to_string(i) + " col " + std::to_string(k) +
                          ": timestamp " + std::to_string(c.timestamp) + " != " +
                          std::to_string(it->second.timestamp);
                    return false;
                }
                if (c.ttl != it->second.ttl) {
                    why = "row " + std::to_string(i) + " col " + std::to_string(k) + ": ttl differs";
                    return false;
                }
                if (c.live != it->second.live) {
                    why = "row " + std::to_string(i) + " col " + std::to_string(k) +
                          ": liveness differs";
                    return false;
                }
                if (c.local_deletion_time != it->second.local_deletion_time) {
                    why = "row " + std::to_string(i) + " col " + std::to_string(k) +
                          ": local deletion time differs";
                    return false;
                }
            }
        }
        // No level may invent cells either, so the counts must match exactly.
        const size_t expect = in[i].cells.size();
        if (out[i].cells.size() != expect) {
            why = "row " + std::to_string(i) + ": cell count " +
                  std::to_string(out[i].cells.size()) + " != " + std::to_string(expect);
            return false;
        }
    }
    return true;
}

int roundtrip() {
    size_t cases = 0, fails = 0, wider = 0, derived_leaves = 0;
    // How many cases actually populated each arm of the folded deletion channel. Without
    // these the new dimension could be a no-op -- every case below would still pass and
    // prove nothing, which is exactly how a real data-loss bug once looked correct for
    // 540 cases.
    size_t del_shared_rows = 0, del_exc_rows = 0;
    const double divs[] = {0.0, 0.05, 0.25, 0.5, 1.0};
    const double nulls[] = {0.0, 0.25, 0.6};
    const double ttls[] = {0.0, 0.3};
    const double dels[] = {0.0, 0.2};
    // Deletion-time divergence: all of a row's dead cells share one time, half the rows
    // diverge, or every dead cell has its own. The fold's whole premise is that the first
    // is the common case, so it has to be generated -- and the last is what the exception
    // channel exists for.
    const double ddels[] = {0.0, 0.5, 1.0};
    const size_t widths[] = {1, 5, 40};

    for (size_t w : widths) {
        auto cols = make_schema(w);
        for (double d : divs) for (double nl : nulls) for (double tt : ttls) for (double dl : dels)
        for (double dd : ddels) {
            gen_opts o; o.rows = 800; o.n_regular = w;
            o.divergence_rate = d; o.null_rate = nl; o.ttl_rate = tt; o.delete_rate = dl;
            o.del_divergence_rate = dd;
            o.marker_rate = 0.9; o.marker_ttl_rate = tt; o.row_del_rate = dl; o.part_del_rate = 0.1;
            o.extreme_ts_rate = 0.3;
            auto rows = generate(cols, o);
            for (auto lvl : {folding_level::verbatim, folding_level::row_folded,
                             folding_level::uniform})
            for (auto exc : {exception_encoding::sparse, exception_encoding::per_column})
            // Both leaf sets, over the same cases. `conservative` is what an incremental
            // row-group writer must use, because Parquet fixes one leaf set per file
            // before the first row group and the flags are otherwise derived from every
            // row. It emits metadata leaves the data does not need, so it exercises a
            // different reassembly path -- every optional leaf present but null -- and
            // losslessness has to hold there too or cutting row groups is unsafe.
            for (auto ls : {leaf_set::derived, leaf_set::conservative}) {
                ++cases;
                auto ms = map_schema(cols, lvl, rows, exc, ls);
                // Guard against the new dimension being a no-op: if `conservative` were
                // ignored, every case above would still pass and prove nothing. The
                // conservative set must never be smaller, and must sometimes be larger.
                if (ls == leaf_set::derived) {
                    derived_leaves = ms.columns.size();
                } else {
                    if (ms.columns.size() < derived_leaves) {
                        ++fails;
                        std::printf("  FAIL conservative leaf set (%zu) smaller than derived (%zu)\n",
                                    ms.columns.size(), derived_leaves);
                    }
                    if (ms.columns.size() > derived_leaves) { ++wider; }
                }
                auto data = shred(ms, cols, rows);
                // Count what the folded deletion channel actually carried, so the
                // dimension cannot pass by never being populated. A row with a dead cell
                // has a __dmask entry; a row whose dead cells disagree also has an
                // __ldtx_mask entry.
                if (ms.dmask_index) {
                    for (auto dl_ : data[*ms.dmask_index].def_levels) {
                        if (dl_) { ++del_shared_rows; }
                    }
                }
                if (ms.ldtx_mask_index) {
                    for (auto dl_ : data[*ms.ldtx_mask_index].def_levels) {
                        if (dl_) { ++del_exc_rows; }
                    }
                }
                auto back = reassemble(ms, cols, data, rows.size());
                std::string why;
                if (!compare(rows, back, ms.level, why)) {
                    ++fails;
                    std::printf("  FAIL w=%zu div=%.2f null=%.2f ttl=%.2f del=%.2f ddel=%.2f %s(->%s) exc=%s leaves=%s: %s\n",
                                w, d, nl, tt, dl, dd, to_string(lvl), to_string(ms.level),
                                exc == exception_encoding::sparse ? "sparse" : "per-col",
                                ls == leaf_set::derived ? "derived" : "conservative", why.c_str());
                    if (fails > 8) { std::printf("  (stopping after 8)\n"); goto done; }
                }
            }
        }
    }
done:
    std::printf("round-trip: %zu cases, %zu failures (%zu with a wider conservative leaf set)\n",
                cases, fails, wider);
    if (!wider) {
        std::printf("  FAIL conservative leaf set was never wider -- the flag is a no-op\n");
        ++fails;
    }
    std::printf("  folded deletion channel: %zu rows carried dead cells, %zu of them needed "
                "an exception entry\n", del_shared_rows, del_exc_rows);
    // Both arms, or the dimension proves nothing. Rows with dead cells exercise the fold;
    // rows needing an exception exercise the divergent path. Passing with either at zero
    // would mean the deletion channel was never written, or never written with divergence.
    if (!del_shared_rows) {
        std::printf("  FAIL no row ever carried a dead cell -- the deletion channel is untested\n");
        ++fails;
    }
    if (!del_exc_rows) {
        std::printf("  FAIL no row ever needed a deletion exception -- the divergent path "
                    "is untested\n");
        ++fails;
    }
    std::printf("%s\n", fails ? "SHRED ROUNDTRIP FAIL" : "SHRED ROUNDTRIP PASS");
    return fails ? 1 : 0;
}

// The real end-to-end: rows -> Parquet file -> rows, through the actual encoders,
// compressor and page layout rather than just the in-memory shred/reassemble pair.
// Until the reader existed, nothing the writer produced could be checked this way.
int filetrip() {
    size_t cases = 0, fails = 0;
    const double divs[] = {0.0, 0.1, 1.0};
    const double nulls[] = {0.0, 0.3, 0.7};
    const size_t widths[] = {1, 6, 25};
    const struct { const char* name; format::codec c; int lvl; } comps[] = {
        {"zstd3", format::codec::zstd, 3},
        {"none",  format::codec::uncompressed, 0},
    };

    for (size_t w : widths) {
        auto cols = make_schema(w);
        for (double d : divs) for (double nl : nulls) for (auto& cp : comps) {
            gen_opts o; o.rows = 1500; o.n_regular = w;
            o.divergence_rate = d; o.null_rate = nl; o.ttl_rate = 0.2;
            // Both at once: with only one of them the __ttl_/__ldt_ groups
            // happened to line up, which hid a ragged row group for years'
            // worth of cases.
            o.delete_rate = 0.15;
            // Mostly-shared deletion times with a divergent minority, so a single file
            // carries both arms of the folded channel through the real encoders, page
            // layout and compressor -- not just the in-memory shred/reassemble pair.
            o.del_divergence_rate = 0.3;
            o.marker_rate = 0.9; o.marker_ttl_rate = 0.1;
            o.row_del_rate = 0.05; o.part_del_rate = 0.1;
            auto rows = generate(cols, o);

            for (auto lvl : {folding_level::verbatim, folding_level::row_folded}) {
                ++cases;
                format::writer_options wo;
                wo.compression = cp.c;
                wo.zstd_level = cp.lvl;
                wo.page_values = 512;          // force multi-page chunks
                auto ms   = map_schema(cols, lvl, rows);
                auto data = shred(ms, cols, rows);
                format::parquet_file_writer fw(ms.columns, wo);
                fw.add_row_group(data);
                auto img = fw.finish();

                std::vector<format::column_data> back;
                try {
                    back = format::read_file(img);
                } catch (const std::exception& e) {
                    ++fails;
                    std::printf("  FAIL w=%zu div=%.1f null=%.1f %s %s: read threw: %s\n",
                                w, d, nl, cp.name, to_string(lvl), e.what());
                    continue;
                }
                if (back.size() != ms.columns.size()) {
                    ++fails;
                    std::printf("  FAIL w=%zu %s: got %zu columns, expected %zu\n",
                                w, to_string(lvl), back.size(), ms.columns.size());
                    continue;
                }
                auto rt = reassemble(ms, cols, back, rows.size());
                std::string why;
                if (!compare(rows, rt, ms.level, why)) {
                    ++fails;
                    std::printf("  FAIL w=%zu div=%.1f null=%.1f %s %s: %s\n",
                                w, d, nl, cp.name, to_string(lvl), why.c_str());
                    if (fails > 6) { std::printf("  (stopping)\n"); return 1; }
                }
            }
        }
    }
    std::printf("file round-trip: %zu cases, %zu failures\n", cases, fails);
    std::printf("%s\n", fails ? "FILE ROUNDTRIP FAIL" : "FILE ROUNDTRIP PASS");
    return fails ? 1 : 0;
}

// L3 is lossy by design, so the tests that matter are that it produces the plain
// CQL schema and that every path which could silently lose data refuses it.
int logical() {
    size_t fails = 0;
    auto cols = make_schema(6);
    gen_opts o; o.rows = 2000; o.n_regular = 6; o.divergence_rate = 0.3;
    o.null_rate = 0.2; o.ttl_rate = 0.2;
    auto rows = generate(cols, o);

    auto ms3 = map_schema(cols, folding_level::logical, rows);
    // pk + ck + 6 regular, and nothing else: no __ts, no exceptions, no ttl.
    if (ms3.leaf_count() != 8) {
        ++fails;
        std::printf("  FAIL L3 emitted %zu leaves, expected 8\n", ms3.leaf_count());
    }
    for (const auto& c : ms3.columns) {
        if (c.name.rfind("__", 0) == 0) {
            ++fails;
            std::printf("  FAIL L3 emitted a metadata column: %s\n", c.name.c_str());
        }
    }
    if (ms3.ts_index || ms3.tsx_mask_index) {
        ++fails; std::printf("  FAIL L3 materialised a timestamp channel\n");
    }

    // It must still produce a valid, readable file.
    auto img = write_rows(cols, rows, folding_level::logical);
    try {
        auto md = format::parse_footer(img);
        if (md.num_rows != int64_t(rows.size())) {
            ++fails; std::printf("  FAIL L3 file has %lld rows\n", (long long)md.num_rows);
        }
    } catch (const std::exception& e) {
        ++fails; std::printf("  FAIL L3 file does not parse: %s\n", e.what());
    }

    // And reassemble must refuse it rather than invent write times.
    bool refused = false;
    try {
        auto data = shred(ms3, cols, rows);
        (void)reassemble(ms3, cols, data, rows.size());
    } catch (const std::exception&) {
        refused = true;
    }
    if (!refused) {
        ++fails;
        std::printf("  FAIL reassemble accepted L3 instead of refusing it\n");
    }

    if (folding_is_lossless(folding_level::logical)) {
        ++fails; std::printf("  FAIL L3 reported as lossless\n");
    }
    for (auto l : {folding_level::verbatim, folding_level::row_folded, folding_level::uniform}) {
        if (!folding_is_lossless(l)) {
            ++fails; std::printf("  FAIL %s reported as lossy\n", to_string(l));
        }
    }

    std::printf("logical/L3: %zu failures\n", fails);
    std::printf("%s\n", fails ? "L3 FAIL" : "L3 PASS");
    return fails ? 1 : 0;
}

int cost() {
    auto cols = make_schema(40);
    std::printf("divergence,variant,bytes,leaf_columns,vs_base\n");
    size_t base = 0;
    for (double d : {0.0, 0.01, 0.05, 0.10, 0.25, 0.50, 1.00}) {
        gen_opts o; o.rows = 20000; o.n_regular = 40; o.divergence_rate = d; o.null_rate = 0.25;
        auto rows = generate(cols, o);
        struct { const char* name; folding_level lvl; exception_encoding exc; } variants[] = {
            {"L0",         folding_level::verbatim,   exception_encoding::sparse},
            {"L1-percol",  folding_level::row_folded, exception_encoding::per_column},
            {"L1-sparse",  folding_level::row_folded, exception_encoding::sparse},
        };
        for (auto& v : variants) {
            auto ms  = map_schema(cols, v.lvl, rows, v.exc);
            auto img = write_rows(cols, rows, v.lvl, {}, v.exc);
            if (d == 0.0 && std::string(v.name) == "L1-sparse") { base = img.size(); }
            std::printf("%.2f,%s,%zu,%zu,%.2fx\n", d, v.name, img.size(),
                        ms.leaf_count(), base ? double(img.size()) / double(base) : 0.0);
        }
    }
    return 0;
}

} // namespace

// Recovering the mapped_schema from the file alone. This is what a reader
// actually has to do: it is handed an image and a CQL schema, and nothing else.
// The write-side mapped_schema is deliberately dropped before reassembling --
// if recovery is wrong, the round trip breaks here and nowhere else.
int recovery() {
    size_t cases = 0, fails = 0;
    const double divs[]  = {0.0, 0.05, 0.5};
    const double nulls[] = {0.0, 0.3};
    const size_t widths[] = {1, 6, 25};

    for (size_t w : widths) {
        auto cols = make_schema(w);
        for (double d : divs) for (double nl : nulls)
        for (auto exc : {exception_encoding::sparse, exception_encoding::per_column}) {
            for (double ttl : {0.0, 0.2}) {
                gen_opts o; o.rows = 800; o.n_regular = w;
                o.divergence_rate = d; o.null_rate = nl; o.ttl_rate = ttl;
                o.marker_rate = 0.9; o.marker_ttl_rate = ttl > 0 ? 0.2 : 0.0;
                // Cell tombstones, which this test never generated: `row_del_rate` is a
                // *row* tombstone. So the folded deletion channel was absent from every
                // recovered schema here, and recovery of it was untested. Both divergence
                // arms, so __dmask and the __ldtx pair all have to be recovered by name.
                o.delete_rate = 0.2; o.del_divergence_rate = d;
                o.row_del_rate = 0.05; o.part_del_rate = 0.1;
                o.extreme_ts_rate = 0.3;
                auto rows = generate(cols, o);

                for (auto lvl : {folding_level::verbatim, folding_level::row_folded,
                                 folding_level::uniform}) {
                    ++cases;
                    format::writer_options wo;
                    wo.page_values = 512;
                    auto img = write_rows(cols, rows, lvl, wo, exc);

                    // Everything below sees only the image and `cols`.
                    try {
                        auto fm = format::parse_footer(img);
                        auto ms = recover_mapped_schema(fm, cols);
                        auto back = format::read_file(img);
                        auto rt = reassemble(ms, cols, back, size_t(fm.num_rows));
                        std::string why;
                        if (!compare(rows, rt, ms.level, why)) {
                            ++fails;
                            std::printf("  FAIL w=%zu div=%.2f null=%.1f ttl=%.1f %s %s: %s\n",
                                        w, d, nl, ttl,
                                        exc == exception_encoding::sparse ? "sparse" : "percol",
                                        to_string(lvl), why.c_str());
                            if (fails > 6) { std::printf("  (stopping)\n"); return 1; }
                        }
                    } catch (const std::exception& e) {
                        ++fails;
                        std::printf("  FAIL w=%zu div=%.2f %s %s: threw: %s\n", w, d,
                                    exc == exception_encoding::sparse ? "sparse" : "percol",
                                    to_string(lvl), e.what());
                        if (fails > 6) { std::printf("  (stopping)\n"); return 1; }
                    }
                }
            }
        }
    }

    // A schema that does not match the file must be refused loudly rather than
    // decoded into the wrong fields.
    {
        auto cols = make_schema(6);
        auto rows = generate(cols, [] { gen_opts o; o.rows = 100; o.n_regular = 6; return o; }());
        auto img = write_rows(cols, rows, folding_level::row_folded);
        auto fm = format::parse_footer(img);
        bool threw = false;
        try { recover_mapped_schema(fm, make_schema(7)); }
        catch (const std::exception&) { threw = true; }
        ++cases;
        if (!threw) { ++fails; std::printf("  FAIL: wrong-width schema was accepted\n"); }
    }
    // L3 is export-only: reading it back would invent cell metadata.
    {
        auto cols = make_schema(4);
        auto rows = generate(cols, [] { gen_opts o; o.rows = 100; o.n_regular = 4; return o; }());
        auto img = write_rows(cols, rows, folding_level::logical);
        auto fm = format::parse_footer(img);
        bool threw = false;
        try { recover_mapped_schema(fm, cols); }
        catch (const std::exception&) { threw = true; }
        ++cases;
        if (!threw) { ++fails; std::printf("  FAIL: L3 file was accepted for read-back\n"); }
    }

    std::printf("schema recovery: %zu cases, %zu failures\n", cases, fails);
    std::printf("%s\n", fails ? "RECOVERY FAIL" : "RECOVERY PASS");
    return fails ? 1 : 0;
}

// ------------------------------------------------- pre-fold files must still read
//
// Before the deletion channel was folded, a dead cell's deletion time went into its own
// column's `__ldt_<col>` leaf, and reassemble() decided deadness by "no value, but
// `__ldt_<col>` has one". Files written that way exist on disk and must keep reading
// exactly, or upgrading the binary resurrects every deleted cell in them.
//
// This builds such a file deliberately rather than checking one in as a fixture: the
// legacy schema comes from build_mapped_schema() with the folded channel switched off, and
// the row data is shredded here by the pre-fold rule -- `__ldt_<col>` carries the deletion
// time regardless of liveness. Then it goes through a real Parquet file and is recovered
// from its footer alone, which is the actual compatibility path: the reader sees no
// `__dmask`, infers the legacy layout, and reassemble()'s per-column clause reads the dead
// cells back.
int legacy() {
    size_t fails = 0;

    // Four bigint regular columns, so every value leaf is INT64 and the hand-shredding
    // below stays readable. The point here is the deletion channel, not type coverage.
    std::vector<cql_column> cols;
    cols.push_back({"pk", cql_type::bigint, column_kind::partition_key});
    cols.push_back({"ck", cql_type::timestamp, column_kind::clustering_key});
    for (int i = 0; i < 4; ++i) {
        cols.push_back({"c" + std::to_string(i), cql_type::bigint, column_kind::regular});
    }
    const size_t nreg = 4;

    // Every combination of the three cell states across four columns, plus the two things
    // that used to share the `__ldt_<col>` leaf: a live cell with a TTL (ldt = expiry) and
    // a dead cell (ldt = deletion time). Deletion times deliberately both shared within a
    // row and divergent.
    const int64_t base_ts = 1700000000000000LL;
    std::vector<row> rows;
    for (size_t i = 0; i < 200; ++i) {
        row r;
        r.key.push_back(int64_t(i));
        r.key.push_back(base_ts + int64_t(i) * 1000);
        for (size_t k = 0; k < nreg; ++k) {
            const size_t state = (i / (k + 1) + k) % 4;
            if (state == 0) { continue; }                    // absent
            cell c;
            c.timestamp = base_ts;                           // no divergence: keeps __tsx out
            if (state == 1) {                                // live, no TTL
                c.live = true;
                c.v = int64_t(i * 10 + k);
            } else if (state == 2) {                          // live with a TTL: ldt = expiry
                c.live = true;
                c.v = int64_t(i * 10 + k);
                c.ttl = int32_t(3600 + k);
                c.local_deletion_time = int32_t(50000 + i);
            } else {                                          // dead: ldt = deletion time
                c.live = false;
                c.local_deletion_time = (i % 3 == 0)
                        ? int32_t(90000 + i)                  // shared across the row
                        : int32_t(90000 + i + int(k) * 7);    // divergent within the row
            }
            r.cells.emplace(k, std::move(c));
        }
        rows.push_back(std::move(r));
    }

    // The legacy leaf set: TTL and per-column deletion leaves present, folded channel
    // absent. This is what build_mapped_schema() produced before the fold existed.
    schema_flags f;
    f.col_diverges.assign(nreg, false);
    f.any_ttl = true;
    f.any_live_expiry = true;    // materialises __ldt_<col>
    f.any_dead_cell = false;     // and no __dmask / __ldt / __ldtx_*
    f.all_same_ts = false;
    auto ms = build_mapped_schema(cols, folding_level::row_folded, f);
    if (ms.dmask_index || ms.ldt_index) {
        std::printf("  FAIL legacy schema materialised the folded channel\n");
        ++fails;
    }

    // Hand-shred by the pre-fold rule.
    std::vector<column_data> data(ms.columns.size());
    for (const auto& r : rows) {
        data[0].i64.push_back(std::get<int64_t>(r.key[0]));
        data[1].i64.push_back(std::get<int64_t>(r.key[1]));
        for (size_t k = 0; k < nreg; ++k) {
            const size_t vcol = ms.value_leaf[k];
            auto it = r.cells.find(k);
            const bool present = it != r.cells.end() && it->second.v.has_value();
            data[vcol].def_levels.push_back(present ? 1 : 0);
            data[vcol].i64.push_back(present ? std::get<int64_t>(*it->second.v) : 0);
            const size_t t = *ms.l1_ttl_index[k];
            const bool has_ttl = it != r.cells.end() && it->second.ttl.has_value();
            data[t].def_levels.push_back(has_ttl ? 1 : 0);
            data[t].i32.push_back(has_ttl ? *it->second.ttl : 0);
            // THE LEGACY RULE: one leaf per column, carrying both a live cell's expiry
            // and a dead cell's deletion time, told apart only by whether the value is
            // present.
            const size_t l = *ms.l1_ldt_index[k];
            const bool has_ldt = it != r.cells.end() &&
                                 it->second.local_deletion_time.has_value();
            data[l].def_levels.push_back(has_ldt ? 1 : 0);
            data[l].i32.push_back(has_ldt ? *it->second.local_deletion_time : 0);
        }
        data[*ms.ts_index].i64.push_back(base_ts);
    }

    // Through a real file, and recovered from the footer alone -- no write-side schema.
    format::writer_options wo;
    wo.page_values = 64;                 // several pages per chunk
    std::vector<std::optional<format::encoding>> hints;
    for (const auto& c : ms.columns) { hints.push_back(c.preferred); }
    format::parquet_file_writer fw(
            format::parquet_file_writer::nested_schema{ms.tree, std::move(hints)}, wo);
    fw.add_key_value("scylla.folding_level", to_string(ms.level));
    fw.add_row_group(data);
    auto img = fw.finish();

    try {
        auto fm = format::parse_footer(img);
        auto rec = recover_mapped_schema(fm, cols);
        if (rec.dmask_index) {
            std::printf("  FAIL recovered a folded channel from a pre-fold file\n");
            ++fails;
        }
        auto back = format::read_file(img);
        auto rt = reassemble(rec, cols, back, size_t(fm.num_rows));
        std::string why;
        if (!compare(rows, rt, rec.level, why)) {
            std::printf("  FAIL pre-fold file did not round-trip: %s\n", why.c_str());
            ++fails;
        }
    } catch (const std::exception& e) {
        std::printf("  FAIL pre-fold file threw: %s\n", e.what());
        ++fails;
    }

    // Count what was actually at stake, so a silently-empty case cannot pass.
    size_t dead = 0, expiry = 0;
    for (const auto& r : rows) {
        for (const auto& [k, c] : r.cells) {
            if (!c.v && c.local_deletion_time) { ++dead; }
            if (c.v && c.local_deletion_time) { ++expiry; }
        }
    }
    std::printf("pre-fold compatibility: %zu dead cells, %zu live-with-expiry cells, "
                "%zu failures\n", dead, expiry, fails);
    if (!dead || !expiry) {
        std::printf("  FAIL the fixture did not contain both cases\n");
        ++fails;
    }
    std::printf("%s\n", fails ? "LEGACY FAIL" : "LEGACY PASS");
    return fails ? 1 : 0;
}

// ---------------------------------------------------------------- collections
// Non-frozen collections are the one thing in the mutation stream that needs
// Dremel nesting rather than another leaf, so they get their own suite: the
// interesting states are absent, present-but-empty, populated, populated with a
// dead element, and deleted-and-empty. Absent and present-but-empty are the pair
// most easily conflated, and conflating them resurrects a cleared collection.
// `coll_first` puts a collection at regular index 0. That matters because the
// TTL/deletion probe in recover_mapped_schema looks at a regular column to decide
// whether the __ttl_/__ldt_ groups exist, and a collection never has them -- so
// probing a collection infers "no TTLs" and recovers a tree with the wrong number
// of leaves.
std::vector<cql_column> make_collection_schema(size_t n_scalar, size_t n_coll,
                                               bool coll_first = false) {
    std::vector<cql_column> c;
    c.push_back({"pk", cql_type::bigint, column_kind::partition_key});
    c.push_back({"ck", cql_type::timestamp, column_kind::clustering_key});
    auto add_colls = [&] {
        for (size_t i = 0; i < n_coll; ++i) {
            cql_column col{"m" + std::to_string(i), cql_type::blob, column_kind::regular};
            col.multi_cell = true;
            c.push_back(col);
        }
    };
    static const cql_type cycle[] = {cql_type::bigint, cql_type::int32,
                                     cql_type::dbl, cql_type::text};
    if (coll_first) { add_colls(); }
    for (size_t i = 0; i < n_scalar; ++i) {
        c.push_back({"c" + std::to_string(i), cycle[i % 4], column_kind::regular});
    }
    if (!coll_first) { add_colls(); }
    return c;
}

std::vector<row> generate_with_collections(const std::vector<cql_column>& cols,
                                          size_t nrows, size_t n_scalar, size_t n_coll,
                                          uint64_t seed, bool coll_first = false) {
    const size_t scalar_base = coll_first ? n_coll : 0;
    const size_t coll_base   = coll_first ? 0 : n_scalar;
    std::mt19937_64 rng(seed);
    auto unit = [&] { return double(rng() % 1000000) / 1000000.0; };
    const int64_t base_ts = 1700000000000000LL;
    std::vector<row> rows;

    for (size_t i = 0; i < nrows; ++i) {
        row r;
        r.key.push_back(int64_t(i) * 13 + 1);
        r.key.push_back(base_ts + int64_t(i) * 1000);
        const int64_t row_ts = base_ts + int64_t(rng() % 1000000);

        for (size_t kk = 0; kk < n_scalar; ++kk) {
            const size_t k = scalar_base + kk;
            if (unit() < 0.3) { continue; }
            cell c;
            c.live = true;
            c.timestamp = row_ts;
            // Some scalars carry a TTL, so the __ttl_/__ldt_ groups exist and the
            // recovery probe has something to find.
            if (unit() < 0.25) { c.ttl = int32_t(3600); }
            if (unit() < 0.15) { c.live = false; c.local_deletion_time = int32_t(kk + 1); }
            switch (cols[2 + k].type) {
            case cql_type::bigint: c.v = int64_t(rng() % 100000); break;
            case cql_type::int32:  c.v = int32_t(rng() % 1000); break;
            case cql_type::dbl:    c.v = double(rng() % 10000) / 100.0; break;
            default:               c.v = std::string("s") + std::to_string(rng() % 20); break;
            }
            if (!c.live) { c.v.reset(); }
            r.cells.emplace(k, std::move(c));
        }

        for (size_t j = 0; j < n_coll; ++j) {
            const size_t k = coll_base + j;
            const size_t kind = (i + j) % 6;
            if (kind == 0) { continue; }                       // absent
            collection_cell cc;
            if (kind == 1) {
                // present but empty
            } else if (kind == 5) {
                // deleted, and empty with it
                cc.tomb = deletion_info{row_ts - 1, int32_t(i % 1000)};
            } else {
                const size_t n = 1 + (rng() % 4);
                for (size_t e = 0; e < n; ++e) {
                    collection_element el;
                    el.key = "k" + std::to_string((i + e) % 11);
                    if (!(kind == 3 && e == 0)) {
                        el.value = "v" + std::to_string(rng() % 50);
                    }                                          // else: dead element
                    el.timestamp = row_ts + int64_t(e);
                    if (kind == 4 && e == 1) {
                        el.ttl = int32_t(3600 + e);
                        el.local_deletion_time = int32_t(i + e);
                    }
                    cc.elements.push_back(std::move(el));
                }
                if (kind == 2 && (i % 5) == 0) {
                    cc.tomb = deletion_info{row_ts - 2, int32_t(i % 500)};
                }
            }
            r.collections.emplace(k, std::move(cc));
        }
        rows.push_back(std::move(r));
    }
    return rows;
}

bool compare_collections(const std::vector<row>& in, const std::vector<row>& out,
                         std::string& why) {
    for (size_t i = 0; i < in.size(); ++i) {
        if (in[i].collections.size() != out[i].collections.size()) {
            why = "row " + std::to_string(i) + ": collection count " +
                  std::to_string(out[i].collections.size()) + " != " +
                  std::to_string(in[i].collections.size());
            return false;
        }
        for (const auto& [k, want] : in[i].collections) {
            auto it = out[i].collections.find(k);
            if (it == out[i].collections.end()) {
                why = "row " + std::to_string(i) + " col " + std::to_string(k) + ": lost";
                return false;
            }
            if (!(it->second == want)) {
                why = "row " + std::to_string(i) + " col " + std::to_string(k) + ": differs (" +
                      std::to_string(want.elements.size()) + " elements in, " +
                      std::to_string(it->second.elements.size()) + " out, tomb " +
                      (want.tomb ? "set" : "unset") + "/" +
                      (it->second.tomb ? "set" : "unset") + ")";
                return false;
            }
        }
    }
    return true;
}

int collections() {
    size_t cases = 0, fails = 0;
    for (bool coll_first : {false, true}) {
    for (size_t n_scalar : {0u, 2u, 5u}) {
        for (size_t n_coll : {1u, 3u}) {
            for (size_t nrows : {1u, 7u, 900u}) {
                auto cols = make_collection_schema(n_scalar, n_coll, coll_first);
                auto rows = generate_with_collections(cols, nrows, n_scalar, n_coll,
                                                      5 + nrows, coll_first);

                // In-memory: shred then reassemble.
                for (auto lvl : {folding_level::row_folded, folding_level::verbatim}) {
                    ++cases;
                    auto ms = map_schema(cols, lvl, rows);
                    auto data = shred(ms, cols, rows);
                    auto back = reassemble(ms, cols, data, rows.size());
                    std::string why;
                    if (back.size() != rows.size()) {
                        ++fails;
                        std::printf("  FAIL sc=%zu coll=%zu rows=%zu %s: %zu rows back\n",
                                    n_scalar, n_coll, nrows, to_string(lvl), back.size());
                        continue;
                    }
                    if (!compare_collections(rows, back, why)) {
                        ++fails;
                        std::printf("  FAIL sc=%zu coll=%zu rows=%zu %s: %s\n",
                                    n_scalar, n_coll, nrows, to_string(lvl), why.c_str());
                        if (fails > 5) { return 1; }
                    }
                }

                // Through a real file, which is what exercises the nesting.
                ++cases;
                try {
                    format::writer_options wo;
                    wo.page_values = 128;              // several pages per chunk
                    auto img = write_rows(cols, rows, folding_level::row_folded, wo);
                    auto fm = format::parse_footer(img);
                    auto ms2 = recover_mapped_schema(fm, cols);
                    auto cd = format::read_row_group(img, fm, 0);
                    auto back = reassemble(ms2, cols, cd, size_t(fm.num_rows));
                    std::string why;
                    if (!compare_collections(rows, back, why)) {
                        ++fails;
                        std::printf("  FAIL file sc=%zu coll=%zu rows=%zu: %s\n",
                                    n_scalar, n_coll, nrows, why.c_str());
                    }
                } catch (const std::exception& e) {
                    ++fails;
                    std::printf("  FAIL file sc=%zu coll=%zu rows=%zu threw: %s\n",
                                n_scalar, n_coll, nrows, e.what());
                }
            }
        }
    }
    }
    std::printf("collections: %zu cases, %zu failures\n", cases, fails);
    std::printf("%s\n", fails ? "COLLECTIONS FAIL" : "COLLECTIONS PASS");
    return fails ? 1 : 0;
}

namespace {

const char* const k_usage = "{roundtrip|filetrip|logical|recovery|legacy|collections|cost}";

struct subcommand {
    const char* name;
    int (*fn)();
};

const subcommand k_subcommands[] = {
    {"roundtrip",   roundtrip},
    {"filetrip",    filetrip},
    {"logical",     logical},
    {"recovery",    recovery},
    {"legacy",      legacy},
    {"collections", collections},
    {"cost",        cost},
};

} // namespace

// The subcommand is found by scanning the whole argv rather than by reading argv[1].
//
// That is not defensive over-engineering, it is what the test/unit harness requires.
// `test/pylib/cpp/base.py` builds the command line as
//
//     [exe] + DEFAULT_SCYLLA_ARGS + shlex.split(custom_args_entry)
//
// and DEFAULT_SCYLLA_ARGS is a hardcoded six-flag seastar list (--overprovisioned,
// --unsafe-bypass-fsync=1, ...) that no suite config can switch off. So under CI
// argv[1] is "--overprovisioned" and the subcommand is last. Reading argv[1] would
// make every CI case exit 2 -- which would at least be loudly red, but reading the
// last argument instead would silently couple us to that flag list's shape. Scanning
// for the one token we recognise works identically under `run_tests.sh`, which passes
// the subcommand as argv[1] and nothing else.
//
// Not finding a subcommand is exit 2, not exit 0: a harness change that stopped
// passing one must fail, not quietly pass having tested nothing.
int main(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        for (const auto& s : k_subcommands) {
            if (std::string(argv[i]) != s.name) { continue; }
            try {
                return s.fn();
            } catch (const std::exception& e) {
                std::fprintf(stderr, "error: %s\n", e.what());
                return 1;
            }
        }
    }
    std::fprintf(stderr, "usage: %s %s\n", argv[0], k_usage);
    std::fprintf(stderr, "no recognised subcommand in %d argument(s)\n", argc - 1);
    return 2;
}
