/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// End-to-end test of the level pipeline: Thrift footer -> column chunk ->
// page header -> RLE/bit-packed definition levels -> null count.
//
// The assertion is independent of our own code: the null count we compute by
// decoding levels ourselves must equal the null count the *writer* recorded in
// the page header and in the column chunk statistics. Two independent sources
// of truth, produced by parquet-cpp, that we never look at while decoding.
//
//   test_levels roundtrip              -- RLE encoder/decoder property test
//   test_levels levels <file.parquet>  -- decode real V2 pages, check nulls

#include "parquet_metadata.hh"
#include "page_header.hh"
#include "rle_bitpack.hh"

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <random>
#include <vector>

using namespace sstables::parquet::format;

static std::vector<uint8_t> slurp(const char* p) {
    std::ifstream f(p, std::ios::binary);
    if (!f) { throw std::runtime_error("cannot open"); }
    return std::vector<uint8_t>((std::istreambuf_iterator<char>(f)),
                                 std::istreambuf_iterator<char>());
}

// ---------------------------------------------------------------- round trip
static int roundtrip() {
    std::mt19937_64 rng(99);
    size_t cases = 0, fails = 0;

    auto try_one = [&](const std::vector<uint64_t>& vals, uint8_t bw, const char* what) {
        ++cases;
        rle_encoder enc(bw);
        enc.encode(vals);
        rle_decoder dec(std::span<const uint8_t>(enc.bytes()), bw);
        auto got = dec.decode_all(vals.size());
        if (got.size() != vals.size() || !std::equal(got.begin(), got.end(), vals.begin())) {
            ++fails;
            std::printf("  FAIL %s bw=%u n=%zu (decoded %zu)\n", what, bw, vals.size(), got.size());
            for (size_t i = 0; i < vals.size() && i < 16; ++i) {
                std::printf("     [%zu] want %llu got %llu\n", i,
                            (unsigned long long)vals[i],
                            i < got.size() ? (unsigned long long)got[i] : 0ull);
            }
        }
    };

    for (uint8_t bw : {1, 2, 3, 4, 5, 7, 8, 12, 16, 20, 32}) {
        uint64_t maxv = bw == 64 ? ~0ull : ((1ull << bw) - 1);

        // all zeros / all max -- pure RLE
        try_one(std::vector<uint64_t>(1000, 0), bw, "all-zero");
        try_one(std::vector<uint64_t>(1000, maxv), bw, "all-max");

        // random -- pure bit-packed
        for (size_t n : {1u, 7u, 8u, 9u, 63u, 64u, 65u, 1000u, 4097u}) {
            std::vector<uint64_t> v(n);
            for (auto& x : v) { x = rng() & maxv; }
            try_one(v, bw, "random");
        }

        // level-like: mostly one value, occasional others (the real shape)
        for (double p : {0.01, 0.1, 0.5}) {
            std::vector<uint64_t> v(5000);
            for (auto& x : v) {
                x = (double(rng() % 1000) / 1000.0 < p) ? (rng() & maxv) : 0;
            }
            try_one(v, bw, "sparse");
        }

        // long runs alternating with noise
        std::vector<uint64_t> v;
        for (int r = 0; r < 20; ++r) {
            uint64_t val = rng() & maxv;
            size_t len = 1 + (rng() % 300);
            for (size_t i = 0; i < len; ++i) { v.push_back(val); }
            for (int k = 0; k < 5; ++k) { v.push_back(rng() & maxv); }
        }
        try_one(v, bw, "runs+noise");
    }

    // bit_width 0 => everything is zero, nothing on the wire
    {
        ++cases;
        std::vector<uint64_t> v(100, 0);
        rle_encoder enc(0); enc.encode(v);
        rle_decoder dec(std::span<const uint8_t>(enc.bytes()), 0);
        auto got = dec.decode_all(100);
        if (got.size() != 100 || got[0] != 0) { ++fails; std::printf("  FAIL bw=0\n"); }
    }

    std::printf("round-trip: %zu cases, %zu failures\n", cases, fails);
    std::printf("%s\n", fails ? "ROUNDTRIP FAIL" : "ROUNDTRIP PASS");
    return fails ? 1 : 0;
}

// ---------------------------------------------------------------- real pages
static int levels(const char* path) {
    auto img = slurp(path);
    auto md = parse_footer(img);
    std::printf("%s\n  %lld rows, %zu row groups, %zu leaves, writer=%s\n", path,
                (long long)md.num_rows, md.row_groups.size(), md.leaf_count(),
                md.created_by.value_or("?").c_str());

    // max definition level for a leaf = number of optional/repeated ancestors.
    // Our test files are flat, so an OPTIONAL leaf has max def level 1.
    size_t checked = 0, mismatches = 0, v2pages = 0, skipped = 0;

    for (size_t g = 0; g < md.row_groups.size(); ++g) {
        for (const auto& cc : md.row_groups[g].columns) {
            if (!cc.meta) { continue; }
            const auto& cm = *cc.meta;
            // Find the schema element for this leaf to get its repetition type.
            const schema_element* se = nullptr;
            for (size_t i = 1; i < md.schema.size(); ++i) {
                if (md.schema[i].is_leaf() && md.schema[i].name == cm.path_in_schema.back()) {
                    se = &md.schema[i]; break;
                }
            }
            if (!se || !se->repetition_type || *se->repetition_type != repetition::optional) {
                continue;   // required leaf: no definition levels stored
            }

            int64_t off = cm.dictionary_page_offset ? *cm.dictionary_page_offset : cm.data_page_offset;
            int64_t end = off + cm.total_compressed_size;
            if (off < 0 || size_t(end) > img.size()) { continue; }

            int64_t nulls_seen = 0, values_seen = 0;
            bool usable = true;

            while (off < end) {
                size_t consumed = 0;
                page_header ph;
                try {
                    ph = parse_page_header(std::span<const uint8_t>(img).subspan(size_t(off),
                                              size_t(std::min<int64_t>(end - off, 4096))), consumed);
                } catch (const std::exception&) { usable = false; break; }

                int64_t body = off + int64_t(consumed);
                if (ph.type == page_type::data_page_v2 && ph.v2) {
                    // V2: level streams are stored uncompressed, always.
                    ++v2pages;
                    const auto& h = *ph.v2;
                    int64_t dl_len = h.definition_levels_byte_length;
                    int64_t rl_len = h.repetition_levels_byte_length;
                    if (dl_len < 0 || body + rl_len + dl_len > int64_t(img.size())) { usable = false; break; }
                    auto dl = std::span<const uint8_t>(img).subspan(size_t(body + rl_len), size_t(dl_len));
                    // Max def level 1 for a flat optional column => bit width 1.
                    rle_decoder dec(dl, 1);
                    auto lv = dec.decode_all(size_t(h.num_values));
                    if (lv.size() != size_t(h.num_values)) { usable = false; break; }
                    int64_t n = 0;
                    for (auto x : lv) { if (x == 0) { ++n; } }
                    // Cross-check against the writer's own count for this page.
                    if (n != h.num_nulls) {
                        ++mismatches;
                        std::printf("  MISMATCH %s page: decoded %lld nulls, header says %d\n",
                                    cm.path().c_str(), (long long)n, h.num_nulls);
                    }
                    nulls_seen += n;
                    values_seen += h.num_values;
                } else {
                    // V1 data pages interleave compressed levels with values;
                    // decoding them needs a codec, which layer 1 does not own yet.
                    ++skipped;
                }
                off = body + ph.compressed_page_size;
                if (ph.compressed_page_size <= 0) { usable = false; break; }
            }

            if (usable && values_seen > 0 && cm.stats && cm.stats->null_count) {
                ++checked;
                if (nulls_seen != *cm.stats->null_count) {
                    ++mismatches;
                    std::printf("  MISMATCH %s chunk: decoded %lld nulls, statistics say %lld\n",
                                cm.path().c_str(), (long long)nulls_seen,
                                (long long)*cm.stats->null_count);
                }
            }
        }
    }
    std::printf("  V2 pages decoded: %zu | chunks cross-checked vs statistics: %zu | "
                "V1 pages skipped (need codec): %zu\n", v2pages, checked, skipped);
    // A run that decoded nothing proves nothing. Treat zero coverage as failure
    // so the test cannot pass vacuously on a file with no V2 data pages.
    if (v2pages == 0) {
        std::printf("  LEVELS FAIL: no V2 data pages in this file (nothing exercised)\n");
        return 1;
    }
    if (checked == 0) {
        std::printf("  LEVELS FAIL: no chunk could be cross-checked against statistics\n");
        return 1;
    }
    std::printf("  %s\n", mismatches == 0 ? "LEVELS PASS" : "LEVELS FAIL");
    return mismatches == 0 ? 0 : 1;
}

int main(int argc, char** argv) {
    if (argc < 2) { std::fprintf(stderr, "usage: %s {roundtrip|levels <file>}\n", argv[0]); return 2; }
    try {
        std::string c = argv[1];
        if (c == "roundtrip") { return roundtrip(); }
        if (c == "levels" && argc >= 3) { return levels(argv[2]); }
        std::fprintf(stderr, "bad args\n"); return 2;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "error: %s\n", e.what()); return 1;
    }
}
