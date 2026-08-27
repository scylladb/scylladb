/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Writes Parquet files with our own writer, for two purposes:
//   * our reader parses them back (self-consistency);
//   * pyarrow/DuckDB read them and agree on every value (interop --
//     checked by writer_interop.py, which owns the real assertion).
//
//   test_writer emit <dir>   -- write the fixture set and a JSON manifest
//
// The manifest records the exact values written so the Python side can compare
// without trusting anything our code produced.

#include "parquet_writer.hh"
#include "parquet_metadata.hh"

#include <cstdio>
#include <fstream>
#include <random>
#include <string>
#include <vector>

using namespace sstables::parquet::format;

namespace {

struct fixture {
    std::string name;
    writer_options opt;
    size_t rows;
    bool with_nulls;
    bool delta_ts;
    // Distinct values in the `status` column. One is the interesting case: the
    // dictionary then needs zero bits to address, which parquet-cpp rejects
    // unless the writer clamps the width. Regression cover for that.
    size_t status_card = 5;
};

void jstr(std::ostream& o, const std::string& s) {
    o << '"';
    for (char c : s) {
        if (c == '"' || c == '\\') { o << '\\' << c; }
        else if (uint8_t(c) < 0x20) { char b[8]; std::snprintf(b, sizeof b, "\\u%04x", c); o << b; }
        else { o << c; }
    }
    o << '"';
}

int emit(const std::string& dir) {
    std::vector<fixture> fx = {
        {"w_plain_nonull",  {.compression = codec::zstd, .zstd_level = 3, .page_values = 20000, .use_dictionary = false, .dictionary_max_bytes = 1u<<20, .write_statistics = true}, 50000, false, false},
        {"w_plain_nulls",   {.compression = codec::zstd, .zstd_level = 3, .page_values = 20000, .use_dictionary = false, .dictionary_max_bytes = 1u<<20, .write_statistics = true}, 50000, true,  false},
        {"w_dict_nulls",    {.compression = codec::zstd, .zstd_level = 3, .page_values = 20000, .use_dictionary = true, .dictionary_max_bytes = 1u<<20, .write_statistics = true}, 50000, true,  false},
        {"w_delta_ts",      {.compression = codec::zstd, .zstd_level = 3, .page_values = 20000, .use_dictionary = true, .dictionary_max_bytes = 1u<<20, .write_statistics = true}, 50000, true,  true},
        {"w_uncompressed",  {.compression = codec::uncompressed, .zstd_level = 0, .page_values = 8192, .use_dictionary = true, .dictionary_max_bytes = 1u<<20, .write_statistics = true}, 20000, true, true},
        {"w_multipage",     {.compression = codec::zstd, .zstd_level = 3, .page_values = 1000, .use_dictionary = false, .dictionary_max_bytes = 1u<<20, .write_statistics = true}, 20000, true,  false},
        {"w_tiny",          {.compression = codec::zstd, .zstd_level = 3, .page_values = 20000, .use_dictionary = true, .dictionary_max_bytes = 1u<<20, .write_statistics = true}, 7,     true,  false},
        {"w_dict_single",   {.compression = codec::zstd, .zstd_level = 3, .page_values = 20000, .use_dictionary = true, .dictionary_max_bytes = 1u<<20, .write_statistics = true}, 30000, true,  false, 1},
        {"w_dict_two",      {.compression = codec::zstd, .zstd_level = 3, .page_values = 20000, .use_dictionary = true, .dictionary_max_bytes = 1u<<20, .write_statistics = true}, 30000, true,  false, 2},
    };

    std::ofstream man(dir + "/manifest.json");
    man << "[\n";
    bool firstfx = true;

    for (const auto& f : fx) {
        std::mt19937_64 rng(1234);
        const size_t n = f.rows;

        std::vector<column_spec> schema = {
            {"id",    phys_type::int64,      repetition::required, std::nullopt, std::nullopt},
            {"grade", phys_type::int32,      repetition::optional, std::nullopt, std::nullopt},
            {"amount",phys_type::dbl,        repetition::optional, std::nullopt, std::nullopt},
            {"status",phys_type::byte_array, repetition::optional,
             int32_t(converted::utf8), std::nullopt},
            {"__ts",  phys_type::int64,      repetition::required, std::nullopt,
             f.delta_ts ? std::optional<encoding>(encoding::delta_binary_packed) : std::nullopt},
        };

        std::vector<column_data> cols(schema.size());
        // id: dense, monotonically increasing
        cols[0].i64.reserve(n);
        // grade / amount / status: optional, ~25% null when with_nulls
        cols[1].i32.reserve(n); cols[1].def_levels.reserve(n);
        cols[2].f64.reserve(n); cols[2].def_levels.reserve(n);
        cols[3].str.reserve(n); cols[3].def_levels.reserve(n);
        cols[4].i64.reserve(n);

        static const char* STATUS[] = {"active", "pending", "closed", "archived", "error"};
        int64_t ts = 1700000000000000LL;

        for (size_t i = 0; i < n; ++i) {
            cols[0].i64.push_back(int64_t(i) * 7 + 3);

            // Every draw below is unconditional so the value stream is trivial
            // to mirror from Python in writer_interop.py. Short-circuiting the
            // draws would make the sequence depend on the null pattern.
            const bool present = !f.with_nulls || (rng() % 4) != 0;
            const int32_t gv = int32_t(rng() % 100);
            cols[1].def_levels.push_back(present ? 1 : 0);
            cols[1].i32.push_back(present ? gv : 0);

            const bool p2 = !f.with_nulls || (rng() % 4) != 0;
            const double av = double(rng() % 1000000) / 100.0;
            cols[2].def_levels.push_back(p2 ? 1 : 0);
            cols[2].f64.push_back(p2 ? av : 0.0);

            const bool p3 = !f.with_nulls || (rng() % 4) != 0;
            const std::string sv = STATUS[rng() % f.status_card];
            cols[3].def_levels.push_back(p3 ? 1 : 0);
            cols[3].str.push_back(p3 ? sv : std::string());

            ts += int64_t(rng() % 1000);        // realistic: monotone with jitter
            cols[4].i64.push_back(ts);
        }

        parquet_file_writer w(schema, f.opt);
        w.add_key_value("scylla.folding_level", "L1");
        w.add_key_value("scylla.table", "pqlab.demo");
        w.add_row_group(cols);
        auto img = w.finish();

        std::string path = dir + "/" + f.name + ".parquet";
        std::ofstream(path, std::ios::binary)
            .write(reinterpret_cast<const char*>(img.data()), std::streamsize(img.size()));

        // Self-check with our own reader before handing to pyarrow.
        auto md = parse_footer(img);
        if (md.num_rows != int64_t(n) || md.leaf_count() != schema.size()) {
            std::printf("  SELF-CHECK FAIL %s: rows=%lld leaves=%zu\n",
                        f.name.c_str(), (long long)md.num_rows, md.leaf_count());
            return 1;
        }

        if (!firstfx) { man << ",\n"; }
        firstfx = false;
        man << "  {\"name\": "; jstr(man, f.name);
        man << ", \"path\": "; jstr(man, path);
        man << ", \"rows\": " << n
            << ", \"bytes\": " << img.size()
            << ", \"compression\": \"" << to_string(f.opt.compression) << "\""
            << ", \"dict\": " << (f.opt.use_dictionary ? "true" : "false")
            << ", \"page_values\": " << f.opt.page_values
            << ", \"delta_ts\": " << (f.delta_ts ? "true" : "false")
            << ", \"nulls\": " << (f.with_nulls ? "true" : "false")
            << ", \"status_card\": " << f.status_card
            << ", \"seed\": 1234}";
        std::printf("  wrote %-16s %8zu rows  %9zu B  (%s%s%s)\n", f.name.c_str(), n, img.size(),
                    to_string(f.opt.compression),
                    f.opt.use_dictionary ? ", dict" : "",
                    f.delta_ts ? ", delta-ts" : "");
    }
    man << "\n]\n";
    return 0;
}

} // namespace


// The whole file image is held in memory until finish(), so peak write memory is O(output).
// Pinned rather than merely noted in the design doc, because the fix changes offset
// arithmetic throughout the writer and this is what will catch a drain that forgets to keep
// absolute file positions: if row groups are ever streamed out, size_so_far() stops tracking
// the total and this assertion has to be updated deliberately.
//
// This is separate from the shredder budget (R-13), which *is* bounded: 5 000 rows or 64 MiB
// of buffered rows, whichever trips first. The bounded thing is the input side; the output
// image is not bounded by anything.
// Peak write memory, both ways, and the property that makes the streaming path safe: the
// streamed bytes must be byte-identical to the buffered image. Every offset the writer
// records -- page locations, column-chunk starts, the OffsetIndex, the footer's pointers --
// is a file position, so a drain that forgot the flushed base would still produce a
// parseable file pointing at the wrong bytes. Comparing the two images catches exactly that,
// because only the buffered path can be wrong in a way the other is not.
int check_streaming_matches_buffered() {
    auto build = [] (bool streaming, size_t* peak_buffered) {
        std::vector<column_spec> schema{
            {.name = "k", .type = phys_type::int64, .rep = repetition::required},
            {.name = "v", .type = phys_type::byte_array, .rep = repetition::optional,
             .max_def = 1},
        };
        static const char* WORDS[] = {"active", "pending", "closed", "archived"};
        parquet_file_writer w(schema, writer_options{});
        std::vector<uint8_t> streamed;
        if (streaming) {
            // The sink sees exactly what was buffered at each drain, so the largest chunk
            // *is* the peak buffer. Sampling buffered_bytes() after add_row_group() cannot
            // see it -- the drain happens inside, so it always reads zero.
            w.set_sink([&streamed, peak_buffered] (std::span<const uint8_t> b) {
                *peak_buffered = std::max(*peak_buffered, b.size());
                streamed.insert(streamed.end(), b.begin(), b.end());
            });
        }
        *peak_buffered = 0;
        for (int rg = 0; rg < 8; ++rg) {
            // In buffered mode this is the running file size, which peaks just before
            // finish() moves the buffer out -- so the footer is not included and the peak
            // reads a little under the final image. Close enough to make the point.
            *peak_buffered = std::max(*peak_buffered, w.buffered_bytes());
            std::vector<column_data> cols(2);
            for (int i = 0; i < 5000; ++i) {
                const int64_t k = int64_t(rg) * 5000 + i;
                cols[0].i64.push_back(k);
                const bool present = (i % 7) != 0;
                cols[1].def_levels.push_back(present ? 1 : 0);
                cols[1].str.push_back(present ? std::string(WORDS[i % 4]) : std::string());
            }
            w.add_row_group(cols);
            *peak_buffered = std::max(*peak_buffered, w.buffered_bytes());
        }
        auto img = w.finish();
        return streaming ? streamed : img;
    };

    int fail = 0;
    size_t peak_buffered = 0, peak_streamed = 0;
    const auto buffered = build(false, &peak_buffered);
    const auto streamed = build(true,  &peak_streamed);

    if (buffered != streamed) {
        std::printf("  FAIL streamed image differs from buffered (%zu vs %zu bytes)\n",
                    buffered.size(), streamed.size());
        ++fail;
    }
    // Buffered mode holds the whole file; streaming must hold far less. The bound is one row
    // group plus the footer, so a small multiple of a row group -- asserted loosely as "under
    // a third", which a regression that stopped draining would blow through immediately.
    // Buffered mode holds essentially the whole file. Not exactly all of it: the peak is
    // sampled before finish() appends the footer and moves the buffer out, so 95 % is the
    // honest bar.
    if (peak_buffered * 100 < buffered.size() * 95) {
        std::printf("  FAIL buffered peak %zu is under 95 %% of the %zu B image\n",
                    peak_buffered, buffered.size());
        ++fail;
    }
    if (peak_streamed * 3 > buffered.size()) {
        std::printf("  FAIL streaming peak %zu is not far below the %zu B image\n",
                    peak_streamed, buffered.size());
        ++fail;
    }
    std::printf("  image %zu B: buffered peak %zu B (%.0f %%), streaming peak %zu B (%.0f %%)"
                " -- identical bytes: %s\n",
                buffered.size(), peak_buffered,
                100.0 * double(peak_buffered) / double(buffered.size()),
                peak_streamed, 100.0 * double(peak_streamed) / double(buffered.size()),
                buffered == streamed ? "yes" : "NO");
    return fail;
}

int main(int argc, char** argv) {
    int extra_fail = check_streaming_matches_buffered();
    // Runs with or without an emit target, so the image-accounting check is not something
    // you can skip by invoking the tool the usual way.
    if (argc < 3) {
        std::fprintf(stderr, "usage: %s emit <dir>\n", argv[0]);
        return extra_fail ? 1 : 2;
    }
    try {
        if (std::string(argv[1]) == "emit") { return emit(argv[2]) + extra_fail; }
        std::fprintf(stderr, "unknown command\n"); return 2;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "error: %s\n", e.what()); return 1;
    }
}
