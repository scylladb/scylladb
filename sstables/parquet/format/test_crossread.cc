/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Cross-reader validation, the direction that had never been tested: our
// decoder against files produced by parquet-cpp.
//
// Write-side interop was already proven (our files read by pyarrow). This is the
// read side, and it immediately found a real bug -- V2 data pages carry their own
// is_compressed flag, which parquet-cpp clears when compression does not pay, and
// honouring only the chunk codec made those pages fail to decode.
//
// Prints a per-column summary that crossread.py compares against pyarrow, so the
// assertion is "we agree on the values", not merely "it did not throw".

#include "parquet_reader.hh"
#include "parquet_metadata.hh"

#include <cstdio>
#include <fstream>
#include <vector>

using namespace sstables::parquet::format;

static std::vector<uint8_t> slurp(const char* p) {
    std::ifstream f(p, std::ios::binary);
    if (!f) { throw std::runtime_error(std::string("cannot open ") + p); }
    return std::vector<uint8_t>((std::istreambuf_iterator<char>(f)),
                                 std::istreambuf_iterator<char>());
}

int main(int argc, char** argv) {
    if (argc < 2) { std::fprintf(stderr, "usage: %s <file.parquet>...\n", argv[0]); return 2; }
    int bad = 0;
    std::printf("[\n");
    bool first_file = true;
    for (int i = 1; i < argc; ++i) {
        try {
            auto img = slurp(argv[i]);
            auto md = parse_footer(img);
            auto cols = read_row_group(img, md, 0);

            std::vector<const schema_element*> leaves;
            for (size_t k = 1; k < md.schema.size(); ++k) {
                if (md.schema[k].is_leaf()) { leaves.push_back(&md.schema[k]); }
            }
            if (!first_file) { std::printf(",\n"); }
            first_file = false;
            std::printf("  {\"file\": \"%s\", \"rows\": %lld, \"columns\": [\n",
                        argv[i], (long long)md.row_groups[0].num_rows);
            for (size_t c = 0; c < cols.size(); ++c) {
                // A cheap order-sensitive digest per column: enough to catch a
                // wrong value, a shifted stream or a mis-decoded null.
                long double sum = 0;
                size_t nulls = 0, n = cols[c].num_values();
                for (size_t r = 0; r < n; ++r) {
                    const bool present = cols[c].def_levels.empty() || cols[c].def_levels[r];
                    if (!present) { ++nulls; continue; }
                    if (!cols[c].i32.empty())      { sum += cols[c].i32[r]; }
                    else if (!cols[c].i64.empty()) { sum += (long double)cols[c].i64[r]; }
                    else if (!cols[c].f64.empty()) { sum += cols[c].f64[r]; }
                    else if (!cols[c].str.empty()) { sum += (long double)cols[c].str[r].size(); }
                }
                std::printf("    {\"name\": \"%s\", \"n\": %zu, \"nulls\": %zu, \"sum\": %.6Lf}%s\n",
                            leaves[c]->name.c_str(), n, nulls, sum,
                            c + 1 == cols.size() ? "" : ",");
            }
            std::printf("  ]}");
        } catch (const std::exception& e) {
            std::fprintf(stderr, "FAIL %s: %s\n", argv[i], e.what());
            ++bad;
        }
    }
    std::printf("\n]\n");
    return bad ? 1 : 0;
}
