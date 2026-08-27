/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// read_row_range() must return exactly what read_row_group() returns, sliced.
//
// That equivalence is the entire safety argument for the bounded reader: a point
// read decodes a few pages instead of the whole file, and it is only correct if
// the values it produces are indistinguishable from the full decode. Pages are
// decoded whole and then trimmed, columns need not share page boundaries, and
// null expansion happens before trimming -- three places where an off-by-one
// would silently shift a column against its neighbours rather than crash.
//
// Also checks the byte-slice form: decoding from a sub-span of the file with a
// base_offset must match decoding from the whole image.

#include "parquet_reader.hh"
#include "parquet_metadata.hh"

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <string>
#include <vector>

using namespace sstables::parquet::format;

namespace {

std::vector<uint8_t> slurp(const std::string& p) {
    std::ifstream f(p, std::ios::binary);
    return std::vector<uint8_t>((std::istreambuf_iterator<char>(f)),
                                 std::istreambuf_iterator<char>());
}

template <typename T>
bool eq_slice(const std::vector<T>& full, const std::vector<T>& part,
              size_t lo, size_t n, const char* what, std::string& why) {
    if (full.empty() && part.empty()) { return true; }
    if (part.size() != n) {
        why = std::string(what) + ": got " + std::to_string(part.size()) +
              " values, want " + std::to_string(n);
        return false;
    }
    for (size_t i = 0; i < n; ++i) {
        if (!(full[lo + i] == part[i])) {
            why = std::string(what) + ": mismatch at " + std::to_string(i);
            return false;
        }
    }
    return true;
}

bool compare(const std::vector<column_data>& full, const std::vector<column_data>& part,
             size_t lo, size_t n, std::string& why) {
    if (full.size() != part.size()) { why = "column count differs"; return false; }
    for (size_t c = 0; c < full.size(); ++c) {
        const std::string tag = "col" + std::to_string(c);
        if (!eq_slice(full[c].def_levels, part[c].def_levels, lo, n,
                      (tag + ".def").c_str(), why)) { return false; }
        if (!eq_slice(full[c].i32, part[c].i32, lo, n, (tag + ".i32").c_str(), why)) { return false; }
        if (!eq_slice(full[c].i64, part[c].i64, lo, n, (tag + ".i64").c_str(), why)) { return false; }
        if (!eq_slice(full[c].f64, part[c].f64, lo, n, (tag + ".f64").c_str(), why)) { return false; }
        if (!eq_slice(full[c].str, part[c].str, lo, n, (tag + ".str").c_str(), why)) { return false; }
    }
    return true;
}

} // namespace

int main(int argc, char** argv) {
    size_t bad = 0, checked = 0;
    for (int a = 1; a < argc; ++a) {
        const std::string path = argv[a];
        const std::string name = path.substr(path.rfind('/') + 1);
        try {
            auto img = slurp(path);
            auto md = parse_footer(img);
            if (md.row_groups.empty()) { continue; }

            const auto& rg = md.row_groups[0];
            const int64_t n = rg.num_rows;
            auto full = read_row_group(img, md, 0);

            // Ranges chosen to straddle page boundaries in both directions, plus
            // the degenerate single-row and empty cases.
            const std::vector<std::pair<int64_t, int64_t>> ranges = {
                {0, n}, {0, 1}, {n - 1, n}, {n / 2, n / 2 + 1},
                {1, std::min<int64_t>(n, 3)},
                {n / 3, std::min<int64_t>(n, n / 3 + 1000)},
                {n / 2, n}, {0, n / 2}, {n / 2, n / 2},
            };

            bool ok = true;
            std::string why;
            for (auto [lo, hi] : ranges) {
                if (lo < 0 || hi > n || hi < lo) { continue; }
                auto part = read_row_range(img, 0, md, 0, lo, hi);
                ++checked;
                if (!compare(full, part, size_t(lo), size_t(hi - lo), why)) {
                    std::printf("  FAIL %s [%lld,%lld): %s\n", name.c_str(),
                                (long long)lo, (long long)hi, why.c_str());
                    ok = false;
                    break;
                }
            }

            // Byte-slice form: hand over only the row group's own bytes.
            if (ok) {
                int64_t lo_off = std::numeric_limits<int64_t>::max(), hi_off = 0;
                for (const auto& cc : rg.columns) {
                    if (!cc.meta) { continue; }
                    const auto& cm = *cc.meta;
                    const int64_t s = cm.dictionary_page_offset ? *cm.dictionary_page_offset
                                                                : cm.data_page_offset;
                    lo_off = std::min(lo_off, s);
                    hi_off = std::max(hi_off, s + cm.total_compressed_size);
                }
                auto sub = std::span<const uint8_t>(img).subspan(size_t(lo_off),
                                                                 size_t(hi_off - lo_off));
                auto part = read_row_range(sub, lo_off, md, 0, 0, n);
                ++checked;
                if (!compare(full, part, 0, size_t(n), why)) {
                    std::printf("  FAIL %s byte-slice: %s\n", name.c_str(), why.c_str());
                    ok = false;
                }
            }

            std::printf("%s %-28s rows=%-8lld\n", ok ? "PASS" : "FAIL", name.c_str(),
                        (long long)n);
            if (!ok) { ++bad; }
        } catch (const std::exception& e) {
            std::printf("FAIL %-28s threw: %s\n", name.c_str(), e.what());
            ++bad;
        }
    }
    std::printf("row-range: %zu range decodes checked\n", checked);
    std::printf("%s\n", bad ? "ROW RANGE FAIL" : "ROW RANGE PASS");
    return bad ? 1 : 0;
}
