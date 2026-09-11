/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// walk_leaves() must agree with parquet-cpp about every leaf's Dremel levels.
//
// The footer stores the schema flat and depth-first with num_children; the
// definition and repetition levels a leaf carries are implied by its ancestors
// and written down nowhere. Getting them wrong by one silently misreads every
// value in a nested column, so this compares against pyarrow's own
// max_definition_level / max_repetition_level for the same file rather than
// against our own expectations.
//
// Usage: test_levels_tree <file.parquet> <file.levels.json>

#include "parquet_metadata.hh"

#include <cstdio>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

using namespace sstables::parquet::format;

namespace {

std::vector<uint8_t> slurp(const std::string& p) {
    std::ifstream f(p, std::ios::binary);
    return std::vector<uint8_t>((std::istreambuf_iterator<char>(f)),
                                 std::istreambuf_iterator<char>());
}

struct expect_leaf {
    std::string path;
    int max_def = 0;
    int max_rep = 0;
};

// Minimal reader for the flat JSON the generator writes. Pulling in a JSON
// library for three keys is not worth it, and the shape is fixed.
std::vector<expect_leaf> read_expected(const std::string& path) {
    std::ifstream f(path);
    std::stringstream ss;
    ss << f.rdbuf();
    const std::string s = ss.str();
    std::vector<expect_leaf> out;
    size_t i = 0;
    auto find_str = [&] (const char* key, size_t from) -> std::pair<std::string, size_t> {
        const std::string pat = std::string("\"") + key + "\":";
        auto k = s.find(pat, from);
        if (k == std::string::npos) { return {"", std::string::npos}; }
        auto q1 = s.find('"', k + pat.size());
        auto q2 = s.find('"', q1 + 1);
        return {s.substr(q1 + 1, q2 - q1 - 1), q2};
    };
    auto find_int = [&] (const char* key, size_t from) -> std::pair<int, size_t> {
        const std::string pat = std::string("\"") + key + "\":";
        auto k = s.find(pat, from);
        if (k == std::string::npos) { return {-1, std::string::npos}; }
        size_t p = k + pat.size();
        while (p < s.size() && (s[p] == ' ' || s[p] == '\t')) { ++p; }
        int v = 0;
        while (p < s.size() && s[p] >= '0' && s[p] <= '9') { v = v * 10 + (s[p] - '0'); ++p; }
        return {v, p};
    };
    for (;;) {
        auto [path_s, p1] = find_str("path", i);
        if (p1 == std::string::npos) { break; }
        auto [d, p2] = find_int("max_def", p1);
        auto [r, p3] = find_int("max_rep", p2);
        out.push_back(expect_leaf{path_s, d, r});
        i = p3;
    }
    return out;
}

std::string join(const std::vector<std::string>& v) {
    std::string s;
    for (size_t i = 0; i < v.size(); ++i) { s += (i ? "." : ""); s += v[i]; }
    return s;
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        std::fprintf(stderr, "usage: %s <file.parquet> <file.levels.json>\n", argv[0]);
        return 2;
    }
    try {
        auto img = slurp(argv[1]);
        auto md = parse_footer(img);
        auto leaves = walk_leaves(md);
        auto want = read_expected(argv[2]);

        size_t bad = 0;
        if (leaves.size() != want.size()) {
            std::printf("FAIL leaf count: got %zu, pyarrow says %zu\n", leaves.size(), want.size());
            ++bad;
        }
        const size_t n = std::min(leaves.size(), want.size());
        for (size_t i = 0; i < n; ++i) {
            const std::string got = join(leaves[i].path);
            const bool ok = got == want[i].path &&
                            int(leaves[i].max_def) == want[i].max_def &&
                            int(leaves[i].max_rep) == want[i].max_rep;
            std::printf("%s %-34s def=%u/%d rep=%u/%d\n", ok ? "PASS" : "FAIL",
                        got.c_str(), leaves[i].max_def, want[i].max_def,
                        leaves[i].max_rep, want[i].max_rep);
            if (!ok) {
                std::printf("     pyarrow path: %s\n", want[i].path.c_str());
                ++bad;
            }
        }
        // The leaf order must match the column chunk order, or every chunk is
        // decoded against the wrong leaf.
        if (!md.row_groups.empty() && md.row_groups[0].columns.size() != leaves.size()) {
            std::printf("FAIL row group has %zu chunks but %zu leaves\n",
                        md.row_groups[0].columns.size(), leaves.size());
            ++bad;
        }
        for (size_t i = 0; i < n && !md.row_groups.empty(); ++i) {
            const auto& cc = md.row_groups[0].columns[i];
            if (cc.meta && cc.meta->path_in_schema != leaves[i].path) {
                std::printf("FAIL leaf %zu path %s != chunk path %s\n", i,
                            join(leaves[i].path).c_str(), join(cc.meta->path_in_schema).c_str());
                ++bad;
            }
        }
        std::printf("%s\n", bad ? "LEVELS TREE FAIL" : "LEVELS TREE PASS");
        return bad ? 1 : 0;
    } catch (const std::exception& e) {
        std::printf("FAIL threw: %s\nLEVELS TREE FAIL\n", e.what());
        return 1;
    }
}
