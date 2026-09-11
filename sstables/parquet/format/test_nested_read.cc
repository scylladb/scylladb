/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Read a nested column out of a parquet-cpp file and reassemble it.
//
// The reader hands back Dremel triples -- repetition levels, definition levels
// and the present values -- because that is what the file holds; turning those
// back into lists is the caller's job. This checks both halves at once against a
// list<string> column written by pyarrow, including the three cases that are easy
// to conflate: a null list, an empty list, and a list containing a null element.
//
// Usage: test_nested_read <file.parquet> <expected.tags.txt> <leaf-path>
//   expected file: one row per line, NULL / EMPTY / elements joined by '|' with
//   '~' for a null element.

#include "parquet_reader.hh"
#include "parquet_metadata.hh"

#include <cstdio>
#include <fstream>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

using namespace sstables::parquet::format;

namespace {

std::vector<uint8_t> slurp(const std::string& p) {
    std::ifstream f(p, std::ios::binary);
    return std::vector<uint8_t>((std::istreambuf_iterator<char>(f)),
                                 std::istreambuf_iterator<char>());
}

std::string join(const std::vector<std::string>& v) {
    std::string s;
    for (size_t i = 0; i < v.size(); ++i) { s += (i ? "." : ""); s += v[i]; }
    return s;
}

// Reassemble a one-level list<optional element> from its levels. For the schema
// pyarrow writes -- optional group (LIST) { repeated group list { optional
// element } } -- max_def is 3 and the levels mean:
//   0  the list itself is null
//   1  the list is present and empty
//   2  one element, and it is null
//   3  one element, present
// A repetition level of 0 starts a new row; 1 continues the current list.
std::vector<std::string> reassemble_list(const column_data& cd, uint8_t max_def) {
    std::vector<std::string> rows;
    std::string cur;
    bool open = false, any = false;
    size_t vi = 0;

    auto flush = [&] {
        if (!open) { return; }
        rows.push_back(any ? cur : cur);   // cur already holds NULL/EMPTY when !any
        cur.clear();
        any = false;
    };

    for (size_t i = 0; i < cd.def_levels.size(); ++i) {
        const uint64_t rep = i < cd.rep_levels.size() ? cd.rep_levels[i] : 0;
        const uint64_t def = cd.def_levels[i];
        if (rep == 0) {
            flush();
            open = true;
            if (def == 0)      { cur = "NULL"; any = false; continue; }
            if (def == 1)      { cur = "EMPTY"; any = false; continue; }
        }
        // An element slot.
        std::string e;
        if (def == max_def) {
            if (vi >= cd.str.size()) { throw std::runtime_error("ran out of values"); }
            e = cd.str[vi++];
        } else {
            e = "~";                       // present list, null element
        }
        if (any) { cur += "|"; }
        cur += e;
        any = true;
    }
    flush();
    return rows;
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 4) {
        std::fprintf(stderr, "usage: %s <file.parquet> <expected.txt> <leaf.path>\n", argv[0]);
        return 2;
    }
    try {
        auto img = slurp(argv[1]);
        auto md = parse_footer(img);
        auto leaves = walk_leaves(md);

        size_t want_leaf = leaves.size();
        for (size_t i = 0; i < leaves.size(); ++i) {
            if (join(leaves[i].path) == argv[3]) { want_leaf = i; break; }
        }
        if (want_leaf == leaves.size()) {
            std::printf("FAIL leaf %s not found\nNESTED READ FAIL\n", argv[3]);
            return 1;
        }

        std::vector<std::string> expected;
        {
            std::ifstream f(argv[2]);
            std::string line;
            while (std::getline(f, line)) { expected.push_back(line); }
        }

        // Every row group, concatenated -- the fixture has several.
        std::vector<std::string> got;
        for (size_t g = 0; g < md.row_groups.size(); ++g) {
            auto cols = read_row_group(img, md, g);
            auto rows = reassemble_list(cols[want_leaf], leaves[want_leaf].max_def);
            got.insert(got.end(), rows.begin(), rows.end());
        }

        size_t bad = 0;
        if (got.size() != expected.size()) {
            std::printf("FAIL row count: got %zu, pyarrow says %zu\n", got.size(), expected.size());
            ++bad;
        }
        for (size_t i = 0; i < std::min(got.size(), expected.size()); ++i) {
            if (got[i] != expected[i]) {
                if (bad < 6) {
                    std::printf("FAIL row %zu: got '%s', want '%s'\n", i,
                                got[i].c_str(), expected[i].c_str());
                }
                ++bad;
            }
        }
        std::printf("nested read: %zu rows, %zu mismatches (leaf %s, max_def=%u max_rep=%u)\n",
                    got.size(), bad, argv[3],
                    leaves[want_leaf].max_def, leaves[want_leaf].max_rep);
        std::printf("%s\n", bad ? "NESTED READ FAIL" : "NESTED READ PASS");
        return bad ? 1 : 0;
    } catch (const std::exception& e) {
        std::printf("FAIL threw: %s\nNESTED READ FAIL\n", e.what());
        return 1;
    }
}
