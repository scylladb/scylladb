/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Write a nested column and let parquet-cpp be the judge.
//
// A file that our own reader round-trips proves only that we are
// self-consistent. Nesting is where that is least convincing: the levels, the
// group structure and the LIST annotation all have to match what other
// implementations expect, and a file can be self-consistently wrong in all
// three. So this writes a list<string> with a null list, an empty list and a
// list holding a null element, then hands it to writer_nested_interop.py.
//
// Usage: test_nested_write <out-dir>
// Writes <out-dir>/w_nested.parquet and <out-dir>/w_nested.tags.txt.

#include "parquet_writer.hh"

#include <cstdio>
#include <fstream>
#include <random>
#include <string>
#include <vector>

using namespace sstables::parquet::format;

namespace {

// optional group tags (LIST) { repeated group list { optional binary element } }
// which is the shape pyarrow writes, and gives max_def=3, max_rep=1.
std::vector<schema_element> list_of_string_schema() {
    std::vector<schema_element> t;
    schema_element root;
    root.name = "schema";
    root.num_children = 1;
    t.push_back(root);

    schema_element tags;
    tags.name = "tags";
    tags.repetition_type = repetition::optional;
    tags.converted_type = int32_t(converted::list);
    tags.num_children = 1;
    t.push_back(tags);

    schema_element list;
    list.name = "list";
    list.repetition_type = repetition::repeated;
    list.num_children = 1;
    t.push_back(list);

    schema_element element;
    element.name = "element";
    element.repetition_type = repetition::optional;
    element.type = phys_type::byte_array;
    element.converted_type = int32_t(converted::utf8);
    t.push_back(element);
    return t;
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 2) { std::fprintf(stderr, "usage: %s <out-dir>\n", argv[0]); return 2; }
    const std::string dir = argv[1];

    try {
        writer_options wo;
        wo.compression = codec::zstd;
        wo.page_values = 700;              // several pages, so row-aligned cuts matter
        wo.use_dictionary = false;         // keep this test about nesting only

        parquet_file_writer w(parquet_file_writer::nested_schema{list_of_string_schema()}, wo);
        if (w.leaves().size() != 1) {
            std::printf("FAIL expected 1 leaf, got %zu\nNESTED WRITE FAIL\n", w.leaves().size());
            return 1;
        }
        if (w.leaves()[0].max_def != 3 || w.leaves()[0].max_rep != 1) {
            std::printf("FAIL leaf levels are def=%u rep=%u, want 3/1\nNESTED WRITE FAIL\n",
                        w.leaves()[0].max_def, w.leaves()[0].max_rep);
            return 1;
        }

        const size_t rows = 3000;
        std::mt19937_64 rng(99);
        column_data cd;
        std::vector<std::string> expected;

        for (size_t i = 0; i < rows; ++i) {
            const size_t kind = i % 7;
            if (kind == 0) {
                // null list
                cd.rep_levels.push_back(0);
                cd.def_levels.push_back(0);
                expected.push_back("NULL");
            } else if (kind == 1) {
                // present but empty
                cd.rep_levels.push_back(0);
                cd.def_levels.push_back(1);
                expected.push_back("EMPTY");
            } else {
                const size_t n = 1 + (rng() % 3);
                std::string want;
                for (size_t k = 0; k < n; ++k) {
                    cd.rep_levels.push_back(k == 0 ? 0 : 1);
                    const bool null_elem = (kind == 2 && k == 1);
                    if (null_elem) {
                        cd.def_levels.push_back(2);      // list present, element null
                        want += (k ? "|" : "");
                        want += "~";
                    } else {
                        cd.def_levels.push_back(3);
                        auto v = "e" + std::to_string(i % 97) + "_" + std::to_string(k);
                        cd.str.push_back(v);
                        want += (k ? "|" : "");
                        want += v;
                    }
                }
                expected.push_back(want);
            }
        }

        if (cd.num_rows() != rows) {
            std::printf("FAIL num_rows() says %zu, built %zu\nNESTED WRITE FAIL\n",
                        cd.num_rows(), rows);
            return 1;
        }

        std::vector<column_data> cols{std::move(cd)};
        w.add_row_group(cols);
        auto img = w.finish();

        const std::string path = dir + "/w_nested.parquet";
        std::ofstream(path, std::ios::binary)
            .write(reinterpret_cast<const char*>(img.data()), std::streamsize(img.size()));
        std::ofstream exp(dir + "/w_nested.tags.txt");
        for (const auto& e : expected) { exp << e << "\n"; }

        std::printf("wrote %s: %zu rows, %zu bytes\n", path.c_str(), rows, img.size());
        std::printf("NESTED WRITE PASS (pyarrow verdict pending)\n");
        return 0;
    } catch (const std::exception& e) {
        std::printf("FAIL threw: %s\nNESTED WRITE FAIL\n", e.what());
        return 1;
    }
}
