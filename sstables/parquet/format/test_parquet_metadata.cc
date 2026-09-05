/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Conformance and robustness driver for the hand-written Parquet footer parser.
//
//   test_parquet_metadata dump  <file.parquet>   -> JSON of the parsed metadata
//   test_parquet_metadata fuzz  <file.parquet>   -> corruption / truncation sweep
//
// The dump output is diffed against pyarrow's view of the same file by
// conformance.py, which is the actual assertion: we agree with a reference
// implementation on real files written by real writers.

#include "parquet_metadata.hh"

#include <cstdio>
#include <fstream>
#include <iostream>
#include <random>
#include <vector>

using namespace sstables::parquet::format;

static std::vector<uint8_t> slurp(const char* path) {
    std::ifstream f(path, std::ios::binary);
    if (!f) { throw std::runtime_error(std::string("cannot open ") + path); }
    return std::vector<uint8_t>((std::istreambuf_iterator<char>(f)),
                                 std::istreambuf_iterator<char>());
}

static void json_str(std::ostream& o, const std::string& s) {
    o << '"';
    for (char c : s) {
        switch (c) {
        case '"':  o << "\\\""; break;
        case '\\': o << "\\\\"; break;
        case '\n': o << "\\n";  break;
        case '\r': o << "\\r";  break;
        case '\t': o << "\\t";  break;
        default:
            if (uint8_t(c) < 0x20) { char b[8]; std::snprintf(b, sizeof b, "\\u%04x", c); o << b; }
            else { o << c; }
        }
    }
    o << '"';
}

static int cmd_dump(const char* path) {
    auto img = slurp(path);
    auto m = parse_footer(img);

    auto& o = std::cout;
    o << "{\n";
    o << "  \"version\": " << m.version << ",\n";
    o << "  \"num_rows\": " << m.num_rows << ",\n";
    o << "  \"num_row_groups\": " << m.row_groups.size() << ",\n";
    o << "  \"num_schema_elements\": " << m.schema.size() << ",\n";
    o << "  \"num_leaf_columns\": " << m.leaf_count() << ",\n";
    o << "  \"created_by\": "; json_str(o, m.created_by.value_or("")); o << ",\n";

    o << "  \"leaves\": [\n";
    bool first = true;
    for (size_t i = 1; i < m.schema.size(); ++i) {
        const auto& e = m.schema[i];
        if (!e.is_leaf()) { continue; }
        if (!first) { o << ",\n"; }
        first = false;
        o << "    {\"name\": "; json_str(o, e.name);
        o << ", \"type\": \"" << (e.type ? to_string(*e.type) : "?") << "\"";
        o << ", \"repetition\": \"" << (e.repetition_type ? to_string(*e.repetition_type) : "?") << "\"}";
    }
    o << "\n  ],\n";

    o << "  \"row_groups\": [\n";
    for (size_t g = 0; g < m.row_groups.size(); ++g) {
        const auto& rg = m.row_groups[g];
        if (g) { o << ",\n"; }
        o << "    {\"num_rows\": " << rg.num_rows
          << ", \"total_byte_size\": " << rg.total_byte_size
          << ", \"num_columns\": " << rg.columns.size()
          << ", \"columns\": [\n";
        for (size_t c = 0; c < rg.columns.size(); ++c) {
            const auto& cc = rg.columns[c];
            if (c) { o << ",\n"; }
            o << "      {";
            if (cc.meta) {
                const auto& cm = *cc.meta;
                o << "\"path\": "; json_str(o, cm.path());
                o << ", \"type\": \"" << to_string(cm.type) << "\"";
                o << ", \"codec\": \"" << to_string(cm.compression) << "\"";
                o << ", \"num_values\": " << cm.num_values;
                o << ", \"total_compressed_size\": " << cm.total_compressed_size;
                o << ", \"total_uncompressed_size\": " << cm.total_uncompressed_size;
                o << ", \"data_page_offset\": " << cm.data_page_offset;
                o << ", \"has_dict_page\": " << (cm.dictionary_page_offset ? "true" : "false");
                o << ", \"has_page_index\": " << (cc.has_page_index() ? "true" : "false");
                o << ", \"encodings\": [";
                for (size_t k = 0; k < cm.encodings.size(); ++k) {
                    if (k) { o << ", "; }
                    o << '"' << to_string(cm.encodings[k]) << '"';
                }
                o << "]";
                if (cm.stats && cm.stats->null_count) {
                    o << ", \"null_count\": " << *cm.stats->null_count;
                }
            }
            o << "}";
        }
        o << "\n    ]}";
    }
    o << "\n  ]\n}\n";
    return 0;
}

// Robustness: a hostile or damaged footer must produce an exception, never a
// crash, a hang, or an unbounded allocation. This is the reason the parser is
// hand-written rather than generated.
static int cmd_fuzz(const char* path) {
    auto orig = slurp(path);
    auto span = std::span<const uint8_t>(orig);
    auto fs = locate_footer(span);
    std::printf("footer at %zu len %zu\n", fs.offset, fs.length);

    size_t ok = 0, threw = 0, bad = 0;

    auto attempt = [&](std::vector<uint8_t>& img, const char* what, size_t n) {
        try {
            auto m = parse_file_metadata(std::span<const uint8_t>(img).subspan(fs.offset, n), {}, semantic_check::no);
            (void)m.leaf_count();
            ++ok;
        } catch (const std::exception&) {
            ++threw;
        } catch (...) {
            ++bad;
            std::printf("  !! non-std exception from %s\n", what);
        }
    };

    // 1. Truncation at every prefix length of the footer.
    for (size_t n = 0; n < fs.length; ++n) {
        auto img = orig;
        attempt(img, "truncate", n);
    }
    std::printf("truncation sweep: %zu parsed, %zu rejected, %zu bad\n", ok, threw, bad);

    // 2. Single-byte corruption, deterministic sample across the footer.
    size_t ok2 = 0, threw2 = 0;
    std::mt19937_64 rng(12345);
    size_t trials = std::min<size_t>(fs.length, 20000);
    for (size_t i = 0; i < trials; ++i) {
        auto img = orig;
        size_t pos = fs.offset + (rng() % fs.length);
        img[pos] = uint8_t(rng() & 0xFF);
        try {
            auto m = parse_file_metadata(std::span<const uint8_t>(img).subspan(fs.offset, fs.length), {}, semantic_check::no);
            (void)m.leaf_count();
            ++ok2;
        } catch (const std::exception&) {
            ++threw2;
        } catch (...) {
            ++bad;
            std::printf("  !! non-std exception from corruption at %zu\n", pos);
        }
    }
    std::printf("corruption sweep: %zu parsed, %zu rejected, %zu bad\n", ok2, threw2, bad);

    // 3. Adversarial hand-built inputs.
    struct { const char* name; std::vector<uint8_t> bytes; } cases[] = {
        {"empty",              {}},
        {"stop-only",          {0x00}},
        {"varint-bomb",        std::vector<uint8_t>(64, 0x80)},
        {"huge-list-header",   {0x19, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}},
        {"huge-binary",        {0x18, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}},
        {"deep-nesting",       std::vector<uint8_t>(4096, 0x1C)},   // struct in struct...
        {"bad-elem-type",      {0x1F, 0x0D}},
    };
    size_t bad3 = 0;
    for (auto& c : cases) {
        try {
            auto m = parse_file_metadata(std::span<const uint8_t>(c.bytes));
            (void)m.leaf_count();
            std::printf("  %-18s -> parsed (no throw)\n", c.name);
        } catch (const std::exception& e) {
            std::printf("  %-18s -> rejected: %s\n", c.name, e.what());
        } catch (...) {
            std::printf("  %-18s -> !! NON-STD EXCEPTION\n", c.name);
            ++bad3;
        }
    }

    size_t total_bad = bad + bad3;
    std::printf("%s\n", total_bad == 0 ? "FUZZ PASS: no crashes, no non-std exceptions"
                                       : "FUZZ FAIL");
    return total_bad == 0 ? 0 : 1;
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::fprintf(stderr, "usage: %s {dump|fuzz} <file.parquet>\n", argv[0]);
        return 2;
    }
    try {
        std::string cmd = argv[1];
        if (cmd == "dump") { return cmd_dump(argv[2]); }
        if (cmd == "fuzz") { return cmd_fuzz(argv[2]); }
        std::fprintf(stderr, "unknown command %s\n", argv[1]);
        return 2;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}
