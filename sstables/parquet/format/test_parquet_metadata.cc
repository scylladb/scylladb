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
#include "thrift_compact_writer.hh"

#include <cstdio>
#include <fstream>
#include <iostream>
#include <limits>
#include <random>
#include <vector>

using namespace sstables::parquet::format;

namespace {

// A well-formed FileMetaData whose *values* are hostile. The byte-level cases in cmd_fuzz cannot
// reach validate() -- they die in the Thrift decoder -- so the semantic checks need footers that
// decode cleanly and lie: row counts that overflow when summed, offsets that go negative, a
// schema whose logical depth is a stack overflow even though its Thrift is flat.
struct hostile_chunk {
    int64_t data_page_offset = 4;
    std::optional<int64_t> dictionary_page_offset;
    int64_t total_compressed_size = 8;
    std::optional<int64_t> offset_index_offset;
    std::optional<int32_t> offset_index_length;
};
struct hostile_row_group {
    int64_t num_rows = 1;
    std::vector<hostile_chunk> chunks;
};

std::vector<uint8_t> build_footer(const std::vector<schema_element>& tree, int64_t num_rows,
                                  const std::vector<hostile_row_group>& rgs) {
    std::vector<uint8_t> out;
    compact_writer w(out);
    {
        compact_writer::struct_scope s(w);
        w.field_i32(1, 2);
        w.field_list(2, ctype::strct, tree.size());
        for (const auto& el : tree) {
            compact_writer::elem_scope e(w);
            if (el.type)            { w.field_i32(1, int32_t(*el.type)); }
            if (el.repetition_type) { w.field_i32(3, int32_t(*el.repetition_type)); }
            w.field_binary(4, el.name);
            if (el.num_children)    { w.field_i32(5, *el.num_children); }
        }
        w.field_i64(3, num_rows);
        w.field_list(4, ctype::strct, rgs.size());
        for (const auto& rg : rgs) {
            compact_writer::elem_scope e(w);
            w.field_list(1, ctype::strct, rg.chunks.size());
            for (const auto& ch : rg.chunks) {
                compact_writer::elem_scope ce(w);
                w.field_i64(2, ch.data_page_offset);
                w.field_struct(3);
                {
                    compact_writer::elem_scope cm(w);
                    w.field_i32(1, int32_t(phys_type::int32));
                    w.field_list(2, ctype::i32, 1);
                    w.zigzag(int32_t(encoding::plain));
                    w.field_list(3, ctype::binary, 1);
                    w.uvarint(1);
                    w.raw("v", 1);
                    w.field_i32(4, int32_t(codec::uncompressed));
                    w.field_i64(5, 1);
                    w.field_i64(6, ch.total_compressed_size);
                    w.field_i64(7, ch.total_compressed_size);
                    w.field_i64(9, ch.data_page_offset);
                    if (ch.dictionary_page_offset) { w.field_i64(11, *ch.dictionary_page_offset); }
                }
                if (ch.offset_index_offset) { w.field_i64(4, *ch.offset_index_offset); }
                if (ch.offset_index_length) { w.field_i32(5, *ch.offset_index_length); }
            }
            w.field_i64(2, 0);
            w.field_i64(3, rg.num_rows);
        }
    }
    return out;
}

std::vector<schema_element> flat_tree(int32_t leaves) {
    std::vector<schema_element> t;
    schema_element root;
    root.name = "schema";
    root.num_children = leaves;
    t.push_back(root);
    for (int32_t i = 0; i < leaves; ++i) {
        schema_element e;
        e.name = "v" + std::to_string(i);
        e.type = phys_type::int32;
        e.repetition_type = repetition::required;
        t.push_back(e);
    }
    return t;
}

// A one-leaf, one-row-group footer whose single chunk has been adjusted by `tweak`.
template <typename F>
std::vector<uint8_t> one_chunk(F tweak) {
    hostile_chunk c;
    tweak(c);
    return build_footer(flat_tree(1), 1, {hostile_row_group{1, {c}}});
}

// root { optional group g { optional group g { ... { required int32 v } } } }, `depth` groups deep.
std::vector<schema_element> deep_tree(size_t depth) {
    std::vector<schema_element> t;
    schema_element root;
    root.name = "schema";
    root.num_children = 1;
    t.push_back(root);
    for (size_t i = 0; i < depth; ++i) {
        schema_element g;
        g.name = "g";
        g.repetition_type = repetition::optional;
        g.num_children = 1;
        t.push_back(g);
    }
    schema_element leaf;
    leaf.name = "v";
    leaf.type = phys_type::int32;
    leaf.repetition_type = repetition::required;
    t.push_back(leaf);
    return t;
}

} // namespace

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

// Adversarial hand-built inputs. Every one of these must be *rejected*: an exception is the
// only acceptable outcome, and "parsed (no throw)" is a failure, because each case is a
// footer that a correct reader has no business accepting. Needs no fixture, so it is also
// reachable on its own as `test_parquet_metadata adversarial`.
static size_t adversarial() {
    const int64_t i64max = std::numeric_limits<int64_t>::max();
    struct { const char* name; std::vector<uint8_t> bytes; } cases[] = {
        {"empty",              {}},
        {"stop-only",          {0x00}},
        {"varint-bomb",        std::vector<uint8_t>(64, 0x80)},
        {"huge-list-header",   {0x19, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}},
        {"huge-binary",        {0x18, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}},
        {"deep-nesting",       std::vector<uint8_t>(4096, 0x1C)},   // struct in struct...
        {"bad-elem-type",      {0x1F, 0x0D}},
        // Three row groups of INT64_MAX rows sum, with wrap-around, to INT64_MAX - 2 -- which is
        // what the file claims, so the row-count check "passed" through signed overflow.
        {"rows-overflow",      build_footer(flat_tree(1), i64max - 2,
                                            std::vector<hostile_row_group>(
                                                    3, hostile_row_group{i64max, {hostile_chunk{}}}))},
        // Offsets are used as size_t indexes into the file image; negative ones wrap.
        {"negative-data-offset", one_chunk([] (hostile_chunk& c) { c.data_page_offset = -1; })},
        {"negative-dict-offset", one_chunk([] (hostile_chunk& c) { c.dictionary_page_offset = -8; })},
        {"negative-oi-offset",   one_chunk([] (hostile_chunk& c) {
                                     c.offset_index_offset = -1; c.offset_index_length = 16; })},
        {"zero-oi-length",       one_chunk([] (hostile_chunk& c) {
                                     c.offset_index_offset = 100; c.offset_index_length = 0; })},
        // The Thrift is a flat 100 002-element list and decodes fine; the *logical* tree it
        // describes is 100 000 groups deep, and walking it recursively is a stack overflow.
        {"deep-schema",        build_footer(deep_tree(100000), 0, {})},
        // A group claiming -1 children: the walk used to treat it as empty and carry on.
        {"negative-children",  [] {
                                    auto t = flat_tree(2);
                                    t[1].type.reset();
                                    t[1].num_children = -1;
                                    return build_footer(t, 0, {});
                                }()},
    };
    size_t bad3 = 0;
    for (auto& c : cases) {
        try {
            auto m = parse_file_metadata(std::span<const uint8_t>(c.bytes));
            (void)m.leaf_count();
            (void)walk_leaves(m);
            std::printf("  %-22s -> !! parsed (no throw)\n", c.name);
            ++bad3;
        } catch (const std::exception& e) {
            std::printf("  %-22s -> rejected: %s\n", c.name, e.what());
        } catch (...) {
            std::printf("  %-22s -> !! NON-STD EXCEPTION\n", c.name);
            ++bad3;
        }
    }
    return bad3;
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

    const size_t bad3 = adversarial();

    size_t total_bad = bad + bad3;
    std::printf("%s\n", total_bad == 0 ? "FUZZ PASS: no crashes, no non-std exceptions"
                                       : "FUZZ FAIL");
    return total_bad == 0 ? 0 : 1;
}

int main(int argc, char** argv) {
    if (argc < 2 || (std::string(argv[1]) != "adversarial" && argc < 3)) {
        std::fprintf(stderr, "usage: %s {dump|fuzz} <file.parquet> | %s adversarial\n",
                     argv[0], argv[0]);
        return 2;
    }
    try {
        std::string cmd = argv[1];
        if (cmd == "dump") { return cmd_dump(argv[2]); }
        if (cmd == "fuzz") { return cmd_fuzz(argv[2]); }
        if (cmd == "adversarial") {
            const size_t bad = adversarial();
            std::printf("%s\n", bad == 0 ? "ADVERSARIAL PASS" : "ADVERSARIAL FAIL");
            return bad == 0 ? 0 : 1;
        }
        std::fprintf(stderr, "unknown command %s\n", argv[1]);
        return 2;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}
