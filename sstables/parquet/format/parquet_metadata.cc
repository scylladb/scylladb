/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "parquet_metadata.hh"

namespace sstables::parquet::format {

const char* to_string(phys_type t) {
    switch (t) {
    case phys_type::boolean:    return "BOOLEAN";
    case phys_type::int32:      return "INT32";
    case phys_type::int64:      return "INT64";
    case phys_type::int96:      return "INT96";
    case phys_type::flt:        return "FLOAT";
    case phys_type::dbl:        return "DOUBLE";
    case phys_type::byte_array: return "BYTE_ARRAY";
    case phys_type::flba:       return "FIXED_LEN_BYTE_ARRAY";
    }
    return "?";
}
const char* to_string(codec c) {
    switch (c) {
    case codec::uncompressed: return "UNCOMPRESSED";
    case codec::snappy:       return "SNAPPY";
    case codec::gzip:         return "GZIP";
    case codec::lzo:          return "LZO";
    case codec::brotli:       return "BROTLI";
    case codec::lz4:          return "LZ4";
    case codec::zstd:         return "ZSTD";
    case codec::lz4_raw:      return "LZ4_RAW";
    }
    return "?";
}
const char* to_string(encoding e) {
    switch (e) {
    case encoding::plain:                   return "PLAIN";
    case encoding::plain_dictionary:        return "PLAIN_DICTIONARY";
    case encoding::rle:                     return "RLE";
    case encoding::bit_packed:              return "BIT_PACKED";
    case encoding::delta_binary_packed:     return "DELTA_BINARY_PACKED";
    case encoding::delta_length_byte_array: return "DELTA_LENGTH_BYTE_ARRAY";
    case encoding::delta_byte_array:        return "DELTA_BYTE_ARRAY";
    case encoding::rle_dictionary:          return "RLE_DICTIONARY";
    case encoding::byte_stream_split:       return "BYTE_STREAM_SPLIT";
    }
    return "?";
}
const char* to_string(repetition r) {
    switch (r) {
    case repetition::required: return "REQUIRED";
    case repetition::optional: return "OPTIONAL";
    case repetition::repeated: return "REPEATED";
    }
    return "?";
}

namespace {

// Every parse_* below follows the same shape: open a struct scope, loop over
// fields, switch on the field id, and skip anything unrecognised so that a
// footer written by a newer Parquet implementation still parses.

statistics parse_statistics(compact_reader& r) {
    compact_reader::struct_scope sc(r);
    statistics s;
    for (;;) {
        auto f = r.field_begin();
        if (f.stop) { break; }
        switch (f.id) {
        case 1: s.max_value = std::string(r.binary_v()); break;   // deprecated max
        case 2: s.min_value = std::string(r.binary_v()); break;   // deprecated min
        case 3: s.null_count = r.i64v(); break;
        case 4: s.distinct_count = r.i64v(); break;
        case 5: s.max_value = std::string(r.binary_v()); break;   // max_value
        case 6: s.min_value = std::string(r.binary_v()); break;   // min_value
        default: r.skip(f.type);
        }
    }
    return s;
}

column_metadata parse_column_metadata(compact_reader& r) {
    compact_reader::struct_scope sc(r);
    column_metadata m;
    for (;;) {
        auto f = r.field_begin();
        if (f.stop) { break; }
        switch (f.id) {
        case 1: m.type = phys_type(r.i32v()); break;
        case 2: {
            auto h = r.list_begin();
            m.encodings.reserve(h.size);
            for (size_t i = 0; i < h.size; ++i) { m.encodings.push_back(encoding(r.i32v())); }
            break;
        }
        case 3: {
            auto h = r.list_begin();
            m.path_in_schema.reserve(h.size);
            for (size_t i = 0; i < h.size; ++i) { m.path_in_schema.emplace_back(r.binary_v()); }
            break;
        }
        case 4:  m.compression = codec(r.i32v()); break;
        case 5:  m.num_values = r.i64v(); break;
        case 6:  m.total_uncompressed_size = r.i64v(); break;
        case 7:  m.total_compressed_size = r.i64v(); break;
        case 9:  m.data_page_offset = r.i64v(); break;
        case 10: m.index_page_offset = r.i64v(); break;
        case 11: m.dictionary_page_offset = r.i64v(); break;
        case 12: m.stats = parse_statistics(r); break;
        case 14: m.bloom_filter_offset = r.i64v(); break;
        default: r.skip(f.type);
        }
    }
    return m;
}

column_chunk parse_column_chunk(compact_reader& r) {
    compact_reader::struct_scope sc(r);
    column_chunk c;
    for (;;) {
        auto f = r.field_begin();
        if (f.stop) { break; }
        switch (f.id) {
        case 1: c.file_path = std::string(r.binary_v()); break;
        case 2: c.file_offset = r.i64v(); break;
        case 3: c.meta = parse_column_metadata(r); break;
        case 4: c.offset_index_offset = r.i64v(); break;
        case 5: c.offset_index_length = r.i32v(); break;
        case 6: c.column_index_offset = r.i64v(); break;
        case 7: c.column_index_length = r.i32v(); break;
        // 8: ColumnCryptoMetaData, a union of EncryptionWithFooterKey (an empty struct) and
        // EncryptionWithColumnKey { path_in_schema, key_metadata }. Reading it is what lets a
        // reader tell "this column is encrypted with a key I was not given" from "this footer
        // is malformed" -- two states that are otherwise identical from the outside.
        case 8: {
            column_crypto_metadata cm;
            compact_reader::struct_scope cs(r);
            for (;;) {
                auto g = r.field_begin();
                if (g.stop) { break; }
                if (g.id == 1) {
                    cm.with_footer_key = true;
                    r.skip(g.type);            // EncryptionWithFooterKey carries nothing
                } else if (g.id == 2) {
                    cm.with_footer_key = false;
                    compact_reader::struct_scope ks(r);
                    for (;;) {
                        auto h = r.field_begin();
                        if (h.stop) { break; }
                        if (h.id == 1 && h.type == ctype::list) {
                            auto lh = r.list_begin();
                            for (size_t i = 0; i < lh.size; ++i) {
                                cm.path_in_schema.emplace_back(r.binary_v());
                            }
                        } else if (h.id == 2 && h.type == ctype::binary) {
                            cm.key_metadata = std::string(r.binary_v());
                        } else {
                            r.skip(h.type);
                        }
                    }
                } else {
                    r.skip(g.type);
                }
            }
            c.crypto_metadata = std::move(cm);
            break;
        }
        case 9: c.encrypted_column_metadata = std::string(r.binary_v()); break;
        default: r.skip(f.type);
        }
    }
    return c;
}

// Same as parse_row_group but skips the column list, recording where it was. Everything else
// is cheap and some of it is load-bearing: num_rows in particular, because mapping a row
// ordinal to a row group needs every group's count even when only one group will be read.
row_group parse_row_group_light(compact_reader& r) {
    compact_reader::struct_scope sc(r);
    row_group g;
    for (;;) {
        auto f = r.field_begin();
        if (f.stop) { break; }
        switch (f.id) {
        case 1: {
            const size_t at = r.position();
            r.skip(f.type);
            g.columns_offset = uint32_t(at);
            g.columns_length = uint32_t(r.position() - at);
            break;
        }
        case 2: g.total_byte_size = r.i64v(); break;
        case 3: g.num_rows = r.i64v(); break;
        case 5: g.file_offset = r.i64v(); break;
        case 6: g.total_compressed_size = r.i64v(); break;
        case 7: g.ordinal = r.i16v(); break;
        default: r.skip(f.type);
        }
    }
    return g;
}

row_group parse_row_group(compact_reader& r) {
    compact_reader::struct_scope sc(r);
    row_group g;
    for (;;) {
        auto f = r.field_begin();
        if (f.stop) { break; }
        switch (f.id) {
        case 1: {
            auto h = r.list_begin();
            g.columns.reserve(h.size);
            for (size_t i = 0; i < h.size; ++i) { g.columns.push_back(parse_column_chunk(r)); }
            break;
        }
        case 2: g.total_byte_size = r.i64v(); break;
        case 3: g.num_rows = r.i64v(); break;
        case 5: g.file_offset = r.i64v(); break;
        case 6: g.total_compressed_size = r.i64v(); break;
        case 7: g.ordinal = r.i16v(); break;
        default: r.skip(f.type);
        }
    }
    return g;
}

schema_element parse_schema_element(compact_reader& r) {
    compact_reader::struct_scope sc(r);
    schema_element e;
    for (;;) {
        auto f = r.field_begin();
        if (f.stop) { break; }
        switch (f.id) {
        case 1: e.type = phys_type(r.i32v()); break;
        case 2: e.type_length = r.i32v(); break;
        case 3: e.repetition_type = repetition(r.i32v()); break;
        case 4: e.name = std::string(r.binary_v()); break;
        case 5: e.num_children = r.i32v(); break;
        case 6: e.converted_type = r.i32v(); break;
        case 9: e.field_id = r.i32v(); break;
        default: r.skip(f.type);   // scale/precision/logicalType
        }
    }
    return e;
}

key_value parse_key_value(compact_reader& r) {
    compact_reader::struct_scope sc(r);
    key_value kv;
    for (;;) {
        auto f = r.field_begin();
        if (f.stop) { break; }
        switch (f.id) {
        case 1: kv.key = std::string(r.binary_v()); break;
        case 2: kv.value = std::string(r.binary_v()); break;
        default: r.skip(f.type);
        }
    }
    return kv;
}

} // anonymous namespace

std::vector<leaf_info> walk_leaves(const file_metadata& m) {
    if (m.schema.empty()) { throw thrift_error("schema has no root"); }
    std::vector<leaf_info> out;
    size_t pos = 1;                       // schema[0] is the root

    // Depth-first descent. A `repeated` element adds a repetition level; anything
    // not `required` adds a definition level. The root itself contributes neither.
    auto descend = [&] (auto&& self, std::vector<std::string>& path,
                        uint8_t def, uint8_t rep, int32_t count) -> void {
        for (int32_t i = 0; i < count; ++i) {
            if (pos >= m.schema.size()) {
                throw thrift_error("schema tree declares more children than it has elements");
            }
            const auto& e = m.schema[pos];
            const size_t here = pos;
            ++pos;

            uint8_t d = def, r = rep;
            const auto rt = e.repetition_type.value_or(repetition::required);
            if (rt == repetition::repeated) { ++d; ++r; }
            else if (rt == repetition::optional) { ++d; }

            path.push_back(e.name);
            if (e.is_leaf()) {
                out.push_back(leaf_info{here, path, d, r});
            } else {
                self(self, path, d, r, *e.num_children);
            }
            path.pop_back();
        }
    };

    std::vector<std::string> path;
    descend(descend, path, 0, 0, m.schema[0].num_children.value_or(0));
    return out;
}

void validate(const file_metadata& m) {
    if (m.schema.empty()) {
        throw thrift_error("footer has no schema");
    }
    if (!m.schema[0].num_children.has_value() || *m.schema[0].num_children <= 0) {
        throw thrift_error("schema root has no children");
    }
    if (m.num_rows < 0) {
        throw thrift_error("negative num_rows");
    }
    const size_t leaves = m.leaf_count();
    if (leaves == 0) {
        throw thrift_error("schema has no leaf columns");
    }
    int64_t rows = 0;
    for (const auto& g : m.row_groups) {
        if (g.num_rows < 0) { throw thrift_error("negative row group num_rows"); }
        // A lazily-parsed group has not decoded its chunks yet, so the per-chunk checks below
        // cannot run on it. They are not skipped, only deferred: materialise_row_group()
        // applies them when the group is decoded, so every chunk a reader actually looks at
        // is still checked. Deciding this here rather than turning semantic_check off keeps
        // the schema and row-count checks, which are exactly the ones that catch a truncated
        // or fabricated footer.
        if (g.columns.empty() && g.columns_length > 0) {
            rows += g.num_rows;
            continue;
        }
        // Every row group must describe exactly one chunk per leaf, otherwise a
        // reader cannot line columns up with the schema.
        if (g.columns.size() != leaves) {
            throw thrift_error("row group has " + std::to_string(g.columns.size()) +
                               " chunks but schema has " + std::to_string(leaves) + " leaves");
        }
        for (const auto& c : g.columns) {
            // A chunk with no inline metadata is malformed *unless* it says its metadata is
            // encrypted under a key we do not hold, which modular encryption makes a normal
            // state rather than an error: the reader is expected to open the columns it has
            // keys for and leave the rest alone.
            if (!c.meta) {
                if (!c.metadata_is_encrypted()) {
                    throw thrift_error("column chunk without metadata");
                }
                continue;
            }
            if (c.meta->num_values < 0 || c.meta->total_compressed_size < 0) {
                throw thrift_error("negative column chunk size");
            }
        }
        rows += g.num_rows;
    }
    if (!m.row_groups.empty() && rows != m.num_rows) {
        throw thrift_error("row group rows (" + std::to_string(rows) +
                           ") != file num_rows (" + std::to_string(m.num_rows) + ")");
    }
}

void materialise_row_group(file_metadata& m, size_t rg, std::span<const uint8_t> blob,
                           limits lim) {
    if (rg >= m.row_groups.size()) {
        throw std::out_of_range("pq: row group index out of range");
    }
    auto& g = m.row_groups[rg];
    if (!g.columns.empty() || g.columns_length == 0) {
        return;             // already materialised, or eagerly parsed
    }
    if (size_t(g.columns_offset) + size_t(g.columns_length) > blob.size()) {
        throw std::runtime_error("pq: row group column extent outside the footer");
    }
    compact_reader r(blob.subspan(g.columns_offset, g.columns_length), lim);
    auto hd = r.list_begin();
    g.columns.reserve(hd.size);
    for (size_t i = 0; i < hd.size; ++i) { g.columns.push_back(parse_column_chunk(r)); }

    // The checks validate() had to defer for this group, applied now that its chunks exist.
    const size_t leaves = m.leaf_count();
    if (g.columns.size() != leaves) {
        throw thrift_error("row group has " + std::to_string(g.columns.size()) +
                           " chunks but schema has " + std::to_string(leaves) + " leaves");
    }
    for (const auto& c : g.columns) {
        if (!c.meta) {
            if (!c.metadata_is_encrypted()) {
                throw thrift_error("column chunk without metadata");
            }
            continue;
        }
        if (c.meta->num_values < 0 || c.meta->total_compressed_size < 0) {
            throw thrift_error("negative column chunk size");
        }
    }
}

column_metadata parse_column_metadata_blob(std::span<const uint8_t> blob, limits lim) {
    compact_reader r(blob, lim);
    return parse_column_metadata(r);
}

file_metadata parse_file_metadata(std::span<const uint8_t> blob, limits lim, semantic_check chk,
                                  metadata_mode mode) {
    compact_reader r(blob, lim);
    compact_reader::struct_scope sc(r);
    file_metadata m;
    for (;;) {
        auto f = r.field_begin();
        if (f.stop) { break; }
        switch (f.id) {
        case 1: m.version = r.i32v(); break;
        case 2: {
            auto h = r.list_begin();
            m.schema.reserve(h.size);
            for (size_t i = 0; i < h.size; ++i) { m.schema.push_back(parse_schema_element(r)); }
            break;
        }
        case 3: m.num_rows = r.i64v(); break;
        case 4: {
            auto h = r.list_begin();
            m.row_groups.reserve(h.size);
            for (size_t i = 0; i < h.size; ++i) {
                m.row_groups.push_back(mode == metadata_mode::lazy ? parse_row_group_light(r)
                                                                  : parse_row_group(r));
            }
            break;
        }
        case 5: {
            auto h = r.list_begin();
            m.key_value_metadata.reserve(h.size);
            for (size_t i = 0; i < h.size; ++i) { m.key_value_metadata.push_back(parse_key_value(r)); }
            break;
        }
        case 6: m.created_by = std::string(r.binary_v()); break;
        default: r.skip(f.type);   // column_orders, encryption
        }
    }
    if (chk == semantic_check::yes) { validate(m); }
    return m;
}

offset_index parse_offset_index_blob(std::span<const uint8_t> blob, limits lim) {
    compact_reader r(blob, lim);
    compact_reader::struct_scope sc(r);
    offset_index oi;
    for (;;) {
        auto f = r.field_begin();
        if (f.stop) { break; }
        if (f.id == 1) {
            auto h = r.list_begin();
            oi.pages.reserve(h.size);
            for (size_t i = 0; i < h.size; ++i) {
                compact_reader::struct_scope ps(r);
                page_loc pl;
                for (;;) {
                    auto pf = r.field_begin();
                    if (pf.stop) { break; }
                    switch (pf.id) {
                    case 1: pl.offset = r.i64v(); break;
                    case 2: pl.compressed_page_size = r.i32v(); break;
                    case 3: pl.first_row_index = r.i64v(); break;
                    default: r.skip(pf.type);
                    }
                }
                oi.pages.push_back(pl);
            }
        } else {
            r.skip(f.type);
        }
    }
    return oi;
}

std::optional<offset_index> parse_offset_index(std::span<const uint8_t> img,
                                               const column_chunk& cc, limits lim) {
    if (!cc.offset_index_offset || !cc.offset_index_length) { return std::nullopt; }
    const size_t off = size_t(*cc.offset_index_offset);
    const size_t len = size_t(*cc.offset_index_length);
    if (off + len > img.size()) { throw thrift_error("offset index extends past EOF"); }
    return parse_offset_index_blob(img.subspan(off, len), lim);
}

footer_span locate_footer(std::span<const uint8_t> img) {
    // Layout: "PAR1" <body> <FileMetaData> <u32 len> "PAR1"
    if (img.size() < 12) { throw thrift_error("file too small to be Parquet"); }
    if (std::memcmp(img.data(), "PAR1", 4) != 0) { throw thrift_error("bad leading magic"); }
    if (std::memcmp(img.data() + img.size() - 4, "PAR1", 4) != 0) {
        throw thrift_error("bad trailing magic (encrypted footers unsupported)");
    }
    uint32_t len;
    std::memcpy(&len, img.data() + img.size() - 8, 4);   // little-endian on disk
    size_t end = img.size() - 8;
    if (len > end || len < 1) { throw thrift_error("footer length out of range"); }
    return {end - len, len};
}

file_metadata parse_footer(std::span<const uint8_t> img, limits lim) {
    auto s = locate_footer(img);
    return parse_file_metadata(img.subspan(s.offset, s.length), lim);
}

} // namespace sstables::parquet::format
