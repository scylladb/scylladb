/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "page_header.hh"

namespace sstables::parquet::format {

namespace {

data_page_header parse_v1(compact_reader& r) {
    compact_reader::struct_scope sc(r);
    data_page_header h;
    for (;;) {
        auto f = r.field_begin();
        if (f.stop) { break; }
        switch (f.id) {
        case 1: h.num_values = r.i32v(); break;
        case 2: h.value_encoding = encoding(r.i32v()); break;
        case 3: h.definition_level_encoding = encoding(r.i32v()); break;
        case 4: h.repetition_level_encoding = encoding(r.i32v()); break;
        default: r.skip(f.type);
        }
    }
    return h;
}

data_page_header_v2 parse_v2(compact_reader& r) {
    compact_reader::struct_scope sc(r);
    data_page_header_v2 h;
    for (;;) {
        auto f = r.field_begin();
        if (f.stop) { break; }
        switch (f.id) {
        case 1: h.num_values = r.i32v(); break;
        case 2: h.num_nulls = r.i32v(); break;
        case 3: h.num_rows = r.i32v(); break;
        case 4: h.value_encoding = encoding(r.i32v()); break;
        case 5: h.definition_levels_byte_length = r.i32v(); break;
        case 6: h.repetition_levels_byte_length = r.i32v(); break;
        case 7:
            // bool fields are carried in the field type itself
            h.is_compressed = (f.type == ctype::boolean_true);
            break;
        default: r.skip(f.type);
        }
    }
    return h;
}

dictionary_page_header parse_dict(compact_reader& r) {
    compact_reader::struct_scope sc(r);
    dictionary_page_header h;
    for (;;) {
        auto f = r.field_begin();
        if (f.stop) { break; }
        switch (f.id) {
        case 1: h.num_values = r.i32v(); break;
        case 2: h.value_encoding = encoding(r.i32v()); break;
        default: r.skip(f.type);
        }
    }
    return h;
}

} // namespace

page_header parse_page_header(std::span<const uint8_t> buf, size_t& consumed, limits lim) {
    compact_reader r(buf, lim);
    size_t before = r.remaining();
    page_header h;
    {
        compact_reader::struct_scope sc(r);
        for (;;) {
            auto f = r.field_begin();
            if (f.stop) { break; }
            switch (f.id) {
            case 1: h.type = page_type(r.i32v()); break;
            case 2: h.uncompressed_page_size = r.i32v(); break;
            case 3: h.compressed_page_size = r.i32v(); break;
            case 4: h.crc = r.i32v(); break;
            case 5: h.v1 = parse_v1(r); break;
            case 7: h.dict = parse_dict(r); break;
            case 8: h.v2 = parse_v2(r); break;
            default: r.skip(f.type);
            }
        }
    }
    consumed = before - r.remaining();
    if (h.compressed_page_size < 0 || h.uncompressed_page_size < 0) {
        throw thrift_error("negative page size");
    }
    return h;
}

} // namespace sstables::parquet::format
