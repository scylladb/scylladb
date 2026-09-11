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
    if (size_t(h.compressed_page_size) > lim.max_page_bytes
        || size_t(h.uncompressed_page_size) > lim.max_page_bytes) {
        throw thrift_error("page size exceeds limit");
    }
    // Every count below is turned into an allocation or a span length by the reader, so a
    // negative or absurd one is refused here, where the header is still just numbers.
    auto check_values = [&] (int32_t n, const char* what) {
        if (n < 0) { throw thrift_error(std::string("negative page ") + what); }
        if (size_t(n) > lim.max_page_values) {
            throw thrift_error(std::string("page ") + what + " exceeds limit");
        }
    };
    if (h.v1) { check_values(h.v1->num_values, "num_values"); }
    if (h.dict) { check_values(h.dict->num_values, "num_values"); }
    if (h.v2) {
        const auto& v2 = *h.v2;
        check_values(v2.num_values, "num_values");
        check_values(v2.num_nulls, "num_nulls");
        check_values(v2.num_rows, "num_rows");
        if (v2.num_nulls > v2.num_values) { throw thrift_error("page num_nulls > num_values"); }
        if (v2.repetition_levels_byte_length < 0 || v2.definition_levels_byte_length < 0) {
            throw thrift_error("negative level byte length");
        }
        // Subtraction form: the two lengths can each be INT32_MAX, and their sum must not be.
        const size_t rl = size_t(v2.repetition_levels_byte_length);
        const size_t dl = size_t(v2.definition_levels_byte_length);
        const size_t page = size_t(h.uncompressed_page_size);
        if (rl > page || dl > page - rl) { throw thrift_error("level lengths exceed page size"); }
    }
    return h;
}

} // namespace sstables::parquet::format
