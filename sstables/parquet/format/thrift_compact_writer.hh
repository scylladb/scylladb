/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Writer half of TCompactProtocol, mirroring thrift_compact.hh.
//
// Only the constructs Parquet metadata uses: structs, lists, integers, binary
// and bool. No maps, no sets -- parquet.thrift does not need them, and leaving
// them out keeps the surface small enough to reason about.

#include "thrift_compact.hh"

#include <cstdint>
#include <string_view>
#include <vector>

namespace sstables::parquet::format {

class compact_writer {
    std::vector<uint8_t>& _out;
    // Field ids are written as deltas from the previous field of the same
    // struct, so the writer carries the same state the reader does.
    int16_t _last_id = 0;

public:
    explicit compact_writer(std::vector<uint8_t>& out) : _out(out) {}

    void raw(uint8_t b) { _out.push_back(b); }
    void raw(const void* p, size_t n) {
        auto q = static_cast<const uint8_t*>(p);
        _out.insert(_out.end(), q, q + n);
    }

    void uvarint(uint64_t v) {
        while (v >= 0x80) { _out.push_back(uint8_t(v) | 0x80); v >>= 7; }
        _out.push_back(uint8_t(v));
    }
    void zigzag(int64_t v) { uvarint((uint64_t(v) << 1) ^ uint64_t(v >> 63)); }

    void field(int16_t id, ctype t) {
        int32_t delta = id - _last_id;
        if (delta > 0 && delta <= 15) {
            _out.push_back(uint8_t((uint8_t(delta) << 4) | uint8_t(t)));
        } else {
            _out.push_back(uint8_t(t));
            zigzag(id);
        }
        _last_id = id;
    }

    void field_i16(int16_t id, int16_t v) { field(id, ctype::i16); zigzag(v); }
    void field_i32(int16_t id, int32_t v) { field(id, ctype::i32); zigzag(v); }
    void field_i64(int16_t id, int64_t v) { field(id, ctype::i64); zigzag(v); }
    void field_bool(int16_t id, bool v) {
        // The value lives in the field type; there is no body.
        field(id, v ? ctype::boolean_true : ctype::boolean_false);
    }
    void field_binary(int16_t id, std::string_view s) {
        field(id, ctype::binary);
        uvarint(s.size());
        raw(s.data(), s.size());
    }
    void field_list(int16_t id, ctype elem, size_t n) {
        field(id, ctype::list);
        list_header(elem, n);
    }
    void list_header(ctype elem, size_t n) {
        if (n < 15) {
            _out.push_back(uint8_t((uint8_t(n) << 4) | uint8_t(elem)));
        } else {
            _out.push_back(uint8_t(0xF0 | uint8_t(elem)));
            uvarint(n);
        }
    }
    void field_struct(int16_t id) { field(id, ctype::strct); }
    void stop() { _out.push_back(0); }

    // Enter a nested struct: field ids restart from 0 inside, and the enclosing
    // struct's delta base is restored on exit.
    class struct_scope {
        compact_writer& _w;
        int16_t _saved;
    public:
        explicit struct_scope(compact_writer& w) : _w(w), _saved(w._last_id) { _w._last_id = 0; }
        ~struct_scope() { _w.stop(); _w._last_id = _saved; }
        struct_scope(const struct_scope&) = delete;
        struct_scope& operator=(const struct_scope&) = delete;
    };
    // Same, but for a struct that is a list element (no STOP suppression differs;
    // list elements are ordinary structs).
    class elem_scope {
        compact_writer& _w;
        int16_t _saved;
    public:
        explicit elem_scope(compact_writer& w) : _w(w), _saved(w._last_id) { _w._last_id = 0; }
        ~elem_scope() { _w.stop(); _w._last_id = _saved; }
        elem_scope(const elem_scope&) = delete;
        elem_scope& operator=(const elem_scope&) = delete;
    };
};

} // namespace sstables::parquet::format
