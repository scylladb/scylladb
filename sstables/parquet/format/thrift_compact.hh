/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// A minimal, allocation-bounded reader for Thrift's TCompactProtocol, covering
// exactly the subset used by Apache Parquet's metadata structures.
//
// Why hand-written rather than libthrift (see docs/dev/parquet-storage-format.md
// section 7.8):
//   * Scylla has no Thrift dependency today and adding one to every distro
//     package for a handful of structs is a poor trade.
//   * Parquet footers can arrive from untrusted places (restore, the upload/
//     directory), so the parser must bound allocation and recursion. A generated
//     parser does not.
//
// This layer knows nothing about Scylla types. It is deliberately confined to
// std:: so it can be unit-tested and fuzzed standalone.

#include <cstdint>
#include <cstring>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>

namespace sstables::parquet::format {

class thrift_error : public std::runtime_error {
public:
    explicit thrift_error(const std::string& what) : std::runtime_error("parquet/thrift: " + what) {}
};

// TCompactProtocol element types, as they appear in field and collection headers.
enum class ctype : uint8_t {
    stop          = 0x0,
    boolean_true  = 0x1,
    boolean_false = 0x2,
    i8            = 0x3,
    i16           = 0x4,
    i32           = 0x5,
    i64           = 0x6,
    dbl           = 0x7,
    binary        = 0x8,
    list          = 0x9,
    set           = 0xA,
    map           = 0xB,
    strct         = 0xC,
};

struct limits {
    // Defaults chosen to be far above any legitimate Parquet footer while still
    // bounding what a hostile file can make us do.
    size_t max_depth       = 64;
    size_t max_collection  = 4u << 20;   // 4 Mi elements
    size_t max_binary      = 64u << 20;  // 64 MiB
};

class compact_reader {
    const uint8_t* _base;
    const uint8_t* _p;
    const uint8_t* _end;
    limits _lim;
    size_t _depth = 0;
    // TCompactProtocol encodes field ids as deltas from the previous field in the
    // same struct, so the reader has to carry that state across a struct's fields.
    int16_t _last_id = 0;

public:
    explicit compact_reader(std::span<const uint8_t> buf, limits l = {})
        : _base(buf.data()), _p(buf.data()), _end(buf.data() + buf.size()), _lim(l) {}

    size_t remaining() const noexcept { return size_t(_end - _p); }
    // Byte offset from the start of the buffer. Needed to record the extent of a struct that
    // was skipped rather than decoded, so it can be decoded later on demand.
    size_t position() const noexcept { return size_t(_p - _base); }
    bool eof() const noexcept { return _p >= _end; }

    uint8_t byte() {
        if (_p >= _end) { throw thrift_error("truncated: expected a byte"); }
        return *_p++;
    }
    uint8_t peek() const {
        if (_p >= _end) { throw thrift_error("truncated: expected a byte"); }
        return *_p;
    }

    // LEB128. Bounded at 10 bytes so a run of 0x80 cannot spin.
    uint64_t uvarint() {
        uint64_t v = 0;
        int shift = 0;
        for (int i = 0; i < 10; ++i) {
            uint8_t b = byte();
            v |= uint64_t(b & 0x7F) << shift;
            if (!(b & 0x80)) { return v; }
            shift += 7;
        }
        throw thrift_error("varint longer than 10 bytes");
    }

    int64_t zigzag() {
        uint64_t u = uvarint();
        return int64_t(u >> 1) ^ -int64_t(u & 1);
    }

    int32_t i32v()  { return int32_t(zigzag()); }
    int64_t i64v()  { return zigzag(); }
    int16_t i16v()  { return int16_t(zigzag()); }

    double dbl_v() {
        if (remaining() < 8) { throw thrift_error("truncated double"); }
        uint64_t bits;
        std::memcpy(&bits, _p, 8);   // TCompactProtocol writes doubles little-endian
        _p += 8;
        double d;
        std::memcpy(&d, &bits, 8);
        return d;
    }

    std::string_view binary_v() {
        uint64_t n = uvarint();
        if (n > _lim.max_binary) { throw thrift_error("binary field exceeds limit"); }
        if (n > remaining())     { throw thrift_error("truncated binary field"); }
        auto sv = std::string_view(reinterpret_cast<const char*>(_p), size_t(n));
        _p += n;
        return sv;
    }

    struct field_header {
        ctype   type;
        int16_t id;
        bool    bool_value;   // meaningful only for boolean_true / boolean_false
        bool    stop;
    };

    field_header field_begin() {
        uint8_t b = byte();
        if (b == 0) { return {ctype::stop, 0, false, true}; }
        uint8_t delta = uint8_t(b >> 4);
        auto t = ctype(b & 0x0F);
        int16_t id;
        if (delta == 0) {
            id = int16_t(zigzag());   // long form: explicit field id follows
        } else {
            id = int16_t(_last_id + delta);
        }
        _last_id = id;
        return {t, id, t == ctype::boolean_true, false};
    }

    struct collection_header { ctype elem; size_t size; };

    collection_header list_begin() {
        uint8_t b = byte();
        size_t n = size_t(b >> 4);
        auto t = ctype(b & 0x0F);
        if (n == 0x0F) { n = size_t(uvarint()); }   // long form
        if (n > _lim.max_collection) { throw thrift_error("collection exceeds limit"); }
        // Cheap structural sanity check: every element needs at least one byte.
        if (n > remaining()) { throw thrift_error("collection larger than remaining input"); }
        return {t, n};
    }

    // Struct scope. Saves and restores the field-id delta base, and enforces the
    // recursion bound.
    class struct_scope {
        compact_reader& _r;
        int16_t _saved;
    public:
        explicit struct_scope(compact_reader& r) : _r(r), _saved(r._last_id) {
            if (++_r._depth > _r._lim.max_depth) { throw thrift_error("nesting too deep"); }
            _r._last_id = 0;
        }
        ~struct_scope() { --_r._depth; _r._last_id = _saved; }
        struct_scope(const struct_scope&) = delete;
        struct_scope& operator=(const struct_scope&) = delete;
    };

    // Skip a value of the given type without interpreting it. Needed because
    // Parquet writers legitimately emit fields we do not model, and forward
    // compatibility requires us to step over them rather than fail.
    void skip(ctype t) {
        if (++_depth > _lim.max_depth) { throw thrift_error("nesting too deep (skip)"); }
        struct depth_guard { size_t& d; ~depth_guard() { --d; } } g{_depth};

        switch (t) {
        case ctype::boolean_true:
        case ctype::boolean_false:
        case ctype::stop:
            break;
        case ctype::i8:   (void)byte(); break;
        case ctype::i16:
        case ctype::i32:
        case ctype::i64:  (void)zigzag(); break;
        case ctype::dbl:  (void)dbl_v(); break;
        case ctype::binary: (void)binary_v(); break;
        case ctype::list:
        case ctype::set: {
            auto h = list_begin();
            for (size_t i = 0; i < h.size; ++i) { skip(h.elem); }
            break;
        }
        case ctype::map: {
            uint64_t n = uvarint();
            if (n) {
                if (n > _lim.max_collection) { throw thrift_error("map exceeds limit"); }
                uint8_t kv = byte();
                auto kt = ctype(kv >> 4), vt = ctype(kv & 0x0F);
                for (uint64_t i = 0; i < n; ++i) { skip(kt); skip(vt); }
            }
            break;
        }
        case ctype::strct: {
            struct_scope s(*this);
            for (;;) {
                auto f = field_begin();
                if (f.stop) { break; }
                skip(f.type);
            }
            break;
        }
        default:
            throw thrift_error("unknown element type " + std::to_string(int(t)));
        }
    }
};

} // namespace sstables::parquet::format
