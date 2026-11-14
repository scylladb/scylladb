/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/thrift.hh"

#include <bit>
#include <cstring>
#include <limits>

#include <seastar/core/format.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/thread.hh>

#include "sstables/exceptions.hh"
#include "utils/log.hh"

namespace sstables::parquet::thrift {

static logging::logger thrift_logger("parquet_thrift");

void on_malformed(std::string_view what, uint64_t position) {
    throw malformed_sstable_exception(seastar::format("Parquet metadata is malformed at offset {}: {}", position, what));
}

static uint64_t zigzag_encode(int64_t v) noexcept {
    return (static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63);
}

static int64_t zigzag_decode(uint64_t v) noexcept {
    return static_cast<int64_t>(v >> 1) ^ -static_cast<int64_t>(v & 1);
}

// encoder

void encoder::write_varint(uint64_t v) {
    char buf[10];
    size_t n = 0;
    while (v >= 0x80) {
        buf[n++] = static_cast<char>((v & 0x7f) | 0x80);
        v >>= 7;
    }
    buf[n++] = static_cast<char>(v);
    _out.write(buf, n);
}

void encoder::begin_struct() {
    if (_depth == max_depth) {
        on_internal_error(thrift_logger, "Thrift encoder: structs nested too deep");
    }
    _last_field_id[_depth++] = 0;
}

void encoder::end_struct() {
    if (_depth == 0) {
        on_internal_error(thrift_logger, "Thrift encoder: end_struct() without begin_struct()");
    }
    --_depth;
    const char stop = 0;
    _out.write(&stop, 1);
}

void encoder::field_header(int16_t id, type t) {
    if (_depth == 0) {
        on_internal_error(thrift_logger, "Thrift encoder: field outside a struct");
    }
    auto& last = _last_field_id[_depth - 1];
    const int32_t delta = int32_t(id) - int32_t(last);
    if (delta > 0 && delta <= 15) {
        const char header = static_cast<char>((delta << 4) | static_cast<uint8_t>(t));
        _out.write(&header, 1);
    } else {
        const char header = static_cast<char>(t);
        _out.write(&header, 1);
        write_varint(zigzag_encode(id));
    }
    last = id;
}

void encoder::write_bool_field(int16_t id, bool v) {
    field_header(id, v ? type::bool_true : type::bool_false);
}

void encoder::write_list_header(type element_type, uint32_t size) {
    // A boolean element type is written as 1, like the field type for true.
    auto t = element_type == type::bool_false ? type::bool_true : element_type;
    if (size < 15) {
        const char header = static_cast<char>((size << 4) | static_cast<uint8_t>(t));
        _out.write(&header, 1);
    } else {
        const char header = static_cast<char>(0xf0 | static_cast<uint8_t>(t));
        _out.write(&header, 1);
        write_varint(size);
    }
}

void encoder::write_bool(bool v) {
    const char b = v ? 1 : 2;
    _out.write(&b, 1);
}

void encoder::write_i8(int8_t v) {
    const char b = static_cast<char>(v);
    _out.write(&b, 1);
}

void encoder::write_i16(int16_t v) {
    write_varint(zigzag_encode(v));
}

void encoder::write_i32(int32_t v) {
    write_varint(zigzag_encode(v));
}

void encoder::write_i64(int64_t v) {
    write_varint(zigzag_encode(v));
}

void encoder::write_binary(std::string_view v) {
    if (v.size() > std::numeric_limits<int32_t>::max()) {
        on_internal_error(thrift_logger, "Thrift encoder: binary value too long");
    }
    write_varint(v.size());
    _out.write(v.data(), v.size());
}

// decoder

void decoder::fail(std::string_view what) const {
    on_malformed(what, position());
}

void decoder::refill() {
    if (!_source || _end_position == _length) {
        fail("unexpected end of input");
    }
    auto fragment = _source->next();
    if (fragment.empty()) {
        fail("input is shorter than its declared length");
    }
    // Bytes past the declared length belong to someone else.
    auto n = std::min<uint64_t>(fragment.size(), _length - _end_position);
    _pos = fragment.data();
    _end = _pos + n;
    _end_position += n;
}

uint64_t decoder::read_varint64() {
    uint64_t v = 0;
    for (unsigned shift = 0; shift < 64; shift += 7) {
        const uint8_t b = read_byte();
        // The tenth byte holds bit 63 only, and has no continuation bit.
        if (shift == 63 && b > 1) {
            fail("varint overflows 64 bits");
        }
        v |= uint64_t(b & 0x7f) << shift;
        if (!(b & 0x80)) {
            return v;
        }
    }
    fail("varint is too long");
}

uint32_t decoder::read_varint32() {
    auto v = read_varint64();
    if (v > std::numeric_limits<uint32_t>::max()) {
        fail("varint overflows 32 bits");
    }
    return v;
}

void decoder::read_bytes(char* out, uint64_t n) {
    if (n > remaining()) {
        fail("value extends past the end of input");
    }
    while (n) {
        if (_pos == _end) {
            refill();
        }
        auto chunk = std::min<uint64_t>(n, _end - _pos);
        std::memcpy(out, _pos, chunk);
        _pos += chunk;
        out += chunk;
        n -= chunk;
    }
}

void decoder::skip_bytes(uint64_t n) {
    if (n > remaining()) {
        fail("value extends past the end of input");
    }
    while (n) {
        if (_pos == _end) {
            refill();
        }
        auto chunk = std::min<uint64_t>(n, _end - _pos);
        _pos += chunk;
        n -= chunk;
    }
}

bool decoder::read_bool() {
    // The specification says 2 for false. Some implementations write 0.
    switch (read_byte()) {
    case 1: return true;
    case 0:
    case 2: return false;
    default: fail("invalid boolean element");
    }
}

int8_t decoder::read_i8() {
    return static_cast<int8_t>(read_byte());
}

int16_t decoder::read_i16() {
    auto v = read_varint32();
    if (v > std::numeric_limits<uint16_t>::max()) {
        fail("i16 out of range");
    }
    return static_cast<int16_t>(zigzag_decode(v));
}

int32_t decoder::read_i32() {
    return static_cast<int32_t>(zigzag_decode(read_varint32()));
}

int64_t decoder::read_i64() {
    return zigzag_decode(read_varint64());
}

uint32_t decoder::read_binary_size() {
    auto n = read_varint32();
    if (n > remaining()) {
        fail("binary value extends past the end of input");
    }
    return n;
}

sstring decoder::read_string() {
    auto n = read_binary_size();
    if (n > max_string_size) {
        fail(seastar::format("a string of {} bytes is longer than {}", n, max_string_size));
    }
    sstring s(sstring::initialized_later(), n);
    read_bytes(s.data(), n);
    return s;
}

type decoder::element_type(uint8_t nibble) {
    switch (nibble) {
    case 1:
    case 2:
        // Both values denote a boolean element type.
        return type::bool_true;
    case 3: case 4: case 5: case 6: case 7: case 8: case 9: case 10: case 11: case 12: case 13:
        return static_cast<type>(nibble);
    default:
        fail("invalid element type");
    }
}

list_header decoder::read_list_header() {
    const uint8_t b = read_byte();
    const auto t = element_type(b & 0x0f);
    uint32_t size = b >> 4;
    if (size == 15) {
        size = read_varint32();
    }
    // Every element takes at least one byte.
    if (size > remaining()) {
        fail("list is longer than the remaining input");
    }
    return {t, size};
}

map_header decoder::read_map_header() {
    const uint32_t size = read_varint32();
    if (size == 0) {
        return {type::stop, type::stop, 0};
    }
    const uint8_t b = read_byte();
    const auto k = element_type(b >> 4);
    const auto v = element_type(b & 0x0f);
    // Every entry takes at least two bytes.
    if (size > remaining() / 2) {
        fail("map is longer than the remaining input");
    }
    return {k, v, size};
}

void decoder::expect(const field_header& f, type t) const {
    if (f.t != t) {
        fail(seastar::format("field {} has type {}, expected {}", f.id, int(f.t), int(t)));
    }
}

bool decoder::read_bool(const field_header& f) {
    switch (f.t) {
    case type::bool_true: return true;
    case type::bool_false: return false;
    default: fail(seastar::format("field {} has type {}, expected a boolean", f.id, int(f.t)));
    }
}

int8_t decoder::read_i8(const field_header& f) {
    expect(f, type::i8);
    return read_i8();
}

int32_t decoder::read_i32(const field_header& f) {
    expect(f, type::i32);
    return read_i32();
}

int64_t decoder::read_i64(const field_header& f) {
    expect(f, type::i64);
    return read_i64();
}

sstring decoder::read_string(const field_header& f) {
    expect(f, type::binary);
    return read_string();
}

list_header decoder::read_list_header(const field_header& f, type element_type) {
    expect(f, type::list);
    auto h = read_list_header();
    // An empty list may carry any element type.
    if (h.element_type != element_type && h.size) {
        fail(seastar::format("list field {} has element type {}, expected {}", f.id, int(h.element_type), int(element_type)));
    }
    return h;
}

std::optional<field_header> decoder::read_field_header(int16_t& last_id) {
    const uint8_t b = read_byte();
    if (b == 0) {
        return std::nullopt;
    }
    const uint8_t t = b & 0x0f;
    if (t == 0 || t > static_cast<uint8_t>(type::uuid)) {
        fail("invalid field type");
    }
    const uint8_t delta = b >> 4;
    int32_t id;
    if (delta) {
        id = int32_t(last_id) + delta;
        if (id > std::numeric_limits<int16_t>::max()) {
            fail("field id out of range");
        }
    } else {
        id = read_i16();
    }
    last_id = id;
    return field_header{int16_t(id), static_cast<type>(t)};
}

void decoder::skip(const field_header& f) {
    switch (f.t) {
    case type::bool_true:
    case type::bool_false:
        // A boolean field has its value in the header.
        return;
    default:
        skip_value(f.t);
    }
}

// The size of a list element of type `t`, if it is fixed. 0 otherwise.
static uint64_t fixed_element_size(type t) noexcept {
    switch (t) {
    case type::bool_true:
    case type::bool_false:
    case type::i8:
        return 1;
    case type::double_:
        return 8;
    case type::uuid:
        return 16;
    default:
        return 0;
    }
}

// A preemption point between the elements of a list, a map, or a struct
// that is skipped. The footer parser runs in a seastar thread, and a footer can be
// large. Elsewhere it does nothing.
static void maybe_yield() {
    if (seastar::thread::running_in_thread()) {
        seastar::thread::maybe_yield();
    }
}

void decoder::skip_value(type t) {
    switch (t) {
    case type::bool_true:
    case type::bool_false:
    case type::i8:
        skip_bytes(1);
        return;
    case type::i16:
    case type::i32:
    case type::i64:
        read_varint64();
        return;
    case type::double_:
        skip_bytes(8);
        return;
    case type::uuid:
        skip_bytes(16);
        return;
    case type::binary:
        skip_bytes(read_binary_size());
        return;
    case type::list:
    case type::set: {
        nesting_guard guard(*this);
        auto h = read_list_header();
        if (const uint64_t n = fixed_element_size(h.element_type)) {
            skip_bytes(n * h.size);
            return;
        }
        for (uint32_t i = 0; i < h.size; ++i) {
            skip_value(h.element_type);
            maybe_yield();
        }
        return;
    }
    case type::map: {
        nesting_guard guard(*this);
        auto h = read_map_header();
        for (uint32_t i = 0; i < h.size; ++i) {
            skip_value(h.key_type);
            skip_value(h.value_type);
            maybe_yield();
        }
        return;
    }
    case type::struct_:
        read_struct([this] (const field_header& f) {
            skip(f);
            maybe_yield();
        });
        return;
    case type::stop:
        break;
    }
    fail("invalid type");
}

void field_set::require(const decoder& d, std::initializer_list<int16_t> ids, std::string_view struct_name) const {
    for (auto id : ids) {
        if (!contains(id)) {
            d.fail(seastar::format("required field {} of {} is missing", id, struct_name));
        }
    }
}

} // namespace sstables::parquet::thrift
