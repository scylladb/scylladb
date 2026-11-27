/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// A hand-written codec for the Thrift compact protocol, which Parquet uses
// for its metadata: the footer, page headers, and page indexes.
//
// Generated Thrift code would materialize the whole footer in large
// contiguous allocations. This codec lets the caller stream a structure
// through its own compact representation, element by element.
//
// The protocol is specified in doc/specs/thrift-compact-protocol.md of the
// Apache Thrift repository.

#include <array>
#include <cstdint>
#include <initializer_list>
#include <optional>
#include <ranges>
#include <span>
#include <string_view>

#include <seastar/core/sstring.hh>

#include "bytes_ostream.hh"
#include "seastarx.hh"

namespace sstables::parquet::thrift {

/// The type of a struct field or of a container element.
/// The values are the field types of the compact protocol.
enum class type : uint8_t {
    stop = 0,
    bool_true = 1,
    bool_false = 2,
    i8 = 3,
    i16 = 4,
    i32 = 5,
    i64 = 6,
    double_ = 7,
    binary = 8,
    list = 9,
    set = 10,
    map = 11,
    struct_ = 12,
    uuid = 13,
};

/// Raises the malformed sstable error for input that does not decode.
/// `position` is the offset of the problem in the decoded input.
[[noreturn]] void on_malformed(std::string_view what, uint64_t position);

/// Writes the compact protocol into a fragmented buffer.
///
/// The encoder tracks the last field id of each open struct, so field
/// headers use the short form whenever the protocol allows it. Every struct,
/// including the outermost one, is opened with begin_struct() or
/// begin_struct_field() and closed with end_struct().
class encoder {
public:
    /// Structs nested deeper than this are a bug in the caller.
    static constexpr unsigned max_depth = 16;
private:
    bytes_ostream& _out;
    std::array<int16_t, max_depth> _last_field_id;
    unsigned _depth = 0;

    void write_varint(uint64_t v);
public:
    explicit encoder(bytes_ostream& out) noexcept : _out(out) {}

    /// Opens a struct that is not a field: the outermost struct or a list element.
    void begin_struct();
    /// Writes the stop field and closes the innermost struct.
    void end_struct();

    /// Writes a field header. The value follows, written by the matching
    /// element function. Boolean fields use write_bool_field() instead,
    /// because their value is part of the header.
    void field_header(int16_t id, type t);

    void write_bool_field(int16_t id, bool v);
    void write_i8_field(int16_t id, int8_t v) { field_header(id, type::i8); write_i8(v); }
    void write_i32_field(int16_t id, int32_t v) { field_header(id, type::i32); write_i32(v); }
    void write_i64_field(int16_t id, int64_t v) { field_header(id, type::i64); write_i64(v); }
    void write_binary_field(int16_t id, std::string_view v) { field_header(id, type::binary); write_binary(v); }
    /// Writes the header of a list field. The caller then writes `size` elements.
    void write_list_field(int16_t id, type element_type, uint32_t size) {
        field_header(id, type::list);
        write_list_header(element_type, size);
    }
    /// Opens a struct field. The caller closes it with end_struct().
    void begin_struct_field(int16_t id) {
        field_header(id, type::struct_);
        begin_struct();
    }

    void write_list_header(type element_type, uint32_t size);
    /// A boolean list element: 1 for true and 2 for false.
    void write_bool(bool v);
    void write_i8(int8_t v);
    void write_i16(int16_t v);
    void write_i32(int32_t v);
    void write_i64(int64_t v);
    void write_binary(std::string_view v);
};

/// A source of input bytes for a decoder that reads a fragmented or a
/// streamed input.
class byte_source {
public:
    virtual ~byte_source() = default;
    /// Returns the next fragment of the input, or an empty span at its end.
    /// The fragment stays valid until the next call.
    virtual std::span<const char> next() = 0;
};

/// A byte_source over a range of contiguous fragments, for example
/// `bytes_ostream::fragments()` or a `fragmented_temporary_buffer::view`.
/// The range must outlive the source.
template <std::ranges::input_range FragmentRange>
class fragment_range_source final : public byte_source {
    std::ranges::iterator_t<const FragmentRange> _it;
    std::ranges::sentinel_t<const FragmentRange> _end;
public:
    explicit fragment_range_source(const FragmentRange& r) : _it(std::ranges::begin(r)), _end(std::ranges::end(r)) {}
    std::span<const char> next() override {
        while (_it != _end) {
            auto frag = *_it++;
            if (!frag.empty()) {
                return {reinterpret_cast<const char*>(frag.data()), frag.size()};
            }
        }
        return {};
    }
};

/// The header of a struct field.
struct field_header {
    int16_t id;
    type t;
};

/// The header of a list or a set.
struct list_header {
    type element_type;
    uint32_t size;
};

/// The header of a map. The key and value types are meaningless when the map is empty.
struct map_header {
    type key_type;
    type value_type;
    uint32_t size;
};

/// Reads the compact protocol from bytes in memory or from a byte_source.
///
/// The input has a known length. The decoder never reads past it, and it
/// raises the malformed sstable error when the input ends early or does not
/// decode. A length from the input is checked against the bytes that
/// remain before anything is allocated for it, so a corrupt length never
/// causes an allocation larger than the input.
///
/// With a byte_source, the decoder only asks for the next fragment when it
/// has consumed the current one. A source that waits for I/O must therefore
/// run the decoder in a seastar thread.
class decoder {
public:
    /// Nesting of structs and containers is limited to bound the stack use
    /// of skip(). Parquet nests at most about six levels.
    static constexpr unsigned max_depth = 32;
    /// The longest string that read_string() reads. A string is one
    /// contiguous allocation, so this bounds it. A longer one is malformed.
    static constexpr uint32_t max_string_size = 128 << 10;
private:
    const char* _pos;
    const char* _end;
    // The input position of _end.
    uint64_t _end_position;
    uint64_t _length;
    byte_source* _source = nullptr;
    unsigned _depth = 0;

    void refill();
    void skip_value(type t);
    type element_type(uint8_t nibble);

    class nesting_guard {
        decoder& _d;
    public:
        explicit nesting_guard(decoder& d) : _d(d) {
            if (++_d._depth > max_depth) {
                _d.fail("nesting too deep");
            }
        }
        ~nesting_guard() { --_d._depth; }
    };
public:
    /// Decodes bytes that are in memory, contiguously.
    explicit decoder(std::span<const char> input) noexcept
        : _pos(input.data()), _end(input.data() + input.size()), _end_position(input.size()), _length(input.size()) {}
    /// Decodes the first `length` bytes of a source.
    decoder(byte_source& source, uint64_t length) noexcept
        : _pos(nullptr), _end(nullptr), _end_position(0), _length(length), _source(&source) {}

    decoder(const decoder&) = delete;
    decoder& operator=(const decoder&) = delete;

    /// The number of bytes consumed so far.
    uint64_t position() const noexcept { return _end_position - (_end - _pos); }
    /// The number of bytes left in the input.
    uint64_t remaining() const noexcept { return _length - position(); }

    [[noreturn]] void fail(std::string_view what) const;

    uint8_t read_byte() {
        if (_pos == _end) [[unlikely]] {
            refill();
        }
        return static_cast<uint8_t>(*_pos++);
    }
    uint64_t read_varint64();
    uint32_t read_varint32();
    void read_bytes(char* out, uint64_t n);
    void skip_bytes(uint64_t n);

    /// Element readers. They also read field values whose type was checked.
    bool read_bool();
    int8_t read_i8();
    int16_t read_i16();
    int32_t read_i32();
    int64_t read_i64();
    /// Reads the length of a binary value and checks it against the remaining input.
    uint32_t read_binary_size();
    /// Reads a string of at most max_string_size bytes.
    sstring read_string();
    /// Reads the header of a list or a set. Checks the size against the remaining input.
    list_header read_list_header();
    /// Reads the header of a map. Checks the size against the remaining input.
    map_header read_map_header();

    /// Field readers. Each checks that the field has the expected type.
    bool read_bool(const field_header& f);
    int8_t read_i8(const field_header& f);
    int32_t read_i32(const field_header& f);
    int64_t read_i64(const field_header& f);
    sstring read_string(const field_header& f);
    /// Reads the header of a list field and checks the element type.
    /// A boolean element type is always reported as type::bool_true.
    list_header read_list_header(const field_header& f, type element_type);
    /// Reads a struct field. See read_struct().
    template <typename OnField>
    void read_struct(const field_header& f, OnField&& on_field) {
        expect(f, type::struct_);
        read_struct(std::forward<OnField>(on_field));
    }

    /// Raises the malformed sstable error unless the field has the given type.
    void expect(const field_header& f, type t) const;

    /// Reads a field header. Returns nothing at the stop field.
    /// `last_id` is the id of the previous field of the same struct, or 0.
    std::optional<field_header> read_field_header(int16_t& last_id);

    /// Skips the value of a field.
    void skip(const field_header& f);

    /// Reads a struct. Calls `on_field(const field_header&)` for each field.
    /// The callback must consume the field's value, with skip() if it does
    /// not know the field.
    template <typename OnField>
    void read_struct(OnField&& on_field) {
        nesting_guard guard(*this);
        int16_t last_id = 0;
        while (auto f = read_field_header(last_id)) {
            on_field(*f);
        }
    }
};

/// Tracks which fields of a struct were seen, to reject duplicates and to
/// check that the required fields are present. Field ids must be below 32.
class field_set {
    uint32_t _seen = 0;
public:
    /// Records a known field. Raises the malformed sstable error if the field was seen before.
    void add(const decoder& d, const field_header& f) {
        auto bit = uint32_t(1) << f.id;
        if (_seen & bit) {
            d.fail("duplicate field");
        }
        _seen |= bit;
    }
    bool contains(int16_t id) const noexcept {
        return _seen & (uint32_t(1) << id);
    }
    /// Raises the malformed sstable error unless all fields in `ids` were seen.
    void require(const decoder& d, std::initializer_list<int16_t> ids, std::string_view struct_name) const;
};

} // namespace sstables::parquet::thrift
