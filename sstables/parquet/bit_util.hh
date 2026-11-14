/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Building blocks of the Parquet encodings: variable-length integers, bit
// packing, and a reader of untrusted bytes in a chain of contiguous pieces.
//
// Everything here works on bytes in memory and does no I/O.

#include <bit>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <type_traits>

#include "bytes_ostream.hh"
#include "utils/fragmented_temporary_buffer.hh"

namespace sstables::parquet {

// Parquet stores numbers in little-endian order, and the codec copies them
// without conversion.
static_assert(std::endian::native == std::endian::little);

/// The read-side buffer type: a chain of contiguous pieces of at most 128 KiB.
/// A view of it also represents a byte array value that the decoders return.
using byte_array_view = fragmented_temporary_buffer::view;

/// Raises the malformed sstable error for page data (levels, values, or a
/// compressed page) that does not decode. `position` is the offset of the
/// problem within the decoded section.
[[noreturn]] void on_malformed_data(std::string_view what, uint64_t position);
[[noreturn]] void on_malformed_data(std::string_view what);

inline uint64_t zigzag_encode(int64_t v) noexcept {
    return (static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63);
}

inline int64_t zigzag_decode(uint64_t v) noexcept {
    return static_cast<int64_t>(v >> 1) ^ -static_cast<int64_t>(v & 1);
}

/// The number of bytes of the ULEB128 encoding of `v`.
inline unsigned uleb128_size(uint64_t v) noexcept {
    return v ? (std::bit_width(v) + 6) / 7 : 1;
}

/// Appends the ULEB128 encoding of `v`.
inline void write_uleb128(bytes_ostream& out, uint64_t v) {
    char buf[10];
    unsigned n = 0;
    while (v >= 0x80) {
        buf[n++] = static_cast<char>((v & 0x7f) | 0x80);
        v >>= 7;
    }
    buf[n++] = static_cast<char>(v);
    out.write(buf, n);
}

/// Appends `v` in little-endian order.
template <typename T>
requires std::is_trivially_copyable_v<T>
inline void write_le(bytes_ostream& out, T v) {
    out.write(reinterpret_cast<const char*>(&v), sizeof(T));
}

/// The number of bits needed for values up to `max_value`.
inline unsigned bit_width_of(uint64_t max_value) noexcept {
    return std::bit_width(max_value);
}

// Bit packing, as in the RLE/bit-packing hybrid encoding and in the
// miniblocks of DELTA_BINARY_PACKED: values are packed from the least
// significant bit of each byte. A group of 8 values of width `bit_width`
// takes exactly `bit_width` bytes.

/// Packs 8 values into `bit_width` bytes at `out`. Each value must fit in
/// `bit_width` bits. `bit_width` is at most 64.
void pack8(const uint64_t* in, unsigned bit_width, uint8_t* out) noexcept;
void pack8(const uint32_t* in, unsigned bit_width, uint8_t* out) noexcept;

/// Unpacks 8 values from `bit_width` bytes at `in`. `bit_width` is at most 64.
void unpack8(const uint8_t* in, unsigned bit_width, uint64_t* out) noexcept;
void unpack8(const uint8_t* in, unsigned bit_width, uint32_t* out) noexcept;

/// The number of bytes after a group that unpack8_padded() may read.
inline constexpr size_t unpack_padding = 8;

/// Like unpack8(), but may also read the unpack_padding bytes after the
/// group, and ignores what they hold. So it needs no copy of the group.
void unpack8_padded(const uint8_t* in, unsigned bit_width, uint64_t* out) noexcept;
void unpack8_padded(const uint8_t* in, unsigned bit_width, uint32_t* out) noexcept;

/// Reads untrusted bytes from a byte_array_view.
///
/// Every read checks the remaining length first and raises the malformed
/// sstable error if the data ends early. Nothing is allocated based on a
/// value read from the input.
class byte_reader {
    byte_array_view _v;
    uint64_t _size;
public:
    byte_reader() noexcept : _size(0) {}
    explicit byte_reader(byte_array_view v) noexcept : _v(v), _size(v.size_bytes()) {}

    uint64_t remaining() const noexcept { return _v.size_bytes(); }
    /// The number of bytes consumed so far.
    uint64_t position() const noexcept { return _size - remaining(); }
    /// The unread part of the input.
    byte_array_view view() const noexcept { return _v; }

    [[noreturn]] void fail(std::string_view what) const;

    void need(uint64_t n) const {
        if (n > remaining()) [[unlikely]] {
            fail("unexpected end of data");
        }
    }

    uint8_t read_byte() {
        need(1);
        auto b = static_cast<uint8_t>(_v.current_fragment()[0]);
        _v.remove_prefix(1);
        return b;
    }

    /// Copies the next `n` bytes to `out`.
    void read(void* out, size_t n) {
        need(n);
        auto p = static_cast<char*>(out);
        while (n) {
            auto frag = _v.current_fragment();
            auto now = std::min(n, frag.size());
            std::memcpy(p, frag.data(), now);
            p += now;
            n -= now;
            _v.remove_prefix(now);
        }
    }

    void skip(uint64_t n) {
        need(n);
        _v.remove_prefix(n);
    }

    /// If the next `n` bytes and the `padding` bytes after them are in one
    /// contiguous piece of the input, returns a pointer to them and skips
    /// `n` bytes. Otherwise returns null and reads nothing.
    const uint8_t* try_read_padded(size_t n, size_t padding) noexcept {
        const auto frag = _v.current_fragment();
        if (frag.size() < n + padding) {
            return nullptr;
        }
        _v.remove_prefix(n);
        return reinterpret_cast<const uint8_t*>(frag.data());
    }

    /// Returns a view of the next `n` bytes and skips them.
    byte_array_view read_view(uint64_t n) {
        const auto frag = _v.current_fragment();
        if (n <= frag.size()) [[likely]] {
            // The bytes are in one contiguous piece of the input.
            _v.remove_prefix(n);
            return byte_array_view(bytes_view(frag.data(), n));
        }
        need(n);
        auto r = _v.prefix(n);
        _v.remove_prefix(n);
        return r;
    }

    template <typename T>
    requires std::is_trivially_copyable_v<T>
    T read_le() {
        T v;
        auto frag = _v.current_fragment();
        if (frag.size() >= sizeof(T)) [[likely]] {
            std::memcpy(&v, frag.data(), sizeof(T));
            _v.remove_prefix(sizeof(T));
        } else {
            read(&v, sizeof(T));
        }
        return v;
    }

    /// Reads a ULEB128 integer of at most 64 bits.
    uint64_t read_uleb128();
    /// Reads a ULEB128 integer and checks that it fits in 32 bits.
    uint32_t read_uleb128_32();
    int64_t read_zigzag() { return zigzag_decode(read_uleb128()); }
};

/// Copies `in` into a fragmented_temporary_buffer with contiguous pieces of
/// at most `piece_size` bytes. Tests use small pieces to exercise values
/// that cross piece boundaries.
fragmented_temporary_buffer to_fragmented_buffer(const bytes_ostream& in,
        size_t piece_size = fragmented_temporary_buffer::default_fragment_size);

} // namespace sstables::parquet
