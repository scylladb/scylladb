/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/bit_util.hh"

#include <limits>

#include <seastar/core/format.hh>

#include "sstables/exceptions.hh"

namespace sstables::parquet {

void on_malformed_data(std::string_view what, uint64_t position) {
    throw malformed_sstable_exception(seastar::format("Parquet page data is malformed at offset {}: {}", position, what));
}

void on_malformed_data(std::string_view what) {
    throw malformed_sstable_exception(seastar::format("Parquet page data is malformed: {}", what));
}

void byte_reader::fail(std::string_view what) const {
    on_malformed_data(what, position());
}

uint64_t byte_reader::read_uleb128() {
    uint64_t v = 0;
    for (unsigned shift = 0; shift < 64; shift += 7) {
        const uint8_t b = read_byte();
        const uint64_t bits = b & 0x7f;
        // The tenth byte may only hold the top bit of a 64-bit value.
        if (shift == 63 && bits > 1) {
            fail("ULEB128 integer longer than 64 bits");
        }
        v |= bits << shift;
        if (!(b & 0x80)) {
            return v;
        }
    }
    fail("ULEB128 integer longer than 64 bits");
}

uint32_t byte_reader::read_uleb128_32() {
    auto v = read_uleb128();
    if (v > std::numeric_limits<uint32_t>::max()) {
        fail("ULEB128 integer longer than 32 bits");
    }
    return v;
}

// The packing helpers work in a zeroed scratch buffer with room for an
// unaligned 8-byte access at the last value plus one spill byte. The last of
// 8 values starts at bit 7 * 64 = 448, which is byte 56, so 72 bytes suffice.
static constexpr size_t pack_scratch_size = 72;

static uint64_t low_mask(unsigned bit_width) noexcept {
    return bit_width == 64 ? ~uint64_t(0) : (uint64_t(1) << bit_width) - 1;
}

template <typename T>
static void do_pack8(const T* in, unsigned bit_width, uint8_t* out) noexcept {
    if (bit_width == 0) {
        return;
    }
    uint8_t buf[pack_scratch_size] = {};
    for (unsigned i = 0; i < 8; ++i) {
        const unsigned bit = i * bit_width;
        const unsigned byte = bit / 8;
        const unsigned shift = bit % 8;
        const uint64_t v = uint64_t(in[i]);
        uint64_t w;
        std::memcpy(&w, buf + byte, 8);
        w |= v << shift;
        std::memcpy(buf + byte, &w, 8);
        if (shift + bit_width > 64) {
            buf[byte + 8] |= uint8_t(v >> (64 - shift));
        }
    }
    std::memcpy(out, buf, bit_width);
}

template <typename T>
static void do_unpack8_padded(const uint8_t* in, unsigned bit_width, T* out) noexcept {
    if (bit_width == 0) {
        std::fill_n(out, 8, T(0));
        return;
    }
    const uint64_t mask = low_mask(bit_width);
    if (bit_width <= 8) {
        // The group fits in one word.
        uint64_t w;
        std::memcpy(&w, in, 8);
        for (unsigned i = 0; i < 8; ++i) {
            out[i] = T((w >> (i * bit_width)) & mask);
        }
        return;
    }
    if (bit_width <= 56) {
        // A value starts at most 7 bits into a byte, so it lies within
        // the 8 bytes from there.
        for (unsigned i = 0; i < 8; ++i) {
            const unsigned bit = i * bit_width;
            uint64_t w;
            std::memcpy(&w, in + bit / 8, 8);
            out[i] = T((w >> (bit % 8)) & mask);
        }
        return;
    }
    for (unsigned i = 0; i < 8; ++i) {
        const unsigned bit = i * bit_width;
        const unsigned byte = bit / 8;
        const unsigned shift = bit % 8;
        uint64_t w;
        std::memcpy(&w, in + byte, 8);
        w >>= shift;
        if (shift + bit_width > 64) {
            w |= uint64_t(in[byte + 8]) << (64 - shift);
        }
        out[i] = T(w & mask);
    }
}

template <typename T>
static void do_unpack8(const uint8_t* in, unsigned bit_width, T* out) noexcept {
    uint8_t buf[pack_scratch_size] = {};
    std::memcpy(buf, in, bit_width);
    do_unpack8_padded(buf, bit_width, out);
}

void pack8(const uint64_t* in, unsigned bit_width, uint8_t* out) noexcept {
    do_pack8(in, bit_width, out);
}

void pack8(const uint32_t* in, unsigned bit_width, uint8_t* out) noexcept {
    do_pack8(in, bit_width, out);
}

void unpack8(const uint8_t* in, unsigned bit_width, uint64_t* out) noexcept {
    do_unpack8(in, bit_width, out);
}

void unpack8(const uint8_t* in, unsigned bit_width, uint32_t* out) noexcept {
    do_unpack8(in, bit_width, out);
}

void unpack8_padded(const uint8_t* in, unsigned bit_width, uint64_t* out) noexcept {
    do_unpack8_padded(in, bit_width, out);
}

void unpack8_padded(const uint8_t* in, unsigned bit_width, uint32_t* out) noexcept {
    do_unpack8_padded(in, bit_width, out);
}

fragmented_temporary_buffer to_fragmented_buffer(const bytes_ostream& in, size_t piece_size) {
    std::vector<temporary_buffer<char>> pieces;
    temporary_buffer<char> current;
    size_t filled = 0;
    for (bytes_view frag : in.fragments()) {
        while (!frag.empty()) {
            if (filled == current.size()) {
                if (!current.empty()) {
                    pieces.push_back(std::move(current));
                }
                current = temporary_buffer<char>(std::min(piece_size, in.size() - pieces.size() * piece_size));
                filled = 0;
            }
            auto now = std::min(frag.size(), current.size() - filled);
            std::memcpy(current.get_write() + filled, frag.data(), now);
            filled += now;
            frag.remove_prefix(now);
        }
    }
    if (!current.empty()) {
        pieces.push_back(std::move(current));
    }
    return fragmented_temporary_buffer(std::move(pieces), in.size());
}

} // namespace sstables::parquet
