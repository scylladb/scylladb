/*
 * Copyright 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#include "vint-serialization.hh"

#include <bit>
#include <seastar/core/bitops.hh>

#include <algorithm>
#include <array>
#include <limits>

static_assert(-1 == ~0, "Not a twos-complement architecture");

static constexpr uint64_t encode_zigzag(int64_t n) noexcept {
    // The right shift has to be arithmetic and not logical.
    return (static_cast<uint64_t>(n) << 1) ^ static_cast<uint64_t>(n >> 63);
}

static constexpr int64_t decode_zigzag(uint64_t n) noexcept {
    return static_cast<int64_t>((n >> 1) ^ -(n & 1));
}

// Mask for extracting from the first byte the part that is not used for indicating the total number of bytes.
static uint64_t first_byte_value_mask(vint_size_type extra_bytes_size) {
    // Include the sentinel zero bit in the mask.
    return uint64_t(0xff) >> extra_bytes_size;
}

vint_size_type signed_vint::serialize(int64_t value, bytes::iterator out) {
    return unsigned_vint::serialize(encode_zigzag(value), out);
}

vint_size_type signed_vint::serialized_size(int64_t value) noexcept {
    return unsigned_vint::serialized_size(encode_zigzag(value));
}

int64_t signed_vint::deserialize(bytes_view v) {
    const auto un = unsigned_vint::deserialize(v);
    return decode_zigzag(un);
}

// The number of additional bytes that we need to read.
static vint_size_type count_extra_bytes(int8_t first_byte) {
    return std::countl_zero(static_cast<uint8_t>(~first_byte));
}

vint_size_type unsigned_vint::serialize(uint64_t value, bytes::iterator out) {
    const auto size = serialized_size(value);

    // `size` is always in the range [1, 9].
    int extra_bytes_size = int(size - 1);
    auto mask = first_byte_value_mask(extra_bytes_size);

    auto shift = (unsigned(extra_bytes_size) * 8) % 64;
    *out++ = static_cast<int8_t>(((value >> shift) & mask) | ~mask);

    // Alternate destination for writes we don't want to reach the output buffer.  This is to
    // avoid conditional branches in the code below. Must be thread-local to avoid
    // the compiler using branches.
    static thread_local int8_t garbage;

    // Encode the remaining bytes in big-endian order, directing unneeded bytes into a garbage array.
    // This avoids conditional branches.

    value = std::rotl(value, (8 - extra_bytes_size) * 8);

#pragma GCC unroll 8

    for (int i = 0; i < 8; ++i) {
        auto* dest = __builtin_unpredictable(extra_bytes_size > 0) ? out : &garbage;
        value = std::rotl(value, 8);
        *dest = uint8_t(value);
        ++out;
        --extra_bytes_size;
    }

    return size;
}

vint_size_type unsigned_vint::serialized_size(uint64_t value) noexcept {
    // No need for the overhead of checking that all bits are zero.
    //
    // A signed quantity, to allow the case of `magnitude == 0` to result in a value of 9 below.
    const auto magnitude = static_cast<int64_t>(count_leading_zeros(value | uint64_t(1)));

    return vint_size_type(9) - vint_size_type((magnitude - 1) / 7);
}

uint64_t unsigned_vint::deserialize(bytes_view v) {
    auto src = v.data();
    const int8_t first_byte = *src;

    const auto extra_bytes_size = count_extra_bytes(first_byte);

    // Extract the bits not used for counting bytes.
    auto result = uint64_t(first_byte) & first_byte_value_mask(extra_bytes_size);

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    uint64_t value;
    // If we can overread do that. It is cheaper to have a single 64-bit read and
    // then mask out the unneeded part than to do 8x 1 byte reads.
    bool can_overread = (reinterpret_cast<uintptr_t>(src+1) ^ reinterpret_cast<uintptr_t>(src+1+sizeof(uint64_t))) < 4096;
#ifdef SEASTAR_ASAN_ENABLED
    can_overread = false;
#endif

    if (can_overread) [[likely]] {
        std::copy_n(src + 1, sizeof(uint64_t), reinterpret_cast<int8_t*>(&value));
    } else {
        value = 0;
        std::copy_n(src + 1, extra_bytes_size, reinterpret_cast<int8_t*>(&value));
    }
    auto shift = 64 - (extra_bytes_size * 8);
    // Can't shift by 64, so shift twice by half
    value = be_to_cpu(value);
    value >>= shift / 2;
    value >>= shift / 2;
    result <<= (extra_bytes_size * 8) % 64;
    result |= value;
#else
    for (vint_size_type index = 0; index < extra_bytes_size; ++index) {
        result <<= 8;
        result |= (uint64_t(v[index + 1]) & uint64_t(0xff));
    }
#endif
    return result;
}
