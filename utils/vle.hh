/*
 * Copyright (C) 2021 ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/bitops.hh>
#include <seastar/core/byteorder.hh>

namespace utils {

/*
 * The express encoder below is optimized to encode a value
 * that may only have non-zeroes in its first 12 bits
 */
static constexpr size_t uleb64_express_bits = 12;
static constexpr uint32_t uleb64_express_supreme = 1 << uleb64_express_bits;

// Returns the number of bytes needed to encode the value
// The value cannot be 0 (not checked)
static inline size_t uleb64_encoded_size(uint32_t val) noexcept {
    return seastar::log2floor(val) / 6 + 1;
}

template <typename Poison, typename Unpoison>
requires std::is_invocable<Poison, const char*, size_t>::value && std::is_invocable<Unpoison, const char*, size_t>::value
static inline void uleb64_encode(char*& pos, uint32_t val, Poison&& poison, Unpoison&& unpoison) noexcept {
    uint64_t b = 64;
    auto start = pos;
    do {
        b |= val & 63;
        val >>= 6;
        if (!val) {
            b |= 128;
        }
        unpoison(pos, 1);
        *pos++ = b;
        b = 0;
    } while (val);
    poison(start, pos - start);
}

template <typename Poison, typename Unpoison>
requires std::is_invocable<Poison, const char*, size_t>::value && std::is_invocable<Unpoison, const char*, size_t>::value
static inline void uleb64_encode(char*& pos, uint32_t val, size_t encoded_size, Poison&& poison, Unpoison&& unpoison) noexcept {
    uint64_t b = 64;
    auto start = pos;
    unpoison(start, encoded_size);
    do {
        b |= val & 63;
        val >>= 6;
        if (!--encoded_size) {
            b |= 128;
        }
        *pos++ = b;
        b = 0;
    } while (encoded_size);
    poison(start, pos - start);
}

#if !defined(SEASTAR_ASAN_ENABLED)
static inline void uleb64_express_encode_impl(char*& pos, uint64_t val, size_t size) noexcept {
    static_assert(uleb64_express_bits == 12);

    if (size > sizeof(uint64_t)) {
        static uint64_t zero = 0;
        std::copy_n(reinterpret_cast<char*>(&zero), sizeof(zero), pos + size - sizeof(uint64_t));
    }
    seastar::write_le(pos, uint64_t(((val & 0xfc0) << 2) | ((val & 0x3f) | 64)));
    pos += size;
    pos[-1] |= 0x80;
}

template <typename Poison, typename Unpoison>
requires std::is_invocable<Poison, const char*, size_t>::value && std::is_invocable<Unpoison, const char*, size_t>::value
static inline void uleb64_express_encode(char*& pos, uint32_t val, size_t encoded_size, size_t gap, Poison&& poison, Unpoison&& unpoison) noexcept {
    if (encoded_size + gap > sizeof(uint64_t)) {
        uleb64_express_encode_impl(pos, val, encoded_size);
    } else {
        uleb64_encode(pos, val, encoded_size, poison, unpoison);
    }
}
#else
template <typename Poison, typename Unpoison>
requires std::is_invocable<Poison, const char*, size_t>::value && std::is_invocable<Unpoison, const char*, size_t>::value
static inline void uleb64_express_encode(char*& pos, uint32_t val, size_t encoded_size, size_t gap, Poison&& poison, Unpoison&& unpoison) noexcept {
    uleb64_encode(pos, val, encoded_size, poison, unpoison);
}
#endif

template <typename Poison, typename Unpoison>
requires std::is_invocable<Poison, const char*, size_t>::value && std::is_invocable<Unpoison, const char*, size_t>::value
static inline uint32_t uleb64_decode_forwards(const char*& pos, Poison&& poison, Unpoison&& unpoison) noexcept {
    uint32_t n = 0;
    unsigned shift = 0;
    auto p = pos; // avoid aliasing; p++ doesn't touch memory
    uint8_t b;
    do {
        unpoison(p, 1);
        b = *p++;
        if (shift < 32) {
            // non-canonical encoding can cause large shift; undefined in C++
            n |= uint32_t(b & 63) << shift;
        }
        shift += 6;
    } while ((b & 128) == 0);
    poison(pos, p - pos);
    pos = p;
    return n;
}

template <typename Poison, typename Unpoison>
requires std::is_invocable<Poison, const char*, size_t>::value && std::is_invocable<Unpoison, const char*, size_t>::value
static inline uint32_t uleb64_decode_bacwards(const char*& pos, Poison&& poison, Unpoison&& unpoison) noexcept {
    uint32_t n = 0;
    uint8_t b;
    auto p = pos; // avoid aliasing; --p doesn't touch memory
    do {
        --p;
        unpoison(p, 1);
        b = *p;
        n = (n << 6) | (b & 63);
    } while ((b & 64) == 0);
    poison(p, pos - p);
    pos = p;
    return n;
}

} // namespace utils
