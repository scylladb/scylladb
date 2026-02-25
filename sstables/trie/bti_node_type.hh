/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <cstdint>
#include <iterator>
#include <span>
#include <seastar/core/byteorder.hh>

using const_bytes = std::span<const std::byte>;

namespace sstables::trie {

// Each node type has a 4-bit identifier which is used in the on-disk format.
// The order of this enum mustn't be modified.
enum node_type {
    PAYLOAD_ONLY,
    SINGLE_NOPAYLOAD_4,
    SINGLE_8,
    SINGLE_NOPAYLOAD_12,
    SINGLE_16,
    SPARSE_8,
    SPARSE_12,
    SPARSE_16,
    SPARSE_24,
    SPARSE_40,
    DENSE_12,
    DENSE_16,
    DENSE_24,
    DENSE_32,
    DENSE_40,
    LONG_DENSE,
    NODE_TYPE_COUNT, // Not a valid value.
};

// For each node type, contains the number of bits used per child offset.
constexpr static const uint8_t bits_per_pointer_arr[] = {
    0,
    4,
    8,
    12,
    16,
    8,
    12,
    16,
    24,
    40,
    12,
    16,
    24,
    32,
    40,
    64,
};
static_assert(std::size(bits_per_pointer_arr) == NODE_TYPE_COUNT);

// Given an array of offsets, each of size bits_per_pointer, read the one with index `idx`.
//
// Assumes that bits_per_pointer is divisible by 8 or equal to 12.
//
// Ordering note: an array of 12-bit offsets [0x123, 0x456, 0x789, 0xabc] is
// represented as the byte array 123456789abc.
//
// We want this to be always inlined so that bits_per_pointer is substituted with a constant,
// and this compiles to a simple load, not to a full-fledged memcpy.
[[gnu::always_inline]]
inline uint64_t read_offset(const_bytes sp, int idx, int bits_per_pointer) {
    if (bits_per_pointer % 8 == 0) {
        auto n = bits_per_pointer / 8;
        uint64_t be = 0;
        memcpy((char*)&be + 8 - n, (const char*)sp.data() + n * idx, n);
        return seastar::be_to_cpu(be);
    } else {
        if (idx % 2 == 0) {
            return seastar::read_be<uint16_t>((const char*)sp.data() + 3 * (idx / 2)) >> 4;
        } else {
            return seastar::read_be<uint16_t>((const char*)sp.data() + 3 * (idx / 2) + 1) & 0xfff;
        }
    }
}

} // namespace sstables::trie
