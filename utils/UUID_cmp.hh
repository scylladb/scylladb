#pragma once

/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <stdint.h>

#include "utils/UUID.hh"
#include "bytes.hh"

#include "utils/UUID_gen.hh"

namespace utils {

class timeuuid_cmp {
    bytes_view _o1;
    bytes_view _o2;

private:
    static inline std::strong_ordering uint64_t_tri_compare(uint64_t a, uint64_t b) noexcept;
    static inline uint64_t timeuuid_v1_read_msb(const int8_t* b) noexcept;
    static inline uint64_t uuid_read_lsb(const int8_t* b) noexcept;
    std::strong_ordering unix_timestamp_tri_compare() noexcept;
    std::strong_ordering tri_compare_v1() noexcept;
    std::strong_ordering tri_compare_v7() noexcept;

public:
    timeuuid_cmp(bytes_view o1, bytes_view o2) noexcept : _o1(o1), _o2(o2) {
#ifndef SCYLLA_BUILD_MODE_RELEASE
        assert(_o1.size() == UUID::serialized_size());
        assert(_o2.size() == UUID::serialized_size());
#endif
    }

    std::strong_ordering tri_compare() noexcept;
    std::strong_ordering uuid_tri_compare() noexcept;
};

inline std::strong_ordering timeuuid_cmp::uint64_t_tri_compare(uint64_t a, uint64_t b) noexcept {
    return a <=> b;
}

inline std::strong_ordering int64_t_tri_compare(int64_t a, int64_t b) noexcept {
    return a <=> b;
}

inline unsigned uuid_read_version(const int8_t* b) noexcept {
    // cast to unsigned so we won't have to mask the 4 most-significant bits
    return static_cast<uint8_t>(b[6]) >> 4;
}

// Read 8 most significant bytes of timeuuid from serialized bytes
inline uint64_t timeuuid_cmp::timeuuid_v1_read_msb(const int8_t* b) noexcept {
    // cast to unsigned to avoid sign-compliment during shift.
    auto u64 = [](uint8_t i) -> uint64_t { return i; };
    // Scylla and Cassandra use a standard UUID memory layout for MSB:
    // 4 bytes    2 bytes    2 bytes
    // time_low - time_mid - time_hi_and_version
    //
    // The storage format uses network byte order.
    // Reorder bytes to allow for an integer compare.
    return u64(b[6] & 0xf) << 56 | u64(b[7]) << 48 |
           u64(b[4]) << 40 | u64(b[5]) << 32 |
           u64(b[0]) << 24 | u64(b[1]) << 16 |
           u64(b[2]) << 8  | u64(b[3]);
}

inline uint64_t timeuuid_cmp::uuid_read_lsb(const int8_t* b) noexcept {
    auto u64 = [](uint8_t i) -> uint64_t { return i; };
    return u64(b[8]) << 56 | u64(b[9]) << 48 |
           u64(b[10]) << 40 | u64(b[11]) << 32 |
           u64(b[12]) << 24 | u64(b[13]) << 16 |
           u64(b[14]) << 8  | u64(b[15]);
}

inline std::strong_ordering timeuuid_cmp::unix_timestamp_tri_compare() noexcept {
    auto u1 = UUID_gen::get_UUID_from_msb(_o1.begin());
    auto t1 = UUID_gen::normalized_timestamp(u1).count();
    auto u2 = UUID_gen::get_UUID_from_msb(_o2.begin());
    auto t2 = UUID_gen::normalized_timestamp(u2).count();
    auto res = int64_t_tri_compare(t1, t2);
    if (res != 0) {
        return res;
    }
    // Compare the random least-significant bytes as unsigned bytes
    auto lsb1 = bytes_view(_o1.begin() + 8, 8);
    auto lsb2 = bytes_view(_o2.begin() + 8, 8);
    return compare_unsigned(lsb1, lsb2);
}

// Compare two values of timeuuid type.
// Cassandra legacy requires:
// - using signed compare for least significant bits.
// - masking off UUID version during compare, to
// treat possible non-version-1 UUID the same way as UUID.
//
// To avoid breaking ordering in existing sstables, Scylla preserves
// Cassandra compare order.
//
inline std::strong_ordering timeuuid_cmp::tri_compare_v1() noexcept {
    const int8_t* o1 = _o1.begin();
    const int8_t* o2 = _o2.begin();
    auto timeuuid_read_lsb = [](const int8_t* o) -> uint64_t {
        return uuid_read_lsb(o) ^ 0x8080808080808080;
    };
    auto res = uint64_t_tri_compare(timeuuid_v1_read_msb(o1), timeuuid_v1_read_msb(o2));
    if (res == 0) {
        res = timeuuid_read_lsb(o1) <=> timeuuid_read_lsb(o2);
    }
    return res;
}

inline std::strong_ordering timeuuid_cmp::tri_compare_v7() noexcept {
    return compare_unsigned(_o1, _o2);
}

// Compare two values of timeuuid type.
//
// For mixed versions, the function first compares the derived timestamp
// and then the least significant bits.
inline std::strong_ordering timeuuid_cmp::tri_compare() noexcept {
    const int8_t* o1 = _o1.begin();
    const int8_t* o2 = _o2.begin();
    auto v1 = uuid_read_version(o1);
    auto v2 = uuid_read_version(o2);
    // For backward compatibility reasons,
    // only timeuuid v7 is special handled.
    switch (v1 + v2) {
    case 1:  // 1 vs. 0
    case 7:  // 7 vs. 0
        return v1 <=> v2;
    case 8: // 7 vs. 1
        if (auto res = unix_timestamp_tri_compare(); res != 0) {
            return res;
        }
        return v1 <=> v2;
    case 14: // 7 vs. 7
        return tri_compare_v7();
    }
    return tri_compare_v1();
}

inline std::strong_ordering timeuuid_tri_compare(const UUID& u1, const UUID& u2) noexcept {
    std::array<int8_t, UUID::serialized_size()> buf1;
    {
        auto i = buf1.begin();
        u1.serialize(i);
    }
    std::array<int8_t, UUID::serialized_size()> buf2;
    {
        auto i = buf2.begin();
        u2.serialize(i);
    }
    return timeuuid_cmp(bytes_view(buf1.begin(), buf1.size()), bytes_view(buf2.begin(), buf2.size())).tri_compare();
}

// Compare two values of UUID type if they happen to be
// both of Version 1 or 7 (timeuuids).
//
// This function uses memory order for least significant bits,
// which is both faster and monotonic, so should be preferred
// to @timeuuid_tri_compare() used for all new features.
inline std::strong_ordering timeuuid_cmp::uuid_tri_compare() noexcept {
    const int8_t* o1 = _o1.begin();
    const int8_t* o2 = _o2.begin();
    auto v1 = uuid_read_version(o1);
    auto v2 = uuid_read_version(o2);
    // For backward compatibility reasons,
    // only timeuuid v7 is special handled.
    switch (v1 + v2) {
    case 1:  // 1 vs. 0
    case 7:  // 7 vs. 0
        return v1 <=> v2;
    case 8: // 7 vs. 1
        if (auto res = unix_timestamp_tri_compare(); res != 0) {
            return res;
        }
        return v1 <=> v2;
    case 14: // 7 vs. 7
        return tri_compare_v7();
    }
    auto res = uint64_t_tri_compare(timeuuid_v1_read_msb(o1), timeuuid_v1_read_msb(o2));
    if (res == 0) {
        res = uuid_read_lsb(o1) <=> uuid_read_lsb(o2);
    }
    return res;
}

} // namespace utils
