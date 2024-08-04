#pragma once

/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

// This class is the parts of java.util.UUID that we need

#include <stdint.h>
#include <cassert>
#include <array>
#include <iosfwd>
#include <compare>

#include <seastar/core/sstring.hh>
#include <seastar/core/print.hh>
#include <seastar/net/byteorder.hh>
#include "bytes.hh"
#include "utils/assert.hh"
#include "utils/hashing.hh"
#include "utils/serialization.hh"

namespace utils {

class UUID {
private:
    int64_t most_sig_bits;
    int64_t least_sig_bits;
public:
    constexpr UUID() noexcept : most_sig_bits(0), least_sig_bits(0) {}
    constexpr UUID(int64_t most_sig_bits, int64_t least_sig_bits) noexcept
        : most_sig_bits(most_sig_bits), least_sig_bits(least_sig_bits) {}

    // May throw marshal_exception is failed to parse uuid string.
    explicit UUID(const sstring& uuid_string) : UUID(sstring_view(uuid_string)) { }
    explicit UUID(const char * s) : UUID(sstring_view(s)) {}
    explicit UUID(sstring_view uuid_string);

    int64_t get_most_significant_bits() const noexcept {
        return most_sig_bits;
    }
    int64_t get_least_significant_bits() const noexcept {
        return least_sig_bits;
    }
    int version() const noexcept {
        return (most_sig_bits >> 12) & 0xf;
    }

    bool is_timestamp() const noexcept {
        return version() == 1;
    }

    int64_t timestamp() const noexcept {
        //if (version() != 1) {
        //     throw new UnsupportedOperationException("Not a time-based UUID");
        //}
        SCYLLA_ASSERT(is_timestamp());

        return ((most_sig_bits & 0xFFF) << 48) |
               (((most_sig_bits >> 16) & 0xFFFF) << 32) |
               (((uint64_t)most_sig_bits) >> 32);

    }

    friend ::fmt::formatter<UUID>;

    friend std::ostream& operator<<(std::ostream& out, const UUID& uuid);

    bool operator==(const UUID& v) const noexcept = default;

    // Please note that this comparator does not preserve timeuuid
    // monotonicity. For this reason you should avoid using it for
    // UUIDs that could store timeuuids, otherwise bugs like
    // https://github.com/scylladb/scylla/issues/7729 may happen.
    //
    // For comparing timeuuids, you can use `timeuuid_tri_compare`
    // functions from this file.
    std::strong_ordering operator<=>(const UUID& v) const noexcept {
        auto cmp = uint64_t(most_sig_bits) <=> uint64_t(v.most_sig_bits);
        if (cmp != 0) {
            return cmp;
        }
        return uint64_t(least_sig_bits) <=> uint64_t(v.least_sig_bits);
    }

    // nibble set to a non-zero value
    bool is_null() const noexcept {
        return !most_sig_bits && !least_sig_bits;
    }

    explicit operator bool() const noexcept {
        return !is_null();
    }

    bytes serialize() const {
        bytes b(bytes::initialized_later(), serialized_size());
        auto i = b.begin();
        serialize(i);
        return b;
    }

    constexpr static size_t serialized_size() noexcept {
        return 16;
    }

    template <typename CharOutputIterator>
    void serialize(CharOutputIterator& out) const {
        serialize_int64(out, most_sig_bits);
        serialize_int64(out, least_sig_bits);
    }
};

// Convert the uuid to a uint32_t using xor.
// It is useful to get a uint32_t number from the uuid.
uint32_t uuid_xor_to_uint32(const UUID& uuid);

inline constexpr UUID null_uuid() noexcept {
    return UUID();
}

UUID make_random_uuid() noexcept;

// Read 8 most significant bytes of timeuuid from serialized bytes
inline uint64_t timeuuid_read_msb(const int8_t *b) noexcept {
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

inline uint64_t uuid_read_lsb(const int8_t *b) noexcept {
    auto u64 = [](uint8_t i) -> uint64_t { return i; };
    return u64(b[8]) << 56 | u64(b[9]) << 48 |
           u64(b[10]) << 40 | u64(b[11]) << 32 |
           u64(b[12]) << 24 | u64(b[13]) << 16 |
           u64(b[14]) << 8  | u64(b[15]);
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
inline std::strong_ordering timeuuid_tri_compare(const int8_t* o1, const int8_t* o2) noexcept {
    auto timeuuid_read_lsb = [](const int8_t* o) -> uint64_t {
        return uuid_read_lsb(o) ^ 0x8080808080808080;
    };
    auto res = timeuuid_read_msb(o1) <=> timeuuid_read_msb(o2);
    if (res == 0) {
        res = timeuuid_read_lsb(o1) <=> timeuuid_read_lsb(o2);
    }
    return res;
}

inline std::strong_ordering timeuuid_tri_compare(bytes_view o1, bytes_view o2) noexcept {
    return timeuuid_tri_compare(o1.begin(), o2.begin());
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
    return timeuuid_tri_compare(buf1.begin(), buf2.begin());
}

// Compare two values of UUID type, if they happen to be
// both of Version 1 (timeuuids).
//
// This function uses memory order for least significant bits,
// which is both faster and monotonic, so should be preferred
// to @timeuuid_tri_compare() used for all new features.
//
inline std::strong_ordering uuid_tri_compare_timeuuid(bytes_view o1, bytes_view o2) noexcept {
    auto res = timeuuid_read_msb(o1.begin()) <=> timeuuid_read_msb(o2.begin());
    if (res == 0) {
        res = uuid_read_lsb(o1.begin()) <=> uuid_read_lsb(o2.begin());
    }
    return res;
}

template<typename Tag>
struct tagged_uuid {
    utils::UUID id;
    std::strong_ordering operator<=>(const tagged_uuid&) const noexcept = default;
    explicit operator bool() const noexcept {
        // The default constructor sets the id to nil, which is
        // guaranteed to not match any valid id.
        return bool(id);
    }
    static tagged_uuid create_random_id() noexcept { return tagged_uuid{utils::make_random_uuid()}; }
    static constexpr tagged_uuid create_null_id() noexcept { return tagged_uuid{}; }
    explicit constexpr tagged_uuid(const utils::UUID& uuid) noexcept : id(uuid) {}
    constexpr tagged_uuid() = default;

    const utils::UUID& uuid() const noexcept {
        return id;
    }

    sstring to_sstring() const {
        return fmt::to_string(id);
    }
};
} // namespace utils

template<>
struct appending_hash<utils::UUID> {
    template<typename Hasher>
    void operator()(Hasher& h, const utils::UUID& id) const noexcept {
        feed_hash(h, id.get_most_significant_bits());
        feed_hash(h, id.get_least_significant_bits());
    }
};

template<typename Tag>
struct appending_hash<utils::tagged_uuid<Tag>> {
    template<typename Hasher>
    void operator()(Hasher& h, const utils::tagged_uuid<Tag>& id) const noexcept {
        appending_hash<utils::UUID>{}(h, id.uuid());
    }
};

namespace std {
template<>
struct hash<utils::UUID> {
    size_t operator()(const utils::UUID& id) const noexcept {
        auto hilo = id.get_most_significant_bits()
                ^ id.get_least_significant_bits();
        return size_t((hilo >> 32) ^ hilo);
    }
};

template<typename Tag>
struct hash<utils::tagged_uuid<Tag>> {
    size_t operator()(const utils::tagged_uuid<Tag>& id) const noexcept {
        return hash<utils::UUID>()(id.id);
    }
};

template<typename Tag>
std::ostream& operator<<(std::ostream& os, const utils::tagged_uuid<Tag>& id) {
    return os << id.id;
}
} // namespace std

template <>
struct fmt::formatter<utils::UUID> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const utils::UUID& id, FormatContext& ctx) const {
        // This matches Java's UUID.toString() actual implementation. Note that
        // that method's documentation suggest something completely different!
        return fmt::format_to(ctx.out(),
                "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
                ((uint64_t)id.most_sig_bits >> 32),
                ((uint64_t)id.most_sig_bits >> 16 & 0xffff),
                ((uint64_t)id.most_sig_bits & 0xffff),
                ((uint64_t)id.least_sig_bits >> 48 & 0xffff),
                ((uint64_t)id.least_sig_bits & 0xffffffffffffLL));
    }
};

template <typename Tag>
struct fmt::formatter<utils::tagged_uuid<Tag>> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const utils::tagged_uuid<Tag>& id, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", id.id);
    }
};
