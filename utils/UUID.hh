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

    bool is_timestamp_v1() const noexcept {
        return version() == 1;
    }

    bool is_timestamp_v7() const noexcept {
        return version() == 7;
    }

    bool is_timestamp() const noexcept {
        return is_timestamp_v1() || is_timestamp_v7();
    }

    // Return the uuid timestamp in decimicroseconds.
    int64_t timestamp() const noexcept {
        switch (version()) {
        case 1:
            return ((most_sig_bits & 0xFFF) << 48) |
                (((most_sig_bits >> 16) & 0xFFFF) << 32) |
                (((uint64_t)most_sig_bits) >> 32);
        case 7:
            // The UUIDv7 msb format as defined in https://datatracker.ietf.org/doc/html/rfc9562#name-uuid-version-7
            // 48 bits - milliseconds since unix epoch of 1970-01-01 GMT (unsigned)
            //  4 bits - version (7)
            // 12 bits - sub-milliseconds (in ms/4096 units, unsigned)
            return 10000 * ((uint64_t)most_sig_bits >> 16) +
                  (10000 * (most_sig_bits & 0xfff)) / 0x1000;
        default:
            not_a_time_uuid();
        }
    }

    // Return the uuid timestamp in milliseconds.
    int64_t millis_timestamp() const noexcept {
        switch (version()) {
        case 1:
            return timestamp() / 10000;
        case 7:
            return (uint64_t)most_sig_bits >> 16;
        default:
            not_a_time_uuid();
        }
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

private:
    [[noreturn]] void not_a_time_uuid() const;
};

// Convert the uuid to a uint32_t using xor.
// It is useful to get a uint32_t number from the uuid.
uint32_t uuid_xor_to_uint32(const UUID& uuid);

inline constexpr UUID null_uuid() noexcept {
    return UUID();
}

UUID make_random_uuid() noexcept;

[[noreturn]] void not_a_time_uuid(UUID uuid);
[[noreturn]] void not_a_time_uuid(bytes_view);

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
