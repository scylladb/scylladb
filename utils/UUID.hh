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
#include "hashing.hh"
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

    bool is_timestamp() const noexcept {
        return is_timestamp_v1();
    }

    int64_t timestamp() const noexcept {
        //if (version() != 1) {
        //     throw new UnsupportedOperationException("Not a time-based UUID");
        //}
        assert(is_timestamp());

        return ((most_sig_bits & 0xFFF) << 48) |
               (((most_sig_bits >> 16) & 0xFFFF) << 32) |
               (((uint64_t)most_sig_bits) >> 32);

    }

    // This matches Java's UUID.toString() actual implementation. Note that
    // that method's documentation suggest something completely different!
    sstring to_sstring() const {
        return format("{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
                ((uint64_t)most_sig_bits >> 32),
                ((uint64_t)most_sig_bits >> 16 & 0xffff),
                ((uint64_t)most_sig_bits & 0xffff),
                ((uint64_t)least_sig_bits >> 48 & 0xffff),
                ((uint64_t)least_sig_bits & 0xffffffffffffLL));
    }

    friend std::ostream& operator<<(std::ostream& out, const UUID& uuid);

    bool operator==(const UUID& v) const noexcept {
        return most_sig_bits == v.most_sig_bits
                && least_sig_bits == v.least_sig_bits
                ;
    }
    bool operator!=(const UUID& v) const noexcept {
        return !(*this == v);
    }

    // Please note that this comparator does not preserve timeuuid
    // monotonicity. For this reason you should avoid using it for
    // UUIDs that could store timeuuids, otherwise bugs like
    // https://github.com/scylladb/scylla/issues/7729 may happen.
    bool operator<(const UUID& v) const noexcept {
         if (most_sig_bits != v.most_sig_bits) {
             return uint64_t(most_sig_bits) < uint64_t(v.most_sig_bits);
         } else {
             return uint64_t(least_sig_bits) < uint64_t(v.least_sig_bits);
         }
    }

    bool operator>(const UUID& v) const noexcept {
        return v < *this;
    }

    bool operator<=(const UUID& v) const noexcept {
        return !(*this > v);
    }

    bool operator>=(const UUID& v) const noexcept {
        return !(*this < v);
    }

    // Valid (non-null) UUIDs always have their version
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

    static size_t serialized_size() noexcept {
        return 16;
    }

    template <typename CharOutputIterator>
    void serialize(CharOutputIterator& out) const {
        serialize_int64(out, most_sig_bits);
        serialize_int64(out, least_sig_bits);
    }
};

inline UUID null_uuid() noexcept {
    return UUID();
}

UUID make_random_uuid() noexcept;

template<typename Tag>
struct tagged_uuid {
    utils::UUID id;
    bool operator==(const tagged_uuid& o) const noexcept {
        return id == o.id;
    }
    bool operator<(const tagged_uuid& o) const noexcept {
        return id < o.id;
    }
    bool operator>(const tagged_uuid& o) const noexcept {
        return id > o.id;
    }
    bool operator<=(const tagged_uuid& o) const noexcept {
        return id <= o.id;
    }
    bool operator>=(const tagged_uuid& o) const noexcept {
        return id >= o.id;
    }
    explicit operator bool() const noexcept {
        // The default constructor sets the id to nil, which is
        // guaranteed to not match any valid id.
        return bool(id);
    }
    static tagged_uuid create_random_id() noexcept { return tagged_uuid{utils::make_random_uuid()}; }
    static tagged_uuid create_null_id() noexcept { return tagged_uuid{utils::null_uuid()}; }
    explicit tagged_uuid(const utils::UUID& uuid) noexcept : id(uuid) {}
    tagged_uuid() = default;

    const utils::UUID& uuid() const noexcept {
        return id;
    }

    sstring to_sstring() const {
        return id.to_sstring();
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
