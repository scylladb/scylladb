#pragma once

/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

// This class is the parts of java.util.UUID that we need

#include <stdint.h>
#include <cassert>
#include <array>
#include <iosfwd>

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
    constexpr UUID() : most_sig_bits(0), least_sig_bits(0) {}
    constexpr UUID(int64_t most_sig_bits, int64_t least_sig_bits)
        : most_sig_bits(most_sig_bits), least_sig_bits(least_sig_bits) {}
    explicit UUID(const sstring& uuid_string) : UUID(sstring_view(uuid_string)) { }
    explicit UUID(const char * s) : UUID(sstring_view(s)) {}
    explicit UUID(sstring_view uuid_string);

    int64_t get_most_significant_bits() const {
        return most_sig_bits;
    }
    int64_t get_least_significant_bits() const {
        return least_sig_bits;
    }
    int version() const {
        return (most_sig_bits >> 12) & 0xf;
    }

    bool is_timestamp() const {
        return version() == 1;
    }

    int64_t timestamp() const {
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

    bool operator==(const UUID& v) const {
        return most_sig_bits == v.most_sig_bits
                && least_sig_bits == v.least_sig_bits
                ;
    }
    bool operator!=(const UUID& v) const {
        return !(*this == v);
    }

    // Please note that this comparator does not preserve timeuuid
    // monotonicity. For this reason you should avoid using it for
    // UUIDs that could store timeuuids, otherwise bugs like
    // https://github.com/scylladb/scylla/issues/7729 may happen.
    bool operator<(const UUID& v) const {
         if (most_sig_bits != v.most_sig_bits) {
             return uint64_t(most_sig_bits) < uint64_t(v.most_sig_bits);
         } else {
             return uint64_t(least_sig_bits) < uint64_t(v.least_sig_bits);
         }
    }

    bool operator>(const UUID& v) const {
        return v < *this;
    }

    bool operator<=(const UUID& v) const {
        return !(*this > v);
    }

    bool operator>=(const UUID& v) const {
        return !(*this < v);
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

UUID make_random_uuid();

inline int uint64_t_tri_compare(uint64_t a, uint64_t b) {
    return a < b ? -1 : a > b;
}

// Read 8 most significant bytes of timeuuid from serialized bytes
inline uint64_t timeuuid_read_msb(const int8_t *b) {
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

inline uint64_t uuid_read_lsb(const int8_t *b) {
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
inline int timeuuid_tri_compare(bytes_view o1, bytes_view o2) {
    auto timeuuid_read_lsb = [](bytes_view o) -> uint64_t {
        return uuid_read_lsb(o.begin()) ^ 0x8080808080808080;
    };
    int res = uint64_t_tri_compare(timeuuid_read_msb(o1.begin()), timeuuid_read_msb(o2.begin()));
    if (res == 0) {
        res = uint64_t_tri_compare(timeuuid_read_lsb(o1), timeuuid_read_lsb(o2));
    }
    return res;
}

// Compare two values of UUID type, if they happen to be
// both of Version 1 (timeuuids).
//
// This function uses memory order for least significant bits,
// which is both faster and monotonic, so should be preferred
// to @timeuuid_tri_compare() used for all new features.
//
inline int uuid_tri_compare_timeuuid(bytes_view o1, bytes_view o2) {
    int res = uint64_t_tri_compare(timeuuid_read_msb(o1.begin()), timeuuid_read_msb(o2.begin()));
    if (res == 0) {
        res = uint64_t_tri_compare(uuid_read_lsb(o1.begin()), uuid_read_lsb(o2.begin()));
    }
    return res;
}

}

template<>
struct appending_hash<utils::UUID> {
    template<typename Hasher>
    void operator()(Hasher& h, const utils::UUID& id) const {
        feed_hash(h, id.get_most_significant_bits());
        feed_hash(h, id.get_least_significant_bits());
    }
};

namespace std {
template<>
struct hash<utils::UUID> {
    size_t operator()(const utils::UUID& id) const {
        auto hilo = id.get_most_significant_bits()
                ^ id.get_least_significant_bits();
        return size_t((hilo >> 32) ^ hilo);
    }
};
}
