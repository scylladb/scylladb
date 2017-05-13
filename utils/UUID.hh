#pragma once

/*
 * Copyright (C) 2015 ScyllaDB
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

#include "core/sstring.hh"
#include "core/print.hh"
#include "net/byteorder.hh"
#include "bytes.hh"
#include "hashing.hh"
#include "utils/serialization.hh"

namespace utils {

class UUID {
private:
    int64_t most_sig_bits;
    int64_t least_sig_bits;
public:
    UUID() : most_sig_bits(0), least_sig_bits(0) {}
    UUID(int64_t most_sig_bits, int64_t least_sig_bits)
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

    int64_t timestamp() const {
        //if (version() != 1) {
        //     throw new UnsupportedOperationException("Not a time-based UUID");
        //}
        assert(version() == 1);

        return ((most_sig_bits & 0xFFF) << 48) |
               (((most_sig_bits >> 16) & 0xFFFF) << 32) |
               (((uint64_t)most_sig_bits) >> 32);

    }

    // This matches Java's UUID.toString() actual implementation. Note that
    // that method's documentation suggest something completely different!
    sstring to_sstring() const {
        return sprint("%08x-%04x-%04x-%04x-%012x",
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
