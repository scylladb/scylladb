/*
 * Copyright (C) 2018 ScyllaDB
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

#pragma once

#include <limits>
#include <type_traits>
#include <seastar/core/future.hh>
#include "gc_clock.hh"
#include "timestamp.hh"
#include "sstables/types.hh"
#include "sstables/exceptions.hh"

namespace sstables {

class random_access_reader;

// Utilities for reading integral values in variable-length format
// See vint-serialization.hh for more details
future<uint64_t> read_unsigned_vint(random_access_reader& in);
future<int64_t> read_signed_vint(random_access_reader& in);

template <typename T>
typename std::enable_if_t<!std::is_integral_v<T>>
read_vint(random_access_reader& in, T& t) = delete;

template <typename T>
inline future<> read_vint(random_access_reader& in, T& value) {
    static_assert(std::is_integral_v<T>, "Non-integral values can't be read using read_vint");
    if constexpr(std::is_unsigned_v<T>) {
        return read_unsigned_vint(in).then([&value] (uint64_t res) {
            value = res;
        });
    } else {
        return read_signed_vint(in).then([&value] (int64_t res) {
            value = res;
        });
    }
}

inline api::timestamp_type parse_timestamp(uint64_t value) {
    if (value > api::max_timestamp) {
        throw malformed_sstable_exception("Too big timestamp: " + value);
    }
    return static_cast<api::timestamp_type >(value);
}

inline api::timestamp_type parse_timestamp(const serialization_header& header,
                                           uint64_t delta) {
    return parse_timestamp(header.min_timestamp.value + delta);
}

inline gc_clock::duration parse_ttl(uint32_t value) {
    if (value > std::numeric_limits<gc_clock::duration::rep>::max()) {
        throw malformed_sstable_exception("Too big ttl: " + value);
    }
    return gc_clock::duration(value);
}

inline gc_clock::duration parse_ttl(const serialization_header& header,
                                    uint32_t delta) {
    return parse_ttl(header.min_ttl.value + delta);
}

inline gc_clock::time_point parse_expiry(uint32_t value) {
    if (value > std::numeric_limits<gc_clock::duration::rep>::max()) {
        throw malformed_sstable_exception("Too big expiry: " + value);
    }
    return gc_clock::time_point(gc_clock::duration(value));
}

inline gc_clock::time_point parse_expiry(const serialization_header& header,
                                   uint32_t delta) {
    return parse_expiry(header.min_local_deletion_time.value + delta);
}

};   // namespace sstables
