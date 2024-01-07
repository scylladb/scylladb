/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <type_traits>
#include <concepts>
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

template <std::integral T>
inline future<> read_vint(random_access_reader& in, T& value) {
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

inline api::timestamp_type parse_timestamp(const serialization_header& header,
                                           uint64_t delta) {
    return static_cast<api::timestamp_type>(header.get_min_timestamp() + delta);
}

inline gc_clock::duration parse_ttl(int64_t value) {
    if (!is_expired_liveness_ttl(value)) {
        if (value < 0) {
            throw malformed_sstable_exception(format("Negative ttl: {}", value));
        }
        if (value > max_ttl.count()) {
            throw malformed_sstable_exception(format("Too big ttl: {}", value));
        }
    }
    return gc_clock::duration(value);
}

inline gc_clock::duration parse_ttl(const serialization_header& header,
                                    uint64_t delta) {
    // sign-extend min_ttl back to 64 bits and
    // add the delta using unsigned arithmetic
    // to prevent signed integer overflow
    uint64_t min_ttl = static_cast<uint64_t>(header.get_min_ttl());
    return parse_ttl(static_cast<int64_t>(min_ttl + delta));
}

inline gc_clock::time_point parse_expiry(int64_t value) {
    return gc_clock::time_point(gc_clock::duration(value));
}

inline gc_clock::time_point parse_expiry(const serialization_header& header,
                                   uint64_t delta) {
    // sign-extend min_local_deletion_time back to 64 bits and
    // add the delta using unsigned arithmetic
    // to prevent signed integer overflow
    uint64_t min_local_deletion_time = static_cast<uint64_t>(header.get_min_local_deletion_time());
    return parse_expiry(static_cast<int64_t>(min_local_deletion_time + delta));
}

};   // namespace sstables
