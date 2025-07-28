/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <optional>
#include <chrono>

namespace utils {
    // rudimentary cache with expiry
    // the value might be unset or expired, in which case get() returns std::nullopt
    // set_now() sets the value to now() + ttl, and returns the expiry time
    // set() sets the value to the given expiry time, but only if it is later
    // you use set_now() if the cached is sharded - call it on one shard,
    // on all others call set() with the expiry time returned by set_now().
    // if two updates are in progress, the one with the later expiry time wins,
    // all shards will eventually converge to the same value.
    template <typename T> class simple_value_with_expiry {
        std::chrono::high_resolution_clock::time_point expire_when;
        std::optional<T> value;

    public:
        simple_value_with_expiry() = default;

        static std::chrono::high_resolution_clock::time_point now() {
            return std::chrono::high_resolution_clock::now();
        }
        static std::chrono::high_resolution_clock::time_point calculate_expiry(std::chrono::nanoseconds ttl) {
            return now() + ttl;
        }

        std::optional<T> get(std::chrono::high_resolution_clock::time_point now_moment = now()) const {
            if (now_moment <= expire_when) {
                return value;
            }
            return std::nullopt;
        }
        void set_if_newer(T v, std::chrono::high_resolution_clock::time_point expiry) {
            if (expiry > expire_when) {
                expire_when = expiry;
                value = v;
            }
        }
    };
}
