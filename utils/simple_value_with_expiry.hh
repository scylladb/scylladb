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
    // simple value with attached expiry timeout
    // the value might be not set or expired, in which case get() returns std::nullopt
    // set_if_longer_expiry() sets the value and expiry time to the given values, only if
    // either value is not set, expired or newer expiry timeout is further into the future
    template <typename T> class simple_value_with_expiry {
    public:
        using time_point = std::chrono::high_resolution_clock::time_point;

    private:
        time_point expire_when;
        std::optional<T> value;

    public:
        simple_value_with_expiry() = default;

        static time_point now() {
            return std::chrono::high_resolution_clock::now();
        }
        static time_point calculate_expiry(std::chrono::nanoseconds ttl, time_point now_moment = now()) {
            return now_moment + ttl;
        }

        std::optional<T> get(time_point now_moment = now()) const {
            if (now_moment <= expire_when) {
                return value;
            }
            return std::nullopt;
        }
        void set_if_longer_expiry(T v, time_point expiry) {
            if (expiry > expire_when) {
                expire_when = expiry;
                value = std::move(v);
            }
        }
    };
}
