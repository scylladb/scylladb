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

    // NOTE: all time points must use the same clock, e.g. std::chrono::high_resolution_clock

    inline std::chrono::high_resolution_clock::time_point get_now_time_point_for_simple_value_with_expiry() {
        return std::chrono::high_resolution_clock::now();
    }

    inline std::chrono::high_resolution_clock::time_point calculate_expiry_for_simple_value_with_expiry(std::chrono::nanoseconds ttl, std::chrono::high_resolution_clock::time_point now_moment = get_now_time_point_for_simple_value_with_expiry()) {
        return now_moment + ttl;
    }

    template <typename T> class simple_value_with_expiry {
    public:
        using time_point = std::chrono::high_resolution_clock::time_point;

    private:
        time_point expire_when;
        std::optional<T> value;

    public:
        simple_value_with_expiry() = default;

        std::optional<T> get(time_point now_moment = get_now_time_point_for_simple_value_with_expiry()) const {
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
