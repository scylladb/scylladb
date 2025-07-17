/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <optional>
#include <chrono>

namespace utils {
    template <typename T> class simple_value_with_expiry {
        std::chrono::nanoseconds expire_when;
        std::optional<T> value;

        static auto now() {
            return std::chrono::high_resolution_clock::now();
        }
    public:
        std::optional<T> get() const {
            auto n = std::chrono::duration_cast<std::chrono::nanoseconds>(now().time_since_epoch());
            if (n <= expire_when) {
                return value;
            }
            return std::nullopt;
        }
        std::chrono::nanoseconds set_now(T v, std::chrono::nanoseconds ttl) {
            auto n = std::chrono::duration_cast<std::chrono::nanoseconds>((now() + ttl).time_since_epoch());
            if (n <= expire_when) {
                n = expire_when + std::chrono::nanoseconds{ 1 };
            }
            value = std::move(v);
            expire_when = n;
            return n;
            
        }
        void set(T v, std::chrono::nanoseconds expiry) {
            if (expiry > expire_when) {
                expire_when = expiry;
                value = v;
            }
        }
    };
}
