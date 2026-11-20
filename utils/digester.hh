/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "utils/digest_algorithm.hh"
#include "utils/hashing.hh"
#include "utils/xx_hasher.hh"

#include <type_traits>
#include <variant>

namespace query {

struct noop_hasher {
    void update(const char* ptr, size_t length) noexcept { }
    std::array<uint8_t, 16> finalize_array() { return std::array<uint8_t, 16>(); };
};

class digester final {
    std::variant<noop_hasher, xx_hasher> _impl;
    bool _row_level = false;

public:
    explicit digester(digest_algorithm algo) {
        switch (algo) {
        case digest_algorithm::xxHash:
            _impl = xx_hasher();
            break;
        case digest_algorithm::xxHash_row:
            _impl = xx_hasher();
            _row_level = true;
            break;
        case digest_algorithm ::none:
            _impl = noop_hasher();
            break;
        }
    }

    // Rows are fed as one hash of their cell hashes, which can be cached per row.
    bool row_level() const noexcept { return _row_level; }

    template<typename T, typename... Args>
    void feed_hash(const T& value, Args&&... args) {
        std::visit([&] (auto& hasher) noexcept(noexcept(::feed_hash(hasher, value, std::forward<Args>(args)...))) -> void {
            ::feed_hash(hasher, value, std::forward<Args>(args)...);
        }, _impl);
    };

    std::array<uint8_t, 16> finalize_array() {
        return std::visit([&] (auto& hasher) {
            return hasher.finalize_array();
        }, _impl);
    }
};

using default_hasher = xx_hasher;

template<typename Hasher>
using using_hash_of_hash = std::negation<std::is_same<Hasher, noop_hasher>>;

template<typename Hasher>
inline constexpr bool using_hash_of_hash_v = using_hash_of_hash<Hasher>::value;

}
