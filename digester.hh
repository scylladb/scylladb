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

#include "digest_algorithm.hh"
#include "md5_hasher.hh"
#include "xx_hasher.hh"

#include <type_traits>
#include <variant>

namespace query {

struct noop_hasher {
    void update(const char* ptr, size_t length) { }
    std::array<uint8_t, 16> finalize_array() { return std::array<uint8_t, 16>(); };
};

class digester final {
    std::variant<noop_hasher, md5_hasher, xx_hasher> _impl;

public:
    explicit digester(digest_algorithm algo) {
        switch (algo) {
        case digest_algorithm::MD5:
            _impl = md5_hasher();
            break;
        case digest_algorithm::xxHash:
            _impl = xx_hasher();
            break;
        case digest_algorithm ::none:
            _impl = noop_hasher();
            break;
        }
    }

    template<typename T, typename... Args>
    void feed_hash(const T& value, Args&&... args) {
        std::visit([&] (auto& hasher) {
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
using using_hash_of_hash = std::negation<std::disjunction<std::is_same<Hasher, md5_hasher>, std::is_same<Hasher, noop_hasher>>>;

template<typename Hasher>
inline constexpr bool using_hash_of_hash_v = using_hash_of_hash<Hasher>::value;

}