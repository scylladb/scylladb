/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/digest_algorithm.hh"
#include "utils/hashers.hh"
#include "utils/xx_hasher.hh"

#include <type_traits>
#include <variant>

namespace query {

struct noop_hasher {
    void update(const char* ptr, size_t length) noexcept { }
    std::array<uint8_t, 16> finalize_array() { return std::array<uint8_t, 16>(); };
};

class digester final {
    std::variant<noop_hasher, md5_hasher, xx_hasher, legacy_xx_hasher_without_null_digest> _impl;

public:
    explicit digester(digest_algorithm algo) {
        switch (algo) {
        case digest_algorithm::MD5:
            _impl = md5_hasher();
            break;
        case digest_algorithm::xxHash:
            _impl = xx_hasher();
            break;
        case digest_algorithm::legacy_xxHash_without_null_digest:
            _impl = legacy_xx_hasher_without_null_digest();
            break;
        case digest_algorithm ::none:
            _impl = noop_hasher();
            break;
        }
    }

    template<typename T, typename... Args>
    void feed_hash(const T& value, Args&&... args) {
        // FIXME uncomment the noexcept marking once clang bug 50994 is fixed or gcc compilation is turned on
        std::visit([&] (auto& hasher) /* noexcept(noexcept(::feed_hash(hasher, value, args...))) */ -> void {
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
