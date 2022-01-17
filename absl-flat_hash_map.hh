/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <absl/container/flat_hash_map.h>
#include <seastar/core/sstring.hh>

using namespace seastar;

struct sstring_hash {
    using is_transparent = void;
    size_t operator()(std::string_view v) const noexcept;
};

struct sstring_eq {
    using is_transparent = void;
    bool operator()(std::string_view a, std::string_view b) const noexcept {
        return a == b;
    }
};

template <typename K, typename V, typename... Ts>
struct flat_hash_map : public absl::flat_hash_map<K, V, Ts...> {
};

template <typename V>
struct flat_hash_map<sstring, V>
    : public absl::flat_hash_map<sstring, V, sstring_hash, sstring_eq> {};
