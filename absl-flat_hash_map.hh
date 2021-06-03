/*
 * Copyright (C) 2020-present ScyllaDB
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
