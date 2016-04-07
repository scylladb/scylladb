/*
 * Copyright (C) 2015 ScyllaDB
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

#include "bytes.hh"
#include <unordered_map>
#include <type_traits>
#include <deque>

namespace sstables {

// Some in-disk structures have an associated integer (of varying sizes) that
// represents how large they are. They can be a byte-length, in the case of a
// string, number of elements, in the case of an array, etc.
//
// For those elements, we encapsulate the underlying type in an outter
// structure that embeds how large is the in-disk size. It is a lot more
// convenient to embed it in the size than explicitly writing it in the parser.
// This way, we don't need to encode this information in multiple places at
// once - it is already part of the type.
template <typename Size>
struct disk_string {
    bytes value;
    explicit operator bytes_view() const {
        return value;
    }
};

template <typename Size>
struct disk_string_view {
    bytes_view value;
};

template <typename Size, typename Members>
struct disk_array {
    static_assert(std::is_integral<Size>::value, "Length type must be convertible to integer");
    std::deque<Members> elements;
};

template <typename Size, typename Key, typename Value>
struct disk_hash {
    std::unordered_map<Key, Value, std::hash<Key>> map;
};

}
