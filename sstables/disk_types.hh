/*
 * Copyright (C) 2015-present ScyllaDB
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
#include "utils/chunked_vector.hh"
#include <seastar/core/enum.hh>
#include <boost/variant/variant.hpp>
#include <boost/variant/get.hpp>
#include <unordered_map>
#include <type_traits>
#include <deque>
#include "atomic_cell.hh"

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
    bool operator==(const disk_string& rhs) const {
        return value == rhs.value;
    }
};

struct disk_string_vint_size {
    bytes value;
    explicit operator bytes_view() const {
        return value;
    }
    bool operator==(const disk_string_vint_size& rhs) const {
        return value == rhs.value;
    }
};

template <typename Size>
struct disk_string_view {
    bytes_view value;
};

template<typename SizeType>
struct disk_data_value_view {
    atomic_cell_value_view value;
};

template <typename Size, typename Members>
struct disk_array {
    static_assert(std::is_integral<Size>::value, "Length type must be convertible to integer");
    utils::chunked_vector<Members> elements;
};

// A wrapper struct for integers to be written using variable-length encoding
template <typename T>
struct vint {
    static_assert(std::is_integral_v<T>, "Can only wrap integral types");
    T value;
};

// Same as disk_array but with its size serialized as variable-length integer
template <typename Members>
struct disk_array_vint_size {
    utils::chunked_vector<Members> elements;
};

template <typename Size, typename Members>
struct disk_array_ref {
    static_assert(std::is_integral<Size>::value, "Length type must be convertible to integer");
    const utils::chunked_vector<Members>& elements;
    disk_array_ref(const utils::chunked_vector<Members>& elements) : elements(elements) {}
};

template <typename Size, typename Key, typename Value>
struct disk_hash {
    std::unordered_map<Key, Value, std::hash<Key>> map;
};

template <typename TagType, TagType Tag, typename T>
struct disk_tagged_union_member {
    // stored as: tag, value-size-on-disk, value
    using tag_type = TagType;
    static constexpr tag_type tag() { return Tag; }
    using type = T;
    T value;
};

template <typename TagType, typename... Members>
struct disk_tagged_union {
    using variant_type = boost::variant<Members...>;
    variant_type data;
};

// Each element of Members... is a disk_tagged_union_member<>
template <typename TagType, typename... Members>
struct disk_set_of_tagged_union {
    using tag_type = TagType;
    using key_type = std::conditional_t<std::is_enum<TagType>::value, std::underlying_type_t<TagType>, TagType>;
    using hash_type = std::conditional_t<std::is_enum<TagType>::value, enum_hash<TagType>, TagType>;
    using value_type = boost::variant<Members...>;
    std::unordered_map<tag_type, value_type, hash_type> data;

    template <TagType Tag, typename T>
    T* get() {
        // FIXME: static_assert that <Tag, T> is a member
        auto i = data.find(Tag);
        if (i == data.end()) {
            return nullptr;
        } else {
            return &boost::get<disk_tagged_union_member<TagType, Tag, T>>(i->second).value;
        }
    }
    template <TagType Tag, typename T>
    const T* get() const {
        return const_cast<disk_set_of_tagged_union*>(this)->get<Tag, T>();
    }
    template <TagType Tag, typename T>
    void set(T&& value) {
        data[Tag] = disk_tagged_union_member<TagType, Tag, T>{std::forward<T>(value)};
    }
    struct serdes;
    static struct serdes s_serdes;
};

}

namespace std {
template <typename Size>
struct hash<sstables::disk_string<Size>> {
    size_t operator()(const sstables::disk_string<Size>& s) const {
        return std::hash<bytes>()(s.value);
    }
};

}
