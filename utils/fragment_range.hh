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

#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm/for_each.hpp>

#include <seastar/util/gcc6-concepts.hh>

#include "bytes.hh"

enum class mutable_view { no, yes, };

GCC6_CONCEPT(

/// Fragmented buffer
///
/// Concept `FragmentedBuffer` is satisfied by any class that is a range of
/// fragments and provides a method `size_bytes()` which returns the total
/// size of the buffer. The interfaces accepting `FragmentedBuffer` will attempt
/// to avoid unnecessary linearisation.
template<typename T>
concept bool FragmentRange = requires (T range) {
    typename T::fragment_type;
    requires std::is_same_v<typename T::fragment_type, bytes_view>
        || std::is_same_v<typename T::fragment_type, bytes_mutable_view>;
    { *range.begin() } -> typename T::fragment_type;
    { *range.end() } -> typename T::fragment_type;
    { range.size_bytes() } -> size_t;
    { range.empty() } -> bool; // returns true iff size_bytes() == 0.
};

)

template<typename T, typename = void>
struct is_fragment_range : std::false_type { };

template<typename T>
struct is_fragment_range<T, std::void_t<typename T::fragment_type>> : std::true_type { };

template<typename T>
static constexpr bool is_fragment_range_v = is_fragment_range<T>::value;

/// Single-element fragment range
///
/// This is a helper that allows converting a bytes_view into a FragmentRange.
template<mutable_view is_mutable>
class single_fragment_range {
public:
    using fragment_type = std::conditional_t<is_mutable == mutable_view::no,
                                             bytes_view, bytes_mutable_view>;
private:
    fragment_type _view;
public:
    using iterator = const fragment_type*;
    using const_iterator = const fragment_type*;

    explicit single_fragment_range(fragment_type f) : _view { f } { }

    const_iterator begin() const { return &_view; }
    const_iterator end() const { return &_view + 1; }

    size_t size_bytes() const { return _view.size(); }
    bool empty() const { return _view.empty(); }
};

single_fragment_range(bytes_view) -> single_fragment_range<mutable_view::no>;
single_fragment_range(bytes_mutable_view) -> single_fragment_range<mutable_view::yes>;

/// Empty fragment range.
struct empty_fragment_range {
    using fragment_type = bytes_view;
    using iterator = bytes_view*;
    using const_iterator = bytes_view*;

    iterator begin() const { return nullptr; }
    iterator end() const { return nullptr; }

    size_t size_bytes() const { return 0; }
    bool empty() const { return true; }
};

GCC6_CONCEPT(

static_assert(FragmentRange<empty_fragment_range>);
static_assert(FragmentRange<single_fragment_range<mutable_view::no>>);
static_assert(FragmentRange<single_fragment_range<mutable_view::yes>>);

)

template<typename FragmentedBuffer>
GCC6_CONCEPT(requires FragmentRange<FragmentedBuffer>)
bytes linearized(const FragmentedBuffer& buffer)
{
    bytes b(bytes::initialized_later(), buffer.size_bytes());
    auto dst = b.begin();
    using boost::range::for_each;
    for_each(buffer, [&] (bytes_view fragment) {
        dst = boost::copy(fragment, dst);
    });
    return b;
}

template<typename FragmentedBuffer, typename Function>
GCC6_CONCEPT(requires FragmentRange<FragmentedBuffer> && requires (Function fn, bytes_view bv) {
    fn(bv);
})
decltype(auto) with_linearized(const FragmentedBuffer& buffer, Function&& fn)
{
    bytes b;
    bytes_view bv;
    if (__builtin_expect(!buffer.empty() && std::next(buffer.begin()) == buffer.end(), true)) {
        bv = *buffer.begin();
    } else if (!buffer.empty()) {
        b = linearized(buffer);
        bv = b;
    }
    return fn(bv);
}
