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

#include <concepts>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm/for_each.hpp>


#include "bytes.hh"

enum class mutable_view { no, yes, };

/// Fragmented buffer
///
/// Concept `FragmentedBuffer` is satisfied by any class that is a range of
/// fragments and provides a method `size_bytes()` which returns the total
/// size of the buffer. The interfaces accepting `FragmentedBuffer` will attempt
/// to avoid unnecessary linearisation.
template<typename T>
concept FragmentRange = requires (T range) {
    typename T::fragment_type;
    requires std::is_same_v<typename T::fragment_type, bytes_view>
        || std::is_same_v<typename T::fragment_type, bytes_mutable_view>;
    { *range.begin() } -> std::convertible_to<const typename T::fragment_type&>;
    { *range.end() } -> std::convertible_to<const typename T::fragment_type&>;
    { range.size_bytes() } -> std::convertible_to<size_t>;
    { range.empty() } -> std::same_as<bool>; // returns true iff size_bytes() == 0.
};

template<typename T, typename = void>
struct is_fragment_range : std::false_type { };

template<typename T>
struct is_fragment_range<T, std::void_t<typename T::fragment_type>> : std::true_type { };

template<typename T>
static constexpr bool is_fragment_range_v = is_fragment_range<T>::value;

/// A non-mutable view of a FragmentRange
///
/// Provide a trivially copyable and movable, non-mutable view on a
/// fragment range. This allows uniform ownership semantics across
/// multi-fragment ranges and the single fragment and empty fragment
/// adaptors below, i.e. it allows treating all fragment ranges
/// uniformly as views.
template <typename T>
requires FragmentRange<T>
class fragment_range_view {
    const T* _range;
public:
    using fragment_type = typename T::fragment_type;
    using iterator = typename T::const_iterator;
    using const_iterator = typename T::const_iterator;

public:
    explicit fragment_range_view(const T& range) : _range(&range) { }

    const_iterator begin() const { return _range->begin(); }
    const_iterator end() const { return _range->end(); }

    size_t size_bytes() const { return _range->size_bytes(); }
    bool empty() const { return _range->empty(); }
};

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

static_assert(FragmentRange<empty_fragment_range>);
static_assert(FragmentRange<single_fragment_range<mutable_view::no>>);
static_assert(FragmentRange<single_fragment_range<mutable_view::yes>>);

template<typename FragmentedBuffer>
requires FragmentRange<FragmentedBuffer>
bytes linearized(const FragmentedBuffer& buffer)
{
    bytes b(bytes::initialized_later(), buffer.size_bytes());
    auto dst = b.begin();
    for (bytes_view fragment : buffer) {
        dst = boost::copy(fragment, dst);
    }
    return b;
}

template<typename FragmentedBuffer, typename Function>
requires FragmentRange<FragmentedBuffer> && requires (Function fn, bytes_view bv) {
    fn(bv);
}
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

template<typename T>
concept FragmentedView = requires (T view, size_t n) {
    typename T::fragment_type;
    requires std::is_same_v<typename T::fragment_type, bytes_view>
            || std::is_same_v<typename T::fragment_type, bytes_mutable_view>;
    // No preconditions.
    { view.current_fragment() } -> std::convertible_to<const typename T::fragment_type&>;
    // No preconditions.
    { view.empty() } -> std::same_as<bool>;
    // No preconditions.
    { view.size_bytes() } -> std::convertible_to<size_t>;
    // Precondition: n <= size_bytes()
    { view.prefix(n) } -> std::same_as<T>;
    // Precondition: n <= size_bytes()
    view.remove_prefix(n);
    // Precondition: size_bytes() > 0
    view.remove_current();
};

template<typename T>
concept FragmentedMutableView = requires (T view) {
    requires FragmentedView<T>;
    requires std::is_same_v<typename T::fragment_type, bytes_mutable_view>;
};

template<FragmentedView View>
requires (!FragmentRange<View>)
bytes linearized(View v)
{
    bytes b(bytes::initialized_later(), v.size_bytes());
    auto out = b.begin();
    while (v.size_bytes()) {
        out = std::copy(v.current_fragment().begin(), v.current_fragment().end(), out);
        v.remove_current();
    }
    return b;
}

template<FragmentedView View, typename Function>
requires (!FragmentRange<View>) && std::invocable<Function, bytes_view>
decltype(auto) with_linearized(const View& v, Function&& fn)
{
    if (v.size_bytes() == v.current_fragment().size()) [[likely]] {
        return fn(v.current_fragment());
    } else {
        return fn(linearized(v));
    }
}

class single_fragmented_view {
    bytes_view _view;
public:
    using fragment_type = bytes_view;
    explicit single_fragmented_view(bytes_view bv) : _view(bv) {}
    size_t size_bytes() const { return _view.size(); }
    bool empty() const { return _view.empty(); }
    void remove_prefix(size_t n) { _view.remove_prefix(n); }
    void remove_current() { _view = bytes_view(); }
    bytes_view current_fragment() const { return _view; }
    single_fragmented_view prefix(size_t n) { return single_fragmented_view(_view.substr(0, n)); }
};
static_assert(FragmentedView<single_fragmented_view>);

template<FragmentedView View, typename Function>
requires std::invocable<Function, View> && std::invocable<Function, single_fragmented_view>
decltype(auto) with_simplified(const View& v, Function&& fn)
{
    if (v.size_bytes() == v.current_fragment().size()) [[likely]] {
        return fn(single_fragmented_view(v.current_fragment()));
    } else {
        return fn(v);
    }
}
