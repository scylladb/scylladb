/*
 * Copyright (C) 2018-present ScyllaDB
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
#include <seastar/net/byteorder.hh>
#include <seastar/core/print.hh>
#include <seastar/util/backtrace.hh>

#include "marshal_exception.hh"
#include "bytes.hh"
#include "utils/bit_cast.hh"

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
struct fragment_range {
    using fragment_type = typename View::fragment_type;
    View view;
    class fragment_iterator {
        using iterator_category = std::input_iterator_tag;
        using value_type = typename View::fragment_type;
        using difference_type = std::ptrdiff_t;
        using pointer = const value_type*;
        using reference = const value_type&;
        View _view;
        value_type _current;
    public:
        fragment_iterator() : _view(value_type()) {}
        fragment_iterator(const View& v) : _view(v) {
            _current = _view.current_fragment();
        }
        fragment_iterator& operator++() {
            _view.remove_current();
            _current = _view.current_fragment(); 
            return *this;
        }
        fragment_iterator operator++(int) {
            fragment_iterator i(*this);
            ++(*this);
            return i;
        }
        reference operator*() const { return _current; }
        pointer operator->() const { return &_current; }
        bool operator==(const fragment_iterator& i) const { return _view.size_bytes() == i._view.size_bytes(); }
    };
    using iterator = fragment_iterator;
    fragment_range(const View& v) : view(v) {}
    fragment_iterator begin() const { return fragment_iterator(view); }
    fragment_iterator end() const { return fragment_iterator(); }
    size_t size_bytes() const { return view.size_bytes(); }
    bool empty() const { return view.empty(); }
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

template <mutable_view is_mutable>
class basic_single_fragmented_view {
public:
    using fragment_type = std::conditional_t<is_mutable == mutable_view::yes, bytes_mutable_view, bytes_view>;
private:
    fragment_type _view;
public:
    explicit basic_single_fragmented_view(fragment_type bv) : _view(bv) {}
    size_t size_bytes() const { return _view.size(); }
    bool empty() const { return _view.empty(); }
    void remove_prefix(size_t n) { _view.remove_prefix(n); }
    void remove_current() { _view = fragment_type(); }
    fragment_type current_fragment() const { return _view; }
    basic_single_fragmented_view prefix(size_t n) { return basic_single_fragmented_view(_view.substr(0, n)); }
};
using single_fragmented_view = basic_single_fragmented_view<mutable_view::no>;
using single_fragmented_mutable_view = basic_single_fragmented_view<mutable_view::yes>;
static_assert(FragmentedView<single_fragmented_view>);
static_assert(FragmentedMutableView<single_fragmented_mutable_view>);
static_assert(FragmentRange<fragment_range<single_fragmented_view>>);
static_assert(FragmentRange<fragment_range<single_fragmented_mutable_view>>);

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

template<FragmentedView View>
void skip_empty_fragments(View& v) {
    while (!v.empty() && v.current_fragment().empty()) {
        v.remove_current();
    }
}

template<FragmentedView V1, FragmentedView V2>
int compare_unsigned(V1 v1, V2 v2) {
    while (!v1.empty() && !v2.empty()) {
        size_t n = std::min(v1.current_fragment().size(), v2.current_fragment().size());
        if (int d = memcmp(v1.current_fragment().data(), v2.current_fragment().data(), n)) {
            return d;
        }
        v1.remove_prefix(n);
        v2.remove_prefix(n);
        skip_empty_fragments(v1);
        skip_empty_fragments(v2);
    }
    return v1.size_bytes() - v2.size_bytes();
}

template<FragmentedView V1, FragmentedView V2>
int equal_unsigned(V1 v1, V2 v2) {
    return v1.size_bytes() == v2.size_bytes() && compare_unsigned(v1, v2) == 0;
}

template<FragmentedMutableView Dest, FragmentedView Src>
void write_fragmented(Dest& dest, Src src) {
    if (dest.size_bytes() < src.size_bytes()) [[unlikely]] {
        throw std::out_of_range(format("tried to copy a buffer of size {} to a buffer of smaller size {}", src.size_bytes(), dest.size_bytes()));
    }
    while (!src.empty()) {
        size_t n = std::min(dest.current_fragment().size(), src.current_fragment().size());
        memcpy(dest.current_fragment().data(), src.current_fragment().data(), n);
        dest.remove_prefix(n);
        src.remove_prefix(n);
        skip_empty_fragments(dest);
        skip_empty_fragments(src);
    }
}

template<FragmentedMutableView Dest, FragmentedView Src>
void copy_fragmented_view(Dest dest, Src src) {
    if (dest.size_bytes() < src.size_bytes()) [[unlikely]] {
        throw std::out_of_range(format("tried to copy a buffer of size {} to a buffer of smaller size {}", src.size_bytes(), dest.size_bytes()));
    }
    while (!src.empty()) {
        size_t n = std::min(dest.current_fragment().size(), src.current_fragment().size());
        memcpy(dest.current_fragment().data(), src.current_fragment().data(), n);
        dest.remove_prefix(n);
        src.remove_prefix(n);
        skip_empty_fragments(dest);
        skip_empty_fragments(src);
    }
}

// Does not check bounds. Must be called only after size is already checked.
template<FragmentedView View>
void read_fragmented(View& v, size_t n, bytes::value_type* out) {
    while (n) {
        if (n <= v.current_fragment().size()) {
            std::copy_n(v.current_fragment().data(), n, out);
            v.remove_prefix(n);
            n = 0;
        } else {
            out = std::copy_n(v.current_fragment().data(), v.current_fragment().size(), out);
            n -= v.current_fragment().size();
            v.remove_current();
        }
    }
}
template<> void inline read_fragmented(single_fragmented_view& v, size_t n, bytes::value_type* out) {
    std::copy_n(v.current_fragment().data(), n, out);
    v.remove_prefix(n);
}

template<typename T, FragmentedView View>
T read_simple_native(View& v) {
    if (v.current_fragment().size() >= sizeof(T)) [[likely]] {
        auto p = v.current_fragment().data();
        v.remove_prefix(sizeof(T));
        return read_unaligned<T>(p);
    } else if (v.size_bytes() >= sizeof(T)) {
        T buf;
        read_fragmented(v, sizeof(T), reinterpret_cast<bytes::value_type*>(&buf));
        return buf;
    } else {
        throw_with_backtrace<marshal_exception>(format("read_simple - not enough bytes (expected {:d}, got {:d})", sizeof(T), v.size_bytes()));
    }
}

template<typename T, FragmentedView View>
T read_simple(View& v) {
    if (v.current_fragment().size() >= sizeof(T)) [[likely]] {
        auto p = v.current_fragment().data();
        v.remove_prefix(sizeof(T));
        return net::ntoh(read_unaligned<T>(p));
    } else if (v.size_bytes() >= sizeof(T)) {
        T buf;
        read_fragmented(v, sizeof(T), reinterpret_cast<bytes::value_type*>(&buf));
        return net::ntoh(buf);
    } else {
        throw_with_backtrace<marshal_exception>(format("read_simple - not enough bytes (expected {:d}, got {:d})", sizeof(T), v.size_bytes()));
    }
}

template<typename T, FragmentedView View>
T read_simple_exactly(View v) {
    if (v.current_fragment().size() == sizeof(T)) [[likely]] {
        auto p = v.current_fragment().data();
        return net::ntoh(read_unaligned<T>(p));
    } else if (v.size_bytes() == sizeof(T)) {
        T buf;
        read_fragmented(v, sizeof(T), reinterpret_cast<bytes::value_type*>(&buf));
        return net::ntoh(buf);
    } else {
        throw_with_backtrace<marshal_exception>(format("read_simple_exactly - size mismatch (expected {:d}, got {:d})", sizeof(T), v.size_bytes()));
    }
}

template<typename T, FragmentedMutableView Out>
static inline
void write(Out& out, std::type_identity_t<T> val) {
    auto v = net::ntoh(val);
    auto p = reinterpret_cast<const bytes_view::value_type*>(&v);
    if (out.current_fragment().size() >= sizeof(v)) [[likely]] {
        std::copy_n(p, sizeof(v), out.current_fragment().data());
        out.remove_prefix(sizeof(v));
    } else {
        write_fragmented(out, single_fragmented_view(bytes_view(p, sizeof(v))));
    }
}

template<typename T, FragmentedMutableView Out>
static inline
void write_native(Out& out, std::type_identity_t<T> v) {
    auto p = reinterpret_cast<const bytes_view::value_type*>(&v);
    if (out.current_fragment().size() >= sizeof(v)) [[likely]] {
        std::copy_n(p, sizeof(v), out.current_fragment().data());
        out.remove_prefix(sizeof(v));
    } else {
        write_fragmented(out, single_fragmented_view(bytes_view(p, sizeof(v))));
    }
}

inline sstring::iterator fragment_to_hex(sstring::iterator out, bytes_view frag) {
    static constexpr char digits[] = "0123456789abcdef";
    for (auto byte : frag) {
        uint8_t x = static_cast<uint8_t>(byte);
        *out++ = digits[x >> 4];
        *out++ = digits[x & 0xf];
    }
    return out;
}

template<FragmentedView View>
sstring to_hex(const View& b) {
    sstring out = uninitialized_string(b.size_bytes() * 2);
    auto it = out.begin();
    for (auto frag : fragment_range(b)) {
        it = fragment_to_hex(it, frag);
    }
    return out;
}
