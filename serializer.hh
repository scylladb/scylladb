/*
 * Copyright 2016-present ScyllaDB
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

#include <vector>
#include <unordered_set>
#include <list>
#include <array>
#include <seastar/core/sstring.hh>
#include <unordered_map>
#include <optional>
#include "enum_set.hh"
#include "utils/managed_bytes.hh"
#include "bytes_ostream.hh"
#include <seastar/core/simple-stream.hh>
#include "boost/variant/variant.hpp"
#include "bytes_ostream.hh"
#include "utils/input_stream.hh"
#include "utils/fragment_range.hh"
#include "utils/chunked_vector.hh"
#include <variant>

#include <boost/range/algorithm/for_each.hpp>
#include <boost/type.hpp>

namespace ser {

/// A fragmented view of an opaque buffer in a stream of serialised data
///
/// This class allows reading large, fragmented blobs serialised by the IDL
/// infrastructure without linearising or copying them. The view remains valid
/// as long as the underlying IDL-serialised buffer is alive.
///
/// Satisfies FragmentRange concept.
template<typename FragmentIterator>
class buffer_view {
    bytes_view _first;
    size_t _total_size;
    FragmentIterator _next;
public:
    using fragment_type = bytes_view;

    class iterator {
        bytes_view _current;
        size_t _left = 0;
        FragmentIterator _next;
    public:
        using iterator_category	= std::input_iterator_tag;
        using value_type = bytes_view;
        using pointer = const bytes_view*;
        using reference = const bytes_view&;
        using difference_type = std::ptrdiff_t;

        iterator() = default;
        iterator(bytes_view current, size_t left, FragmentIterator next)
            : _current(current), _left(left), _next(next) { }

        bytes_view operator*() const {
            return _current;
        }
        const bytes_view* operator->() const {
            return &_current;
        }

        iterator& operator++() {
            _left -= _current.size();
            if (_left) {
                auto next_view = bytes_view(reinterpret_cast<const bytes::value_type*>((*_next).begin()),
                                            (*_next).size());
                auto next_size = std::min(_left, next_view.size());
                _current = bytes_view(next_view.data(), next_size);
                ++_next;
            }
            return *this;
        }
        iterator operator++(int) {
            iterator it(*this);
            operator++();
            return it;
        }

        bool operator==(const iterator& other) const {
            return _left == other._left;
        }
        bool operator!=(const iterator& other) const {
            return !(*this == other);
        }
    };
    using const_iterator = iterator;

    explicit buffer_view(bytes_view current)
        : _first(current), _total_size(current.size()) { }

    buffer_view(bytes_view current, size_t size, FragmentIterator it)
        : _first(current), _total_size(size), _next(it)
    {
        if (_first.size() > _total_size) {
            _first.remove_suffix(_first.size() - _total_size);
        }
    }

    explicit buffer_view(typename seastar::memory_input_stream<FragmentIterator>::simple stream)
        : buffer_view(bytes_view(reinterpret_cast<const int8_t*>(stream.begin()), stream.size()))
    { }

    explicit buffer_view(typename seastar::memory_input_stream<FragmentIterator>::fragmented stream)
        : buffer_view(bytes_view(reinterpret_cast<const int8_t*>(stream.first_fragment_data()), stream.first_fragment_size()),
                      stream.size(), stream.fragment_iterator())
    { }

    iterator begin() const {
        return iterator(_first, _total_size, _next);
    }
    iterator end() const {
        return iterator();
    }

    size_t size_bytes() const {
        return _total_size;
    }
    bool empty() const {
        return !_total_size;
    }

    // FragmentedView implementation
    void remove_prefix(size_t n) {
        while (n >= _first.size() && n > 0) {
            n -= _first.size();
            remove_current();
        }
        _total_size -= n;
        _first.remove_prefix(n);
    }
    void remove_current() {
        _total_size -= _first.size();
        if (_total_size) {
            auto next_data = reinterpret_cast<const bytes::value_type*>((*_next).begin());
            size_t next_size = std::min(_total_size, (*_next).size());
            _first = bytes_view(next_data, next_size);
            ++_next;
        } else {
            _first = bytes_view();
        }
    }
    buffer_view prefix(size_t n) const {
        auto tmp = *this;
        tmp._total_size = std::min(tmp._total_size, n);
        tmp._first = tmp._first.substr(0, n);
        return tmp;
    }
    bytes_view current_fragment() {
        return _first;
    }

    bytes linearize() const {
        bytes b(bytes::initialized_later(), size_bytes());
        using boost::range::for_each;
        auto dst = b.begin();
        for_each(*this, [&] (bytes_view fragment) {
            dst = std::copy(fragment.begin(), fragment.end(), dst);
        });
        return b;
    }

    template<typename Function>
    decltype(auto) with_linearized(Function&& fn) const
    {
        bytes b;
        bytes_view bv;
        if (_first.size() != _total_size) {
            b = linearize();
            bv = b;
        } else {
            bv = _first;
        }
        return fn(bv);
    }
};
static_assert(FragmentedView<buffer_view<bytes_ostream::fragment_iterator>>);

using size_type = uint32_t;

template<typename T, typename Input>
inline T deserialize_integral(Input& input) {
    static_assert(std::is_integral<T>::value, "T should be integral");
    T data;
    input.read(reinterpret_cast<char*>(&data), sizeof(T));
    return le_to_cpu(data);
}

template<typename T, typename Output>
inline void serialize_integral(Output& output, T data) {
    static_assert(std::is_integral<T>::value, "T should be integral");
    data = cpu_to_le(data);
    output.write(reinterpret_cast<const char*>(&data), sizeof(T));
}

template<typename T>
struct serializer;

template<typename T>
struct integral_serializer {
    template<typename Input>
    static T read(Input& v) {
        return deserialize_integral<T>(v);
    }
    template<typename Output>
    static void write(Output& out, T v) {
        serialize_integral(out, v);
    }
    template<typename Input>
    static void skip(Input& v) {
        read(v);
    }
};

template<> struct serializer<bool> {
    template <typename Input>
    static bool read(Input& i) {
        return deserialize_integral<uint8_t>(i);
    }
    template< typename Output>
    static void write(Output& out, bool v) {
        serialize_integral(out, uint8_t(v));
    }
    template <typename Input>
    static void skip(Input& i) {
        read(i);
    }

};
template<> struct serializer<int8_t> : public integral_serializer<int8_t> {};
template<> struct serializer<uint8_t> : public integral_serializer<uint8_t> {};
template<> struct serializer<int16_t> : public integral_serializer<int16_t> {};
template<> struct serializer<uint16_t> : public integral_serializer<uint16_t> {};
template<> struct serializer<int32_t> : public integral_serializer<int32_t> {};
template<> struct serializer<uint32_t> : public integral_serializer<uint32_t> {};
template<> struct serializer<int64_t> : public integral_serializer<int64_t> {};
template<> struct serializer<uint64_t> : public integral_serializer<uint64_t> {};

template<typename Output>
void safe_serialize_as_uint32(Output& output, uint64_t data);

template<typename T, typename Output>
inline void serialize(Output& out, const T& v) {
    serializer<T>::write(out, v);
};

template<typename T, typename Output>
inline void serialize(Output& out, const std::reference_wrapper<T> v) {
    serializer<T>::write(out, v.get());
}

template<typename T, typename Input>
inline auto deserialize(Input& in, boost::type<T> t) {
    return serializer<T>::read(in);
}

template<typename T, typename Input>
inline void skip(Input& v, boost::type<T>) {
    return serializer<T>::skip(v);
}

template<typename T>
size_type get_sizeof(const T& obj);

template<typename T>
void set_size(seastar::measuring_output_stream& os, const T& obj);

template<typename Stream, typename T>
void set_size(Stream& os, const T& obj);

template<typename Buffer, typename T>
Buffer serialize_to_buffer(const T& v, size_t head_space = 0);

template<typename T, typename Buffer>
T deserialize_from_buffer(const Buffer&, boost::type<T>, size_t head_space = 0);

template<typename Output, typename ...T>
void serialize(Output& out, const boost::variant<T...>& v);

template<typename Input, typename ...T>
boost::variant<T...> deserialize(Input& in, boost::type<boost::variant<T...>>);

template<typename Output, typename ...T>
void serialize(Output& out, const std::variant<T...>& v);

template<typename Input, typename ...T>
std::variant<T...> deserialize(Input& in, boost::type<std::variant<T...>>);

struct unknown_variant_type {
    size_type index;
    sstring data;
};

template<typename Output>
void serialize(Output& out, const unknown_variant_type& v);

template<typename Input>
unknown_variant_type deserialize(Input& in, boost::type<unknown_variant_type>);

template <typename T>
struct normalize {
    using type = T;
};

template <>
struct normalize<bytes_view> {
     using type = bytes;
};

template <>
struct normalize<managed_bytes> {
     using type = bytes;
};

template <>
struct normalize<bytes_ostream> {
    using type = bytes;
};

template <typename T, typename U>
struct is_equivalent : std::is_same<typename normalize<std::remove_const_t<std::remove_reference_t<T>>>::type, typename normalize<std::remove_const_t <std::remove_reference_t<U>>>::type> {
};

template <typename T, typename U>
struct is_equivalent<std::reference_wrapper<T>, U> : is_equivalent<T, U> {
};

template <typename T, typename U>
struct is_equivalent<T, std::reference_wrapper<U>> : is_equivalent<T, U> {
};

template <typename T, typename U>
struct is_equivalent<std::optional<T>, std::optional<U>> : is_equivalent<T, U> {
};

template <typename T, typename U, bool>
struct is_equivalent_arity;

template <typename ...T, typename ...U>
struct is_equivalent_arity<std::tuple<T...>, std::tuple<U...>, false> : std::false_type {
};

template <typename ...T, typename ...U>
struct is_equivalent_arity<std::tuple<T...>, std::tuple<U...>, true> {
    static constexpr bool value = (is_equivalent<T, U>::value && ...);
};

template <typename ...T, typename ...U>
struct is_equivalent<std::tuple<T...>, std::tuple<U...>> : is_equivalent_arity<std::tuple<T...>, std::tuple<U...>, sizeof...(T) == sizeof...(U)> {
};

template <typename ...T, typename ...U>
struct is_equivalent<std::variant<T...>, std::variant<U...>> : is_equivalent<std::tuple<T...>, std::tuple<U...>> {
};

// gc_clock duration values were serialized as 32-bit prior to 3.1, and
// are serialized as 64-bit in 3.1.0.
//
// TTL values are capped to 20 years, which fits into 32 bits, so
// truncation is not a concern.

inline bool gc_clock_using_3_1_0_serialization = false;

template <typename Output>
void
serialize_gc_clock_duration_value(Output& out, int64_t v) {
    if (!gc_clock_using_3_1_0_serialization) {
        // This should have been caught by the CQL layer, so this is just
        // for extra safety.
        assert(int32_t(v) == v);
        serializer<int32_t>::write(out, v);
    } else {
        serializer<int64_t>::write(out, v);
    }
}

template <typename Input>
int64_t
deserialize_gc_clock_duration_value(Input& in) {
    if (!gc_clock_using_3_1_0_serialization) {
        return serializer<int32_t>::read(in);
    } else {
        return serializer<int64_t>::read(in);
    }
}

}

/*
 * Import the auto generated forward decleration code
 */
