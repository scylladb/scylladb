/*
 * Copyright 2016 ScyllaDB
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
#include <array>
#include "core/sstring.hh"
#include <unordered_map>
#include <experimental/optional>
#include "enum_set.hh"
#include "utils/managed_bytes.hh"
#include "bytes_ostream.hh"
#include "core/simple-stream.hh"
#include "boost/variant/variant.hpp"
#include "bytes_ostream.hh"
#include "utils/input_stream.hh"

namespace ser {
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

template<> struct serializer<bool> : public integral_serializer<int8_t> {};
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

template<typename T, typename Input>
inline auto deserialize(Input& in, boost::type<T> t) {
    return serializer<T>::read(in);
};

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
}

/*
 * Import the auto generated forward decleration code
 */
