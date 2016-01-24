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
#include "core/sstring.hh"
#include <unordered_map>
namespace rpc {
    class simple_output_stream;
    class measuring_output_stream;
}
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

// For integer type
template<typename Input>
bool deserialize(Input& input, rpc::type<bool>) {
    return deserialize(input, rpc::type<uint8_t>());
}
template<typename Input>
int8_t deserialize(Input& input, rpc::type<int8_t>) {
    return deserialize_integral<int8_t>(input);
}
template<typename Input>
uint8_t deserialize(Input& input, rpc::type<uint8_t>) {
    return deserialize_integral<uint8_t>(input);
}
template<typename Input>
int16_t deserialize(Input& input, rpc::type<int16_t>) {
    return deserialize_integral<int16_t>(input);
}
template<typename Input>
uint16_t deserialize(Input& input, rpc::type<uint16_t>) {
    return deserialize_integral<uint16_t>(input);
}
template<typename Input>
int32_t deserialize(Input& input, rpc::type<int32_t>) {
    return deserialize_integral<int32_t>(input);
}
template<typename Input>
uint32_t deserialize(Input& input, rpc::type<uint32_t>) {
    return deserialize_integral<uint32_t>(input);
}
template<typename Input>
int64_t deserialize(Input& input, rpc::type<int64_t>) {
    return deserialize_integral<int64_t>(input);
}
template<typename Input>
uint64_t deserialize(Input& input, rpc::type<uint64_t>) {
    return deserialize_integral<uint64_t>(input);
}

template<typename Output>
void serialize(Output& output, bool data) {
    serialize(output, uint8_t(data));
}
template<typename Output>
void serialize(Output& output, int8_t data) {
    serialize_integral(output, data);
}
template<typename Output>
void serialize(Output& output, uint8_t data) {
    serialize_integral(output, data);
}
template<typename Output>
void serialize(Output& output, int16_t data) {
    serialize_integral(output, data);
}
template<typename Output>
void serialize(Output& output, uint16_t data) {
    serialize_integral(output, data);
}
template<typename Output>
void serialize(Output& output, int32_t data) {
    serialize_integral(output, data);
}
template<typename Output>
void serialize(Output& output, uint32_t data) {
    serialize_integral(output, data);
}
template<typename Output>
void serialize(Output& output, int64_t data) {
    serialize_integral(output, data);
}
template<typename Output>
void serialize(Output& output, uint64_t data) {
    serialize_integral(output, data);
}
template<typename Output>
void safe_serialize_as_uint32(Output& output, uint64_t data);

// For vectors

template<typename T, typename Output>
inline void serialize(Output& out, const std::vector<T>& v);
template<typename T, typename Input>
inline std::vector<T> deserialize(Input& in, rpc::type<std::vector<T>>);

template<typename K, typename V, typename Output>
inline void serialize(Output& out, const std::map<K, V>& v);
template<typename K, typename V, typename Input>
inline std::map<K, V> deserialize(Input& in, rpc::type<std::map<K, V>>);
template<typename T>
size_type get_sizeof(const T& obj);
// For sstring
template<typename Output>
void serialize(Output& out, const sstring& v);
template<typename Input>
sstring deserialize(Input& in, rpc::type<sstring>);

template<typename T>
void set_size(rpc::simple_output_stream& os, const T& obj);

template<typename T>
void set_size(rpc::measuring_output_stream& os, const T& obj);
}

/*
 * Import the auto generated forward decleration code
 */

#include "serializer.inc.hh"
