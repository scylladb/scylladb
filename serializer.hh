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
#include <experimental/optional>
#include "enum_set.hh"
#include "core/simple-stream.hh"

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
int8_t deserialize(Input& input, boost::type<int8_t>) {
    return deserialize_integral<int8_t>(input);
}
template<typename Input>
uint8_t deserialize(Input& input, boost::type<uint8_t>) {
    return deserialize_integral<uint8_t>(input);
}
template<typename Input>
bool deserialize(Input& input, boost::type<bool>) {
    return deserialize(input, boost::type<uint8_t>());
}
template<typename Input>
int16_t deserialize(Input& input, boost::type<int16_t>) {
    return deserialize_integral<int16_t>(input);
}
template<typename Input>
uint16_t deserialize(Input& input, boost::type<uint16_t>) {
    return deserialize_integral<uint16_t>(input);
}
template<typename Input>
int32_t deserialize(Input& input, boost::type<int32_t>) {
    return deserialize_integral<int32_t>(input);
}
template<typename Input>
uint32_t deserialize(Input& input, boost::type<uint32_t>) {
    return deserialize_integral<uint32_t>(input);
}
template<typename Input>
int64_t deserialize(Input& input, boost::type<int64_t>) {
    return deserialize_integral<int64_t>(input);
}
template<typename Input>
uint64_t deserialize(Input& input, boost::type<uint64_t>) {
    return deserialize_integral<uint64_t>(input);
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
void serialize(Output& output, bool data) {
    serialize(output, uint8_t(data));
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
inline std::vector<T> deserialize(Input& in, boost::type<std::vector<T>>);

template<typename K, typename V, typename Output>
inline void serialize(Output& out, const std::map<K, V>& v);
template<typename K, typename V, typename Input>
inline std::map<K, V> deserialize(Input& in, boost::type<std::map<K, V>>);
template<typename T>
size_type get_sizeof(const T& obj);
// For sstring
template<typename Output>
void serialize(Output& out, const sstring& v);
template<typename Input>
sstring deserialize(Input& in, boost::type<sstring>);
// For optional
template<typename T, typename Output>
inline void serialize(Output& out, const std::experimental::optional<T>& v);
template<typename T, typename Input>
inline std::experimental::optional<T> deserialize(Input& in, boost::type<std::experimental::optional<T>>);
template<typename T, typename Output>
// For unique_ptr
inline void serialize(Output& out, const std::unique_ptr<T>& v);
template<typename T, typename Input>
inline std::unique_ptr<T> deserialize(Input& in, boost::type<std::unique_ptr<T>>);
// For time_point
template<typename Clock, typename Duration, typename Output>
inline void serialize(Output& out, const std::chrono::time_point<Clock, Duration>& v);
template<typename Clock, typename Duration, typename Input>
inline std::chrono::time_point<Clock, Duration> deserialize(Input& in, boost::type<std::chrono::time_point<Clock, Duration>>);
// For enum_set
template<typename Enum, typename Output>
inline void serialize(Output& out, const enum_set<Enum>& v);
template<typename Enum, typename Input>
inline enum_set<Enum> deserialize(Input& in, boost::type<enum_set<Enum>>);
// For bytes/bytes_view
template<typename Output>
void serialize(Output& out, const bytes_view& v);
template<typename Output>
void serialize(Output& out, const bytes& v);
template<typename Input>
bytes deserialize(Input& in, boost::type<bytes>);
// For bytes_ostream
template<typename Output>
void serialize(Output& out, const bytes_ostream& v);
template<typename Input>
bytes_ostream deserialize(Input& in, boost::type<bytes_ostream>);

template<typename T>
void set_size(seastar::simple_output_stream& os, const T& obj);

template<typename T>
void set_size(seastar::measuring_output_stream& os, const T& obj);

template<typename Buffer, typename T>
Buffer serialize_to_buffer(const T& v, size_t head_space = 0);
}

/*
 * Import the auto generated forward decleration code
 */
