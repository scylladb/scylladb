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

#include "serializer.hh"
#include "serializer.inc.impl.hh"

namespace ser {

template<typename T>
void set_size(seastar::simple_output_stream& os, const T& obj) {
    serialize(os, get_sizeof(obj));
}

template<typename T>
void set_size(seastar::measuring_output_stream& os, const T& obj) {
    serialize(os, uint32_t(0));
}


template<typename Output>
void safe_serialize_as_uint32(Output& out, uint64_t data) {
    if (data > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error("Size is too big for serialization");
    }
    serialize(out, uint32_t(data));
}
template<typename T, typename Output>
inline void serialize(Output& out, const std::vector<T>& v) {
    safe_serialize_as_uint32(out, v.size());
    for (auto&& e : v) {
        serialize(out, e);
    }
}
template<typename T, typename Input>
inline std::vector<T> deserialize(Input& in, rpc::type<std::vector<T>>) {
    auto sz = deserialize(in, rpc::type<uint32_t>());
    std::vector<T> v;
    v.reserve(sz);
    while (sz--) {
        v.push_back(deserialize(in, rpc::type<T>()));
    }
    return v;
}

template<typename K, typename V, typename Output>
inline void serialize(Output& out, const std::map<K, V>& v) {
    safe_serialize_as_uint32(out, v.size());
    for (auto&& e : v) {
        serialize(out, e.first);
        serialize(out, e.second);
    }
}
template<typename K, typename V, typename Input>
inline std::map<K, V> deserialize(Input& in, rpc::type<std::map<K, V>>) {
    auto sz = deserialize(in, rpc::type<uint32_t>());
    std::map<K, V> m;
    while (sz--) {
        K k = deserialize(in, rpc::type<K>());
        V v = deserialize(in, rpc::type<V>());
        m[k] = v;
    }
    return m;
}

template<typename Output>
void serialize(Output& out, const bytes_view& v) {
    safe_serialize_as_uint32(out, uint32_t(v.size()));
    out.write(reinterpret_cast<const char*>(v.begin()), v.size());
}
template<typename Output>
void serialize(Output& out, const managed_bytes& v) {
    safe_serialize_as_uint32(out, uint32_t(v.size()));
    out.write(reinterpret_cast<const char*>(v.begin()), v.size());
}
template<typename Output>
void serialize(Output& out, const bytes& v) {
    safe_serialize_as_uint32(out, uint32_t(v.size()));
    out.write(reinterpret_cast<const char*>(v.begin()), v.size());
}
template<typename Input>
bytes deserialize(Input& in, rpc::type<bytes>) {
    auto sz = deserialize(in, rpc::type<uint32_t>());
    bytes v(bytes::initialized_later(), sz);
    in.read(reinterpret_cast<char*>(v.begin()), sz);
    return v;
}

template<typename Output>
void serialize(Output& out, const bytes_ostream& v) {
    safe_serialize_as_uint32(out, uint32_t(v.size()));
    for (bytes_view frag : v.fragments()) {
        out.write(reinterpret_cast<const char*>(frag.begin()), frag.size());
    }
}
template<typename Input>
bytes_ostream deserialize(Input& in, rpc::type<bytes_ostream>) {
    bytes_ostream v;
    v.write(deserialize(in, rpc::type<bytes>()));
    return v;
}

template<typename T, typename Output>
inline void serialize(Output& out, const std::experimental::optional<T>& v) {
    serialize(out, bool(v));
    if (v) {
        serialize(out, v.value());
    }
}
template<typename T, typename Input>
inline std::experimental::optional<T> deserialize(Input& in, rpc::type<std::experimental::optional<T>>) {
    std::experimental::optional<T> v;
    auto b = deserialize(in, rpc::type<bool>());
    if (b) {
        v = deserialize(in, rpc::type<T>());
    }
    return v;
}

template<typename Output>
void serialize(Output& out, const sstring& v) {
    safe_serialize_as_uint32(out, uint32_t(v.size()));
    out.write(v.begin(), v.size());
}
template<typename Input>
sstring deserialize(Input& in, rpc::type<sstring>) {
    auto sz = deserialize(in, rpc::type<uint32_t>());
    sstring v(sstring::initialized_later(), sz);
    in.read(v.begin(), sz);
    return v;
}

template<typename T, typename Output>
inline void serialize(Output& out, const std::unique_ptr<T>& v) {
    serialize(out, bool(v));
    if (v) {
        serialize(out, *v);
    }
}
template<typename T, typename Input>
inline std::unique_ptr<T> deserialize(Input& in, rpc::type<std::unique_ptr<T>>) {
    std::unique_ptr<T> v;
    auto b = deserialize(in, rpc::type<bool>());
    if (b) {
        v = std::make_unique<T>(deserialize(in, rpc::type<T>()));
    }
    return v;
}

template<typename Clock, typename Duration, typename Output>
inline void serialize(Output& out, const std::chrono::time_point<Clock, Duration>& v) {
    serialize(out, uint64_t(v.time_since_epoch().count()));
}
template<typename Clock, typename Duration, typename Input>
inline std::chrono::time_point<Clock, Duration> deserialize(Input& in, rpc::type<std::chrono::time_point<Clock, Duration>>) {
    return typename Clock::time_point(Duration(deserialize(in, rpc::type<uint64_t>())));
}

template<typename Enum, typename Output>
inline void serialize(Output& out, const enum_set<Enum>& v) {
    serialize(out, uint64_t(v.mask()));
}
template<typename Enum, typename Input>
inline enum_set<Enum> deserialize(Input& in, rpc::type<enum_set<Enum>>) {
    return enum_set<Enum>::from_mask(deserialize(in, rpc::type<uint64_t>()));
}

template<typename T>
size_type get_sizeof(const T& obj) {
    seastar::measuring_output_stream ms;
    serialize(ms, obj);
    auto size = ms.size();
    if (size > std::numeric_limits<size_type>::max()) {
        throw std::runtime_error("Object is too big for get_sizeof");
    }
    return size;
}

}
