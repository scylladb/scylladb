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
namespace ser {

template<typename T>
void set_size(seastar::simple_output_stream& os, const T& obj) {
    serialize(os, get_sizeof(obj));
}

template<typename T>
void set_size(seastar::measuring_output_stream& os, const T& obj) {
    serialize(os, uint32_t(0));
}

template<typename T>
void set_size(bytes_ostream& os, const T& obj) {
    serialize(os, get_sizeof(obj));
}


template<typename Output>
void safe_serialize_as_uint32(Output& out, uint64_t data) {
    if (data > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error("Size is too big for serialization");
    }
    serialize(out, uint32_t(data));
}

template<typename T>
constexpr bool can_serialize_fast() {
    return !std::is_same<T, bool>::value && std::is_integral<T>::value && (sizeof(T) == 1 || __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__);
}

template<bool Fast, typename T>
struct serialize_array_helper;

template<typename T>
struct serialize_array_helper<true, T> {
    template<typename Container, typename Output>
    static void doit(Output& out, const Container& v) {
        out.write(reinterpret_cast<const char*>(v.data()), v.size() * sizeof(T));
    }
};

template<typename T>
struct serialize_array_helper<false, T> {
    template<typename Container, typename Output>
    static void doit(Output& out, const Container& v) {
        for (auto&& e : v) {
            serialize(out, e);
        }
    }
};

template<typename T, typename Container, typename Output>
static inline void serialize_array(Output& out, const Container& v) {
    serialize_array_helper<can_serialize_fast<T>(), T>::doit(out, v);
}

template<typename Container>
struct container_traits;

template<typename T>
struct container_traits<std::vector<T>> {
    struct back_emplacer {
        std::vector<T>& c;
        back_emplacer(std::vector<T>& c_) : c(c_) {}
        void operator()(T&& v) {
            c.emplace_back(std::move(v));
        }
    };
    void resize(std::vector<T>& c, size_t size) {
        c.resize(size);
    }
};

template<typename T, size_t N>
struct container_traits<std::array<T, N>> {
    struct back_emplacer {
        std::array<T, N>& c;
        size_t idx = 0;
        back_emplacer(std::array<T, N>& c_) : c(c_) {}
        void operator()(T&& v) {
            c[idx++] = std::move(v);
        }
    };
    void resize(std::array<T, N>& c, size_t size) {}
};

template<bool Fast, typename T>
struct deserialize_array_helper;

template<typename T>
struct deserialize_array_helper<true, T> {
    template<typename Input, typename Container>
    static void doit(Input& in, Container& v, size_t sz) {
        container_traits<Container> t;
        t.resize(v, sz);
        in.read(reinterpret_cast<char*>(v.data()), v.size() * sizeof(T));
    }
};

template<typename T>
struct deserialize_array_helper<false, T> {
    template<typename Input, typename Container>
    static void doit(Input& in, Container& v, size_t sz) {
        typename container_traits<Container>::back_emplacer be(v);
        while (sz--) {
            be(deserialize(in, boost::type<T>()));
        }
    }
};

template<typename T, typename Input, typename Container>
static inline void deserialize_array(Input& in, Container& v, size_t sz) {
    deserialize_array_helper<can_serialize_fast<T>(), T>::doit(in, v, sz);
}

template<typename T, typename Output>
inline void serialize(Output& out, const std::vector<T>& v) {
    safe_serialize_as_uint32(out, v.size());
    serialize_array<T>(out, v);
}
template<typename T, typename Input>
inline std::vector<T> deserialize(Input& in, boost::type<std::vector<T>>) {
    auto sz = deserialize(in, boost::type<uint32_t>());
    std::vector<T> v;
    v.reserve(sz);
    deserialize_array<T>(in, v, sz);
    return v;
}

template<typename T, typename Ratio, typename Output>
inline void serialize(Output& out, const std::chrono::duration<T, Ratio>& d) {
    serialize(out, d.count());
};

template<typename T, typename Ratio, typename Input>
inline std::chrono::duration<T, Ratio> deserialize(Input& in, boost::type<std::chrono::duration<T, Ratio>>) {
    return std::chrono::duration<T, Ratio>(deserialize(in, boost::type<T>()));
};


template<size_t N, typename T, typename Output>
inline void serialize(Output& out, const std::array<T, N>& v) {
    serialize_array<T>(out, v);
}
template<size_t N, typename T, typename Input>
inline std::array<T, N> deserialize(Input& in, boost::type<std::array<T, N>>) {
    std::array<T, N> v;
    deserialize_array<T>(in, v, N);
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
inline std::map<K, V> deserialize(Input& in, boost::type<std::map<K, V>>) {
    auto sz = deserialize(in, boost::type<uint32_t>());
    std::map<K, V> m;
    while (sz--) {
        K k = deserialize(in, boost::type<K>());
        V v = deserialize(in, boost::type<V>());
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
bytes deserialize(Input& in, boost::type<bytes>) {
    auto sz = deserialize(in, boost::type<uint32_t>());
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
bytes_ostream deserialize(Input& in, boost::type<bytes_ostream>) {
    bytes_ostream v;
    v.write(deserialize(in, boost::type<bytes>()));
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
inline std::experimental::optional<T> deserialize(Input& in, boost::type<std::experimental::optional<T>>) {
    std::experimental::optional<T> v;
    auto b = deserialize(in, boost::type<bool>());
    if (b) {
        v = deserialize(in, boost::type<T>());
    }
    return v;
}

template<typename Output>
void serialize(Output& out, const sstring& v) {
    safe_serialize_as_uint32(out, uint32_t(v.size()));
    out.write(v.begin(), v.size());
}
template<typename Input>
sstring deserialize(Input& in, boost::type<sstring>) {
    auto sz = deserialize(in, boost::type<uint32_t>());
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
inline std::unique_ptr<T> deserialize(Input& in, boost::type<std::unique_ptr<T>>) {
    std::unique_ptr<T> v;
    auto b = deserialize(in, boost::type<bool>());
    if (b) {
        v = std::make_unique<T>(deserialize(in, boost::type<T>()));
    }
    return v;
}

template<typename Clock, typename Duration, typename Output>
inline void serialize(Output& out, const std::chrono::time_point<Clock, Duration>& v) {
    serialize(out, uint64_t(v.time_since_epoch().count()));
}
template<typename Clock, typename Duration, typename Input>
inline std::chrono::time_point<Clock, Duration> deserialize(Input& in, boost::type<std::chrono::time_point<Clock, Duration>>) {
    return typename Clock::time_point(Duration(deserialize(in, boost::type<uint64_t>())));
}

template<typename Enum, typename Output>
inline void serialize(Output& out, const enum_set<Enum>& v) {
    serialize(out, uint64_t(v.mask()));
}
template<typename Enum, typename Input>
inline enum_set<Enum> deserialize(Input& in, boost::type<enum_set<Enum>>) {
    return enum_set<Enum>::from_mask(deserialize(in, boost::type<uint64_t>()));
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

template<typename Buffer, typename T>
Buffer serialize_to_buffer(const T& v, size_t head_space) {
    seastar::measuring_output_stream measure;
    ser::serialize(measure, v);
    Buffer ret(typename Buffer::initialized_later(), measure.size() + head_space);
    seastar::simple_output_stream out(reinterpret_cast<char*>(ret.begin()), head_space);
    ser::serialize(out, v);
    return ret;
}

template<typename T, typename Buffer>
T deserialize_from_buffer(const Buffer& buf, boost::type<T> type, size_t head_space) {
    seastar::simple_input_stream in(reinterpret_cast<const char*>(buf.begin() + head_space), buf.size() - head_space);
    return deserialize(in, std::move(type));
}

template<typename Output, typename ...T>
void serialize(Output& out, const boost::variant<T...>& v) {}

template<typename Input, typename ...T>
boost::variant<T...> deserialize(Input& in, boost::type<boost::variant<T...>>) {
    return boost::variant<T...>();
}


template<typename Output>
void serialize(Output& out, const unknown_variant_type& v) {
    out.write(v.data.begin(), v.data.size());
}
template<typename Input>
unknown_variant_type deserialize(Input& in, boost::type<unknown_variant_type>) {
    auto size = deserialize(in, boost::type<size_type>());
    auto index = deserialize(in, boost::type<size_type>());
    auto sz = size - sizeof(size_type) * 2;
    sstring v(sstring::initialized_later(), sz);
    in.read(v.begin(), sz);
    return unknown_variant_type{index, std::move(v)};
}
}
