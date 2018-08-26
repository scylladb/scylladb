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
#include <seastar/util/bool_class.hh>
#include <boost/range/algorithm/for_each.hpp>

namespace ser {

template<typename T>
void set_size(seastar::measuring_output_stream& os, const T& obj) {
    serialize(os, uint32_t(0));
}

template<typename Stream, typename T>
void set_size(Stream& os, const T& obj) {
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

template<typename T>
struct container_traits<utils::chunked_vector<T>> {
    struct back_emplacer {
        utils::chunked_vector<T>& c;
        back_emplacer(utils::chunked_vector<T>& c_) : c(c_) {}
        void operator()(T&& v) {
            c.emplace_back(std::move(v));
        }
    };
    void resize(utils::chunked_vector<T>& c, size_t size) {
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
    template<typename Input>
    static void skip(Input& in, size_t sz) {
        in.skip(sz * sizeof(T));
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
    template<typename Input>
    static void skip(Input& in, size_t sz) {
        while (sz--) {
            serializer<T>::skip(in);
        }
    }
};

template<typename T, typename Input, typename Container>
static inline void deserialize_array(Input& in, Container& v, size_t sz) {
    deserialize_array_helper<can_serialize_fast<T>(), T>::doit(in, v, sz);
}

template<typename T, typename Input>
static inline void skip_array(Input& in, size_t sz) {
    deserialize_array_helper<can_serialize_fast<T>(), T>::skip(in, sz);
}

template<typename T>
struct serializer<std::vector<T>> {
    template<typename Input>
    static std::vector<T> read(Input& in) {
        auto sz = deserialize(in, boost::type<uint32_t>());
        std::vector<T> v;
        v.reserve(sz);
        deserialize_array<T>(in, v, sz);
        return v;
    }
    template<typename Output>
    static void write(Output& out, const std::vector<T>& v) {
        safe_serialize_as_uint32(out, v.size());
        serialize_array<T>(out, v);
    }
    template<typename Input>
    static void skip(Input& in) {
        auto sz = deserialize(in, boost::type<uint32_t>());
        skip_array<T>(in, sz);
    }
};

template<typename T>
struct serializer<utils::chunked_vector<T>> {
    template<typename Input>
    static utils::chunked_vector<T> read(Input& in) {
        auto sz = deserialize(in, boost::type<uint32_t>());
        utils::chunked_vector<T> v;
        v.reserve(sz);
        deserialize_array<T>(in, v, sz);
        return v;
    }
    template<typename Output>
    static void write(Output& out, const utils::chunked_vector<T>& v) {
        safe_serialize_as_uint32(out, v.size());
        serialize_array<T>(out, v);
    }
    template<typename Input>
    static void skip(Input& in) {
        auto sz = deserialize(in, boost::type<uint32_t>());
        skip_array<T>(in, sz);
    }
};

template<typename T, typename Ratio>
struct serializer<std::chrono::duration<T, Ratio>> {
    template<typename Input>
    static std::chrono::duration<T, Ratio> read(Input& in) {
        return std::chrono::duration<T, Ratio>(deserialize(in, boost::type<T>()));
    }
    template<typename Output>
    static void write(Output& out, const std::chrono::duration<T, Ratio>& d) {
        serialize(out, d.count());
    }
    template<typename Input>
    static void skip(Input& in) {
        read(in);
    }
};

template<typename Clock, typename Duration>
struct serializer<std::chrono::time_point<Clock, Duration>> {
    using value_type = std::chrono::time_point<Clock, Duration>;

    template<typename Input>
    static value_type read(Input& in) {
        return typename Clock::time_point(Duration(deserialize(in, boost::type<uint64_t>())));
    }
    template<typename Output>
    static void write(Output& out, const value_type& v) {
        serialize(out, uint64_t(v.time_since_epoch().count()));
    }
    template<typename Input>
    static void skip(Input& in) {
        read(in);
    }
};

template<size_t N, typename T>
struct serializer<std::array<T, N>> {
    template<typename Input>
    static std::array<T, N> read(Input& in) {
        std::array<T, N> v;
        deserialize_array<T>(in, v, N);
        return v;
    }
    template<typename Output>
    static void write(Output& out, const std::array<T, N>& v) {
        serialize_array<T>(out, v);
    }
    template<typename Input>
    static void skip(Input& in) {
        skip_array<T>(in, N);
    }
};

template<typename K, typename V>
struct serializer<std::map<K, V>> {
    template<typename Input>
    static std::map<K, V> read(Input& in) {
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
    static void write(Output& out, const std::map<K, V>& v) {
        safe_serialize_as_uint32(out, v.size());
        for (auto&& e : v) {
            serialize(out, e.first);
            serialize(out, e.second);
        }
    }
    template<typename Input>
    static void skip(Input& in) {
        auto sz = deserialize(in, boost::type<uint32_t>());
        while (sz--) {
            serializer<K>::skip(in);
            serializer<V>::skip(in);
        }
    }
};

template<typename K, typename V>
struct serializer<std::unordered_map<K, V>> {
    template<typename Input>
    static std::unordered_map<K, V> read(Input& in) {
        auto sz = deserialize(in, boost::type<uint32_t>());
        std::unordered_map<K, V> m;
        m.reserve(sz);
        while (sz--) {
            auto k = deserialize(in, boost::type<K>());
            auto v = deserialize(in, boost::type<V>());
            m.emplace(std::move(k), std::move(v));
        }
        return m;
    }
    template<typename Output>
    static void write(Output& out, const std::unordered_map<K, V>& v) {
        safe_serialize_as_uint32(out, v.size());
        for (auto&& e : v) {
            serialize(out, e.first);
            serialize(out, e.second);
        }
    }
    template<typename Input>
    static void skip(Input& in) {
        auto sz = deserialize(in, boost::type<uint32_t>());
        while (sz--) {
            serializer<K>::skip(in);
            serializer<V>::skip(in);
        }
    }
};

template<typename Tag>
struct serializer<bool_class<Tag>> {
    template<typename Input>
    static bool_class<Tag> read(Input& in) {
        return bool_class<Tag>(deserialize(in, boost::type<bool>()));
    }

    template<typename Output>
    static void write(Output& out, bool_class<Tag> v) {
        serialize(out, bool(v));
    }

    template<typename Input>
    static void skip(Input& in) {
        read(in);
    }
};

template<typename Stream>
class deserialized_bytes_proxy {
    Stream _stream;

    template<typename OtherStream>
    friend class deserialized_bytes_proxy;
public:
    explicit deserialized_bytes_proxy(Stream stream)
        : _stream(std::move(stream)) { }

    template<typename OtherStream, typename = std::enable_if_t<std::is_convertible_v<OtherStream, Stream>>>
    deserialized_bytes_proxy(deserialized_bytes_proxy<OtherStream> proxy)
        : _stream(std::move(proxy._stream)) { }

    auto view() const {
      if constexpr (std::is_same_v<Stream, simple_input_stream>) {
        return bytes_view(reinterpret_cast<const int8_t*>(_stream.begin()), _stream.size());
      } else {
        using iterator_type = typename Stream::iterator_type ;
        GCC6_CONCEPT(static_assert(FragmentRange<buffer_view<iterator_type>>));
        return seastar::with_serialized_stream(_stream, seastar::make_visitor(
            [&] (typename seastar::memory_input_stream<iterator_type >::simple stream) {
                return buffer_view<iterator_type>(bytes_view(reinterpret_cast<const int8_t*>(stream.begin()),
                                                        stream.size()));
            },
            [&] (typename seastar::memory_input_stream<iterator_type >::fragmented stream) {
                return buffer_view<iterator_type>(bytes_view(reinterpret_cast<const int8_t*>(stream.first_fragment_data()),
                                                        stream.first_fragment_size()),
                                             stream.size(), stream.fragment_iterator());
            }
        ));
      }
    }

    [[gnu::always_inline]]
    operator bytes() && {
        bytes v(bytes::initialized_later(), _stream.size());
        _stream.read(reinterpret_cast<char*>(v.begin()), _stream.size());
        return v;
    }

    [[gnu::always_inline]]
    operator managed_bytes() && {
        managed_bytes v(managed_bytes::initialized_later(), _stream.size());
        _stream.read(reinterpret_cast<char*>(v.begin()), _stream.size());
        return v;
    }

    [[gnu::always_inline]]
    operator bytes_ostream() && {
        bytes_ostream v;
        _stream.copy_to(v);
        return v;
    }
};

template<>
struct serializer<bytes> {
    template<typename Input>
    static deserialized_bytes_proxy<Input> read(Input& in) {
        auto sz = deserialize(in, boost::type<uint32_t>());
        return deserialized_bytes_proxy<Input>(in.read_substream(sz));
    }
    template<typename Output>
    static void write(Output& out, bytes_view v) {
        safe_serialize_as_uint32(out, uint32_t(v.size()));
        out.write(reinterpret_cast<const char*>(v.begin()), v.size());
    }
    template<typename Output>
    static void write(Output& out, const bytes& v) {
        write(out, static_cast<bytes_view>(v));
    }
    template<typename Output>
    static void write(Output& out, const managed_bytes& v) {
        write(out, static_cast<bytes_view>(v));
    }
    template<typename Output>
    static void write(Output& out, const bytes_ostream& v) {
        safe_serialize_as_uint32(out, uint32_t(v.size()));
        for (bytes_view frag : v.fragments()) {
            out.write(reinterpret_cast<const char*>(frag.begin()), frag.size());
        }
    }
    template<typename Output, typename FragmentedBuffer>
    GCC6_CONCEPT(requires FragmentRange<FragmentedBuffer>)
    static void write_fragmented(Output& out, FragmentedBuffer&& fragments) {
        safe_serialize_as_uint32(out, uint32_t(fragments.size_bytes()));
        using boost::range::for_each;
        for_each(fragments, [&out] (bytes_view frag) {
            out.write(reinterpret_cast<const char*>(frag.begin()), frag.size());
        });
    }
    template<typename Input>
    static void skip(Input& in) {
        auto sz = deserialize(in, boost::type<uint32_t>());
        in.skip(sz);
    }
};

template<typename Output>
void serialize(Output& out, const bytes_view& v) {
    serializer<bytes>::write(out, v);
}
template<typename Output>
void serialize(Output& out, const managed_bytes& v) {
    serializer<bytes>::write(out, v);
}
template<typename Output>
void serialize(Output& out, const bytes_ostream& v) {
    serializer<bytes>::write(out, v);
}
template<typename Output, typename FragmentedBuffer>
GCC6_CONCEPT(requires FragmentRange<FragmentedBuffer>)
void serialize_fragmented(Output& out, FragmentedBuffer&& v) {
    serializer<bytes>::write_fragmented(out, std::forward<FragmentedBuffer>(v));
}

template<typename T>
struct serializer<std::experimental::optional<T>> {
    template<typename Input>
    static std::experimental::optional<T> read(Input& in) {
        std::experimental::optional<T> v;
        auto b = deserialize(in, boost::type<bool>());
        if (b) {
            v = deserialize(in, boost::type<T>());
        }
        return v;
    }
    template<typename Output>
    static void write(Output& out, const std::experimental::optional<T>& v) {
        serialize(out, bool(v));
        if (v) {
            serialize(out, v.value());
        }
    }
    template<typename Input>
    static void skip(Input& in) {
        auto present = deserialize(in, boost::type<bool>());
        if (present) {
            serializer<T>::skip(in);
        }
    }
};

template<>
struct serializer<sstring> {
    template<typename Input>
    static sstring read(Input& in) {
        auto sz = deserialize(in, boost::type<uint32_t>());
        sstring v(sstring::initialized_later(), sz);
        in.read(v.begin(), sz);
        return v;
    }
    template<typename Output>
    static void write(Output& out, const sstring& v) {
        safe_serialize_as_uint32(out, uint32_t(v.size()));
        out.write(v.begin(), v.size());
    }
    template<typename Input>
    static void skip(Input& in) {
        in.skip(deserialize(in, boost::type<size_type>()));
    }
};

template<typename T>
struct serializer<std::unique_ptr<T>> {
    template<typename Input>
    static std::unique_ptr<T> read(Input& in) {
        std::unique_ptr<T> v;
        auto b = deserialize(in, boost::type<bool>());
        if (b) {
            v = std::make_unique<T>(deserialize(in, boost::type<T>()));
        }
        return v;
    }
    template<typename Output>
    static void write(Output& out, const std::unique_ptr<T>& v) {
        serialize(out, bool(v));
        if (v) {
            serialize(out, *v);
        }
    }
    template<typename Input>
    static void skip(Input& in) {
        auto present = deserialize(in, boost::type<bool>());
        if (present) {
            serializer<T>::skip(in);
        }
    }
};

template<typename Enum>
struct serializer<enum_set<Enum>> {
    template<typename Input>
    static enum_set<Enum> read(Input& in) {
        return enum_set<Enum>::from_mask(deserialize(in, boost::type<uint64_t>()));
    }
    template<typename Output>
    static void write(Output& out, enum_set<Enum> v) {
        serialize(out, uint64_t(v.mask()));
    }
    template<typename Input>
    static void skip(Input& in) {
        read(in);
    }
};

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
    seastar::simple_output_stream out(reinterpret_cast<char*>(ret.begin()), ret.size(), head_space);
    ser::serialize(out, v);
    return ret;
}

template<typename T, typename Buffer>
T deserialize_from_buffer(const Buffer& buf, boost::type<T> type, size_t head_space) {
    seastar::simple_input_stream in(reinterpret_cast<const char*>(buf.begin() + head_space), buf.size() - head_space);
    return deserialize(in, std::move(type));
}

inline
utils::input_stream as_input_stream(bytes_view b) {
    return utils::input_stream::simple(reinterpret_cast<const char*>(b.begin()), b.size());
}

inline
utils::input_stream as_input_stream(const bytes_ostream& b) {
    if (b.is_linearized()) {
        return as_input_stream(b.view());
    }
    return utils::input_stream::fragmented(b.fragments().begin(), b.size());
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
    return seastar::with_serialized_stream(in, [] (auto& in) {
        auto size = deserialize(in, boost::type<size_type>());
        auto index = deserialize(in, boost::type<size_type>());
        auto sz = size - sizeof(size_type) * 2;
        sstring v(sstring::initialized_later(), sz);
        in.read(v.begin(), sz);
        return unknown_variant_type{ index, std::move(v) };
    });
}
}
