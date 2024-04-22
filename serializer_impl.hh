/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <list>
#include <unordered_set>

#include "serializer.hh"
#include "enum_set.hh"
#include "utils/chunked_vector.hh"
#include "utils/input_stream.hh"
#include <seastar/util/bool_class.hh>
#include "utils/small_vector.hh"
#include <absl/container/btree_set.h>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/on_internal_error.hh>
#include "log.hh"

namespace seastar {
extern logger seastar_logger;
}

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
struct container_traits<absl::btree_set<T>> {
    struct back_emplacer {
        absl::btree_set<T>& c;
        back_emplacer(absl::btree_set<T>& c_) : c(c_) {}
        void operator()(T&& v) {
            c.emplace(std::move(v));
        }
    };
};

template<typename T, typename... Args>
struct container_traits<std::unordered_set<T, Args...>> {
    struct back_emplacer {
        std::unordered_set<T, Args...>& c;
        back_emplacer(std::unordered_set<T, Args...>& c_) : c(c_) {}
        void operator()(T&& v) {
            c.emplace(std::move(v));
        }
    };
};

template<typename T>
struct container_traits<std::list<T>> {
    struct back_emplacer {
        std::list<T>& c;
        back_emplacer(std::list<T>& c_) : c(c_) {}
        void operator()(T&& v) {
            c.emplace_back(std::move(v));
        }
    };
    void resize(std::list<T>& c, size_t size) {
        c.resize(size);
    }
};

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
struct container_traits<utils::small_vector<T, N>> {
    struct back_emplacer {
        utils::small_vector<T, N>& c;
        back_emplacer(utils::small_vector<T, N>& c_) : c(c_) {}
        void operator()(T&& v) {
            c.emplace_back(std::move(v));
        }
    };
    void resize(utils::small_vector<T, N>& c, size_t size) {
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

namespace idl::serializers::internal {

template<typename Vector>
struct vector_serializer {
    using value_type = typename Vector::value_type;
    template<typename Input>
    static Vector read(Input& in) {
        auto sz = deserialize(in, boost::type<uint32_t>());
        Vector v;
        v.reserve(sz);
        deserialize_array<value_type>(in, v, sz);
        return v;
    }
    template<typename Output>
    static void write(Output& out, const Vector& v) {
        safe_serialize_as_uint32(out, v.size());
        serialize_array<value_type>(out, v);
    }
    template<typename Input>
    static void skip(Input& in) {
        auto sz = deserialize(in, boost::type<uint32_t>());
        skip_array<value_type>(in, sz);
    }
};

}

template<typename T>
struct serializer<std::list<T>> {
    template<typename Input>
    static std::list<T> read(Input& in) {
        auto sz = deserialize(in, boost::type<uint32_t>());
        std::list<T> v;
        deserialize_array_helper<false, T>::doit(in, v, sz);
        return v;
    }
    template<typename Output>
    static void write(Output& out, const std::list<T>& v) {
        safe_serialize_as_uint32(out, v.size());
        serialize_array_helper<false, T>::doit(out, v);
    }
    template<typename Input>
    static void skip(Input& in) {
        auto sz = deserialize(in, boost::type<uint32_t>());
        skip_array<T>(in, sz);
    }
};

template<typename T>
struct serializer<absl::btree_set<T>> {
    template<typename Input>
    static absl::btree_set<T> read(Input& in) {
        auto sz = deserialize(in, boost::type<uint32_t>());
        absl::btree_set<T> v;
        deserialize_array_helper<false, T>::doit(in, v, sz);
        return v;
    }
    template<typename Output>
    static void write(Output& out, const absl::btree_set<T>& v) {
        safe_serialize_as_uint32(out, v.size());
        serialize_array_helper<false, T>::doit(out, v);
    }
    template<typename Input>
    static void skip(Input& in) {
        auto sz = deserialize(in, boost::type<uint32_t>());
        skip_array<T>(in, sz);
    }
};

template<typename T, typename... Args>
struct serializer<std::unordered_set<T, Args...>> {
    template<typename Input>
    static std::unordered_set<T, Args...> read(Input& in) {
        auto sz = deserialize(in, boost::type<uint32_t>());
        std::unordered_set<T, Args...> v;
        v.reserve(sz);
        deserialize_array_helper<false, T>::doit(in, v, sz);
        return v;
    }
    template<typename Output>
    static void write(Output& out, const std::unordered_set<T, Args...>& v) {
        safe_serialize_as_uint32(out, v.size());
        serialize_array_helper<false, T>::doit(out, v);
    }
    template<typename Input>
    static void skip(Input& in) {
        auto sz = deserialize(in, boost::type<uint32_t>());
        skip_array<T>(in, sz);
    }
};

template<typename T>
struct serializer<std::vector<T>>
    : idl::serializers::internal::vector_serializer<std::vector<T>>
{ };

template<typename T>
struct serializer<utils::chunked_vector<T>>
    : idl::serializers::internal::vector_serializer<utils::chunked_vector<T>>
{ };

template<typename T, size_t N>
struct serializer<utils::small_vector<T, N>>
    : idl::serializers::internal::vector_serializer<utils::small_vector<T, N>>
{ };

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

    template<typename OtherStream>
    requires std::convertible_to<OtherStream, Stream>
    deserialized_bytes_proxy(deserialized_bytes_proxy<OtherStream> proxy)
        : _stream(std::move(proxy._stream)) { }

    auto view() const {
      if constexpr (std::is_same_v<Stream, simple_input_stream>) {
        return bytes_view(reinterpret_cast<const int8_t*>(_stream.begin()), _stream.size());
      } else {
        using iterator_type = typename Stream::iterator_type ;
        static_assert(FragmentRange<buffer_view<iterator_type>>);
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
        managed_bytes mb(managed_bytes::initialized_later(), _stream.size());
        for (bytes_mutable_view frag : fragment_range(managed_bytes_mutable_view(mb))) {
            _stream.read(reinterpret_cast<char*>(frag.data()), frag.size());
        }
        return mb;
    }

    [[gnu::always_inline]]
    operator bytes_ostream() && {
        bytes_ostream v;
        _stream.copy_to(v);
        return v;
    }
};

template<>
struct serializer<temporary_buffer<char>> {
    template<typename Input>
    static temporary_buffer<char> read(Input& in) {
        auto sz = deserialize(in, boost::type<uint32_t>());
		auto v = temporary_buffer<char>(sz);
        in.read(reinterpret_cast<char*>(v.get_write()), sz);
        return v;
    }
    template<typename Output>
    static void write(Output& out, const temporary_buffer<char>& v) {
        safe_serialize_as_uint32(out, uint32_t(v.size()));
        out.write(reinterpret_cast<const char*>(v.get()), v.size());
    }
    template<typename Input>
    static void skip(Input& in) {
        auto sz = deserialize(in, boost::type<uint32_t>());
        in.skip(sz);
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
    static void write(Output& out, const managed_bytes& mb) {
        safe_serialize_as_uint32(out, uint32_t(mb.size()));
        for (bytes_view frag : fragment_range(managed_bytes_view(mb))) {
            out.write(reinterpret_cast<const char*>(frag.data()), frag.size());
        }
    }
    template<typename Output>
    static void write(Output& out, const bytes_ostream& v) {
        safe_serialize_as_uint32(out, uint32_t(v.size()));
        for (bytes_view frag : v.fragments()) {
            out.write(reinterpret_cast<const char*>(frag.begin()), frag.size());
        }
    }
    template<typename Output, typename FragmentedBuffer>
    requires FragmentRange<FragmentedBuffer>
    static void write_fragmented(Output& out, FragmentedBuffer&& fragments) {
        safe_serialize_as_uint32(out, uint32_t(fragments.size_bytes()));
        for (bytes_view frag : fragments) {
            out.write(reinterpret_cast<const char*>(frag.begin()), frag.size());
        }
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
template<typename Input>
bytes_ostream deserialize(Input& in, boost::type<bytes_ostream>) {
    return serializer<bytes>::read(in);
}
template<typename Output, typename FragmentedBuffer>
requires FragmentRange<FragmentedBuffer>
void serialize_fragmented(Output& out, FragmentedBuffer&& v) {
    serializer<bytes>::write_fragmented(out, std::forward<FragmentedBuffer>(v));
}

template<typename T>
struct serializer<std::optional<T>> {
    template<typename Input>
    static std::optional<T> read(Input& in) {
        std::optional<T> v;
        auto b = deserialize(in, boost::type<bool>());
        if (b) {
            v.emplace(deserialize(in, boost::type<T>()));
        }
        return v;
    }
    template<typename Output>
    static void write(Output& out, const std::optional<T>& v) {
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

extern logging::logger serlog;

// Warning: assumes that pointer is never null
template<typename T>
struct serializer<seastar::lw_shared_ptr<T>> {
    template<typename Input>
    static seastar::lw_shared_ptr<T> read(Input& in) {
        return seastar::make_lw_shared<T>(deserialize(in, boost::type<T>()));
    }
    template<typename Output>
    static void write(Output& out, const seastar::lw_shared_ptr<T>& v) {
        if (!v) {
            on_internal_error(serlog, "Unexpected nullptr while serializing a pointer");
        }
        serialize(out, *v);
    }
    template<typename Input>
    static void skip(Input& in) {
        serializer<T>::skip(in);
    }
};

template<>
struct serializer<sstring> {
    template<typename Input>
    static sstring read(Input& in) {
        auto sz = deserialize(in, boost::type<uint32_t>());
        sstring v = uninitialized_string(sz);
        in.read(v.data(), sz);
        return v;
    }
    template<typename Output>
    static void write(Output& out, const sstring& v) {
        safe_serialize_as_uint32(out, uint32_t(v.size()));
        out.write(v.data(), v.size());
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

template<>
struct serializer<std::monostate> {
    template<typename Input>
    static std::monostate read(Input& in) {
        return std::monostate{};
    }
    template<typename Output>
    static void write(Output& out, std::monostate v) {}
    template<typename Input>
    static void skip(Input& in) {
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

template<typename Output, typename ...T>
void serialize(Output& out, const std::variant<T...>& v) {
    static_assert(std::variant_size_v<std::variant<T...>> < 256);
    size_t type_index = v.index();
    serialize(out, uint8_t(type_index));
    std::visit([&out] (const auto& member) {
        serialize(out, member);
    }, v);
}

template<typename Input, typename T, size_t... I>
T deserialize_std_variant(Input& in, boost::type<T> t,  size_t idx, std::index_sequence<I...>) {
    T v;
    (void)((I == idx ? v.template emplace<I>(deserialize(in, boost::type<std::variant_alternative_t<I, T>>())), true : false) || ...);
    return v;
}

template<typename Input, typename ...T>
std::variant<T...> deserialize(Input& in, boost::type<std::variant<T...>> v) {
    size_t idx = deserialize(in, boost::type<uint8_t>());
    return deserialize_std_variant(in, v, idx, std::make_index_sequence<sizeof...(T)>());
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
        sstring v = uninitialized_string(sz);
        in.read(v.data(), sz);
        return unknown_variant_type{ index, std::move(v) };
    });
}

// Class for iteratively deserializing a frozen vector
// using a range.
// Use begin() and end() to iterate through the frozen vector,
// deserializing (or skipping) one element at a time.
template <typename T, bool IsForward=true>
class vector_deserializer {
public:
    using value_type = T;
    using input_stream = utils::input_stream;

private:
    input_stream _in;
    size_t _size;
    utils::chunked_vector<input_stream> _substreams;

    void fill_substreams() requires (!IsForward) {
        input_stream in = _in;
        input_stream in2 = _in;
        for (size_t i = 0; i < size(); ++i) {
            size_t old_size = in.size();
            serializer<T>::skip(in);
            size_t new_size = in.size();

            _substreams.push_back(in2.read_substream(old_size - new_size));
        }
    }

    struct forward_iterator_data {
        input_stream _in = simple_input_stream();
        void skip() {
            serializer<T>::skip(_in);
        }
        value_type deserialize_next() {
            return deserialize(_in, boost::type<T>());
        }
    };
    struct reverse_iterator_data {
        std::reverse_iterator<utils::chunked_vector<input_stream>::const_iterator> _substream_it;
        void skip() {
            ++_substream_it;
        }
        value_type deserialize_next() {
            input_stream is = *_substream_it;
            ++_substream_it;
            return deserialize(is, boost::type<T>());
        }
    };


public:
    vector_deserializer() noexcept
        : _in(simple_input_stream())
        , _size(0)
    { }

    explicit vector_deserializer(input_stream in)
        : _in(std::move(in))
        , _size(deserialize(_in, boost::type<uint32_t>()))
    {
        if constexpr (!IsForward) {
            fill_substreams();
        }
    }

    // Get the number of items in the vector
    size_t size() const noexcept {
        return _size;
    }

    bool empty() const noexcept {
        return _size == 0;
    }

    // Input iterator
    class iterator {
        // _idx is the distance from .begin(). It is used only for comparing iterators.
        size_t _idx = 0;
        bool _consumed = false;
        std::conditional_t<IsForward, forward_iterator_data, reverse_iterator_data> _data;

        iterator(input_stream in, size_t idx) noexcept requires(IsForward)
            : _idx(idx)
            , _data{in}
        { }
        iterator(decltype(reverse_iterator_data::_substream_it) substreams, size_t idx) noexcept requires(!IsForward)
            : _idx(idx)
            , _data{substreams}
        { }

        friend class vector_deserializer;
   public:
        using iterator_category = std::input_iterator_tag;
        using value_type = T;
        using pointer = value_type*;
        using reference = value_type&;
        using difference_type = ssize_t;

        iterator() noexcept = default;

        bool operator==(const iterator& it) const noexcept {
            return _idx == it._idx;
        }

        // Deserializes and returns the item, effectively incrementing the iterator..
        value_type operator*() const {
            auto zis = const_cast<iterator*>(this);
            zis->_idx++;
            zis->_consumed = true;
            return zis->_data.deserialize_next();
        }

        iterator& operator++() {
            if (!_consumed) {
                _data.skip();
                ++_idx;
            } else {
                _consumed = false;
            }
            return *this;
        }
        iterator operator++(int) {
            auto pre = *this;
            ++*this;
            return pre;
        }

        ssize_t operator-(const iterator& it) const noexcept {
            return _idx - it._idx;
        }
    };

    using const_iterator = iterator;

    static_assert(std::input_iterator<iterator>);
    static_assert(std::sentinel_for<iterator, iterator>);

    iterator begin() noexcept requires(IsForward) {
        return {_in, 0};
    }
    const_iterator begin() const noexcept requires(IsForward) {
        return {_in, 0};
    }
    const_iterator cbegin() const noexcept requires(IsForward) {
        return {_in, 0};
    }

    iterator end() noexcept requires(IsForward) {
        return {_in, _size};
    }
    const_iterator end() const noexcept requires(IsForward) {
        return {_in, _size};
    }
    const_iterator cend() const noexcept requires(IsForward) {
        return {_in, _size};
    }

    iterator begin() noexcept requires(!IsForward) {
        return {_substreams.crbegin(), 0};
    }
    const_iterator begin() const noexcept requires(!IsForward) {
        return {_substreams.crbegin(), 0};
    }
    const_iterator cbegin() const noexcept requires(!IsForward) {
        return {_substreams.crbegin(), 0};
    }

    iterator end() noexcept requires(!IsForward) {
        return {_substreams.crend(), _size};
    }
    const_iterator end() const noexcept requires(!IsForward) {
        return {_substreams.crend(), _size};
    }
    const_iterator cend() const noexcept requires(!IsForward) {
        return {_substreams.crend(), _size};
    }

};

static_assert(std::ranges::range<vector_deserializer<int>>);

}
