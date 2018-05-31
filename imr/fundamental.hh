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

#include <boost/range/algorithm/for_each.hpp>

#include <seastar/core/align.hh>
#include <seastar/core/bitops.hh>
#include <seastar/util/gcc6-concepts.hh>

#include "bytes.hh"
#include "utils/meta.hh"

#include "imr/core.hh"

namespace imr {

namespace internal {

template<typename T, typename CharT>
GCC6_CONCEPT(requires std::is_pod<T>::value && sizeof(CharT) == 1)
inline T read_pod(const CharT* in) noexcept {
    T obj;
    std::copy_n(in, sizeof(T), reinterpret_cast<CharT*>(&obj));
    return obj;
}

template<typename T, typename CharT>
GCC6_CONCEPT(requires std::is_pod<T>::value && sizeof(CharT) == 1)
inline void write_pod(T obj, CharT* out) noexcept {
    std::copy_n(reinterpret_cast<const CharT*>(&obj), sizeof(T), out);
}

}

template<typename Tag>
class set_flag {
    bool _value = true;
public:
    set_flag() = default;
    explicit set_flag(bool v) noexcept : _value(v) { }
    bool value() const noexcept { return _value; }
};

/// Set of flags
///
/// Represents a fixed-size set of tagged flags.
template<typename... Tags>
class flags {
    static constexpr auto object_size = seastar::align_up<size_t>(sizeof...(Tags), 8) / 8;
private:
    template<typename Tag>
    static void do_set_or_clear(uint8_t* ptr, bool set) noexcept {
        const auto idx = meta::find<Tag, Tags...>;
        const auto byte_idx = idx / 8;
        const auto bit_idx = idx % 8;

        auto value = ptr[byte_idx];
        value &= ~uint8_t(1 << bit_idx);
        value |= uint8_t(set) << bit_idx;
        ptr[byte_idx] = value;
    }

    template<typename Tag>
    static bool do_get(const uint8_t* ptr) noexcept {
        const auto idx = meta::find<Tag, Tags...>;
        const auto byte_idx = idx / 8;
        const auto bit_idx = idx % 8;

        return ptr[byte_idx] & (1 << bit_idx);
    }
public:
    template<::mutable_view is_mutable>
    class basic_view {
        using pointer_type = std::conditional_t<is_mutable == ::mutable_view::no,
                                                const uint8_t*, uint8_t*>;
        pointer_type _ptr;
    public:
        explicit basic_view(pointer_type ptr) noexcept : _ptr(ptr) { }

        operator basic_view<::mutable_view::no>() const noexcept {
            return basic_view<::mutable_view::no>(_ptr);
        }

        template<typename Tag>
        bool get() const noexcept {
            return do_get<Tag>(_ptr);
        }

        template<typename Tag>
        void set(bool value = true) noexcept {
            do_set_or_clear<Tag>(_ptr, value);
        }
    };

    using view = basic_view<::mutable_view::no>;
    using mutable_view = basic_view<::mutable_view::yes>;

public:
    template<typename Context = no_context_t>
    static view make_view(const uint8_t* in, const Context& = no_context) noexcept {
        return view(in);
    }
    template<typename Context = no_context_t>
    static mutable_view make_view(uint8_t* in, const Context& = no_context) noexcept {
        return mutable_view(in);
    }

public:
    template<typename Context = no_context_t>
    static size_t serialized_object_size(const uint8_t*, const Context& = no_context) noexcept {
        return object_size;
    }

    template<typename... Tags1>
    static size_t size_when_serialized(set_flag<Tags1>...) noexcept {
        return object_size;
    }

    template<typename... Tags1>
    static size_t serialize(uint8_t* out, set_flag<Tags1>... sfs) noexcept {
        std::fill_n(out, object_size, 0);
        (do_set_or_clear<Tags1>(out, sfs.value()), ...);
        return object_size;
    }

    static size_t size_when_serialized(placeholder<flags<Tags...>>&) noexcept {
        return object_size;
    }

    static size_t serialize(uint8_t* out, placeholder<flags<Tags...>>& phldr) noexcept {
        phldr.set_pointer(out);
        return object_size;
    }
};

/// POD object
///
/// Represents a fixed-size POD value.
template<typename Type>
GCC6_CONCEPT(requires std::is_pod<Type>::value)
struct pod {
    using underlying = Type;
    enum : size_t {
        size = sizeof(Type),
    };

    template<::mutable_view is_mutable>
    class basic_view {
        using pointer_type = std::conditional_t<is_mutable == ::mutable_view::no,
                                                const uint8_t*, uint8_t*>;
        pointer_type _ptr;
    public:
        explicit basic_view(pointer_type ptr) noexcept : _ptr(ptr) { }

        operator basic_view<::mutable_view::no>() const noexcept {
            return basic_view<::mutable_view::no>(_ptr);
        }

        Type load() const noexcept {
            return internal::read_pod<Type>(_ptr);
        }

        void store(const Type& object) noexcept {
            internal::write_pod(object, _ptr);
        }
    };

    using view = basic_view<::mutable_view::no>;
    using mutable_view = basic_view<::mutable_view::yes>;

public:
    template<typename Context = no_context_t>
    static view make_view(const uint8_t* in, const Context& = no_context) noexcept {
        return view(in);
    }
    template<typename Context = no_context_t>
    static mutable_view make_view(uint8_t* in, const Context& = no_context) noexcept {
        return mutable_view(in);
    }

public:
    template<typename Context = no_context_t>
    static size_t serialized_object_size(const uint8_t*, const Context& = no_context) noexcept {
        return sizeof(Type);
    }

    static size_t size_when_serialized(const Type&) noexcept {
        return sizeof(Type);
    }

    static size_t serialize(uint8_t* out, const Type& value) noexcept {
        internal::write_pod(value, out);
        return sizeof(Type);
    }

    static size_t size_when_serialized(placeholder<pod<Type>>&) noexcept {
        return sizeof(Type);
    }

    static size_t serialize(uint8_t* out, placeholder<pod<Type>>& phldr) noexcept {
        phldr.set_pointer(out);
        return sizeof(Type);
    }
};

/// Buffer
///
/// Represents an opaque buffer. The size of the buffer is not stored and must
/// be provided by external context.
/// A buffer can be created from a bytes_view, a fragments range or a
/// (size, serializer) pair.
template<typename Tag>
struct buffer {
    using view = bytes_view;
    using mutable_view = bytes_mutable_view;
    template<::mutable_view is_mutable>
    using basic_view = std::conditional_t<is_mutable == ::mutable_view::no, view, mutable_view>;

    template<typename Context>
    GCC6_CONCEPT(requires requires(const Context& ctx) {
        { ctx.template size_of<Tag>() } noexcept -> size_t;
    })
    static view make_view(const uint8_t* in, const Context& context) noexcept {
        auto ptr = reinterpret_cast<bytes_view::pointer>(in);
        return bytes_view(ptr, context.template size_of<Tag>());
    }

    template<typename Context>
    GCC6_CONCEPT(requires requires(const Context& ctx) {
        { ctx.template size_of<Tag>() } noexcept -> size_t;
    })
    static mutable_view make_view(uint8_t* in, const Context& context) noexcept {
        auto ptr = reinterpret_cast<bytes_mutable_view::pointer>(in);
        return bytes_mutable_view(ptr, context.template size_of<Tag>());
    }

public:
    template<typename Context>
    GCC6_CONCEPT(requires requires(const Context& ctx) {
        { ctx.template size_of<Tag>() } noexcept -> size_t;
    })
    static size_t serialized_object_size(const uint8_t*, const Context& context) noexcept {
        return context.template size_of<Tag>();
    }

    static size_t size_when_serialized(bytes_view src) noexcept {
        return src.size();
    }

    template<typename Serializer>
    GCC6_CONCEPT(requires requires (Serializer ser, uint8_t* ptr) {
        { ser(ptr) } noexcept;
    })
    static size_t size_when_serialized(size_t size, Serializer&&) noexcept {
        return size;
    }

    template<typename FragmentRange, typename = std::enable_if_t<is_fragment_range_v<std::decay_t<FragmentRange>>>>
    static size_t size_when_serialized(FragmentRange&& fragments) {
        return fragments.size_bytes();
    }

    static size_t serialize(uint8_t* out, bytes_view src) {
        std::copy_n(src.begin(), src.size(),
                    reinterpret_cast<bytes_view::value_type*>(out));
        return src.size();
    }

    template<typename FragmentRange, typename = std::enable_if_t<is_fragment_range_v<std::decay_t<FragmentRange>>>>
    static size_t serialize(uint8_t* out, FragmentRange&& fragments) {
        auto dst = reinterpret_cast<bytes_view::value_type*>(out);
        using boost::range::for_each;
        for_each(fragments, [&] (bytes_view fragment) {
            dst = std::copy(fragment.begin(), fragment.end(), dst);
        });
        return fragments.size_bytes();
    }

    template<typename Serializer>
    GCC6_CONCEPT(requires requires (Serializer ser, uint8_t* ptr) {
        { ser(ptr) } noexcept;
    })
    static size_t serialize(uint8_t* out, size_t size, Serializer&& serializer) noexcept {
        std::forward<Serializer>(serializer)(out);
        return size;
    }
};

}
