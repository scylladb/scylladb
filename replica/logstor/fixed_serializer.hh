/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <concepts>
#include <functional>
#include <type_traits>
#include <utility>

#include "serializer.hh"

namespace replica::logstor::ondisk {

// field<T> gives the encoding of a field of type T in a fixed-size on-disk type. An integer type
// uses the specialization below, and every other field type needs its own specialization. Each
// specialization names the on-disk type, so that a change to a type that is defined outside the
// format fails the build and does not silently change the format.
template <typename T>
struct field;

// This specialization leaves out bool, because ser writes a bool as a uint8_t and not as its own
// bytes.
template <typename T>
requires (std::is_integral_v<T> && !std::is_same_v<T, bool>)
struct field<T> {
    static constexpr size_t size = sizeof(T);

    template <typename Output>
    [[gnu::always_inline]]
    static void write(Output& out, T v) {
        ser::serialize_integral(out, v);
    }

    template <typename Input>
    [[gnu::always_inline]]
    static T read(Input& in) {
        return ser::deserialize_integral<T>(in);
    }
};

// enum_field encodes an enum as its underlying type, which must be `Wire`.
template <typename T, typename Wire>
requires std::is_enum_v<T>
struct enum_field {
    static_assert(std::is_same_v<std::underlying_type_t<T>, Wire>, "The underlying type of the enum must be the on-disk type");

    static constexpr size_t size = field<Wire>::size;

    template <typename Output>
    [[gnu::always_inline]]
    static void write(Output& out, T v) {
        field<Wire>::write(out, static_cast<Wire>(v));
    }

    template <typename Input>
    [[gnu::always_inline]]
    static T read(Input& in) {
        return static_cast<T>(field<Wire>::read(in));
    }
};

template <typename V>
using field_of = field<std::remove_cvref_t<V>>;

namespace detail {

template <typename C, typename M>
C member_class(M C::*);

template <typename... Ts>
struct type_list {};

// The code uses field_types() only in decltype, to get the field types of `T`.
template <typename T>
auto field_types(const T& v) {
    const auto& [...fields] = v;
    return type_list<std::remove_cvref_t<decltype(fields)>...>{};
}

template <typename T>
using field_types_of = decltype(field_types(std::declval<const T&>()));

template <typename... Fs>
consteval size_t fields_size(type_list<Fs...>) {
    return (field<Fs>::size + ... + 0);
}

// The compiler evaluates the elements of a braced initializer list in order, so read_fields()
// reads the fields in declaration order and builds `T` from them, without a default
// initialization and an assignment. The first element initializes the fixed_layout base.
template <typename T, typename Input, typename... Fs>
[[gnu::always_inline]]
T read_fields(Input& in, type_list<Fs...>) {
    return T{{}, field<Fs>::read(in)...};
}

} // namespace detail

// projected_field encodes a field type as one of its parts, which must be of type `Wire`. `get` is
// a pointer to the data member or to the member function that gives the part. `make` builds the
// field type from the part, and if `make` is not given, the part initializes the field type with
// braces.
template <typename Wire, auto get, auto make = nullptr>
struct projected_field {
    using type = decltype(detail::member_class(get));
    using part = std::remove_cvref_t<std::invoke_result_t<decltype(get), const type&>>;

    static_assert(std::is_same_v<part, Wire>, "The projected part must be of the on-disk type");

    static constexpr size_t size = field<Wire>::size;

    template <typename Output>
    [[gnu::always_inline]]
    static void write(Output& out, const type& v) {
        field<Wire>::write(out, std::invoke(get, v));
    }

    template <typename Input>
    [[gnu::always_inline]]
    static type read(Input& in) {
        if constexpr (std::is_null_pointer_v<decltype(make)>) {
            return type{field<Wire>::read(in)};
        } else {
            return std::invoke(make, field<Wire>::read(in));
        }
    }
};

// fixed_layout is the base of every fixed-size on-disk type, and fixed_serializer serializes every
// type that derives from it. fixed_layout has no fields, so the on-disk fields of a derived type
// are only the fields that the derived type declares.
struct fixed_layout {
    bool operator==(const fixed_layout&) const noexcept = default;
};

static_assert(std::is_empty_v<fixed_layout>, "A structured binding finds the fields only if the base has none");

template <typename T>
concept fixed_layout_type = std::derived_from<T, fixed_layout>;

template <typename T>
inline constexpr size_t fixed_size = detail::fields_size(detail::field_types_of<T>{});

// Serializes a fixed-size on-disk type field by field, in declaration order, so the order of the
// members is the on-disk layout. `T` must be an aggregate whose only base is fixed_layout, because
// fixed_serializer finds the fields with a structured binding and reads them into `T` with
// aggregate initialization.
template <fixed_layout_type T>
struct fixed_serializer {
    static constexpr size_t serialized_size = fixed_size<T>;

    template <typename Output>
    [[gnu::always_inline]]
    static void write(Output& out, const T& v) {
        const auto& [...fields] = v;
        (field_of<decltype(fields)>::write(out, fields), ...);
    }

    template <typename Input>
    [[gnu::always_inline]]
    static T read(Input& in) {
        return detail::read_fields<T>(in, detail::field_types_of<T>{});
    }

    template <typename Input>
    static void skip(Input& in) {
        in.skip(serialized_size);
    }
};

} // namespace replica::logstor::ondisk

namespace ser {

template <replica::logstor::ondisk::fixed_layout_type T>
struct serializer<T> : replica::logstor::ondisk::fixed_serializer<T> {};

} // namespace ser
