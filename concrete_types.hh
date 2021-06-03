/*
 * Copyright (C) 2019-present ScyllaDB
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

#include <seastar/net/inet_address.hh>

#include "types.hh"
#include "types/list.hh"
#include "types/map.hh"
#include "types/set.hh"
#include "types/tuple.hh"
#include "types/user.hh"
#include "utils/big_decimal.hh"

struct empty_type_impl final : public abstract_type {
    empty_type_impl();
};

struct counter_type_impl final : public abstract_type {
    counter_type_impl();
};

template <typename T>
struct simple_type_impl : public concrete_type<T> {
    simple_type_impl(abstract_type::kind k, sstring name, std::optional<uint32_t> value_length_if_fixed);
};

template<typename T>
struct integer_type_impl : public simple_type_impl<T> {
    integer_type_impl(abstract_type::kind k, sstring name, std::optional<uint32_t> value_length_if_fixed);
};

struct byte_type_impl final : public integer_type_impl<int8_t> {
    byte_type_impl();
};

struct short_type_impl final : public integer_type_impl<int16_t> {
    short_type_impl();
};

struct int32_type_impl final : public integer_type_impl<int32_t> {
    int32_type_impl();
};

struct long_type_impl final : public integer_type_impl<int64_t> {
    long_type_impl();
};

struct boolean_type_impl final : public simple_type_impl<bool> {
    boolean_type_impl();
};

template <typename T>
struct floating_type_impl : public simple_type_impl<T> {
    floating_type_impl(abstract_type::kind k, sstring name, std::optional<uint32_t> value_length_if_fixed);
};

struct double_type_impl final : public floating_type_impl<double> {
    double_type_impl();
};

struct float_type_impl final : public floating_type_impl<float> {
    float_type_impl();
};

struct decimal_type_impl final : public concrete_type<big_decimal> {
    decimal_type_impl();
};

struct duration_type_impl final : public concrete_type<cql_duration> {
    duration_type_impl();
};

struct timestamp_type_impl final : public simple_type_impl<db_clock::time_point> {
    timestamp_type_impl();
    static db_clock::time_point from_sstring(sstring_view s);
};

struct simple_date_type_impl final : public simple_type_impl<uint32_t> {
    simple_date_type_impl();
    static uint32_t from_sstring(sstring_view s);
};

struct time_type_impl final : public simple_type_impl<int64_t> {
    time_type_impl();
    static int64_t from_sstring(sstring_view s);
};

struct string_type_impl : public concrete_type<sstring> {
    string_type_impl(kind k, sstring name);
};

struct ascii_type_impl final : public string_type_impl {
    ascii_type_impl();
};

struct utf8_type_impl final : public string_type_impl {
    utf8_type_impl();
};

struct bytes_type_impl final : public concrete_type<bytes> {
    bytes_type_impl();
};

// This is the old version of timestamp_type_impl, but has been replaced as it
// wasn't comparing pre-epoch timestamps correctly. This is kept for backward
// compatibility but shouldn't be used in new code.
struct date_type_impl final : public concrete_type<db_clock::time_point> {
    date_type_impl();
};

using timestamp_date_base_class = concrete_type<db_clock::time_point>;

struct timeuuid_type_impl final : public concrete_type<utils::UUID> {
    timeuuid_type_impl();
    static utils::UUID from_sstring(sstring_view s);
};

struct varint_type_impl final : public concrete_type<utils::multiprecision_int> {
    varint_type_impl();
};

struct inet_addr_type_impl final : public concrete_type<seastar::net::inet_address> {
    inet_addr_type_impl();
    static sstring to_sstring(const seastar::net::inet_address& addr);
    static seastar::net::inet_address from_sstring(sstring_view s);
};

struct uuid_type_impl final : public concrete_type<utils::UUID> {
    uuid_type_impl();
    static utils::UUID from_sstring(sstring_view s);
};

template <typename Func> using visit_ret_type = std::invoke_result_t<Func, const ascii_type_impl&>;

template <typename Func> concept CanHandleAllTypes = requires(Func f) {
    { f(*static_cast<const ascii_type_impl*>(nullptr)) }       -> std::same_as<visit_ret_type<Func>>;
    { f(*static_cast<const boolean_type_impl*>(nullptr)) }     -> std::same_as<visit_ret_type<Func>>;
    { f(*static_cast<const byte_type_impl*>(nullptr)) }        -> std::same_as<visit_ret_type<Func>>;
    { f(*static_cast<const bytes_type_impl*>(nullptr)) }       -> std::same_as<visit_ret_type<Func>>;
    { f(*static_cast<const counter_type_impl*>(nullptr)) }     -> std::same_as<visit_ret_type<Func>>;
    { f(*static_cast<const date_type_impl*>(nullptr)) }        -> std::same_as<visit_ret_type<Func>>;
    { f(*static_cast<const decimal_type_impl*>(nullptr)) }     -> std::same_as<visit_ret_type<Func>>;
    { f(*static_cast<const double_type_impl*>(nullptr)) }      -> std::same_as<visit_ret_type<Func>>;
    { f(*static_cast<const duration_type_impl*>(nullptr)) }    -> std::same_as<visit_ret_type<Func>>;
    { f(*static_cast<const empty_type_impl*>(nullptr)) }       -> std::same_as<visit_ret_type<Func>>;
    { f(*static_cast<const float_type_impl*>(nullptr)) }       -> std::same_as<visit_ret_type<Func>>;
    { f(*static_cast<const inet_addr_type_impl*>(nullptr)) }   -> std::same_as<visit_ret_type<Func>>;
    { f(*static_cast<const int32_type_impl*>(nullptr)) }       -> std::same_as<visit_ret_type<Func>>;
    { f(*static_cast<const list_type_impl*>(nullptr)) }        -> std::same_as<visit_ret_type<Func>>;
    { f(*static_cast<const long_type_impl*>(nullptr)) }        -> std::same_as<visit_ret_type<Func>>;
    { f(*static_cast<const map_type_impl*>(nullptr)) }         -> std::same_as<visit_ret_type<Func>>;
    { f(*static_cast<const reversed_type_impl*>(nullptr)) }    -> std::same_as<visit_ret_type<Func>>;
    { f(*static_cast<const set_type_impl*>(nullptr)) }         -> std::same_as<visit_ret_type<Func>>;
    { f(*static_cast<const short_type_impl*>(nullptr)) }       -> std::same_as<visit_ret_type<Func>>;
    { f(*static_cast<const simple_date_type_impl*>(nullptr)) } -> std::same_as<visit_ret_type<Func>>;
    { f(*static_cast<const time_type_impl*>(nullptr)) }        -> std::same_as<visit_ret_type<Func>>;
    { f(*static_cast<const timestamp_type_impl*>(nullptr)) }   -> std::same_as<visit_ret_type<Func>>;
    { f(*static_cast<const timeuuid_type_impl*>(nullptr)) }    -> std::same_as<visit_ret_type<Func>>;
    { f(*static_cast<const tuple_type_impl*>(nullptr)) }       -> std::same_as<visit_ret_type<Func>>;
    { f(*static_cast<const user_type_impl*>(nullptr)) }        -> std::same_as<visit_ret_type<Func>>;
    { f(*static_cast<const utf8_type_impl*>(nullptr)) }        -> std::same_as<visit_ret_type<Func>>;
    { f(*static_cast<const uuid_type_impl*>(nullptr)) }        -> std::same_as<visit_ret_type<Func>>;
    { f(*static_cast<const varint_type_impl*>(nullptr)) }      -> std::same_as<visit_ret_type<Func>>;
};

template<typename Func>
requires CanHandleAllTypes<Func>
static inline visit_ret_type<Func> visit(const abstract_type& t, Func&& f) {
    switch (t.get_kind()) {
    case abstract_type::kind::ascii:
        return f(*static_cast<const ascii_type_impl*>(&t));
    case abstract_type::kind::boolean:
        return f(*static_cast<const boolean_type_impl*>(&t));
    case abstract_type::kind::byte:
        return f(*static_cast<const byte_type_impl*>(&t));
    case abstract_type::kind::bytes:
        return f(*static_cast<const bytes_type_impl*>(&t));
    case abstract_type::kind::counter:
        return f(*static_cast<const counter_type_impl*>(&t));
    case abstract_type::kind::date:
        return f(*static_cast<const date_type_impl*>(&t));
    case abstract_type::kind::decimal:
        return f(*static_cast<const decimal_type_impl*>(&t));
    case abstract_type::kind::double_kind:
        return f(*static_cast<const double_type_impl*>(&t));
    case abstract_type::kind::duration:
        return f(*static_cast<const duration_type_impl*>(&t));
    case abstract_type::kind::empty:
        return f(*static_cast<const empty_type_impl*>(&t));
    case abstract_type::kind::float_kind:
        return f(*static_cast<const float_type_impl*>(&t));
    case abstract_type::kind::inet:
        return f(*static_cast<const inet_addr_type_impl*>(&t));
    case abstract_type::kind::int32:
        return f(*static_cast<const int32_type_impl*>(&t));
    case abstract_type::kind::list:
        return f(*static_cast<const list_type_impl*>(&t));
    case abstract_type::kind::long_kind:
        return f(*static_cast<const long_type_impl*>(&t));
    case abstract_type::kind::map:
        return f(*static_cast<const map_type_impl*>(&t));
    case abstract_type::kind::reversed:
        return f(*static_cast<const reversed_type_impl*>(&t));
    case abstract_type::kind::set:
        return f(*static_cast<const set_type_impl*>(&t));
    case abstract_type::kind::short_kind:
        return f(*static_cast<const short_type_impl*>(&t));
    case abstract_type::kind::simple_date:
        return f(*static_cast<const simple_date_type_impl*>(&t));
    case abstract_type::kind::time:
        return f(*static_cast<const time_type_impl*>(&t));
    case abstract_type::kind::timestamp:
        return f(*static_cast<const timestamp_type_impl*>(&t));
    case abstract_type::kind::timeuuid:
        return f(*static_cast<const timeuuid_type_impl*>(&t));
    case abstract_type::kind::tuple:
        return f(*static_cast<const tuple_type_impl*>(&t));
    case abstract_type::kind::user:
        return f(*static_cast<const user_type_impl*>(&t));
    case abstract_type::kind::utf8:
        return f(*static_cast<const utf8_type_impl*>(&t));
    case abstract_type::kind::uuid:
        return f(*static_cast<const uuid_type_impl*>(&t));
    case abstract_type::kind::varint:
        return f(*static_cast<const varint_type_impl*>(&t));
    }
    __builtin_unreachable();
}

template <typename Func> struct data_value_visitor {
    const void* v;
    Func& f;
    auto operator()(const empty_type_impl& t) { return f(t, v); }
    auto operator()(const counter_type_impl& t) { return f(t, v); }
    auto operator()(const reversed_type_impl& t) { return f(t, v); }
    template <typename T> auto operator()(const T& t) {
        return f(t, reinterpret_cast<const typename T::native_type*>(v));
    }
};

// Given an abstract_type and a void pointer to an object of that
// type, call f with the runtime type of t and v casted to the
// corresponding native type.
// This takes an abstract_type and a void pointer instead of a
// data_value to support reversed_type_impl without requiring that
// each visitor create a new data_value just to recurse.
template <typename Func> inline auto visit(const abstract_type& t, const void* v, Func&& f) {
    return ::visit(t, data_value_visitor<Func>{v, f});
}

template <typename Func> inline auto visit(const data_value& v, Func&& f) {
    return ::visit(*v.type(), v._value, f);
}
