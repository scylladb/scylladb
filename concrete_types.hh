/*
 * Copyright (C) 2019 ScyllaDB
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

#include "types.hh"
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
