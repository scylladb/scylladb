/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
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


#include "utils/big_decimal.hh"
#include "aggregate_fcts.hh"
#include "functions.hh"
#include "native_aggregate_function.hh"
#include "exceptions/exceptions.hh"

using namespace cql3;
using namespace functions;
using namespace aggregate_fcts;

namespace {
class impl_count_function : public aggregate_function::aggregate {
    int64_t _count = 0;
public:
    virtual void reset() override {
        _count = 0;
    }
    virtual opt_bytes compute(cql_serialization_format sf) override {
        return long_type->decompose(_count);
    }
    virtual void add_input(cql_serialization_format sf, const std::vector<opt_bytes>& values) override {
        ++_count;
    }
};

class count_rows_function final : public native_aggregate_function {
public:
    count_rows_function() : native_aggregate_function(COUNT_ROWS_FUNCTION_NAME, long_type, {}) {}
    virtual std::unique_ptr<aggregate> new_aggregate() override {
        return std::make_unique<impl_count_function>();
    }
    virtual sstring column_name(const std::vector<sstring>& column_names) const override {
        return "count";
    }
};

// We need a wider accumulator for sum and average,
// since summing the inputs can overflow the input type
template <typename T>
struct accumulator_for;

template <typename T>
struct int128_accumulator_for {
    using type = __int128;

    static T narrow(type acc) {
        T ret = static_cast<T>(acc);
        if (static_cast<type>(ret) != acc) {
            throw exceptions::overflow_error_exception("Sum overflow. Values should be casted to a wider type.");
        }
        return ret;
    }
};

template <typename T>
struct same_type_accumulator_for {
    using type = T;

    static T narrow(type acc) {
        return acc;
    }
};

template <typename T>
struct accumulator_for : public std::conditional_t<std::is_integral_v<T>,
                                                   int128_accumulator_for<T>,
                                                   same_type_accumulator_for<T>>
{ };

template <typename Type>
class impl_sum_function_for final : public aggregate_function::aggregate {
    using accumulator_type = typename accumulator_for<Type>::type;
    accumulator_type _sum{};
public:
    virtual void reset() override {
        _sum = {};
    }
    virtual opt_bytes compute(cql_serialization_format sf) override {
        return data_type_for<Type>()->decompose(accumulator_for<Type>::narrow(_sum));
    }
    virtual void add_input(cql_serialization_format sf, const std::vector<opt_bytes>& values) override {
        if (!values[0]) {
            return;
        }
        _sum += value_cast<Type>(data_type_for<Type>()->deserialize(*values[0]));
    }
};

template <typename Type>
class sum_function_for final : public native_aggregate_function {
public:
    sum_function_for() : native_aggregate_function("sum", data_type_for<Type>(), { data_type_for<Type>() }) {}
    virtual std::unique_ptr<aggregate> new_aggregate() override {
        return std::make_unique<impl_sum_function_for<Type>>();
    }
};


template <typename Type>
static
shared_ptr<aggregate_function>
make_sum_function() {
    return make_shared<sum_function_for<Type>>();
}

template <typename Type>
class impl_div_for_avg {
public:
    static Type div(const typename accumulator_for<Type>::type& x, const int64_t y) {
        return x/y;
    }
};

template <>
class impl_div_for_avg<big_decimal> {
public:
    static big_decimal div(const big_decimal& x, const int64_t y) {
        return x.div(y, big_decimal::rounding_mode::HALF_EVEN);
    }
};

template <typename Type>
class impl_avg_function_for final : public aggregate_function::aggregate {
   typename accumulator_for<Type>::type _sum{};
   int64_t _count = 0;
public:
    virtual void reset() override {
        _sum = {};
        _count = 0;
    }
    virtual opt_bytes compute(cql_serialization_format sf) override {
        Type ret{};
        if (_count) {
            ret = impl_div_for_avg<Type>::div(_sum, _count);
        }
        return data_type_for<Type>()->decompose(ret);
    }
    virtual void add_input(cql_serialization_format sf, const std::vector<opt_bytes>& values) override {
        if (!values[0]) {
            return;
        }
        ++_count;
        _sum += value_cast<Type>(data_type_for<Type>()->deserialize(*values[0]));
    }
};

template <typename Type>
class avg_function_for final : public native_aggregate_function {
public:
    avg_function_for() : native_aggregate_function("avg", data_type_for<Type>(), { data_type_for<Type>() }) {}
    virtual std::unique_ptr<aggregate> new_aggregate() override {
        return std::make_unique<impl_avg_function_for<Type>>();
    }
};

template <typename Type>
static
shared_ptr<aggregate_function>
make_avg_function() {
    return make_shared<avg_function_for<Type>>();
}

template <typename T>
struct aggregate_type_for {
    using type = T;
};

template<>
struct aggregate_type_for<ascii_native_type> {
    using type = ascii_native_type::primary_type;
};

template<>
struct aggregate_type_for<simple_date_native_type> {
    using type = simple_date_native_type::primary_type;
};

template<>
struct aggregate_type_for<timeuuid_native_type> {
    using type = timeuuid_native_type;
};

template<>
struct aggregate_type_for<time_native_type> {
    using type = time_native_type::primary_type;
};

// WARNING: never invoke this on temporary values; it will return a dangling reference.
template <typename Type>
const Type& max_wrapper(const Type& t1, const Type& t2) {
    using std::max;
    return max(t1, t2);
}

inline const net::inet_address& max_wrapper(const net::inet_address& t1, const net::inet_address& t2) {
    using family = seastar::net::inet_address::family;
    const size_t len =
            (t1.in_family() == family::INET || t2.in_family() == family::INET)
            ? sizeof(::in_addr) : sizeof(::in6_addr);
    return std::memcmp(t1.data(), t2.data(), len) >= 0 ? t1 : t2;
}

inline const timeuuid_native_type& max_wrapper(const timeuuid_native_type& t1, const timeuuid_native_type& t2) {
    return t1.uuid.timestamp() > t2.uuid.timestamp() ? t1 : t2;
}

template <typename Type>
class impl_max_function_for final : public aggregate_function::aggregate {
   std::optional<typename aggregate_type_for<Type>::type> _max{};
public:
    virtual void reset() override {
        _max = {};
    }
    virtual opt_bytes compute(cql_serialization_format sf) override {
        if (!_max) {
            return {};
        }
        return data_type_for<Type>()->decompose(data_value(Type{*_max}));
    }
    virtual void add_input(cql_serialization_format sf, const std::vector<opt_bytes>& values) override {
        if (!values[0]) {
            return;
        }
        auto val = value_cast<typename aggregate_type_for<Type>::type>(data_type_for<Type>()->deserialize(*values[0]));
        if (!_max) {
            _max = val;
        } else {
            _max = max_wrapper(*_max, val);
        }
    }
};

/// The same as `impl_max_function_for' but without compile-time dependency on `Type'.
class impl_max_dynamic_function final : public aggregate_function::aggregate {
    data_type _io_type;
    opt_bytes _max;
public:
    impl_max_dynamic_function(data_type io_type) : _io_type(std::move(io_type)) {}

    virtual void reset() override {
        _max = {};
    }
    virtual opt_bytes compute(cql_serialization_format sf) override {
        return _max.value_or(bytes{});
    }
    virtual void add_input(cql_serialization_format sf, const std::vector<opt_bytes>& values) override {
        if (values.empty() || !values[0]) {
            return;
        }
        if (!_max || _io_type->less(*_max, *values[0])) {
            _max = values[0];
        }
    }
};

template <typename Type>
class max_function_for final : public native_aggregate_function {
public:
    max_function_for() : native_aggregate_function("max", data_type_for<Type>(), { data_type_for<Type>() }) {}
    virtual std::unique_ptr<aggregate> new_aggregate() override {
        return std::make_unique<impl_max_function_for<Type>>();
    }
};

class max_dynamic_function final : public native_aggregate_function {
    data_type _io_type;
public:
    max_dynamic_function(data_type io_type)
            : native_aggregate_function("max", io_type, { io_type })
            , _io_type(std::move(io_type)) {}
    virtual std::unique_ptr<aggregate> new_aggregate() override {
        return std::make_unique<impl_max_dynamic_function>(_io_type);
    }
};

/**
 * Creates a MAX function for the specified type.
 *
 * @param inputType the function input and output type
 * @return a MAX function for the specified type.
 */
template <typename Type>
static
shared_ptr<aggregate_function>
make_max_function() {
    return make_shared<max_function_for<Type>>();
}

// WARNING: never invoke this on temporary values; it will return a dangling reference.
template <typename Type>
const Type& min_wrapper(const Type& t1, const Type& t2) {
    using std::min;
    return min(t1, t2);
}

inline const net::inet_address& min_wrapper(const net::inet_address& t1, const net::inet_address& t2) {
    using family = seastar::net::inet_address::family;
    const size_t len =
            (t1.in_family() == family::INET || t2.in_family() == family::INET)
            ? sizeof(::in_addr) : sizeof(::in6_addr);
    return std::memcmp(t1.data(), t2.data(), len) <= 0 ? t1 : t2;
}

inline timeuuid_native_type min_wrapper(timeuuid_native_type t1, timeuuid_native_type t2) {
    return t1.uuid.timestamp() < t2.uuid.timestamp() ? t1 : t2;
}

template <typename Type>
class impl_min_function_for final : public aggregate_function::aggregate {
   std::optional<typename aggregate_type_for<Type>::type> _min{};
public:
    virtual void reset() override {
        _min = {};
    }
    virtual opt_bytes compute(cql_serialization_format sf) override {
        if (!_min) {
            return {};
        }
        return data_type_for<Type>()->decompose(data_value(Type{*_min}));
    }
    virtual void add_input(cql_serialization_format sf, const std::vector<opt_bytes>& values) override {
        if (!values[0]) {
            return;
        }
        auto val = value_cast<typename aggregate_type_for<Type>::type>(data_type_for<Type>()->deserialize(*values[0]));
        if (!_min) {
            _min = val;
        } else {
            _min = min_wrapper(*_min, val);
        }
    }
};

/// The same as `impl_min_function_for' but without compile-time dependency on `Type'.
class impl_min_dynamic_function final : public aggregate_function::aggregate {
    data_type _io_type;
    opt_bytes _min;
public:
    impl_min_dynamic_function(data_type io_type) : _io_type(std::move(io_type)) {}

    virtual void reset() override {
        _min = {};
    }
    virtual opt_bytes compute(cql_serialization_format sf) override {
        return _min.value_or(bytes{});
    }
    virtual void add_input(cql_serialization_format sf, const std::vector<opt_bytes>& values) override {
        if (values.empty() || !values[0]) {
            return;
        }
        if (!_min || _io_type->less(*values[0], *_min)) {
            _min = values[0];
        }
    }
};

template <typename Type>
class min_function_for final : public native_aggregate_function {
public:
    min_function_for() : native_aggregate_function("min", data_type_for<Type>(), { data_type_for<Type>() }) {}
    virtual std::unique_ptr<aggregate> new_aggregate() override {
        return std::make_unique<impl_min_function_for<Type>>();
    }
};

class min_dynamic_function final : public native_aggregate_function {
    data_type _io_type;
public:
    min_dynamic_function(data_type io_type)
            : native_aggregate_function("min", io_type, { io_type })
            , _io_type(std::move(io_type)) {}
    virtual std::unique_ptr<aggregate> new_aggregate() override {
        return std::make_unique<impl_min_dynamic_function>(_io_type);
    }
};

/**
 * Creates a MIN function for the specified type.
 *
 * @param inputType the function input and output type
 * @return a MIN function for the specified type.
 */
template <typename Type>
static
shared_ptr<aggregate_function>
make_min_function() {
    return make_shared<min_function_for<Type>>();
}

template <typename Type>
class impl_count_function_for final : public aggregate_function::aggregate {
   int64_t _count = 0;
public:
    virtual void reset() override {
        _count = 0;
    }
    virtual opt_bytes compute(cql_serialization_format sf) override {
        return long_type->decompose(_count);
    }
    virtual void add_input(cql_serialization_format sf, const std::vector<opt_bytes>& values) override {
        if (!values[0]) {
            return;
        }
        ++_count;
    }
};

template <typename Type>
class count_function_for final : public native_aggregate_function {
public:
    count_function_for() : native_aggregate_function("count", long_type, { data_type_for<Type>() }) {}
    virtual std::unique_ptr<aggregate> new_aggregate() override {
        return std::make_unique<impl_count_function_for<Type>>();
    }
};

/**
 * Creates a COUNT function for the specified type.
 *
 * @param inputType the function input type
 * @return a COUNT function for the specified type.
 */
template <typename Type>
static shared_ptr<aggregate_function> make_count_function() {
    return make_shared<count_function_for<Type>>();
}
}

shared_ptr<aggregate_function>
aggregate_fcts::make_count_rows_function() {
    return make_shared<count_rows_function>();
}

shared_ptr<aggregate_function>
aggregate_fcts::make_max_dynamic_function(data_type io_type) {
    return make_shared<max_dynamic_function>(io_type);
}

shared_ptr<aggregate_function>
aggregate_fcts::make_min_dynamic_function(data_type io_type) {
    return make_shared<min_dynamic_function>(io_type);
}

void cql3::functions::add_agg_functions(declared_t& funcs) {
    auto declare = [&funcs] (shared_ptr<function> f) { funcs.emplace(f->name(), f); };

    declare(make_count_function<int8_t>());
    declare(make_max_function<int8_t>());
    declare(make_min_function<int8_t>());

    declare(make_count_function<int16_t>());
    declare(make_max_function<int16_t>());
    declare(make_min_function<int16_t>());

    declare(make_count_function<int32_t>());
    declare(make_max_function<int32_t>());
    declare(make_min_function<int32_t>());

    declare(make_count_function<int64_t>());
    declare(make_max_function<int64_t>());
    declare(make_min_function<int64_t>());

    declare(make_count_function<utils::multiprecision_int>());
    declare(make_max_function<utils::multiprecision_int>());
    declare(make_min_function<utils::multiprecision_int>());

    declare(make_count_function<big_decimal>());
    declare(make_max_function<big_decimal>());
    declare(make_min_function<big_decimal>());

    declare(make_count_function<float>());
    declare(make_max_function<float>());
    declare(make_min_function<float>());

    declare(make_count_function<double>());
    declare(make_max_function<double>());
    declare(make_min_function<double>());

    declare(make_count_function<sstring>());
    declare(make_max_function<sstring>());
    declare(make_min_function<sstring>());

    declare(make_count_function<ascii_native_type>());
    declare(make_max_function<ascii_native_type>());
    declare(make_min_function<ascii_native_type>());

    declare(make_count_function<simple_date_native_type>());
    declare(make_max_function<simple_date_native_type>());
    declare(make_min_function<simple_date_native_type>());

    declare(make_count_function<db_clock::time_point>());
    declare(make_max_function<db_clock::time_point>());
    declare(make_min_function<db_clock::time_point>());

    declare(make_count_function<timeuuid_native_type>());
    declare(make_max_function<timeuuid_native_type>());
    declare(make_min_function<timeuuid_native_type>());

    declare(make_count_function<time_native_type>());
    declare(make_max_function<time_native_type>());
    declare(make_min_function<time_native_type>());

    declare(make_count_function<utils::UUID>());
    declare(make_max_function<utils::UUID>());
    declare(make_min_function<utils::UUID>());

    declare(make_count_function<bytes>());
    declare(make_max_function<bytes>());
    declare(make_min_function<bytes>());

    declare(make_count_function<bool>());
    declare(make_max_function<bool>());
    declare(make_min_function<bool>());

    declare(make_count_function<net::inet_address>());
    declare(make_max_function<net::inet_address>());
    declare(make_min_function<net::inet_address>());

    // FIXME: more count/min/max

    declare(make_sum_function<int8_t>());
    declare(make_sum_function<int16_t>());
    declare(make_sum_function<int32_t>());
    declare(make_sum_function<int64_t>());
    declare(make_sum_function<float>());
    declare(make_sum_function<double>());
    declare(make_sum_function<utils::multiprecision_int>());
    declare(make_sum_function<big_decimal>());
    declare(make_avg_function<int8_t>());
    declare(make_avg_function<int16_t>());
    declare(make_avg_function<int32_t>());
    declare(make_avg_function<int64_t>());
    declare(make_avg_function<float>());
    declare(make_avg_function<double>());
    declare(make_avg_function<utils::multiprecision_int>());
    declare(make_avg_function<big_decimal>());
}
