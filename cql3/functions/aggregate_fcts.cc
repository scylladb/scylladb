/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "bytes.hh"
#include "types.hh"
#include "types/tuple.hh"
#include "cql3/functions/scalar_function.hh"
#include "cql_serialization_format.hh"
#include "utils/big_decimal.hh"
#include "aggregate_fcts.hh"
#include "user_aggregate.hh"
#include "functions.hh"
#include "native_aggregate_function.hh"
#include "exceptions/exceptions.hh"
#include "utils/multiprecision_int.hh"
#include <cstddef>
#include <cstdint>
#include <optional>
#include <type_traits>

using namespace cql3;
using namespace functions;
using namespace aggregate_fcts;

namespace cql3::functions {
    extern logging::logger log;
}

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
    virtual void set_accumulator(const opt_bytes& acc) override {
        if (acc) {
            _count = value_cast<int64_t>(long_type->deserialize(bytes_view(*acc)));
        } else {
            reset();
        }
    }
    virtual opt_bytes get_accumulator() const override {
        return long_type->decompose(_count);
    }
    virtual void reduce(cql_serialization_format sf, const opt_bytes& acc) override {
        if (acc) {
            auto other = value_cast<int64_t>(long_type->deserialize(bytes_view(*acc)));
            _count += other;
        }
    }
};

class count_rows_function final : public native_aggregate_function {
public:
    count_rows_function() : native_aggregate_function(COUNT_ROWS_FUNCTION_NAME, long_type, {}) {}
    virtual bool is_reducible() const override {
        return true;
    }
    virtual std::unique_ptr<aggregate> new_aggregate() override {
        return std::make_unique<impl_count_function>();
    }
    virtual ::shared_ptr<aggregate_function> reducible_aggregate_function() override {
        return ::make_shared<count_rows_function>();
    };
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

    static data_value decompose_to_data_value(const type& acc) {
        uint64_t upper = acc >> 64;
        uint64_t lower = acc;

        utils::multiprecision_int value(upper);
        value = (value << 64) + lower;
        return value;
    }

    static bytes_opt decompose(const data_value& value) {
        return varint_type->decompose(value);
    }

    static bytes_opt decompose(const type& acc) {
        return varint_type->decompose(decompose_to_data_value(acc));
    }

    static type cast_to_accumulator(const data_value& value) {
        auto mint = value_cast<utils::multiprecision_int>(value);
        uint64_t upper = (uint64_t)(mint >> 64);
        uint64_t lower = (uint64_t)mint;

        __int128 result = upper;
        return (result << 64) | lower;
    }

    static type deserialize(const bytes_opt& acc) {
        return cast_to_accumulator(varint_type->deserialize(*acc));
    }

    static shared_ptr<const abstract_type> data_type() {
        return varint_type;
    }
};

template <typename T>
struct same_type_accumulator_for {
    using type = T;

    static T narrow(type acc) {
        return acc;
    }

    static data_value decompose_to_data_value(const type& acc) {
        return narrow(acc);
    }

    static bytes_opt decompose(const data_value& value) {
        return data_type_for<type>()->decompose(value);
    }

    static bytes_opt decompose(const type& acc) {
        return data_type_for<type>()->decompose(decompose_to_data_value(acc));
    }

    static type cast_to_accumulator(const data_value& value) {
        return value_cast<type>(value);
    }

    static type deserialize(const bytes_opt& acc) {
        return cast_to_accumulator(data_type_for<type>()->deserialize(*acc));
    }

    static shared_ptr<const abstract_type> data_type() {
        return data_type_for<T>();
    }
};

template <typename T>
struct accumulator_for : public std::conditional_t<std::is_integral_v<T>,
                                                   int128_accumulator_for<T>,
                                                   same_type_accumulator_for<T>>
{ };

class impl_user_aggregate : public aggregate_function::aggregate {
    ::shared_ptr<scalar_function> _sfunc;
    ::shared_ptr<scalar_function> _rfunc;
    ::shared_ptr<scalar_function> _finalfunc;
    const bytes_opt _initcond;
    bytes_opt _acc;
public:
    impl_user_aggregate(bytes_opt initcond, ::shared_ptr<scalar_function> sfunc, ::shared_ptr<scalar_function> rfunc, ::shared_ptr<scalar_function> finalfunc)
            : _sfunc(std::move(sfunc))
            , _rfunc(std::move(rfunc))
            , _finalfunc(std::move(finalfunc))
            , _initcond(std::move(initcond))
            , _acc(_initcond)
        {}
    virtual void reset() override {
        _acc = _initcond;
    }
    virtual opt_bytes compute(cql_serialization_format sf) override {
        return _finalfunc ? _finalfunc->execute(sf, std::vector<bytes_opt>{_acc}) : _acc;
    }
    virtual void add_input(cql_serialization_format sf, const std::vector<opt_bytes>& values) override {
        std::vector<bytes_opt> args{_acc};
        args.insert(args.end(), values.begin(), values.end());
        _acc = _sfunc->execute(sf, args);
    }
    virtual void set_accumulator(const opt_bytes& acc) override {
        _acc = acc;
    }
    virtual opt_bytes get_accumulator() const override {
        return _acc;
    }
    virtual void reduce(cql_serialization_format sf, const opt_bytes& acc) override {
        std::vector<bytes_opt> args{_acc, acc};
        _acc = _rfunc->execute(sf, args);
    }
};

template <typename Type>
class impl_sum_function_for : public aggregate_function::aggregate {
protected:
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
    virtual void set_accumulator(const opt_bytes& acc) override {
        if (acc) {
            _sum = accumulator_for<Type>::deserialize(acc);
        } else {
            reset();
        }
    }
    virtual opt_bytes get_accumulator() const override {
        return accumulator_for<Type>::decompose(_sum);
    }
    virtual void reduce(cql_serialization_format sf, const opt_bytes& acc) override {
        if (acc) {
            auto other = accumulator_for<Type>::deserialize(acc);
            _sum += other;
        }
    }
};

template <typename Type>
class impl_reducible_sum_function final : public impl_sum_function_for<Type> {
public:
    virtual bytes_opt compute(cql_serialization_format sf) override {
        return this->get_accumulator();
    }
};

template <typename Type>
class sum_function_for : public native_aggregate_function {
public:
    sum_function_for() : native_aggregate_function("sum", data_type_for<Type>(), { data_type_for<Type>() }) {}
    sum_function_for(data_type return_type, std::vector<data_type> arg_types) 
      : native_aggregate_function("sum", std::move(return_type), std::move(arg_types)) {}

    virtual bool is_reducible() const override {
        return true;
    }

    virtual std::unique_ptr<aggregate> new_aggregate() override {
        return std::make_unique<impl_sum_function_for<Type>>();
    }

    virtual ::shared_ptr<aggregate_function> reducible_aggregate_function() override {
        class reducible_sum_function : public sum_function_for<Type> {
        public:
            reducible_sum_function() : sum_function_for(accumulator_for<Type>::data_type(), { data_type_for<Type>() }) {}

            virtual std::unique_ptr<aggregate> new_aggregate() override {
                return std::make_unique<impl_reducible_sum_function<Type>>();
            }
        };

        return ::make_shared<reducible_sum_function>();
    };
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
class impl_avg_function_for : public aggregate_function::aggregate {
protected:
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
    virtual void set_accumulator(const opt_bytes& acc) override {
        if (acc) {
            data_type tuple_type = tuple_type_impl::get_instance({accumulator_for<Type>::data_type(), long_type});
            auto tuple = value_cast<tuple_type_impl::native_type>(tuple_type->deserialize(bytes_view(*acc)));

            _sum = accumulator_for<Type>::cast_to_accumulator(tuple[0]);
            _count = value_cast<int64_t>(tuple[1]);
        } else {
            reset();
        }
    }
    virtual opt_bytes get_accumulator() const override {
        data_value tuple_val = make_tuple_value(
            tuple_type_impl::get_instance({accumulator_for<Type>::data_type(), long_type}),
            {accumulator_for<Type>::decompose_to_data_value(_sum), data_value(_count)}
        );
        return tuple_val.serialize();
    }
    virtual void reduce(cql_serialization_format sf, const opt_bytes& acc) override {
        if (acc) {
            data_type tuple_type = tuple_type_impl::get_instance({accumulator_for<Type>::data_type(), long_type});
            auto tuple = value_cast<tuple_type_impl::native_type>(tuple_type->deserialize(bytes_view(*acc)));

            _sum += accumulator_for<Type>::cast_to_accumulator(tuple[0]);
            _count += value_cast<int64_t>(tuple[1]);
        }
    }
};

template <typename Type>
class impl_reducible_avg_function : public impl_avg_function_for<Type> {
public:
    virtual bytes_opt compute(cql_serialization_format sf) override {
        return this->get_accumulator();
    }
};

template <typename Type>
class avg_function_for : public native_aggregate_function {
public:
    avg_function_for() : native_aggregate_function("avg", data_type_for<Type>(), { data_type_for<Type>() }) {}
    avg_function_for(data_type return_type, std::vector<data_type> arg_types) 
      : native_aggregate_function("avg", std::move(return_type), std::move(arg_types)) {}

    virtual bool is_reducible() const override {
        return true;
    }

    virtual std::unique_ptr<aggregate> new_aggregate() override {
        return std::make_unique<impl_avg_function_for<Type>>();
    }

    virtual ::shared_ptr<aggregate_function> reducible_aggregate_function() override {
        class reducible_avg_function : public avg_function_for<Type> {
        public:
            reducible_avg_function() : avg_function_for(
                tuple_type_impl::get_instance({accumulator_for<Type>::data_type(), long_type}), 
                { data_type_for<Type>() }
            ) {}

            virtual std::unique_ptr<aggregate> new_aggregate() override {
                return std::make_unique<impl_reducible_avg_function<Type>>();
            }
        };

        return ::make_shared<reducible_avg_function>();
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
    virtual void set_accumulator(const opt_bytes& acc) override {
        if (acc) {
            _max = value_cast<typename aggregate_type_for<Type>::type>(data_type_for<Type>()->deserialize(*acc));
        } else {
            reset();
        }
    }
    virtual opt_bytes get_accumulator() const override {
        if (_max) {
            return data_type_for<Type>()->decompose(data_value(Type{*_max}));
        }
        return {};
    }
    virtual void reduce(cql_serialization_format sf, const opt_bytes& acc) override {
        return add_input(sf, {acc});
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
        if (!_max || !_max->length() || _io_type->less(*_max, *values[0])) {
            _max = values[0];
        }
    }
    virtual void set_accumulator(const opt_bytes& acc) override {
        _max = acc;
    }
    virtual opt_bytes get_accumulator() const override {
        return _max;
    }
    virtual void reduce(cql_serialization_format sf, const opt_bytes& acc) override {
        if (acc && !acc->length()) {
            return;
        }
        return add_input(sf, {acc});
    }
};

template <typename Type>
class max_function_for final : public native_aggregate_function {
public:
    max_function_for() : native_aggregate_function("max", data_type_for<Type>(), { data_type_for<Type>() }) {}
    virtual bool is_reducible() const override {
        return true;
    }
    virtual std::unique_ptr<aggregate> new_aggregate() override {
        return std::make_unique<impl_max_function_for<Type>>();
    }
    virtual ::shared_ptr<aggregate_function> reducible_aggregate_function() override {
        return ::make_shared<max_function_for<Type>>();
    };
};

class max_dynamic_function final : public native_aggregate_function {
    data_type _io_type;
public:
    max_dynamic_function(data_type io_type)
            : native_aggregate_function("max", io_type, { io_type })
            , _io_type(std::move(io_type)) {}
    virtual bool is_reducible() const override {
        return true;
    }            
    virtual std::unique_ptr<aggregate> new_aggregate() override {
        return std::make_unique<impl_max_dynamic_function>(_io_type);
    }
    virtual ::shared_ptr<aggregate_function> reducible_aggregate_function() override {
        return ::make_shared<max_dynamic_function>(_io_type);
    };
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
    virtual void set_accumulator(const opt_bytes& acc) override {
        if (acc) {
            _min = value_cast<typename aggregate_type_for<Type>::type>(data_type_for<Type>()->deserialize(*acc));
        } else {
            reset();
        }
    }
    virtual opt_bytes get_accumulator() const override {
        if (_min) {
            return data_type_for<Type>()->decompose(data_value(Type{*_min}));
        }
        return {};
    }
    virtual void reduce(cql_serialization_format sf, const opt_bytes& acc) override {
        return add_input(sf, {acc});
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
        if (!_min || !_min->length() || _io_type->less(*values[0], *_min)) {
            _min = values[0];
        }
    }
    virtual void set_accumulator(const opt_bytes& acc) override {
        _min = acc;
    }
    virtual opt_bytes get_accumulator() const override {
        return _min;
    }
    virtual void reduce(cql_serialization_format sf, const opt_bytes& acc) override {
        if (acc && !acc->length()) {
            return;
        }
        return add_input(sf, {acc});
    }
};

template <typename Type>
class min_function_for final : public native_aggregate_function {
public:
    min_function_for() : native_aggregate_function("min", data_type_for<Type>(), { data_type_for<Type>() }) {}
    virtual bool is_reducible() const override {
        return true;
    }
    virtual std::unique_ptr<aggregate> new_aggregate() override {
        return std::make_unique<impl_min_function_for<Type>>();
    }
    virtual ::shared_ptr<aggregate_function> reducible_aggregate_function() override {
        return ::make_shared<min_function_for<Type>>();
    };
};

class min_dynamic_function final : public native_aggregate_function {
    data_type _io_type;
public:
    min_dynamic_function(data_type io_type)
            : native_aggregate_function("min", io_type, { io_type })
            , _io_type(std::move(io_type)) {}
    virtual bool is_reducible() const override {
        return true;
    }
    virtual std::unique_ptr<aggregate> new_aggregate() override {
        return std::make_unique<impl_min_dynamic_function>(_io_type);
    }
    virtual ::shared_ptr<aggregate_function> reducible_aggregate_function() override {
        return ::make_shared<min_dynamic_function>(_io_type);
    };
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
    virtual void set_accumulator(const opt_bytes& acc) override {
        if (acc) {
            _count = value_cast<int64_t>(long_type->deserialize(bytes_view(*acc)));
        } else {
            reset();
        }
    }
    virtual opt_bytes get_accumulator() const override {
        return long_type->decompose(_count);
    }
    virtual void reduce(cql_serialization_format sf, const opt_bytes& acc) override {
        if (acc) {
            auto other = value_cast<int64_t>(long_type->deserialize(bytes_view(*acc)));
            _count += other;
        }
    }
};

template <typename Type>
class count_function_for final : public native_aggregate_function {
public:
    count_function_for() : native_aggregate_function("count", long_type, { data_type_for<Type>() }) {}
    virtual bool is_reducible() const override {
        return true;
    }
    virtual std::unique_ptr<aggregate> new_aggregate() override {
        return std::make_unique<impl_count_function_for<Type>>();
    }
    virtual ::shared_ptr<aggregate_function> reducible_aggregate_function() override {
        return ::make_shared<count_function_for<Type>>();
    };
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

// Drops the first arg type from the types declaration (which denotes the accumulator)
// in order to compute the actual type of given user-defined-aggregate (UDA)
static std::vector<data_type> state_arg_types_to_uda_arg_types(const std::vector<data_type>& arg_types) {
    if(arg_types.size() < 2) {
        on_internal_error(cql3::functions::log, "State function for user-defined aggregates needs at least two arguments");
    }
    std::vector<data_type> types;
    types.insert(types.end(), std::next(arg_types.begin()), arg_types.end());
    return types;
}

static data_type uda_return_type(const ::shared_ptr<scalar_function>& ffunc, const ::shared_ptr<scalar_function>& sfunc) {
    return ffunc ? ffunc->return_type() : sfunc->return_type();
}

user_aggregate::user_aggregate(function_name fname, bytes_opt initcond, ::shared_ptr<scalar_function> sfunc, ::shared_ptr<scalar_function> reducefunc, ::shared_ptr<scalar_function> finalfunc)
        : abstract_function(std::move(fname), state_arg_types_to_uda_arg_types(sfunc->arg_types()), uda_return_type(finalfunc, sfunc))
        , _initcond(std::move(initcond))
        , _sfunc(std::move(sfunc))
        , _reducefunc(std::move(reducefunc))
        , _finalfunc(std::move(finalfunc))
{}

std::unique_ptr<aggregate_function::aggregate> user_aggregate::new_aggregate() {
    return std::make_unique<impl_user_aggregate>(_initcond, _sfunc, _reducefunc, _finalfunc);
}

::shared_ptr<aggregate_function> user_aggregate::reducible_aggregate_function() {
    auto name = _name;
    name.name += "_reducible";
    return ::make_shared<user_aggregate>(name, _initcond, _sfunc, _reducefunc, nullptr);
}

bool user_aggregate::is_pure() const { return _sfunc->is_pure() && (!_finalfunc || _finalfunc->is_pure()); }
bool user_aggregate::is_native() const { return false; }
bool user_aggregate::is_aggregate() const { return true; }
bool user_aggregate::is_reducible() const { return _reducefunc != nullptr; }
bool user_aggregate::requires_thread() const { return _sfunc->requires_thread() || (_finalfunc && _finalfunc->requires_thread()); }
bool user_aggregate::has_finalfunc() const { return _finalfunc != nullptr; }

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
