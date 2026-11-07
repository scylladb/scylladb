/*
 * Copyright (C) 2026-present ScyllaDB
 *
 * Ported from Apache Cassandra MathFcts (CASSANDRA-17221).
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#include "functions.hh"
#include "native_scalar_function.hh"
#include "utils/big_decimal.hh"
#include "utils/multiprecision_int.hh"

#include <boost/multiprecision/cpp_dec_float.hpp>
#include <boost/multiprecision/cpp_int.hpp>

#include <cmath>
#include <limits>
#include <string>
#include <type_traits>

namespace cql3::functions {
namespace {

// Match Cassandra DecimalType transcendental precision (MIN_SIGNIFICANT_DIGITS = 32).
constexpr int min_significant_digits = 32;
constexpr int max_precision = 10000;

using dec_float = boost::multiprecision::number<boost::multiprecision::cpp_dec_float<50>>;

template <typename T>
requires std::is_integral_v<T>
T java_cast_from_double(double d) {
    // Java casting rules used by Cassandra's integral Math.* overloads.
    if (std::isnan(d)) {
        return 0;
    }
    if (d >= static_cast<double>(std::numeric_limits<T>::max())) {
        return std::numeric_limits<T>::max();
    }
    if (d <= static_cast<double>(std::numeric_limits<T>::min())) {
        return std::numeric_limits<T>::min();
    }
    return static_cast<T>(d);
}

// Java Math.round(float): (int) floor(a + 0.5f)
float java_round_float(float a) {
    return static_cast<float>(static_cast<int32_t>(std::floor(a + 0.5f)));
}

// Java Math.round(double): (long) floor(a + 0.5)
double java_round_double(double a) {
    return static_cast<double>(static_cast<int64_t>(std::floor(a + 0.5)));
}

int decimal_precision(const big_decimal& d) {
    const auto& u = d.unscaled_value();
    if (!u) {
        return 1;
    }
    boost::multiprecision::cpp_int abs_u = boost::multiprecision::abs(u);
    return static_cast<int>(abs_u.str().size());
}

int math_context_precision(const big_decimal& input) {
    int precision = decimal_precision(input);
    precision = std::max(min_significant_digits, precision);
    return std::min(max_precision, precision);
}

big_decimal decimal_from_dec_float(const dec_float& v, int precision) {
    // Use scientific string so big_decimal's parser preserves significant digits.
    auto s = v.str(precision, std::ios_base::scientific);
    return big_decimal(s);
}

dec_float decimal_to_dec_float(const big_decimal& d) {
    return dec_float(std::string(d.to_string()));
}

big_decimal decimal_abs(const big_decimal& input) {
    return big_decimal(input.scale(), boost::multiprecision::abs(input.unscaled_value()));
}

big_decimal decimal_exp(const big_decimal& input) {
    auto precision = math_context_precision(input);
    return decimal_from_dec_float(exp(decimal_to_dec_float(input)), precision);
}

big_decimal decimal_log(const big_decimal& input) {
    if (input <=> big_decimal(0) != std::strong_ordering::greater) {
        throw std::domain_error("Natural log of number zero or less");
    }
    auto precision = math_context_precision(input);
    return decimal_from_dec_float(log(decimal_to_dec_float(input)), precision);
}

big_decimal decimal_log10(const big_decimal& input) {
    if (input <=> big_decimal(0) != std::strong_ordering::greater) {
        throw std::domain_error("Log10 of number zero or less");
    }
    auto precision = math_context_precision(input);
    return decimal_from_dec_float(log10(decimal_to_dec_float(input)), precision);
}

big_decimal decimal_round_half_up(const big_decimal& input) {
    // BigDecimal.setScale(0, RoundingMode.HALF_UP)
    using cpp_int = boost::multiprecision::cpp_int;
    const cpp_int ten(10);
    if (input.scale() <= 0) {
        cpp_int val = input.unscaled_value() * boost::multiprecision::pow(ten, -input.scale());
        return big_decimal(0, std::move(val));
    }
    cpp_int divisor = boost::multiprecision::pow(ten, input.scale());
    bool negative = input.unscaled_value() < 0;
    cpp_int abs_unscaled = boost::multiprecision::abs(input.unscaled_value());
    cpp_int q = abs_unscaled / divisor;
    cpp_int rem = abs_unscaled % divisor;
    if (rem * 2 >= divisor) {
        q += 1;
    }
    if (negative) {
        q = -q;
    }
    return big_decimal(0, std::move(q));
}

utils::multiprecision_int truncate_decimal_to_varint(const big_decimal& d) {
    auto r = d.as_rational();
    return utils::multiprecision_int(
            boost::multiprecision::numerator(r) / boost::multiprecision::denominator(r));
}

template <typename T>
bytes_opt serialize_result(T value) {
    return data_value(std::move(value)).serialize_nonnull();
}

template <typename T, typename Op>
shared_ptr<function> make_unary_math_function(sstring name, data_type type, Op op) {
    return make_native_scalar_function<true>(std::move(name), type, {type},
            [type, op = std::move(op)] (std::span<const bytes_opt> parameters) -> bytes_opt {
        if (!parameters[0]) {
            return std::nullopt;
        }
        auto v = value_cast<T>(type->deserialize_value(*parameters[0]));
        return serialize_result(op(std::move(v)));
    });
}

// Counter shares bigint serialization but is a distinct CQL type.
shared_ptr<function> make_counter_math_function(sstring name, int64_t (*op)(int64_t)) {
    return make_native_scalar_function<true>(std::move(name), counter_type, {counter_type},
            [op] (std::span<const bytes_opt> parameters) -> bytes_opt {
        if (!parameters[0]) {
            return std::nullopt;
        }
        auto v = value_cast<int64_t>(counter_type->deserialize_value(*parameters[0]));
        return serialize_result(op(v));
    });
}

template <typename T>
requires std::is_integral_v<T>
T integral_abs(T v) {
    // Match Java: abs(MIN) stays MIN for fixed-width signed types after narrowing.
    if (v < 0 && v == std::numeric_limits<T>::min()) {
        return v;
    }
    return v < 0 ? static_cast<T>(-v) : v;
}

template <typename T>
requires std::is_integral_v<T>
T integral_exp(T v) {
    return java_cast_from_double<T>(std::exp(static_cast<double>(v)));
}

template <typename T>
requires std::is_integral_v<T>
T integral_log(T v) {
    return java_cast_from_double<T>(std::log(static_cast<double>(v)));
}

template <typename T>
requires std::is_integral_v<T>
T integral_log10(T v) {
    return java_cast_from_double<T>(std::log10(static_cast<double>(v)));
}

template <typename T>
requires std::is_integral_v<T>
T integral_round(T v) {
    return v;
}

utils::multiprecision_int varint_abs(utils::multiprecision_int v) {
    const auto& cpp = static_cast<const boost::multiprecision::cpp_int&>(v);
    return utils::multiprecision_int(boost::multiprecision::abs(cpp));
}

utils::multiprecision_int varint_exp(utils::multiprecision_int v) {
    big_decimal as_decimal(0, static_cast<const boost::multiprecision::cpp_int&>(v));
    return truncate_decimal_to_varint(decimal_exp(as_decimal));
}

utils::multiprecision_int varint_log(utils::multiprecision_int v) {
    big_decimal as_decimal(0, static_cast<const boost::multiprecision::cpp_int&>(v));
    return truncate_decimal_to_varint(decimal_log(as_decimal));
}

utils::multiprecision_int varint_log10(utils::multiprecision_int v) {
    big_decimal as_decimal(0, static_cast<const boost::multiprecision::cpp_int&>(v));
    return truncate_decimal_to_varint(decimal_log10(as_decimal));
}

utils::multiprecision_int varint_round(utils::multiprecision_int v) {
    return v;
}

template <typename T>
void declare_integral_math(declared_t& funcs, data_type type) {
    auto declare = [&funcs] (shared_ptr<function> f) { funcs.emplace(f->name(), std::move(f)); };
    declare(make_unary_math_function<T>("abs", type, integral_abs<T>));
    declare(make_unary_math_function<T>("exp", type, integral_exp<T>));
    declare(make_unary_math_function<T>("log", type, integral_log<T>));
    declare(make_unary_math_function<T>("log10", type, integral_log10<T>));
    declare(make_unary_math_function<T>("round", type, integral_round<T>));
}

void declare_counter_math(declared_t& funcs) {
    auto declare = [&funcs] (shared_ptr<function> f) { funcs.emplace(f->name(), std::move(f)); };
    declare(make_counter_math_function("abs", integral_abs<int64_t>));
    declare(make_counter_math_function("exp", integral_exp<int64_t>));
    declare(make_counter_math_function("log", integral_log<int64_t>));
    declare(make_counter_math_function("log10", integral_log10<int64_t>));
    declare(make_counter_math_function("round", integral_round<int64_t>));
}

void declare_float_math(declared_t& funcs) {
    auto declare = [&funcs] (shared_ptr<function> f) { funcs.emplace(f->name(), std::move(f)); };
    declare(make_unary_math_function<float>("abs", float_type, [] (float v) { return std::fabs(v); }));
    declare(make_unary_math_function<float>("exp", float_type, [] (float v) { return static_cast<float>(std::exp(v)); }));
    declare(make_unary_math_function<float>("log", float_type, [] (float v) { return static_cast<float>(std::log(v)); }));
    declare(make_unary_math_function<float>("log10", float_type, [] (float v) { return static_cast<float>(std::log10(v)); }));
    declare(make_unary_math_function<float>("round", float_type, java_round_float));
}

void declare_double_math(declared_t& funcs) {
    auto declare = [&funcs] (shared_ptr<function> f) { funcs.emplace(f->name(), std::move(f)); };
    declare(make_unary_math_function<double>("abs", double_type, [] (double v) { return std::fabs(v); }));
    declare(make_unary_math_function<double>("exp", double_type, [] (double v) { return std::exp(v); }));
    declare(make_unary_math_function<double>("log", double_type, [] (double v) { return std::log(v); }));
    declare(make_unary_math_function<double>("log10", double_type, [] (double v) { return std::log10(v); }));
    declare(make_unary_math_function<double>("round", double_type, java_round_double));
}

void declare_varint_math(declared_t& funcs) {
    auto declare = [&funcs] (shared_ptr<function> f) { funcs.emplace(f->name(), std::move(f)); };
    declare(make_unary_math_function<utils::multiprecision_int>("abs", varint_type, varint_abs));
    declare(make_unary_math_function<utils::multiprecision_int>("exp", varint_type, varint_exp));
    declare(make_unary_math_function<utils::multiprecision_int>("log", varint_type, varint_log));
    declare(make_unary_math_function<utils::multiprecision_int>("log10", varint_type, varint_log10));
    declare(make_unary_math_function<utils::multiprecision_int>("round", varint_type, varint_round));
}

void declare_decimal_math(declared_t& funcs) {
    auto declare = [&funcs] (shared_ptr<function> f) { funcs.emplace(f->name(), std::move(f)); };
    declare(make_unary_math_function<big_decimal>("abs", decimal_type, decimal_abs));
    declare(make_unary_math_function<big_decimal>("exp", decimal_type, decimal_exp));
    declare(make_unary_math_function<big_decimal>("log", decimal_type, decimal_log));
    declare(make_unary_math_function<big_decimal>("log10", decimal_type, decimal_log10));
    declare(make_unary_math_function<big_decimal>("round", decimal_type, decimal_round_half_up));
}

} // anonymous namespace

void add_math_functions(declared_t& funcs) {
    declare_integral_math<int8_t>(funcs, byte_type);
    declare_integral_math<int16_t>(funcs, short_type);
    declare_integral_math<int32_t>(funcs, int32_type);
    declare_integral_math<int64_t>(funcs, long_type);
    declare_counter_math(funcs);
    declare_float_math(funcs);
    declare_double_math(funcs);
    declare_varint_math(funcs);
    declare_decimal_math(funcs);
}

}
