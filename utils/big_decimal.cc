/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "utils/assert.hh"
#include "big_decimal.hh"
#include <cassert>
#include "marshal_exception.hh"
#include <seastar/core/format.hh>

#ifdef __clang__

// Clang or boost have a problem navigating the enable_if maze
// that is cpp_int's constructor. It ends up treating the
// string_view as binary and "0" ends up 48.

// Work around by casting to string.
using string_view_workaround = std::string;

#else

using string_view_workaround = std::string_view;

#endif

uint64_t from_varint_to_integer(const utils::multiprecision_int& varint) {
    // The behavior CQL expects on overflow is for values to wrap
    // around. For cpp_int conversion functions, the behavior is to
    // return the largest or smallest number that the target type can
    // represent. To implement one with the other, we first mask the
    // low 64 bits, convert to a uint64_t, and then let c++ convert,
    // with possible overflow, to ToType.
    return static_cast<uint64_t>(~static_cast<uint64_t>(0) & boost::multiprecision::cpp_int(varint));
}

big_decimal::big_decimal() : big_decimal(0, 0) {}
big_decimal::big_decimal(int32_t scale, boost::multiprecision::cpp_int unscaled_value)
    : _scale(scale), _unscaled_value(std::move(unscaled_value)) {}

big_decimal::big_decimal(std::string_view text)
{
    size_t e_pos = text.find_first_of("eE");
    std::string_view base = text.substr(0, e_pos);
    std::string_view exponent;
    if (e_pos != std::string_view::npos) {
        exponent = text.substr(e_pos + 1);
        if (exponent.empty()) {
            throw marshal_exception(seastar::format("big_decimal - incorrect empty exponent: {}", text));
        }
    }
    size_t dot_pos = base.find_first_of(".");
    std::string integer_str(base.substr(0, dot_pos));
    std::string_view fraction;
    if (dot_pos != std::string_view::npos) {
        fraction = base.substr(dot_pos + 1);
        integer_str.append(fraction);
    }
    std::string_view integer(integer_str);
    const bool negative = !integer.empty() && integer.front() == '-';
    integer.remove_prefix(negative || (!integer.empty() && integer.front() == '+'));

    if (integer.empty()) {
        throw marshal_exception(format("big_decimal - both integer and fraction are empty"));
    } else if (!::isdigit(integer.front())) {
        throw marshal_exception(seastar::format("big_decimal - incorrect integer: {}", text));
    }

    integer.remove_prefix(std::min(integer.find_first_not_of("0"), integer.size() - 1));
    try {
        _unscaled_value = boost::multiprecision::cpp_int(string_view_workaround(integer));
    } catch (...) {
        throw marshal_exception(seastar::format("big_decimal - failed to parse integer value: {}", integer));
    }
    if (negative) {
        _unscaled_value *= -1;
    }
    try {
        _scale = exponent.empty() ? 0 : -boost::lexical_cast<int32_t>(exponent);
    } catch (...) {
        throw marshal_exception(seastar::format("big_decimal - failed to parse exponent: {}", exponent));
    }
    _scale += fraction.size();
}

boost::multiprecision::cpp_rational big_decimal::as_rational() const {
    boost::multiprecision::cpp_int ten(10);
    auto unscaled_value = static_cast<const boost::multiprecision::cpp_int&>(_unscaled_value);
    boost::multiprecision::cpp_rational r = unscaled_value;
    int32_t abs_scale = std::abs(_scale);
    auto pow = boost::multiprecision::pow(ten, abs_scale);
    if (_scale < 0) {
        r *= pow;
    } else {
        r /= pow;
    }
    return r;
}

sstring big_decimal::to_string() const
{
    if (!_unscaled_value) {
        return "0";
    }
    boost::multiprecision::cpp_int num = boost::multiprecision::abs(_unscaled_value);
    auto str = num.str();
    if (_scale < 0) {
        for (int i = 0; i > _scale; i--) {
            str.push_back('0');
        }
    } else if (_scale > 0) {
        if (str.size() > unsigned(_scale)) {
            str.insert(str.size() - _scale, 1, '.');
        } else {
            std::string nstr = "0.";
            nstr.append(_scale - str.size(), '0');
            nstr.append(str);
            str = std::move(nstr);
        }

        while (str.back() == '0') {
            str.pop_back();
        }
        if (str.back() == '.') {
            str.pop_back();
        }
    }
    if (_unscaled_value < 0) {
        str.insert(0, 1, '-');
    }
    return str;
}

std::strong_ordering big_decimal::tri_cmp_slow(const big_decimal& other) const
{
    auto max_scale = std::max(_scale, other._scale);
    boost::multiprecision::cpp_int rescale(10);
    boost::multiprecision::cpp_int x = _unscaled_value * boost::multiprecision::pow(rescale, max_scale - _scale);
    boost::multiprecision::cpp_int y = other._unscaled_value * boost::multiprecision::pow(rescale, max_scale - other._scale);
    return x.compare(y) <=> 0;
}

std::strong_ordering big_decimal::operator<=>(const big_decimal& other) const
{
    if (_scale == other._scale) {
        return _unscaled_value.compare(other._unscaled_value) <=> 0;
    }

    // boost::multiprecision::sign() returns -1, 0 or 1
    const int sign = boost::multiprecision::sign(_unscaled_value);
    const int sign_other = boost::multiprecision::sign(other._unscaled_value);
    if (sign != sign_other) {
        return sign <=> sign_other;
    }
    // At this point we know the two signs are equal, so if sign == 0, both signs
    // and consequently both numbers are zeros.
    if (sign == 0) {
        return std::strong_ordering::equal;
    }

    // At this point we know that both numbers have the same sign, so if one is negative, the other is too.
    // If the number are negative, we invert the sign and compare them in reverse.
    // This creates a copy, but the copy cannot be avoided anyway, because
    // boost::multiprecision::msb() (used below) doesn't work with negative numbers.
    if (sign < 0) {
        auto a = -*this;
        auto b = -other;
        return b.tri_cmp_positive_nonzero_different_scale(a);
    }
    return tri_cmp_positive_nonzero_different_scale(other);
}

std::strong_ordering big_decimal::tri_cmp_positive_nonzero_different_scale(const big_decimal& other) const
{
    // At this point we know that the numbers:
    // * have different scale
    // * are positive
    // * neither is zero
    //
    // The numbers have the form:
    //
    //      auto number = _unscaled_value * std::pow(10, -_scale);
    //      auto number_other = other._unscaled_value * std::pow(10, -other._scale);
    //
    // To compare them, we make the unscaled values the same (or close), so we can directly compare the scales.
    // To do that we want to compute unscaled_ratio_log2:
    //
    //      auto unscaled_ratio = _unscaled_value / other._unscaled_value;
    //      auto unscaled_ratio_log2 = log2(unscaled_ratio);
    //
    // To avoid using division and then calculating a log2(), we use the MSB of
    // both numbers to infer unscaled_ratio_log2 directly.
    const int64_t msb = boost::multiprecision::msb(_unscaled_value);
    const int64_t msb_other = boost::multiprecision::msb(other._unscaled_value);
    const int64_t unscaled_ratio_log2 = msb - msb_other;

    // Now we can rewrite the original numbers as follows:
    //
    //      auto number = _unscaled_value * std::pow(10, -_scale);
    //      auto number_other = other._unscaled_value * std::pow(10, -other._scale);
    //      auto number_other_approx = _unscaled_value * std::pow(2, unscaled_ratio_log2) * std::pow(10, -other._scale);
    //
    // Notice that number_other_approx != number_other, but it is close, the following holds:
    //
    //      assert(number_other/2 <= number_other_approx && number_other_approx <= number_other*2);
    //
    // Now we can almost compare the two scales, we just need to bring the scale bases to the same base of 10.
    // We can observe that:
    //
    //      std::pow(2, x) = std::pow(10, x / log2(10));
    //
    // Using this we can rewrite the above numbers again:
    //
    //      auto scale_adjustement = unscaled_ratio_log2 / log2_10;
    //
    //      auto number = _unscaled_value * std::pow(10, -_scale);
    //      auto number_other = other._unscaled_value * std::pow(10, -other._scale);
    //      auto number_other_approx = _unscaled_value * std::pow(10, scale_adjustement - other._scale);
    const static double log2_10 = std::log2(10.0);
    const double scale_adjustement = double(unscaled_ratio_log2) / log2_10;

    // Now the scales are directly comparable.
    double diff_scale = double(_scale) - double(other._scale);
    // Note that diff_scale has inverted sign, because the implicit sign of _scale is negative,
    // We have to use subtraction here to account for that.
    diff_scale -= scale_adjustement;

    // This is our confidence window for estimating the difference (in the power of 10) between the numbers.
    // We have to account for two things here:
    // * inaccuracy in the log2(10)
    // * maximum difference between the unscaled values, after normalizing them to the same bit-count, which is order of 2
    //
    // If the numbers are closer than our confidence window, we fall back to slow but precise tri_cmp_slow().
    if (-1.0 < diff_scale && diff_scale < 1.0) {
        return tri_cmp_slow(other);
    }
    // Need to invert the sign, see comment above calculating diff_scale.
    return int64_t(-diff_scale) <=> 0;
}

big_decimal& big_decimal::operator+=(const big_decimal& other)
{
    if (_scale == other._scale) {
        _unscaled_value += other._unscaled_value;
    } else {
        boost::multiprecision::cpp_int rescale(10);
        auto max_scale = std::max(_scale, other._scale);
        boost::multiprecision::cpp_int u = _unscaled_value * boost::multiprecision::pow(rescale,  max_scale - _scale);
        boost::multiprecision::cpp_int v = other._unscaled_value * boost::multiprecision::pow(rescale, max_scale - other._scale);
        _unscaled_value = u + v;
        _scale = max_scale;
    }
    return *this;
}

big_decimal& big_decimal::operator-=(const big_decimal& other) {
    if (_scale == other._scale) {
        _unscaled_value -= other._unscaled_value;
    } else {
        boost::multiprecision::cpp_int rescale(10);
        auto max_scale = std::max(_scale, other._scale);
        boost::multiprecision::cpp_int u = _unscaled_value * boost::multiprecision::pow(rescale,  max_scale - _scale);
        boost::multiprecision::cpp_int v = other._unscaled_value * boost::multiprecision::pow(rescale, max_scale - other._scale);
        _unscaled_value = u - v;
        _scale = max_scale;
    }
    return *this;
}

big_decimal big_decimal::operator+(const big_decimal& other) const {
    big_decimal ret(*this);
    ret += other;
    return ret;
}

big_decimal big_decimal::operator-(const big_decimal& other) const {
    big_decimal ret(*this);
    ret -= other;
    return ret;
}

big_decimal big_decimal::operator-() const {
    big_decimal ret;
    ret._unscaled_value = -_unscaled_value;
    ret._scale = _scale;
    return ret;
}

big_decimal big_decimal::div(const ::uint64_t y, const rounding_mode mode) const
{
    if (mode != rounding_mode::HALF_EVEN) {
        SCYLLA_ASSERT(0);
    }

    // Implementation of Division with Half to Even (aka Bankers) Rounding
    const boost::multiprecision::cpp_int sign = _unscaled_value >= 0 ? +1 : -1;
    const boost::multiprecision::cpp_int a = sign * _unscaled_value;
    // cpp_int uses lazy evaluation and for older versions of boost and some
    //   versions of gcc, expression templates have problem to implicitly
    //   convert to cpp_int, so we force the conversion explicitly before cpp_int
    //   is converted to uint64_t.
    const uint64_t r = boost::multiprecision::cpp_int{a % y}.convert_to<uint64_t>();

    boost::multiprecision::cpp_int q = a / y;

    /*
     * Value r/y is fractional part of (*this)/y that is used to determine
     *   the direction of rounding.
     * For rounding one has to consider r/y cmp 1/2 or equivalently:
     *   2*r cmp y.
     */
    if (2*r < y) {
        /* Number has its final value */
    } else if (2*r > y) {
        q += 1;
    } else if (q % 2 == 1) {
        /* Change to closest even number */
        q += 1;
    }

    return big_decimal(_scale, sign * q);
}
