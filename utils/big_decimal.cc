/*
 * Copyright (C) 2015-present ScyllaDB
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

#include "big_decimal.hh"
#include <cassert>
#include "marshal_exception.hh"
#include <seastar/core/print.hh>

#include <regex>

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

big_decimal::big_decimal(sstring_view text)
{
    size_t e_pos = text.find_first_of("eE");
    std::string_view base = text.substr(0, e_pos);
    std::string_view exponent;
    if (e_pos != std::string_view::npos) {
        exponent = text.substr(e_pos + 1);
        if (exponent.empty()) {
            throw marshal_exception(format("big_decimal - incorrect empty exponent: {}", text));
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
        throw marshal_exception(format("big_decimal - incorrect integer: {}", text));
    }

    integer.remove_prefix(std::min(integer.find_first_not_of("0"), integer.size() - 1));
    try {
        _unscaled_value = boost::multiprecision::cpp_int(string_view_workaround(integer));
    } catch (...) {
        throw marshal_exception(format("big_decimal - failed to parse integer value: {}", integer));
    }
    if (negative) {
        _unscaled_value *= -1;
    }
    try {
        _scale = exponent.empty() ? 0 : -boost::lexical_cast<int32_t>(exponent);
    } catch (...) {
        throw marshal_exception(format("big_decimal - failed to parse exponent: {}", exponent));
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

int big_decimal::compare(const big_decimal& other) const
{
    auto max_scale = std::max(_scale, other._scale);
    boost::multiprecision::cpp_int rescale(10);
    boost::multiprecision::cpp_int x = _unscaled_value * boost::multiprecision::pow(rescale, max_scale - _scale);
    boost::multiprecision::cpp_int y = other._unscaled_value * boost::multiprecision::pow(rescale, max_scale - other._scale);
    return x == y ? 0 : x < y ? -1 : 1;
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

big_decimal big_decimal::div(const ::uint64_t y, const rounding_mode mode) const
{
    if (mode != rounding_mode::HALF_EVEN) {
        assert(0);
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
