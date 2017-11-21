/*
 * Copyright (C) 2015 ScyllaDB
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
#include "core/print.hh"

#include <regex>

big_decimal::big_decimal(sstring_view text)
{
    std::string str = text.to_string();
    static const std::regex big_decimal_re("^([\\+\\-]?)([0-9]*)(\\.([0-9]*))?([eE]([\\+\\-]?[0-9]+))?");
    std::smatch sm;
    if (!std::regex_match(str, sm, big_decimal_re)) {
        throw marshal_exception(sprint("big_decimal contains invalid characters: '%s'", str));
    }
    bool negative = sm[1] == "-";
    auto integer = sm[2].str();
    auto fraction = sm[4].str();
    auto exponent = sm[6].str();
    if (integer.empty() && fraction.empty()) {
        throw marshal_exception(sprint("big_decimal - both integer and fraction are empty: '%s'", str));
    }
    integer.append(fraction);
    unsigned i;
    for (i = 0; i < integer.size() - 1 && integer[i] == '0'; i++);
    integer = integer.substr(i);

    _unscaled_value = boost::multiprecision::cpp_int(integer);
    if (negative) {
        _unscaled_value *= -1;
    }
    _scale = exponent.empty() ? 0 : -boost::lexical_cast<int32_t>(exponent);
    _scale += fraction.size();
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
