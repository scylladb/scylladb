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

#pragma once

#include "multiprecision_int.hh"
#include <boost/multiprecision/cpp_int.hpp>
#include <ostream>

#include "bytes.hh"

uint64_t from_varint_to_integer(const utils::multiprecision_int& varint);

class big_decimal {
private:
    int32_t _scale;
    boost::multiprecision::cpp_int _unscaled_value;
public:
    enum class rounding_mode {
        HALF_EVEN,
    };

    explicit big_decimal(sstring_view text);
    big_decimal();
    big_decimal(int32_t scale, boost::multiprecision::cpp_int unscaled_value);

    int32_t scale() const { return _scale; }
    const boost::multiprecision::cpp_int& unscaled_value() const { return _unscaled_value; }
    boost::multiprecision::cpp_rational as_rational() const;

    sstring to_string() const;

    int compare(const big_decimal& other) const;

    big_decimal& operator+=(const big_decimal& other);
    big_decimal& operator-=(const big_decimal& other);
    big_decimal operator+(const big_decimal& other) const;
    big_decimal operator-(const big_decimal& other) const;
    big_decimal div(const ::uint64_t y, const rounding_mode mode) const;
    friend bool operator<(const big_decimal& x, const big_decimal& y) { return x.compare(y) < 0; }
    friend bool operator<=(const big_decimal& x, const big_decimal& y) { return x.compare(y) <= 0; }
    friend bool operator==(const big_decimal& x, const big_decimal& y) { return x.compare(y) == 0; }
    friend bool operator!=(const big_decimal& x, const big_decimal& y) { return x.compare(y) != 0; }
    friend bool operator>=(const big_decimal& x, const big_decimal& y) { return x.compare(y) >= 0; }
    friend bool operator>(const big_decimal& x, const big_decimal& y) { return x.compare(y) > 0; }
};

inline std::ostream& operator<<(std::ostream& s, const big_decimal& v) {
    return s << v.to_string();
}
