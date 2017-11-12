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

#pragma once

#include <boost/multiprecision/cpp_int.hpp>

#include "bytes.hh"

class big_decimal {
private:
    int32_t _scale;
    boost::multiprecision::cpp_int _unscaled_value;
public:
    enum class rounding_mode {
        HALF_EVEN,
    };

    big_decimal(sstring_view text);
    big_decimal() : big_decimal(0, 0) {}
    big_decimal(int32_t scale, boost::multiprecision::cpp_int unscaled_value)
        : _scale(scale), _unscaled_value(unscaled_value)
    { }

    int32_t scale() const { return _scale; }
    const boost::multiprecision::cpp_int& unscaled_value() const { return _unscaled_value; }

    sstring to_string() const;

    int compare(const big_decimal& other) const;

    big_decimal& operator+=(const big_decimal& other);
    big_decimal div(const ::uint64_t y, const rounding_mode mode) const;
    friend bool operator<(const big_decimal& x, const big_decimal& y) { return x.compare(y) < 0; }
    friend bool operator<=(const big_decimal& x, const big_decimal& y) { return x.compare(y) <= 0; }
    friend bool operator==(const big_decimal& x, const big_decimal& y) { return x.compare(y) == 0; }
    friend bool operator!=(const big_decimal& x, const big_decimal& y) { return x.compare(y) != 0; }
    friend bool operator>=(const big_decimal& x, const big_decimal& y) { return x.compare(y) >= 0; }
    friend bool operator>(const big_decimal& x, const big_decimal& y) { return x.compare(y) > 0; }
};
