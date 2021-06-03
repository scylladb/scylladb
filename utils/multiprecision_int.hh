/*
 * Copyright (C) 2020-present ScyllaDB
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
#include <iosfwd>

namespace utils {

// multiprecision_int is a thin wrapper around boost::multiprecision::cpp_int.
// cpp_int is worth about 20,000 lines of header files that are rarely used.
// Forward-declaring cpp_int is very difficult as it is a complicated template,
// hence this wrapper.
//
// Because cpp_int uses a lot of expression templates, the code below contains
// many casts since the expression templates defeat regular C++ conversion rules.

class multiprecision_int final {
public:
    using cpp_int = boost::multiprecision::cpp_int;
private:
    cpp_int _v;
private:
    // maybe_unwrap() selectively unwraps multiprecision_int values (leaving
    // anything else unchanged), so avoid confusing boost::multiprecision.
    static const cpp_int& maybe_unwrap(const multiprecision_int& x) {
        return x._v;
    }
    template <typename T>
    static const T& maybe_unwrap(const T& x) {
        return x;
    }
public:
    multiprecision_int() = default;
    multiprecision_int(cpp_int x) : _v(std::move(x)) {}
    explicit multiprecision_int(int x) : _v(x) {}
    explicit multiprecision_int(unsigned x) : _v(x) {}
    explicit multiprecision_int(long x) : _v(x) {}
    explicit multiprecision_int(unsigned long x) : _v(x) {}
    explicit multiprecision_int(long long x) : _v(x) {}
    explicit multiprecision_int(unsigned long long x) : _v(x) {}
    explicit multiprecision_int(float x) : _v(x) {}
    explicit multiprecision_int(double x) : _v(x) {}
    explicit multiprecision_int(long double x) : _v(x) {}
    explicit multiprecision_int(const std::string x) : _v(x) {}
    explicit multiprecision_int(const char* x) : _v(x) {}
    operator const cpp_int&() const {
        return _v;
    }
    explicit operator signed char() const {
        return static_cast<signed char>(_v);
    }
    explicit operator unsigned char() const {
        return static_cast<unsigned char>(_v);
    }
    explicit operator short() const {
        return static_cast<short>(_v);
    }
    explicit operator unsigned short() const {
        return static_cast<unsigned short>(_v);
    }
    explicit operator int() const {
        return static_cast<int>(_v);
    }
    explicit operator unsigned() const {
        return static_cast<unsigned>(_v);
    }
    explicit operator long() const {
        return static_cast<long>(_v);
    }
    explicit operator unsigned long() const {
        return static_cast<unsigned long>(_v);
    }
    explicit operator long long() const {
        return static_cast<long long>(_v);
    }
    explicit operator unsigned long long() const {
        return static_cast<unsigned long long>(_v);
    }
    explicit operator float() const {
        return static_cast<float>(_v);
    }
    explicit operator double() const {
        return static_cast<double>(_v);
    }
    explicit operator long double() const {
        return static_cast<long double>(_v);
    }
    template <typename T>
    multiprecision_int& operator+=(const T& x) {
        _v += maybe_unwrap(x);
        return *this;
    }
    template <typename T>
    multiprecision_int& operator-=(const T& x) {
        _v -= maybe_unwrap(x);
        return *this;
    }
    template <typename T>
    multiprecision_int& operator*=(const T& x) {
        _v *= maybe_unwrap(x);
        return *this;
    }
    template <typename T>
    multiprecision_int& operator/=(const T& x) {
        _v /= maybe_unwrap(x);
        return *this;
    }
    template <typename T>
    multiprecision_int& operator%=(const T& x) {
        _v %= maybe_unwrap(x);
        return *this;
    }
    template <typename T>
    multiprecision_int& operator<<=(const T& x) {
        _v <<= maybe_unwrap(x);
        return *this;
    }
    template <typename T>
    multiprecision_int& operator>>=(const T& x) {
        _v >>= maybe_unwrap(x);
        return *this;
    }
    multiprecision_int operator-() const {
        return cpp_int(-_v);
    }
    multiprecision_int operator+(const multiprecision_int& x) const {
        return cpp_int(_v + x._v);
    }
    template <typename T>
    multiprecision_int operator+(const T& x) const {
        return cpp_int(_v + maybe_unwrap(x));
    }
    template <typename T>
    multiprecision_int operator-(const T& x) const {
        return cpp_int(_v - maybe_unwrap(x));
    }
    template <typename T>
    multiprecision_int operator*(const T& x) const {
        return cpp_int(_v * maybe_unwrap(x));
    }
    template <typename T>
    multiprecision_int operator/(const T& x) const {
        return cpp_int(_v / maybe_unwrap(x));
    }
    template <typename T>
    multiprecision_int operator%(const T& x) const {
        return cpp_int(_v % maybe_unwrap(x));
    }
    template <typename T>
    multiprecision_int operator<<(const T& x) const {
        return cpp_int(_v << maybe_unwrap(x));
    }
    template <typename T>
    multiprecision_int operator>>(const T& x) const {
        return cpp_int(_v >> maybe_unwrap(x));
    }
    bool operator==(const multiprecision_int& x) const {
        return _v == x._v;
    }
    bool operator!=(const multiprecision_int& x) const {
        return _v != x._v;
    }
    bool operator>(const multiprecision_int& x) const {
        return _v > x._v;
    }
    bool operator>=(const multiprecision_int& x) const {
        return _v >= x._v;
    }
    bool operator<(const multiprecision_int& x) const {
        return _v < x._v;
    }
    bool operator<=(const multiprecision_int& x) const {
        return _v <= x._v;
    }
    template <typename T>
    bool operator==(const T& x) const {
        return _v == maybe_unwrap(x);
    }
    template <typename T>
    bool operator!=(const T& x) const {
        return _v != maybe_unwrap(x);
    }
    template <typename T>
    bool operator>(const T& x) const {
        return _v > maybe_unwrap(x);
    }
    template <typename T>
    bool operator>=(const T& x) const {
        return _v >= maybe_unwrap(x);
    }
    template <typename T>
    bool operator<(const T& x) const {
        return _v < maybe_unwrap(x);
    }
    template <typename T>
    bool operator<=(const T& x) const {
        return _v <= maybe_unwrap(x);
    }
    template <typename T>
    friend multiprecision_int operator+(const T& x, const multiprecision_int& y) {
        return cpp_int(maybe_unwrap(x) + y._v);
    }
    template <typename T>
    friend multiprecision_int operator-(const T& x, const multiprecision_int& y) {
        return cpp_int(maybe_unwrap(x) - y._v);
    }
    template <typename T>
    friend multiprecision_int operator*(const T& x, const multiprecision_int& y) {
        return cpp_int(maybe_unwrap(x) * y._v);
    }
    template <typename T>
    friend multiprecision_int operator/(const T& x, const multiprecision_int& y) {
        return cpp_int(maybe_unwrap(x) / y._v);
    }
    template <typename T>
    friend multiprecision_int operator%(const T& x, const multiprecision_int& y) {
        return cpp_int(maybe_unwrap(x) % y._v);
    }
    template <typename T>
    friend multiprecision_int operator<<(const T& x, const multiprecision_int& y) {
        return cpp_int(maybe_unwrap(x) << y._v);
    }
    template <typename T>
    friend multiprecision_int operator>>(const T& x, const multiprecision_int& y) {
        return cpp_int(maybe_unwrap(x) >> y._v);
    }
    template <typename T>
    friend bool operator==(const T& x, const multiprecision_int& y) {
        return maybe_unwrap(x) == y._v;
    }
    template <typename T>
    friend bool operator!=(const T& x, const multiprecision_int& y) {
        return maybe_unwrap(x) != y._v;
    }
    template <typename T>
    friend bool operator>(const T& x, const multiprecision_int& y) {
        return maybe_unwrap(x) > y._v;
    }
    template <typename T>
    friend bool operator>=(const T& x, const multiprecision_int& y) {
        return maybe_unwrap(x) >= y._v;
    }
    template <typename T>
    friend bool operator<(const T& x, const multiprecision_int& y) {
        return maybe_unwrap(x) < y._v;
    }
    template <typename T>
    friend bool operator<=(const T& x, const multiprecision_int& y) {
        return maybe_unwrap(x) <= y._v;
    }
    std::string str() const;
    friend std::ostream& operator<<(std::ostream& os, const multiprecision_int& x);
};


}

