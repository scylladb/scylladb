/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright 2014 Cloudius Systems
 */

#ifndef SSTRING_HH_
#define SSTRING_HH_

#include <stdint.h>
#include <algorithm>
#include <string>
#include <cstring>
#include <stdexcept>
#include <initializer_list>
#include <iostream>
#include <functional>
#include <cstdio>
#include <type_traits>
#include <experimental/string_view>
#include "core/temporary_buffer.hh"

template <typename char_type, typename Size, Size max_size>
class basic_sstring {
    static_assert(
            (std::is_same<char_type, char>::value
             || std::is_same<char_type, signed char>::value
             || std::is_same<char_type, unsigned char>::value),
            "basic_sstring only supports single byte char types");
    union contents {
        struct external_type {
            char_type* str;
            Size size;
            int8_t pad;
        } external;
        struct internal_type {
            char_type str[max_size];
            int8_t size;
        } internal;
        static_assert(sizeof(external_type) <= sizeof(internal_type), "max_size too small");
        static_assert(max_size <= 127, "max_size too large");
    } u;
    bool is_internal() const noexcept {
        return u.internal.size >= 0;
    }
    bool is_external() const noexcept {
        return !is_internal();
    }
    const char_type* str() const {
        return is_internal() ? u.internal.str : u.external.str;
    }
    char_type* str() {
        return is_internal() ? u.internal.str : u.external.str;
    }
public:
    using value_type = char_type;
    using traits_type = std::char_traits<char_type>;
    using allocator_type = std::allocator<char_type>;
    using reference = char_type&;
    using const_reference = const char_type&;
    using pointer = char_type*;
    using const_pointer = const char_type*;
    using iterator = char_type*;
    using const_iterator = const char_type*;
    // FIXME: add reverse_iterator and friend
    using difference_type = ssize_t;  // std::make_signed_t<Size> can be too small
    using size_type = Size;
    static constexpr size_type  npos = static_cast<size_type>(-1);
public:
    struct initialized_later {};

    basic_sstring() noexcept {
        u.internal.size = 0;
        u.internal.str[0] = '\0';
    }
    basic_sstring(const basic_sstring& x) {
        if (x.is_internal()) {
            u.internal = x.u.internal;
        } else {
            u.internal.size = -1;
            u.external.str = reinterpret_cast<char_type*>(std::malloc(x.u.external.size + 1));
            if (!u.external.str) {
                throw std::bad_alloc();
            }
            std::copy(x.u.external.str, x.u.external.str + x.u.external.size + 1, u.external.str);
            u.external.size = x.u.external.size;
        }
    }
    basic_sstring(basic_sstring&& x) noexcept {
        u = x.u;
        x.u.internal.size = 0;
        x.u.internal.str[0] = '\0';
    }
    basic_sstring(initialized_later, size_t size) {
        if (size_type(size) != size) {
            throw std::overflow_error("sstring overflow");
        }
        if (size + 1 <= sizeof(u.internal.str)) {
            u.internal.str[size] = '\0';
            u.internal.size = size;
        } else {
            u.internal.size = -1;
            u.external.str = reinterpret_cast<char_type*>(std::malloc(size + 1));
            if (!u.external.str) {
                throw std::bad_alloc();
            }
            u.external.size = size;
            u.external.str[size] = '\0';
        }
    }
    basic_sstring(const char_type* x, size_t size) {
        if (size_type(size) != size) {
            throw std::overflow_error("sstring overflow");
        }
        if (size + 1 <= sizeof(u.internal.str)) {
            std::copy(x, x + size, u.internal.str);
            u.internal.str[size] = '\0';
            u.internal.size = size;
        } else {
            u.internal.size = -1;
            u.external.str = reinterpret_cast<char_type*>(std::malloc(size + 1));
            if (!u.external.str) {
                throw std::bad_alloc();
            }
            u.external.size = size;
            std::copy(x, x + size, u.external.str);
            u.external.str[size] = '\0';
        }
    }
    basic_sstring(const char* x) : basic_sstring(reinterpret_cast<const char_type*>(x), std::strlen(x)) {}
    basic_sstring(std::basic_string<char_type>& x) : basic_sstring(x.c_str(), x.size()) {}
    basic_sstring(std::initializer_list<char_type> x) : basic_sstring(x.begin(), x.end() - x.begin()) {}
    basic_sstring(const char_type* b, const char_type* e) : basic_sstring(b, e - b) {}
    basic_sstring(const std::basic_string<char_type>& s)
        : basic_sstring(s.data(), s.size()) {}
    ~basic_sstring() noexcept {
        if (is_external()) {
            std::free(u.external.str);
        }
    }
    basic_sstring& operator=(const basic_sstring& x) {
        basic_sstring tmp(x);
        swap(tmp);
        return *this;
    }
    basic_sstring& operator=(basic_sstring&& x) noexcept {
        if (this != &x) {
            swap(x);
            x.reset();
        }
        return *this;
    }
    operator std::string() const {
        return { str(), size() };
    }
    size_t size() const noexcept {
        return is_internal() ? u.internal.size : u.external.size;
    }

    size_t length() const noexcept {
        return size();
    }

    size_t find(char_type t, size_t pos = 0) const noexcept {
        const char_type* it = str() + pos;
        const char_type* end = it + size();
        while (it < end) {
            if (*it == t) {
                return it - str();
            }
            it++;
        }
        return npos;
    }

    size_t find(const basic_sstring& s, size_t pos = 0) const noexcept {
        const char_type* it = str() + pos;
        const char_type* end = it + size();
        const char_type* c_str = s.str();
        const char_type* c_str_end = s.str() + s.size();

        while (it < end) {
            auto i = it;
            auto j = c_str;
            while ( i < end && j < c_str_end && *i == *j) {
                i++;
                j++;
            }
            if (j == c_str_end) {
                return it - str();
            }
            it++;
        }
        return npos;
    }

    basic_sstring substr(size_t from, size_t len = npos)  const {
        if (from > size()) {
            throw std::out_of_range("sstring::substr out of range");
        }
        if (len > size() - from) {
            len = size() - from;
        }
        if (len == 0) {
            return "";
        }
        return { str() + from , len };
    }

    const char_type& at(size_t pos) const {
        if (pos >= size()) {
            throw std::out_of_range("sstring::at out of range");
        }
        return *(str() + pos);
    }

    char_type& at(size_t pos) {
        if (pos >= size()) {
            throw std::out_of_range("sstring::at out of range");
        }
        return *(str() + pos);
    }

    bool empty() const noexcept {
        return u.internal.size == 0;
    }
    void reset() noexcept {
        if (is_external()) {
            std::free(u.external.str);
        }
        u.internal.size = 0;
        u.internal.str[0] = '\0';
    }
    temporary_buffer<char_type> release() && {
        if (is_external()) {
            auto ptr = u.external.str;
            auto size = u.external.size;
            u.external.str = nullptr;
            u.external.size = 0;
            return temporary_buffer<char_type>(ptr, size, make_free_deleter(ptr));
        } else {
            auto buf = temporary_buffer<char_type>(u.internal.size);
            std::copy(u.internal.str, u.internal.str + u.internal.size, buf.get_write());
            u.internal.size = 0;
            u.internal.str[0] = '\0';
            return buf;
        }
    }
    int compare(const basic_sstring& x) const noexcept {
        auto n = traits_type::compare(begin(), x.begin(), std::min(size(), x.size()));
        if (n != 0) {
            return n;
        }
        if (size() < x.size()) {
            return -1;
        } else if (size() > x.size()) {
            return 1;
        } else {
            return 0;
        }
    }
    void swap(basic_sstring& x) noexcept {
        contents tmp;
        tmp = x.u;
        x.u = u;
        u = tmp;
    }
    const char_type* c_str() const {
        return str();
    }
    const char_type* begin() const { return str(); }
    const char_type* end() const { return str() + size(); }
    char_type* begin() { return str(); }
    char_type* end() { return str() + size(); }
    bool operator==(const basic_sstring& x) const {
        return size() == x.size() && std::equal(begin(), end(), x.begin());
    }
    bool operator!=(const basic_sstring& x) const {
        return !operator==(x);
    }
    bool operator<(const basic_sstring& x) const {
        return compare(x) < 0;
    }
    basic_sstring operator+(const basic_sstring& x) const {
        basic_sstring ret(initialized_later(), size() + x.size());
        std::copy(begin(), end(), ret.begin());
        std::copy(x.begin(), x.end(), ret.begin() + size());
        return ret;
    }
    basic_sstring& operator+=(const basic_sstring& x) {
        return *this = *this + x;
    }
    char_type& operator[](size_type pos) {
        return str()[pos];
    }
    const char_type& operator[](size_type pos) const {
        return str()[pos];
    }

    operator std::experimental::string_view() const {
        return std::experimental::string_view(str(), size());
    }
};
template <typename char_type, typename Size, Size max_size>
constexpr Size basic_sstring<char_type, Size, max_size>::npos;

template <typename char_type, typename size_type, size_type Max, size_type N>
inline
basic_sstring<char_type, size_type, Max>
operator+(const char(&s)[N], const basic_sstring<char_type, size_type, Max>& t) {
    using sstring = basic_sstring<char_type, size_type, Max>;
    // don't copy the terminating NUL character
    sstring ret(typename sstring::initialized_later(), N-1 + t.size());
    auto p = std::copy(std::begin(s), std::end(s)-1, ret.begin());
    std::copy(t.begin(), t.end(), p);
    return ret;
}

template <size_t N>
static inline
size_t str_len(const char(&s)[N]) { return N - 1; }

template <size_t N>
static inline
const char* str_begin(const char(&s)[N]) { return s; }

template <size_t N>
static inline
const char* str_end(const char(&s)[N]) { return str_begin(s) + str_len(s); }

template <typename char_type, typename size_type, size_type max_size>
static inline
const char_type* str_begin(const basic_sstring<char_type, size_type, max_size>& s) { return s.begin(); }

template <typename char_type, typename size_type, size_type max_size>
static inline
const char_type* str_end(const basic_sstring<char_type, size_type, max_size>& s) { return s.end(); }

template <typename char_type, typename size_type, size_type max_size>
static inline
size_type str_len(const basic_sstring<char_type, size_type, max_size>& s) { return s.size(); }

template <typename First, typename Second, typename... Tail>
static inline
const size_t str_len(const First& first, const Second& second, const Tail&... tail) {
    return str_len(first) + str_len(second, tail...);
}

template <typename char_type, typename size_type, size_type max_size>
inline
void swap(basic_sstring<char_type, size_type, max_size>& x,
          basic_sstring<char_type, size_type, max_size>& y) noexcept
{
    return x.swap(y);
}

template <typename char_type, typename size_type, size_type max_size, typename char_traits>
inline
std::basic_ostream<char_type, char_traits>&
operator<<(std::basic_ostream<char_type, char_traits>& os,
        const basic_sstring<char_type, size_type, max_size>& s) {
    return os.write(s.begin(), s.size());
}

namespace std {

template <typename char_type, typename size_type, size_type max_size>
struct hash<basic_sstring<char_type, size_type, max_size>> {
    size_t operator()(const basic_sstring<char_type, size_type, max_size>& s) const {
        return std::hash<std::experimental::string_view>()(s);
    }
};

}

using sstring = basic_sstring<char, uint32_t, 15>;

static inline
char* copy_str_to(char* dst) {
    return dst;
}

template <typename Head, typename... Tail>
static inline
char* copy_str_to(char* dst, const Head& head, const Tail&... tail) {
    return copy_str_to(std::copy(str_begin(head), str_end(head), dst), tail...);
}

template <typename String = sstring, typename... Args>
static String make_sstring(Args&&... args)
{
    String ret(sstring::initialized_later(), str_len(args...));
    copy_str_to(ret.begin(), args...);
    return ret;
}

template <typename string_type, typename T>
inline
string_type to_sstring_sprintf(T value, const char* fmt) {
    char tmp[sizeof(value) * 3 + 3];
    auto len = std::sprintf(tmp, fmt, value);
    using char_type = typename string_type::value_type;
    return string_type(reinterpret_cast<char_type*>(tmp), len);
}

template <typename T>
inline
sstring
to_sstring_sprintf(T value, const char* fmt) {
    return to_sstring_sprintf<sstring>(value, fmt);
}

template <typename string_type = sstring>
inline
string_type to_sstring(int value, void* = nullptr) {
    return to_sstring_sprintf<string_type>(value, "%d");
}

template <typename string_type = sstring>
inline
string_type to_sstring(unsigned value, void* = nullptr) {
    return to_sstring_sprintf<string_type>(value, "%u");
}

template <typename string_type = sstring>
inline
string_type to_sstring(long value, void* = nullptr) {
    return to_sstring_sprintf<string_type>(value, "%ld");
}

template <typename string_type = sstring>
inline
string_type to_sstring(unsigned long value, void* = nullptr) {
    return to_sstring_sprintf<string_type>(value, "%lu");
}

template <typename string_type = sstring>
inline
string_type to_sstring(long long value, void* = nullptr) {
    return to_sstring_sprintf<string_type>(value, "%lld");
}

template <typename string_type = sstring>
inline
string_type to_sstring(unsigned long long value, void* = nullptr) {
    return to_sstring_sprintf<string_type>(value, "%llu");
}

template <typename string_type = sstring>
inline
string_type to_sstring(float value, void* = nullptr) {
    return to_sstring_sprintf<string_type>(value, "%f");
}

template <typename string_type = sstring>
inline
string_type to_sstring(double value, void* = nullptr) {
    return to_sstring_sprintf<string_type>(value, "%f");
}

template <typename string_type = sstring>
inline
string_type to_sstring(long double value, void* = nullptr) {
    return to_sstring_sprintf<string_type>(value, "%Lf");
}

template <typename string_type = sstring>
inline
string_type to_sstring(const char* value, void* = nullptr) {
    return string_type(value);
}

template <typename string_type = sstring>
inline
string_type to_sstring(sstring value, void* = nullptr) {
    return value;
}

template <typename string_type = sstring>
static string_type to_sstring(const temporary_buffer<char>& buf) {
    return string_type(buf.get(), buf.size());
}

template <typename T>
inline
std::ostream& operator<<(std::ostream& os, const std::vector<T>& v) {
    bool first = true;
    os << "{";
    for (auto&& elem : v) {
        if (!first) {
            os << ", ";
        } else {
            first = false;
        }
        os << elem;
    }
    os << "}";
    return os;
}

#endif /* SSTRING_HH_ */
