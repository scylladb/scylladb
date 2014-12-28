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
#include <experimental/string_view>
#include "core/temporary_buffer.hh"

template <typename char_type, typename size_type, size_type max_size>
class basic_sstring {
    union contents {
        struct external_type {
            char* str;
            size_type size;
            int8_t pad;
        } external;
        struct internal_type {
            char str[max_size];
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
    const char* str() const {
        return is_internal() ? u.internal.str : u.external.str;
    }
    char* str() {
        return is_internal() ? u.internal.str : u.external.str;
    }
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
            u.external.str = reinterpret_cast<char*>(std::malloc(x.u.external.size + 1));
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
            u.external.str = reinterpret_cast<char*>(std::malloc(size + 1));
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
            u.external.str = reinterpret_cast<char*>(std::malloc(size + 1));
            if (!u.external.str) {
                throw std::bad_alloc();
            }
            u.external.size = size;
            std::copy(x, x + size, u.external.str);
            u.external.str[size] = '\0';
        }
    }
    basic_sstring(const char_type* x) : basic_sstring(x, std::strlen(x)) {}
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
    void swap(basic_sstring& x) noexcept {
        contents tmp;
        tmp = x.u;
        x.u = u;
        u = tmp;
    }
    const char* c_str() const {
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
    basic_sstring operator+(const basic_sstring& x) const {
        basic_sstring ret(initialized_later(), size() + x.size());
        std::copy(begin(), end(), ret.begin());
        std::copy(x.begin(), x.end(), ret.begin() + size());
        return ret;
    }
    basic_sstring& operator+=(const basic_sstring& x) {
        return *this = *this + x;
    }

    operator std::experimental::string_view() const {
        return std::experimental::string_view(str(), size());
    }
};

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

template <typename T, typename String = sstring, typename for_enable_if = void*>
String to_sstring(T value, for_enable_if);

template <typename T>
inline
sstring to_sstring_sprintf(T value, const char* fmt) {
    char tmp[sizeof(value) * 3 + 3];
    auto len = std::sprintf(tmp, fmt, value);
    return sstring(tmp, len);
}

template <typename string_type = sstring>
inline
string_type to_sstring(int value, void* = nullptr) {
    return to_sstring_sprintf(value, "%d");
}

template <typename string_type = sstring>
inline
string_type to_sstring(unsigned value, void* = nullptr) {
    return to_sstring_sprintf(value, "%u");
}

template <typename string_type = sstring>
inline
string_type to_sstring(long value, void* = nullptr) {
    return to_sstring_sprintf(value, "%ld");
}

template <typename string_type = sstring>
inline
string_type to_sstring(unsigned long value, void* = nullptr) {
    return to_sstring_sprintf(value, "%lu");
}

template <typename string_type = sstring>
inline
string_type to_sstring(long long value, void* = nullptr) {
    return to_sstring_sprintf(value, "%lld");
}

template <typename string_type = sstring>
inline
string_type to_sstring(unsigned long long value, void* = nullptr) {
    return to_sstring_sprintf(value, "%llu");
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
