/*
 * sstring.hh
 *
 *  Created on: Jul 31, 2014
 *      Author: avi
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
    struct initialized_later {};
public:
    basic_sstring() noexcept {
        u.internal.size = 0;
        u.internal.str[0] = '\0';
    }
    basic_sstring(const basic_sstring& x) {
        if (x.is_internal()) {
            u.internal = x.u.internal;
        } else {
            u.internal.size = -1;
            u.external.str = new char[x.u.external.size + 1];
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
            u.external.str = new char[size + 1];
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
            u.external.str = new char[size + 1];
            u.external.size = size;
            std::copy(x, x + size, u.external.str);
            u.external.str[size] = '\0';
        }
    }
    basic_sstring(const char_type* x) : basic_sstring(x, std::strlen(x)) {}
    basic_sstring(std::basic_string<char_type>& x) : basic_sstring(x.c_str(), x.size()) {}
    basic_sstring(std::initializer_list<char_type> x) : basic_sstring(x.begin(), x.end() - x.begin()) {}
    basic_sstring(const char_type* b, const char_type* e) : basic_sstring(b, e - b) {}
    ~basic_sstring() noexcept {
        if (is_external()) {
            delete[] u.external.str;
        }
    }
    basic_sstring& operator=(const basic_sstring& x) {
        basic_sstring tmp(x);
        swap(tmp);
    }
    basic_sstring& operator=(basic_sstring&& x) noexcept {
        if (this != &x) {
            swap(x);
            x.reset();
        }
        return *this;
    }
    operator std::string() const {
        return str();
    }
    size_t size() const noexcept {
        return is_internal() ? u.internal.size : u.external.size;
    }
    bool empty() const noexcept {
        return u.internal.size == 0;
    }
    void reset() noexcept {
        if (is_external()) {
            delete[] u.external.str;
        }
        u.internal.size = 0;
        u.internal.str[0] = '\0';
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
};

template <typename char_type, typename size_type, size_type max_size>
inline
void swap(basic_sstring<char_type, size_type, max_size>& x,
          basic_sstring<char_type, size_type, max_size>& y) noexcept
{
    return x.swap(y);
}

template <typename char_type, typename size_type, size_type max_size, typename char_traits>
std::basic_ostream<char_type, char_traits>&
operator<<(std::basic_ostream<char_type, char_traits>& os, const basic_sstring<char_type, size_type, max_size>& s) {
    return os << s.c_str();
}

namespace std {

template <typename char_type, typename size_type, size_type max_size>
struct hash<basic_sstring<char_type, size_type, max_size>> {
    size_t operator()(const basic_sstring<char_type, size_type, max_size>& s) const {
        size_t ret = 0;
        for (auto c : s) {
            ret = (ret << 6) | (ret >> (sizeof(ret) * 8 - 6));
            ret ^= c;
        }
        return ret;
    }
};

}

using sstring = basic_sstring<char, uint32_t, 15>;

#endif /* SSTRING_HH_ */
