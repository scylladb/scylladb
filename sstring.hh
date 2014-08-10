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

template <typename char_type, typename size_type, size_type max_size>
class basic_sstring {
    union {
        struct external_type {
            char* str;
            size_type size;
        } external;
        struct internal_type {
            char str[max_size - 1];
            int8_t size;
        } internal;
        static_assert(sizeof(external_type) < sizeof(internal_type), "max_size too small");
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
    basic_sstring() noexcept {
        u.internal.size = 0;
        u.internal.str[0] = '\0';
    }
    basic_sstring(const basic_sstring& x) {
        if (x.is_internal()) {
            u.internal = x.u.internal;
        } else {
            u.external.str = new char[x.u.external.size + 1];
            std::copy(x.u.str, x.u.str + x.u.extenal.size + 1, u.external.str);
            u.external.size = x.u.external.size;
        }
    }
    basic_sstring(basic_sstring&& x) noexcept {
        u = x.u;
        x.u.internal.size = 0;
        x.u.internal.str[0] = '\0';
    }
    basic_sstring(const char* x, size_t len) {
        if (size_type(size) != size) {
            throw std::overflow_error("sstring overflow");
        }
        if (size + 1 <= sizeof(u.internal.str)) {
            std::copy(x, x + size + 1, u.internal.str);
            u.internal.size = size;
        } else {
            u.internal.size = -1;
            u.external.str = new char[size + 1];
            u.external.size = size;
            std::copy(x, x + size + 1, u.external.str);
        }
    }
    basic_sstring(const char* x) : basic_sstring(x, std::strlen(x)) {}
    basic_sstring(std::string& x) : basic_sstring(x.c_str(), x.size()) {}
    ~basic_sstring() noexcept {
        if (!is_external()) {
            delete[] u.external.str;
        }
    }
    basic_sstring& operator=(const basic_sstring& x) {
        basic_sstring tmp(x);
        swap(tmp);
    }
    basic_sstring& operator=(basic_sstring&& x) noexcept {
        reset();
        swap(x);
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
            delete u.external.str;
        }
        u.internal.size = 0;
    }
    void swap(basic_sstring& x) noexcept {
        basic_sstring tmp;
        tmp.u = x.u;
        x.u = u;
        u = tmp.u;
    }
};

template <typename char_type, typename size_type, size_type max_size>
inline
void swap(basic_sstring<char_type, size_type, max_size>& x,
          basic_sstring<char_type, size_type, max_size>& y) noexcept
{
    return x.swap(y);
}

using sstring = basic_sstring<char, uint32_t, 15>;

#endif /* SSTRING_HH_ */
