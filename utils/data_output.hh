/*
 * Copyright 2014 Cloudius Systems
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

#ifndef UTILS_DATA_OUTPUT_HH_
#define UTILS_DATA_OUTPUT_HH_

#include "bytes.hh"
#include "net/byteorder.hh"

/**
 * Data output interface for Java-esqe wire serialization of
 * basic type, including strings and bytes. The latter I've opted
 * to keep for a few reasons:
 * 1.) It matches better with data_input.read<sstring>, which in turn
 *     is somewhat meaningful because it can create a string more efficiently
 *     without adding another slightly contrived abstraction (iterators?)
 * 2.) At some point this interface should maybe be extended to support stream-like
 *     underlying mediums (such as a back_insert iterator etc), at which point
 *     having to know the exact size of what it being serialized becomes less of an
 *     issue.
 */
class data_output {
public:
    data_output(char* p, char* e)
            : _ptr(p), _end(e) {
    }
    data_output(char* p, size_t n)
            : data_output(p, p + n) {
    }
    data_output(bytes& b)
            : data_output(reinterpret_cast<char*>(b.begin()), b.size()) {
    }
    data_output(bytes& b, size_t off, size_t n = bytes::npos)
            : data_output(reinterpret_cast<char*>(b.begin()) + off,
                    reinterpret_cast<char*>(b.begin()) + off + std::min(b.size() - off, n)) {
        if (off > b.size()) {
            throw std::out_of_range("Offset out of range");
        }
    }

    template<typename T>
    static inline std::enable_if_t<std::is_fundamental<T>::value, size_t> serialized_size(const T&) {
        return sizeof(T);
    }
    template<typename T>
    static inline std::enable_if_t<std::is_fundamental<T>::value, size_t> serialized_size() {
        return sizeof(T);
    }
    static inline size_t serialized_size(const sstring& s) {
        if (s.size() > std::numeric_limits<uint16_t>::max()) {
            throw std::out_of_range("String too large");
        }
        return sizeof(uint16_t) + s.size();
    }
    static inline size_t serialized_size(const bytes_view& s) {
        if (s.size() > std::numeric_limits<uint32_t>::max()) {
            throw std::out_of_range("Buffer too large");
        }
        return sizeof(uint32_t) + s.size();
    }
    static inline size_t serialized_size(const bytes& s) {
        return serialized_size(bytes_view(s));
    }

    size_t avail() const {
        return _end - _ptr;
    }
    void ensure(size_t s) const {
        if (avail() < s) {
            throw std::out_of_range("Buffer overflow");
        }
    }
    data_output& skip(size_t s) {
        ensure(s);
        _ptr += s;
        return *this;
    }

    template<typename T>
    inline std::enable_if_t<std::is_fundamental<T>::value, data_output&> write(T t);

    data_output& write(const sstring& s) {
        ensure(serialized_size(s));
        write(uint16_t(s.size()));
        _ptr = std::copy(s.begin(), s.end(), _ptr);
        return *this;
    }
    data_output& write(const bytes_view& s) {
        ensure(serialized_size(s));
        write(uint32_t(s.size()));
        _ptr = std::copy(s.begin(), s.end(), _ptr);
        return *this;
    }
    data_output& write(const bytes & s) {
        return write(bytes_view(s));
    }
    template<typename Iter>
    data_output& write(Iter s, Iter e) {
        while (s != e) {
            write(*s++);
        }
        return *this;
    }
    data_output& write(const char* s, const char* e) {
        ensure(e - s);
        _ptr = std::copy(s, e, _ptr);
        return *this;
    }
    template<typename T>
    data_output& write(T t, size_t n) {
        while (n-- > 0) {
            write(t);
        }
        return *this;
    }
private:
    char * _ptr;
    char * _end;
};

template<>
inline data_output& data_output::write(bool b) {
    return write<uint8_t>(b);
}
template<>
inline data_output& data_output::write(char c) {
    ensure(1);
    *_ptr++ = c;
    return *this;
}
template<typename T>
inline std::enable_if_t<std::is_fundamental<T>::value, data_output&> data_output::write(
        T t) {
    ensure(sizeof(T));
    *reinterpret_cast<net::packed<T> *>(_ptr) = net::hton(t);
    _ptr += sizeof(T);
    return *this;
}

#endif /* UTILS_DATA_OUTPUT_HH_ */
