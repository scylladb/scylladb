#pragma once

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

// The following is a redesigned subset of Java's DataOutput,
// DataOutputStream, DataInput, DataInputStream, etc. It allows serializing
// several primitive types (e.g., integer, string, etc.) to an object which
// is only capable of write()ing a single byte (write(char)) or an array of
// bytes (write(char *, int)), and deserializing the same data from an object
// with a char read() interface.
//
// The format of this serialization is identical to the format used by
// Java's DataOutputStream class. This is important to allow us communicate
// with nodes running Java version of the code.
//
// We only support the subset actually used in Cassandra, and the subset
// that is reversible, i.e., can be read back by data_input. For example,
// we only support DataOutput.writeUTF(string) and not
// DataOutput.writeChars(string) - because the latter does not include
// the length, which is necessary for reading the string back.

#include <stdint.h>

#include "core/sstring.hh"
#include "net/byteorder.hh"
#include "bytes.hh"
#include <iostream>


class UTFDataFormatException { };
class EOFException { };

inline
void serialize_int8(std::ostream& out, uint8_t val) {
    out.put(val);
}

inline
void serialize_int8(std::ostream& out, int8_t val) {
    out.put(val);
}

inline
void serialize_int8(bytes::iterator& out, uint8_t val) {
    uint8_t nval = net::hton(val);
    out = std::copy_n(reinterpret_cast<const char*>(&nval), sizeof(nval), out);
}

static constexpr size_t serialize_int8_size = 1;

inline
void serialize_int8(std::ostream& out, char val) {
    out.put(val);
}

inline
int8_t deserialize_int8(std::istream& in) {
    char ret;
    if (in.get(ret)) {
        return ret;
    } else {
        throw EOFException();
    }
}

inline
void serialize_bool(std::ostream& out, bool b) {
    out.put(b ? (char)1 : (char)0);
}

static constexpr size_t serialize_bool_size = 1;

inline
void serialize_bool(bytes::iterator& out, bool val) {
    serialize_int8(out, val ? 1 : 0);
}

inline
bool deserialize_bool(std::istream& in) {
    char ret;
    if (in.get(ret)) {
        return ret;
    } else {
        throw EOFException();
    }
}


inline
void serialize_int16(std::ostream& out, uint16_t val) {
    out.put((char)((val >>  8) & 0xFF));
    out.put((char)((val >>  0) & 0xFF));
}

inline
void serialize_int16(std::ostream& out, int16_t val) {
    serialize_int16(out, (uint16_t) val);
}

inline
void serialize_int16(bytes::iterator& out, uint16_t val) {
    uint16_t nval = net::hton(val);
    out = std::copy_n(reinterpret_cast<const char*>(&nval), sizeof(nval), out);
}

inline
int16_t deserialize_int16(std::istream& in) {
    char a1, a2;
    in.get(a1);
    in.get(a2);
    if (!in) {
        throw EOFException();
    }
    return  ((int16_t)(uint8_t)a1 << 8) | ((int16_t)(uint8_t)a2 << 0);
}

static constexpr size_t serialize_int16_size = 2;

inline
void serialize_int32(std::ostream& out, uint32_t val) {
    out.put((char)((val >> 24) & 0xFF));
    out.put((char)((val >> 16) & 0xFF));
    out.put((char)((val >>  8) & 0xFF));
    out.put((char)((val >>  0) & 0xFF));
}

inline
void serialize_int32(std::ostream& out, int32_t val) {
    serialize_int32(out, (uint32_t) val);
}

inline
void serialize_int32(bytes::iterator& out, uint32_t val) {
    uint32_t nval = net::hton(val);
    out = std::copy_n(reinterpret_cast<const char*>(&nval), sizeof(nval), out);
}

static constexpr size_t serialize_int32_size = 4;

inline
int32_t deserialize_int32(std::istream& in) {
    char a1, a2, a3, a4;
    in.get(a1);
    in.get(a2);
    in.get(a3);
    in.get(a4);
    return  ((int32_t)(uint8_t)a1 << 24) |
            ((int32_t)(uint8_t)a2 << 16) |
            ((int32_t)(uint8_t)a3 << 8) |
            ((int32_t)(uint8_t)a4 << 0);
}

inline
void serialize_int64(std::ostream& out, uint64_t val) {
    out.put((char)((val >> 56) & 0xFF));
    out.put((char)((val >> 48) & 0xFF));
    out.put((char)((val >> 40) & 0xFF));
    out.put((char)((val >> 32) & 0xFF));
    out.put((char)((val >> 24) & 0xFF));
    out.put((char)((val >> 16) & 0xFF));
    out.put((char)((val >>  8) & 0xFF));
    out.put((char)((val >>  0) & 0xFF));
}

inline
void serialize_int64(std::ostream& out, int64_t val) {
    serialize_int64(out, (uint64_t) val);
}

inline
void serialize_int64(bytes::iterator& out, uint64_t val) {
    uint64_t nval = net::hton(val);
    out = std::copy_n(reinterpret_cast<const char*>(&nval), sizeof(nval), out);
}

static constexpr size_t serialize_int64_size = 8;

inline
int64_t deserialize_int64(std::istream& in) {
    char a1, a2, a3, a4, a5, a6, a7, a8;
    in.get(a1);
    in.get(a2);
    in.get(a3);
    in.get(a4);
    in.get(a5);
    in.get(a6);
    in.get(a7);
    in.get(a8);
    return  ((int64_t)(uint8_t)a1 << 56) |
            ((int64_t)(uint8_t)a2 << 48) |
            ((int64_t)(uint8_t)a3 << 40) |
            ((int64_t)(uint8_t)a4 << 32) |
            ((int64_t)(uint8_t)a5 << 24) |
            ((int64_t)(uint8_t)a6 << 16) |
            ((int64_t)(uint8_t)a7 <<  8) |
            ((int64_t)(uint8_t)a8 <<  0);
}

// The following serializer is compatible with Java's writeUTF().
// In our C++ implementation, we assume the string is already UTF-8
// encoded. Unfortunately, Java's implementation is a bit different from
// UTF-8 for encoding characters above 16 bits in unicode (see
// http://docs.oracle.com/javase/7/docs/api/java/io/DataInput.html#modified-utf-8)
// For now we'll just assume those aren't in the string...
// TODO: fix the compatibility with Java even in this case.
inline
void serialize_string(std::ostream& out, const sstring& s) {
    // Java specifies that nulls in the string need to be replaced by the
    // two bytes 0xC0, 0x80. Let's not bother with such transformation
    // now, but just verify wasn't needed.
    for (char c : s) {
        if (c == '\0') {
            throw UTFDataFormatException();
        }
    }
    if (s.size() > std::numeric_limits<uint16_t>::max()) {
        // Java specifies the string length is written as uint16_t, so we
        // can't serialize longer strings.
        throw UTFDataFormatException();
    }
    serialize_int16(out, (uint16_t) s.size());
    out.write(s.c_str(), s.size());
}

inline
void serialize_string(bytes::iterator& out, const sstring& s) {
    for (char c : s) {
        if (c == '\0') {
            throw UTFDataFormatException();
        }
    }
    if (s.size() > std::numeric_limits<uint16_t>::max()) {
        throw UTFDataFormatException();
    }
    serialize_int16(out, (uint16_t) s.size());
    out = std::copy(s.begin(), s.end(), out);
}

inline
size_t serialize_string_size(const sstring& s) {;
    // As above, this code is missing the case of modified utf-8
    return serialize_int16_size + s.size();
}


inline
void serialize_string(std::ostream& out, const char *s) {
    // TODO: like above, need to change UTF-8 when above 16-bit.
    auto len = strlen(s);
    if (len > std::numeric_limits<uint16_t>::max()) {
        // Java specifies the string length is written as uint16_t, so we
        // can't serialize longer strings.
        throw UTFDataFormatException();
    }
    serialize_int16(out, (uint16_t) len);
    out.write(s, len);
}

inline
sstring deserialize_string(std::istream& in) {
    int len = deserialize_int16(in);
    sstring ret(sstring::initialized_later(), len);
    for (int i = 0; i < len; i++) {
        in.get(ret[i]);
    }
    if (!in) {
        throw EOFException();
    }
    return ret;
}

template<typename T>
static inline
void write(bytes::iterator& out, const T& val) {
    auto v = net::ntoh(val);
    out = std::copy_n(reinterpret_cast<char*>(&v), sizeof(v), out);
}
