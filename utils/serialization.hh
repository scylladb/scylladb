#pragma once

/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
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

#include <seastar/core/sstring.hh>
#include <seastar/net/byteorder.hh>
#include "seastarx.hh"

class UTFDataFormatException { };
class EOFException { };

static constexpr size_t serialize_int8_size = 1;
static constexpr size_t serialize_bool_size = 1;
static constexpr size_t serialize_int16_size = 2;
static constexpr size_t serialize_int32_size = 4;
static constexpr size_t serialize_int64_size = 8;

namespace internal_impl {

template <typename ExplicitIntegerType, typename CharOutputIterator, typename IntegerType>
requires std::is_integral<ExplicitIntegerType>::value && std::is_integral<IntegerType>::value && requires (CharOutputIterator it) {
    *it++ = 'a';
}
inline
void serialize_int(CharOutputIterator& out, IntegerType val) {
    ExplicitIntegerType nval = net::hton(ExplicitIntegerType(val));
    out = std::copy_n(reinterpret_cast<const char*>(&nval), sizeof(nval), out);
}

}

template <typename CharOutputIterator>
inline
void serialize_int8(CharOutputIterator& out, uint8_t val) {
    internal_impl::serialize_int<uint8_t>(out, val);
}

template <typename CharOutputIterator>
inline
void serialize_int16(CharOutputIterator& out, uint16_t val) {
    internal_impl::serialize_int<uint16_t>(out, val);
}

template <typename CharOutputIterator>
inline
void serialize_int32(CharOutputIterator& out, uint32_t val) {
    internal_impl::serialize_int<uint32_t>(out, val);
}

template <typename CharOutputIterator>
inline
void serialize_int64(CharOutputIterator& out, uint64_t val) {
    internal_impl::serialize_int<uint64_t>(out, val);
}

template <typename CharOutputIterator>
inline
void serialize_bool(CharOutputIterator& out, bool val) {
    serialize_int8(out, val ? 1 : 0);
}

// The following serializer is compatible with Java's writeUTF().
// In our C++ implementation, we assume the string is already UTF-8
// encoded. Unfortunately, Java's implementation is a bit different from
// UTF-8 for encoding characters above 16 bits in unicode (see
// http://docs.oracle.com/javase/7/docs/api/java/io/DataInput.html#modified-utf-8)
// For now we'll just assume those aren't in the string...
// TODO: fix the compatibility with Java even in this case.
template <typename CharOutputIterator>
requires requires (CharOutputIterator it) {
    *it++ = 'a';
}
inline
void serialize_string(CharOutputIterator& out, const sstring& s) {
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
    serialize_int16(out, s.size());
    out = std::copy(s.begin(), s.end(), out);
}

template <typename CharOutputIterator>
requires requires (CharOutputIterator it) {
    *it++ = 'a';
}
inline
void serialize_string(CharOutputIterator& out, const char* s) {
    // TODO: like above, need to change UTF-8 when above 16-bit.
    auto len = strlen(s);
    if (len > std::numeric_limits<uint16_t>::max()) {
        // Java specifies the string length is written as uint16_t, so we
        // can't serialize longer strings.
        throw UTFDataFormatException();
    }
    serialize_int16(out, len);
    out = std::copy_n(s, len, out);
}

inline
size_t serialize_string_size(const sstring& s) {;
    // As above, this code is missing the case of modified utf-8
    return serialize_int16_size + s.size();
}

template<typename T, typename CharOutputIterator>
inline
void write(CharOutputIterator& out, const T& val) {
    auto v = net::ntoh(val);
    out = std::copy_n(reinterpret_cast<char*>(&v), sizeof(v), out);
}
