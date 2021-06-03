/*
 * Copyright 2017-present ScyllaDB
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

//
// For reference, see https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v5.spec.
//
// Relevant excerpt:
//
//     [unsigned vint]   An unsigned variable length integer. A vint is encoded with the most significant byte (MSB) first.
//                       The most significant byte will contains the information about how many extra bytes need to be read
//                       as well as the most significant bits of the integer.
//                       The number of extra bytes to read is encoded as 1 bits on the left side.
//                       For example, if we need to read 2 more bytes the first byte will start with 110
//                       (e.g. 256 000 will be encoded on 3 bytes as [110]00011 11101000 00000000)
//                       If the encoded integer is 8 bytes long the vint will be encoded on 9 bytes and the first
//                       byte will be: 11111111
//
//    [vint]             A signed variable length integer. This is encoded using zig-zag encoding and then sent
//                       like an [unsigned vint]. Zig-zag encoding converts numbers as follows:
//                       0 = 0, -1 = 1, 1 = 2, -2 = 3, 2 = 4, -3 = 5, 3 = 6 and so forth.
//                       The purpose is to send small negative values as small unsigned values, so that we save bytes on the wire.
//                       To encode a value n use "(n >> 31) ^ (n << 1)" for 32 bit values, and "(n >> 63) ^ (n << 1)"
//                       for 64 bit values where "^" is the xor operation, "<<" is the left shift operation and ">>" is
//                       the arithemtic right shift operation (highest-order bit is replicated).
//                       Decode with "(n >> 1) ^ -(n & 1)".
//

#pragma once

#include "bytes.hh"

#include <cstdint>

using vint_size_type = bytes::size_type;

static constexpr size_t max_vint_length = 9;

struct unsigned_vint final {
    using value_type = uint64_t;

    static vint_size_type serialized_size(value_type) noexcept;

    static vint_size_type serialize(value_type, bytes::iterator out);

    static value_type deserialize(bytes_view v);

    static vint_size_type serialized_size_from_first_byte(bytes::value_type first_byte);
};

struct signed_vint final {
    using value_type = int64_t;

    static vint_size_type serialized_size(value_type) noexcept;

    static vint_size_type serialize(value_type, bytes::iterator out);

    static value_type deserialize(bytes_view v);

    static vint_size_type serialized_size_from_first_byte(bytes::value_type first_byte);
};
