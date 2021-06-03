/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
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

#include "vint-serialization.hh"

#include <seastar/core/bitops.hh>

#include <algorithm>
#include <array>
#include <limits>
#include <type_traits>

static_assert(-1 == ~0, "Not a twos-complement architecture");

// Accounts for the case that all bits are zero.
static vint_size_type count_leading_zero_bits(uint64_t n) noexcept {
    if (n == 0) {
        return vint_size_type(std::numeric_limits<uint64_t>::digits);
    }

    return vint_size_type(count_leading_zeros(n));
}

static constexpr uint64_t encode_zigzag(int64_t n) noexcept {
    // The right shift has to be arithmetic and not logical.
    return (static_cast<uint64_t>(n) << 1) ^ static_cast<uint64_t>(n >> 63);
}

static constexpr int64_t decode_zigzag(uint64_t n) noexcept {
    return static_cast<int64_t>((n >> 1) ^ -(n & 1));
}

// Mask for extracting from the first byte the part that is not used for indicating the total number of bytes.
static uint64_t first_byte_value_mask(vint_size_type extra_bytes_size) {
    // Include the sentinel zero bit in the mask.
    return uint64_t(0xff) >> extra_bytes_size;
}

vint_size_type signed_vint::serialize(int64_t value, bytes::iterator out) {
    return unsigned_vint::serialize(encode_zigzag(value), out);
}

vint_size_type signed_vint::serialized_size(int64_t value) noexcept {
    return unsigned_vint::serialized_size(encode_zigzag(value));
}

int64_t signed_vint::deserialize(bytes_view v) {
    const auto un = unsigned_vint::deserialize(v);
    return decode_zigzag(un);
}

vint_size_type signed_vint::serialized_size_from_first_byte(bytes::value_type first_byte) {
    return unsigned_vint::serialized_size_from_first_byte(first_byte);
}

// The number of additional bytes that we need to read.
static vint_size_type count_extra_bytes(int8_t first_byte) {
    // Sign extension.
    const int64_t v(first_byte);

    return count_leading_zero_bits(static_cast<uint64_t>(~v)) - vint_size_type(64 - 8);
}

static void encode(uint64_t value, vint_size_type size, bytes::iterator out) {
    std::array<int8_t, 9> buffer({});

    // `size` is always in the range [1, 9].
    const auto extra_bytes_size = size - 1;

    for (vint_size_type i = 0; i <= extra_bytes_size; ++i) {
        buffer[extra_bytes_size - i] = static_cast<int8_t>(value & 0xff);
        value >>= 8;
    }

    buffer[0] |= ~first_byte_value_mask(extra_bytes_size);
    std::copy_n(buffer.cbegin(), size, out);
}

vint_size_type unsigned_vint::serialize(uint64_t value, bytes::iterator out) {
    const auto size = serialized_size(value);

    if (size == 1) {
        *out = static_cast<int8_t>(value & 0xff);
        return 1;
    }

    encode(value, size, out);
    return size;
}

vint_size_type unsigned_vint::serialized_size(uint64_t value) noexcept {
    // No need for the overhead of checking that all bits are zero.
    //
    // A signed quantity, to allow the case of `magnitude == 0` to result in a value of 9 below.
    const auto magnitude = static_cast<int64_t>(count_leading_zeros(value | uint64_t(1)));

    return vint_size_type(9) - vint_size_type((magnitude - 1) / 7);
}

uint64_t unsigned_vint::deserialize(bytes_view v) {
    auto src = v.data();
    auto len = v.size();
    const int8_t first_byte = *src;

    // No additional bytes, since the most significant bit is not set.
    if (first_byte >= 0) {
        return uint64_t(first_byte);
    }

    const auto extra_bytes_size = count_extra_bytes(first_byte);

    // Extract the bits not used for counting bytes.
    auto result = uint64_t(first_byte) & first_byte_value_mask(extra_bytes_size);

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    uint64_t value;
    // If we can overread do that. It is cheaper to have a single 64-bit read and
    // then mask out the unneeded part than to do 8x 1 byte reads.
    if (__builtin_expect(len >= sizeof(uint64_t) + 1, true)) {
        std::copy_n(src + 1, sizeof(uint64_t), reinterpret_cast<int8_t*>(&value));
    } else {
        value = 0;
        std::copy_n(src + 1, extra_bytes_size, reinterpret_cast<int8_t*>(&value));
    }
    value = be_to_cpu(value << (64 - (extra_bytes_size * 8)));
    result <<= (extra_bytes_size * 8) % 64;
    result |= value;
#else
    for (vint_size_type index = 0; index < extra_bytes_size; ++index) {
        result <<= 8;
        result |= (uint64_t(v[index + 1]) & uint64_t(0xff));
    }
#endif
    return result;
}

vint_size_type unsigned_vint::serialized_size_from_first_byte(bytes::value_type first_byte) {
    int8_t first_byte_casted = first_byte;
    return 1 + (first_byte_casted >= 0 ? 0 : count_extra_bytes(first_byte_casted));
}
