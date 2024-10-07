/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "utils/comparable_bytes.hh"

#include <seastar/core/on_internal_error.hh>

#include "bytes_ostream.hh"
#include "concrete_types.hh"

logging::logger cblogger("comparable_bytes");

static constexpr uint8_t BYTE_SIGN_MASK = (1 << 7);

// Encode/Decode the given signed fixed-length integer into byte comparable format.
// To encode, invert the sign bit so that negative numbers are ordered before the positive ones.
template <std::signed_integral T>
static void convert_signed_fixed_length_integer(managed_bytes_view& src, bytes_ostream& out) {
    constexpr auto sign_mask = T(1) << (sizeof(T) * 8 - 1);
    out.write<T>(read_simple<T>(src) ^ sign_mask);
}

/**
 * Variable-length encoding for signed long data type (int64_t).
 * The first bit of the coding stores the inverted sign bit followed by as many matching bits as
 * there are additional bytes in the encoding, followed by the two's complement of the number.
 *
 * (i.e) <n bits with inverted sign bit><2's complement of the number>
 *       where n = number of bytes in encoding and n >= 1
 *
 * Because of the inverted sign bit, negative numbers compare smaller than positives, and because the length
 * bits match the sign, longer positive numbers compare greater and longer negative ones compare smaller.
 *
 * Examples:
 *      0              encodes as           80
 *      1              encodes as           81
 *     -1              encodes as           7F
 *     63              encodes as           BF
 *     64              encodes as           C040
 *    -64              encodes as           40
 *    -65              encodes as           3FBF
 *   2^20-1            encodes as           EFFFFF
 *   2^20              encodes as           F0100000
 *  -2^20              encodes as           100000
 *   2^64-1            encodes as           FFFFFFFFFFFFFFFFFF
 *  -2^64              encodes as           000000000000000000
 *
 * The encoded value will have a maximum of 9 bytes including the length bits.
 * As the number of bytes is specified in bits 2-9, no value is a prefix of another.
 */
static void encode_signed_long_type(managed_bytes_view src, bytes_ostream& out) {
    // Create a byte filled with the sign bit
    uint8_t curr_byte = read_simple<uint8_t>(src);
    const uint8_t sign_only_byte = (int8_t(curr_byte) >> 7);

    // Discard the leading sign-only bytes
    while (curr_byte == sign_only_byte && !src.empty()) {
        curr_byte = read_simple<uint8_t>(src);
    }

    // The value to be encoded is in curr_byte and src
    const size_t leading_sign_bits_in_curr_byte = sign_only_byte ? std::countl_one(curr_byte) : std::countl_zero(curr_byte);
    size_t num_bits = (8 - leading_sign_bits_in_curr_byte) + src.size_bytes() * 8;
    // Calculate the number of length bits (inverted sign bits in the prefix)
    // that indicate the total size of the encoded format in bytes.
    // 0-6 bits => 1 byte; 7-13 => 2 bytes; etc to 56-63 => 9 bytes.
    // (63 bits will need 10 bytes, but this encoding does it with 9)
    size_t num_length_bits_to_write = std::min(size_t(9), num_bits / 7 + 1);

    if (num_length_bits_to_write >= 8) {
        // Write one byte full of length bits (i.e. inverted sign bits)
        out.write<uint8_t>(~sign_only_byte);
        num_length_bits_to_write -= 8;
    }

    if (num_length_bits_to_write > 0) {
        if (leading_sign_bits_in_curr_byte > num_length_bits_to_write || num_bits == 63) {
            // The remaining length bits can be safely incorporated into curr_byte by
            // inverting the existing sign bits while keeping at least one sign bit intact.
            // Note that num_bits == 63, is a special case where the only remaining sign bit is flipped but
            // this doesn't corrupt the actual value, as the decoding logic accounts for this scenario.
            curr_byte ^= (int8_t(BYTE_SIGN_MASK) >> (num_length_bits_to_write - 1));
        } else {
            // Insufficient number of sign bits in curr_byte.
            // Write the remaining length bits as a separate byte.
            out.write<uint8_t>(int8_t(sign_only_byte ^ BYTE_SIGN_MASK) >> (num_length_bits_to_write - 1));
        }
    }

    // Write the curr_byte and rest of src
    out.write<uint8_t>(curr_byte);
    out.write(src);
}

// Refer encode_signed_long_type() for the encoding details
static void decode_signed_long_type(managed_bytes_view& src, bytes_ostream& out) {
    uint8_t curr_byte = read_simple<uint8_t>(src);
    uint8_t length_bit = curr_byte & BYTE_SIGN_MASK;
    // Deduce bytes to read from the length bits (i.e inverted sign bits) in curr_byte
    uint8_t length_bits_in_curr_byte = length_bit ? std::countl_one(curr_byte) : std::countl_zero(curr_byte);
    uint8_t bytes_to_read = length_bits_in_curr_byte - 1; // -1 to account for the byte already read
    if (bytes_to_read == 7) {
        // check also the first bit in next byte for length bit
        curr_byte = read_simple<uint8_t>(src);
        length_bits_in_curr_byte = (curr_byte & BYTE_SIGN_MASK) == length_bit ? 1 : 0;
        if (length_bits_in_curr_byte == 0) {
            // not a length bit but a byte has been read
            bytes_to_read--;
        }
    }

    // Fill all leading bytes with sign bits, leaving space for the remaining bytes in src and curr_byte.
    uint8_t bytes_with_sign_bit = sizeof(int64_t) - (1 + bytes_to_read);
    const uint8_t sign_only_byte = int8_t(~length_bit) >> 7;
    while (bytes_with_sign_bit--) {
        out.write<uint8_t>(sign_only_byte);
    }

    if (length_bits_in_curr_byte) {
        // Flip the length bits in curr_byte
        curr_byte ^= (int8_t(BYTE_SIGN_MASK) >> (length_bits_in_curr_byte - 1));
    }

    // Write the curr_byte and rest of src
    out.write<uint8_t>(curr_byte);
    out.write(src, bytes_to_read);
}

// Extract and return a prefix of the specified length from the
// managed_bytes_view, advancing the original view past the extracted prefix.
static managed_bytes_view consume_prefix(managed_bytes_view& mbv, size_t length) {
    auto prefix = mbv.prefix(length);
    mbv.remove_prefix(length);
    return prefix;
}

// to_comparable_bytes_visitor provides methods to
// convert serialized bytes into byte comparable format.
struct to_comparable_bytes_visitor {
    managed_bytes_view& serialized_bytes_view;
    bytes_ostream& out;

    // Fixed length signed integers encoding
    template <std::signed_integral T>
    void operator()(const integer_type_impl<T>& type) {
        convert_signed_fixed_length_integer<T>(serialized_bytes_view, out);
    }

    void operator()(const long_type_impl&) {
        encode_signed_long_type(consume_prefix(serialized_bytes_view, sizeof(int64_t)), out);
    }

    // TODO: Handle other types

    void operator()(const abstract_type& type) {
        // Unimplemented
        on_internal_error(cblogger, fmt::format("byte comparable format not supported for type {}", type.name()));
    }
};

comparable_bytes::comparable_bytes(const abstract_type& type, managed_bytes_view serialized_bytes_view) {
    if (serialized_bytes_view.empty()) {
        // nothing to do
        return;
    }

    bytes_ostream encoded_bytes_ostream;
    visit(type, to_comparable_bytes_visitor{serialized_bytes_view, encoded_bytes_ostream});
    _encoded_bytes = std::move(encoded_bytes_ostream).to_managed_bytes();
}

comparable_bytes_opt comparable_bytes::from_data_value(const data_value& value) {
    if (value.is_null()) {
        return comparable_bytes_opt();
    }

    auto mb = value.serialize_nonnull();
    return comparable_bytes(*value.type(), managed_bytes_view(mb));
}

// from_comparable_bytes_visitor provides methods to
// convert byte comparable format into standard serialized bytes.
struct from_comparable_bytes_visitor {
    managed_bytes_view& comparable_bytes_view;
    bytes_ostream& out;

    template <std::signed_integral T>
    void operator()(const integer_type_impl<T>& type) {
        // First bit (sign bit) is inverted for the fixed length signed integers.
        // Reuse encode logic to unflip the sign bit
        convert_signed_fixed_length_integer<T>(comparable_bytes_view, out);
    }

    void operator()(const long_type_impl&) {
        decode_signed_long_type(comparable_bytes_view, out);
    }

    // TODO: Handle other types

    void operator()(const abstract_type& type) {
        // Unimplemented
        on_internal_error(cblogger, fmt::format("byte comparable format not supported for type {}", type.name()));
    }
};

managed_bytes_opt comparable_bytes::to_managed_bytes(const abstract_type& type) const {
    if (_encoded_bytes.empty()) {
        return managed_bytes_opt();
    }

    managed_bytes_view comparable_bytes_view(_encoded_bytes);
    bytes_ostream serialized_bytes_ostream;
    visit(type, from_comparable_bytes_visitor{comparable_bytes_view, serialized_bytes_ostream});
    return std::move(serialized_bytes_ostream).to_managed_bytes();
}

data_value comparable_bytes::to_data_value(const data_type& type) const {
    auto decoded_bytes = to_managed_bytes(*type);
    if (!decoded_bytes) {
        return data_value::make_null(type);
    }

    return type->deserialize(decoded_bytes.value());
}
