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
static constexpr int VARINT_FULL_FORM_THRESHOLD = 7;

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
// If prefix_sign_bytes is false, any redundant leading sign bits will not be written.
static void decode_signed_long_type(managed_bytes_view& src, bytes_ostream& out, bool prefix_sign_bytes) {
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

    if (prefix_sign_bytes) {
        // Fill all leading bytes with sign bits, leaving space for the remaining bytes in src and curr_byte.
        uint8_t bytes_with_sign_bit = sizeof(int64_t) - (1 + bytes_to_read);
        const uint8_t sign_only_byte = int8_t(~length_bit) >> 7;
        while (bytes_with_sign_bit--) {
            out.write<uint8_t>(sign_only_byte);
        }
    } else if (length_bits_in_curr_byte == 0) {
        // curr_byte has no sign bit, and no sign bytes are prefixed.
        // Add an extra byte with sign bits to preserve the value's sign.
        out.write<uint8_t>(int8_t(~length_bit) >> 7);
    }

    if (length_bits_in_curr_byte) {
        // Flip the length bits in curr_byte
        curr_byte ^= (int8_t(BYTE_SIGN_MASK) >> (length_bits_in_curr_byte - 1));
    }

    // Write the curr_byte and rest of src
    out.write<uint8_t>(curr_byte);
    out.write(src, bytes_to_read);
}

// Encode the length of a varint value as comparable bytes.
// The length will be treated as an unsigned variable length integer and will use
// an encoding similar to encode_signed_long_type.
// Numbers between 0 and 127 are encoded in one byte, using 0 in the most significant bit.
// Larger values have 1s in as many of the most significant bits as the number of additional bytes
// in the representation, followed by a 0 and then the serialized value itself.
//
// (i.e) <(n - 1) msb bits of 1s><1 or more bits of 0 padding><serialized value>
//       where n = number of bytes in encoding
//
// The encoding ensures that longer numbers compare larger than shorter ones.
// Since we never use a longer representation than necessary, this implies numbers compare correctly.
// As the number of bytes is specified in the bits of the first, no value is a prefix of another.
// The encoded byte is XORed with the provided sign byte before writing,
// enabling the caller to invert the encoding for negative varint values.
// Note: The encoding does not support lengths greater than `(1 << 64) − 1`,
// but this is okay as the length of a varint in bytes cannot reach that limit.
void encode_varint_length(uint64_t length, uint8_t sign_only_byte, bytes_ostream& out) {
    const size_t bitsMinusOne = std::bit_width(length | 1) - 1; // 0 to 63 (the | 1 is to make sure 0 maps to 0 (1 bit))
    const size_t bytesMinusOne = bitsMinusOne / 7;
    const int mask = -256 >> bytesMinusOne; // sequence of bytesMinusOne 1s in the most-significant bits
    int pos = bytesMinusOne * 8;
    out.write<uint8_t>(uint8_t((length >> pos) | mask) ^ sign_only_byte);
    while (pos > 0) {
        pos -= 8;
        out.write<uint8_t>(uint8_t((length >> pos) & 0xFF) ^ sign_only_byte);
    }
}

// Decode the length of a varint from comparable bytes.
// Refer encode_varint_length() for the encoding details.
uint64_t decode_varint_length(managed_bytes_view& src, uint8_t sign_only_byte) {
    uint8_t first_byte = read_simple<uint8_t>(src) ^ sign_only_byte;

    uint64_t length = 0;
    int bytes;
    // Read an extra byte while the next most significant bit is 1.
    for (bytes = 0; bytes <= 7 && ((first_byte << bytes) & 0x80) != 0; ++bytes) {
        length = (length << 8) | (read_simple<uint8_t>(src) ^ sign_only_byte);
    }

    // Strip the length bits from the leading byte.
    first_byte &= ~(-256 >> bytes);
    // Add the rest of the bits of the leading byte.
    return length | (uint64_t(first_byte) << bytes * 8);
}

// Fixed length signed floating point number encode/decode.
// To encode :
//   If positive : invert first bit to make it greater than all negatives
//   If negative : invert all bytes to make negatives with bigger magnitude smaller
// Decoding is identical except the logic to identify postive/negative values
template <std::floating_point T>
static void convert_fixed_length_float(managed_bytes_view& src, bytes_ostream& out, bool perform_encode) {
    // Read and write float as uint32_t; double as uint64_t;
    using uint_t = std::conditional_t<std::is_same_v<T, float>, uint32_t, uint64_t>;

    // Peek the first byte to deduce sign.
    // Positive values have their sign bit flipped in their encoded format.
    uint8_t curr_byte = uint8_t(src[0]);
    const bool src_is_positive = perform_encode ? curr_byte < 0x80 : curr_byte >= 0x80;

    if (src_is_positive) {
        constexpr uint_t sign_mask = uint_t(1) << (sizeof(T) * 8 - 1);
        // flip sign bit and write rest of the bits unchanged
        out.write<uint_t>(read_simple<uint_t>(src) ^ sign_mask);
    } else {
        // invert all bytes;
        out.write<uint_t>(~read_simple<uint_t>(src));
    }
}

// Reorder the given timeuuid msb and make it byte comparable
// Scylla and Cassandra use a standard UUID memory layout for MSB:
// 4 bytes    2 bytes    2 bytes
// time_low - time_mid - time_hi_and_version
// It is reordered as the following to make it byte comparable :
// time_hi_and_version - time_mid - time_low
static uint64_t timeuuid_msb_to_comparable_bytes(uint64_t msb) {
    return (msb <<  48)
            | ((msb <<  16) & 0xFFFF00000000L)
            |  (msb >> 32);
}

// Reconstruct timeuuid msb from the byte comparable value
static uint64_t timeuuid_msb_from_comparable_bytes(uint64_t byte_comparable_msb) {
    return (byte_comparable_msb << 32)
            | ((byte_comparable_msb >> 16) & 0xFFFF0000L)
            | (byte_comparable_msb >> 48);
}

// Returns the number of significant digits in the big decimal.
// For example, the "precision" of 12.34e56 is 4.
std::size_t count_digits(const boost::multiprecision::cpp_int& value) {
    // special case 0
    if (value.is_zero()) {
        return 1;
    }

    // Count the number of bits in the value.
    // The value is stored as array of 'limbs' in boost::multiprecision::cpp_int
    // and all limbs except the most significant limb will have bits_per_limb.
    // So, total bits = (num of limbs - 1) * bits per limb + bits in ms limb.
    const auto& backend = value.backend();
    const auto limb_count = backend.size();
    const auto num_bits_in_ms_limb = boost::multiprecision::bits_per_limb - std::countl_zero(backend.limbs()[limb_count - 1]);
    const auto total_num_bits = (limb_count - 1) * boost::multiprecision::bits_per_limb + num_bits_in_ms_limb;

    // The number of digits = floor(log10(2) * total_num_bits) + 1.
    // Since total_num_bits is always positive, an explicit cast to std::size is sufficient, making floor() redundant.
    static const double log10_2 = std::log10(2);
    auto num_of_digits = std::size_t(log10_2 * total_num_bits) + 1;

    // Adjust for overestimation (e.g., 999 -> 3 digits, not 4)
    static const boost::multiprecision::cpp_int multiprecision_10(10);
    boost::multiprecision::cpp_int threshold = boost::multiprecision::pow(multiprecision_10, num_of_digits - 1);
    if (value < 0 ? (-value < threshold) : (value < threshold)) {
        num_of_digits--;
    }

    return num_of_digits;
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

    void operator()(const boolean_type_impl&) {
        // Any non zero byte value is encoded as 1 else 0
        out.write<uint8_t>(read_simple<uint8_t>(serialized_bytes_view) != 0);
    }

    // Fixed length signed integers encoding
    template <std::signed_integral T>
    void operator()(const integer_type_impl<T>& type) {
        convert_signed_fixed_length_integer<T>(serialized_bytes_view, out);
    }

    void operator()(const long_type_impl&) {
        encode_signed_long_type(consume_prefix(serialized_bytes_view, sizeof(int64_t)), out);
    }

    // Constructs a byte-comparable representation of the varint number.
    //
    // We encode the number :
    //    directly as long type, if the length is 6 or smaller (the encoding has non-00/FF first byte)
    //    <signbyte><length as unsigned integer - 7><7 or more bytes>, otherwise
    // where <signbyte> is 00 for negative numbers and FF for positive ones, and the length's bytes are inverted if
    // the number is negative (so that longer length sorts smaller).
    //
    // Because we present the sign separately, we don't need to include 0x00 prefix for positive integers whose first
    // byte is >= 0x80 or 0xFF prefix for negative integers whose first byte is < 0x80. Note that we do this before
    // taking the length for the purposes of choosing between long and full-form encoding.
    //
    // The representations are prefix-free, because the choice between long and full-form encoding is determined by
    // the first byte where signed longs are properly ordered between full-form negative and full-form positive, long
    // encoding is prefix-free, and full-form representations of different length always have length bytes that differ.
    //
    // Examples:
    //    -1            as 7F
    //    0             as 80
    //    1             as 81
    //    127           as C07F
    //    255           as C0FF
    //    2^32-1        as F8FFFFFFFF
    //    2^32          as F900000000
    //    2^56-1        as FEFFFFFFFFFFFFFF
    //    2^56          as FF000100000000000000
    void operator()(const varint_type_impl&) {
        if (serialized_bytes_view.size_bytes() < VARINT_FULL_FORM_THRESHOLD ||
                (serialized_bytes_view.size_bytes() == VARINT_FULL_FORM_THRESHOLD && (uint8_t(serialized_bytes_view[0]) == 0 || uint8_t(serialized_bytes_view[0]) == 0xFF))) {
            // Length is 6 bytes or less - encode it as a signed long.
            // The second condition covers cases where the length is 7 bytes,
            // but the first byte is a sign-only byte (0 or 0xFF). For example,
            // `2^48 - 1` has a 7-byte serialized form with a leading 0 byte.
            // The sign-only byte will be discarded, and only the remaining
            // 6 bytes are encoded.
            encode_signed_long_type(serialized_bytes_view, out);
            return;
        }

        // Construct a sign only byte and discard any leading sign only bytes
        uint8_t first_byte = uint8_t(serialized_bytes_view[0]);
        const uint8_t sign_only_byte = (int8_t(first_byte) >> 7);
        if (first_byte == sign_only_byte) {
            serialized_bytes_view.remove_prefix(1);
        }

        // Invert and write the sign byte
        out.write<uint8_t>(~sign_only_byte);

        // Encode length section (length - 7)
        encode_varint_length(serialized_bytes_view.size_bytes() - VARINT_FULL_FORM_THRESHOLD, sign_only_byte, out);

        // Encode rest of data as it is
        out.write(serialized_bytes_view);
    }

    // Encoding for float and double
    template <std::floating_point T>
    void operator()(const floating_type_impl<T>&) {
        convert_fixed_length_float<T>(serialized_bytes_view, out, true);
    }

    // Encoding for simple_date_type_impl and time_type_impl
    // They are both fixed length unsigned integers and are already byte comparable in their serialized form
    template <std::integral T>
    void operator()(const simple_type_impl<T>&) {
        out.write(serialized_bytes_view, sizeof(T));
    }

    // timestamp_type is encoded as fixed length signed integer
    void operator()(const timestamp_type_impl&) {
        convert_signed_fixed_length_integer<db_clock::rep>(serialized_bytes_view, out);
    }

    // UUIDs are fixed-length unsigned integers, where the UUID version/type
    // has to be compared first, so pull the version digit first for a byte
    // comparable representation.
    //
    // For time-based UUIDs (version 1), additional reordering is required
    // to maintain time-based ordering in byte-comparable form.
    void operator()(const uuid_type_impl&) {
        uint64_t msb = read_simple<uint64_t>(serialized_bytes_view);
        auto version = ((msb >> 12) & 0xf);
        if (version == 1) {
            // This is a time-based UUID and the msb needs to be rearranged to make it byte comparable
            out.write<uint64_t>(timeuuid_msb_to_comparable_bytes(msb));
        } else {
            // For non-time UUIDs, write the msb after shifting the version bits to the beginning
            out.write<uint64_t>((version << 60) | ((msb >> 4) & 0x0FFFFFFFFFFFF000L) | (msb & 0xFFFL));
        }

        // Write the lsb
        out.write(serialized_bytes_view, sizeof(uint64_t));
    }

    // Time based UUIDS. Similar to above, pull the version digit to the
    // beginning and rearrange the other bits to maintain time-based ordering
    // in byte-comparable form.
    // Additionally, invert the sign bits of all bytes in the lower bits to
    // preserve Cassandra's legacy comparison order, which compared individual
    // bytes as signed values.
    void operator()(const timeuuid_type_impl&) {
        out.write<uint64_t>(timeuuid_msb_to_comparable_bytes(read_simple<uint64_t>(serialized_bytes_view)));
        out.write<uint64_t>(read_simple<uint64_t>(serialized_bytes_view) ^ 0x8080808080808080L);
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

    void operator()(const boolean_type_impl&) {
        // return as it is;
        out.write<uint8_t>(read_simple<uint8_t>(comparable_bytes_view));
    }

    template <std::signed_integral T>
    void operator()(const integer_type_impl<T>& type) {
        // First bit (sign bit) is inverted for the fixed length signed integers.
        // Reuse encode logic to unflip the sign bit
        convert_signed_fixed_length_integer<T>(comparable_bytes_view, out);
    }

    void operator()(const long_type_impl&) {
        decode_signed_long_type(comparable_bytes_view, out, true);
    }

    // Decode a byte-comparable representation to the varint number.
    void operator()(const varint_type_impl&) {
        // Check the first byte to determine if the bytes were encoded using long or full-form encoding
        const uint8_t sign_byte = ~uint8_t(comparable_bytes_view[0]);
        if (sign_byte != 0 && sign_byte != 0xFF) {
            // First byte is in the range 01-FE, extract the varint using signed long decoder
            decode_signed_long_type(comparable_bytes_view, out, false);
            return;
        }

        // The encoded bytes are in full-form
        // Consume the sign byte
        comparable_bytes_view.remove_prefix(1);

        // Read the length
        uint64_t length = decode_varint_length(comparable_bytes_view, sign_byte) + VARINT_FULL_FORM_THRESHOLD;

        // Add a leading sign byte if there is no sign bit in first bit
        if ((comparable_bytes_view[0] & BYTE_SIGN_MASK) != (sign_byte & BYTE_SIGN_MASK)) {
            out.write<uint8_t>(sign_byte);
        }

        // Consume length bytes from src and write them into out
        out.write(comparable_bytes_view, length);
    }

    // Decoding for float and double
    template <std::floating_point T>
    void operator()(const floating_type_impl<T>&) {
        convert_fixed_length_float<T>(comparable_bytes_view, out, false);
    }

    // Decoder for simple_date_type_impl and time_type_impl; they are written as it is.
    template <std::integral T>
    void operator()(const simple_type_impl<T>&) {
        out.write(comparable_bytes_view, sizeof(T));
    }

    void operator()(const timestamp_type_impl&) {
        convert_signed_fixed_length_integer<db_clock::rep>(comparable_bytes_view, out);
    }

    void operator()(const uuid_type_impl&) {
        uint64_t hi_bits = read_simple<uint64_t>(comparable_bytes_view);
        auto version = (hi_bits >> 60) & 0xF;
        if (version == 1) {
            // This is a time-based UUID and the msb needs to be reshuffled
            out.write<uint64_t>(timeuuid_msb_from_comparable_bytes(hi_bits));
        } else {
            // For non-time UUIDs, the only thing that's needed is to put the version bits back where they were originally.
            out.write<uint64_t>(((hi_bits << 4) & 0xFFFFFFFFFFFF0000L) | version << 12 | (hi_bits & 0x0000000000000FFFL));
        }

        // Write the lsb
        out.write(comparable_bytes_view, sizeof(uint64_t));
    }

    void operator()(const timeuuid_type_impl&) {
        out.write<uint64_t>(timeuuid_msb_from_comparable_bytes(read_simple<uint64_t>(comparable_bytes_view)));
        out.write<uint64_t>(read_simple<uint64_t>(comparable_bytes_view) ^ 0x8080808080808080L);
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
