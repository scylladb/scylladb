/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "types/comparable_bytes.hh"

#include <seastar/core/byteorder.hh>
#include <seastar/core/on_internal_error.hh>

#include "bytes_ostream.hh"
#include "concrete_types.hh"
#include "types/collection.hh"
#include "types/listlike_partial_deserializing_iterator.hh"
#include "types/types.hh"
#include "types/vector.hh"
#include "utils/managed_bytes.hh"
#include "utils/multiprecision_int.hh"
#include "vint-serialization.hh"

logging::logger cblogger("comparable_bytes");

static constexpr uint8_t BYTE_SIGN_MASK = 0x80;
static constexpr int VARINT_FULL_FORM_THRESHOLD = 7;

// Constants or escaping values needed to encode/decode variable-length
// floating point numbers (decimals) to the byte comparable format.
static constexpr uint8_t POSITIVE_DECIMAL_HEADER_MASK = 0x80;
static constexpr uint8_t NEGATIVE_DECIMAL_HEADER_MASK = 0x00;
static constexpr uint8_t DECIMAL_EXPONENT_LENGTH_HEADER_MASK = 0x40;
static constexpr uint8_t DECIMAL_LAST_BYTE = 0x00;

// Escape value. Used, among other things, to mark the end of subcomponents (so that shorter
// compares before anything longer). Actual zeros in input need to be escaped if this is in use.
static constexpr uint8_t ESCAPE = 0x00;
// Zeros are encoded as a sequence of ESCAPE, 0 or more of ESCAPED_0_CONT, ESCAPED_0_DONE
// so zeroed spaces only grow by 1 byte
static constexpr uint8_t ESCAPED_0_CONT = 0xFE;
static constexpr uint8_t ESCAPED_0_DONE = 0xFF;

// Next component marker.
static constexpr uint8_t NEXT_COMPONENT = 0x40;
// Marker for null components in tuples, maps, sets and clustering keys.
static constexpr uint8_t NEXT_COMPONENT_NULL = 0x3E;
// Terminator byte in sequences.
static constexpr uint8_t TERMINATOR = 0x38;

static void read_fragmented_checked(managed_bytes_view& view, size_t bytes_to_read, bytes::value_type* out) {
    if (view.size_bytes() < bytes_to_read) {
        throw_with_backtrace<marshal_exception>(
            format("read_fragmented_checked - not enough bytes (expected {:d}, got {:d})", bytes_to_read, view.size_bytes()));
    }

    return read_fragmented(view, bytes_to_read, out);
}

template<std::integral T, std::size_t array_size>
static std::array<T, array_size> read_fragmented_into_array(managed_bytes_view& view) {
    std::array<T, array_size> buffer;
    constexpr auto bytes_to_read = array_size * sizeof(T);
    read_fragmented(view, bytes_to_read, reinterpret_cast<bytes::value_type*>(buffer.data()));
    return buffer;
}

// Write the given integer array to the bytes stream without changing its endianness.
template<std::integral T, std::size_t array_size>
static void write_native_int_array(bytes_ostream& out, std::array<T, array_size>& buffer) {
    constexpr auto bytes_to_write = array_size * sizeof(T);
    out.write(bytes_view(reinterpret_cast<const signed char*>(buffer.data()), bytes_to_write));
}

template<std::integral T, std::size_t array_size>
static void write_native_int_array(bytes_ostream& out, std::array<T, array_size>& buffer, size_t n) {
    out.write(bytes_view(reinterpret_cast<const signed char*>(buffer.data()), n * sizeof(T)));
}

template <std::integral T>
static void write_native_int(bytes_ostream& out, T value) {
    out.write(bytes_view(reinterpret_cast<const signed char*>(&value), sizeof(T)));
}

// Encode/Decode the given signed fixed-length integer into byte comparable format.
// To encode, invert the sign bit so that negative numbers are ordered before the positive ones.
template <std::signed_integral T>
static void convert_signed_fixed_length_integer(managed_bytes_view& src, bytes_ostream& out) {
    // This function converts between serialized bytes and comparable byte representations,
    // so it is safe to treat the value as an unsigned variant.
    using unsigned_type = std::make_unsigned_t<T>;
    // The serialized bytes are in big-endian format. Instead of converting them to host byte order,
    // XOR'ing with a sign bit mask, and converting the result back to big-endian, we can convert
    // the sign mask to big-endian and XOR it directly with the serialized bytes to flip the sign bit in place.
    static const auto sign_mask_be = seastar::cpu_to_be<unsigned_type>(unsigned_type(1) << (sizeof(unsigned_type) * 8 - 1));
    unsigned_type flipped_value = read_simple_native<unsigned_type>(src) ^ sign_mask_be;
    write_native_int<unsigned_type>(out, flipped_value);
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
 *   2^63-1            encodes as           FFFFFFFFFFFFFFFFFF
 *  -2^63              encodes as           000000000000000000
 *
 * The encoded value will have a maximum of 9 bytes including the length bits.
 * As the number of bytes is specified in bits 2-9, no value is a prefix of another.
 */
static void encode_signed_long_type(int64_t value, bytes_ostream& out) {
    // Create a negative mask : -1 for negative numbers and 0 for positive
    int64_t negative_mask = value >> 63;
    // If the value is negative, flip all bits so we can treat it like a positive number.
    // This allows us to unify the transformation logic for both positive and negative values.
    auto encoded_value = uint64_t(value ^ negative_mask);

    // Count number of significant bits (1-63) that actually needed to be encoded.
    // It can never be 64 as we have already flipped negative numbers.
    int num_bits = 64 - std::countl_zero(uint64_t(encoded_value | 1));
    // Calculate number of bytes required to represent the bits in comparable format.
    // The encoding includes the encoded length as inverted sign bits and the
    // actual value is stored as a 2's complement with at least one sign bit preserved.
    // For example, 32 (0b00100000) can be encoded in a single byte 0b10100000,
    //          but 64 (0b01000000) requires two bytes : 0b00000001 0b01000000.
    // So, 0-6 bits => 1 byte; 7-13 => 2 bytes; etc to 56-63 => 9 bytes.
    // Note that 63 bits require 10 bytes but we handle it as a special case and
    // encode it within 9 bytes - no sign bit is preserved and the decoding logic
    // accounts for this scenario.
    int num_bytes = std::min(9, num_bits / 7 + 1);
    // Encode the number of bytes as inverted sign bits in the comparable format
    std::array<char, 9> encoded_buffer;
    if (num_bytes == 9) {
        // An extra byte full of inverted sign bits has to be included in the comparable format
        encoded_buffer[0] = uint8_t(negative_mask) ^ 0xFF;
        // Incorporate the 9th length bit into the value
        encoded_value |= 0x8000000000000000L;
    } else {
        // Incorporate the length bits into the first non-zero byte of the value
        int64_t mask = (-0x100 >> num_bytes) & 0xFF;
        encoded_value = encoded_value | (mask << ((num_bytes * 8) - 8));
    }

    // Flip the value back to original form and copy the big endian form to the buffer.
    seastar::write_be(encoded_buffer.data() + 1, encoded_value ^ negative_mask);
    // Write out the non-zero bytes from the big endian form
    out.write(encoded_buffer.data() + 9 - num_bytes, num_bytes);
}

// Refer encode_signed_long_type() for the encoding details
// If prefix_sign_bytes is false, any redundant leading sign bits will not be written.
template <bool prefix_sign_bytes>
static void decode_signed_long_type(managed_bytes_view& src, bytes_ostream& out) {
    const int8_t first_byte = static_cast<int8_t>(src[0]);
    // Count the number of bytes to read by counting the inverted sign bits in the first byte
    const int8_t inverted_sign_mask = first_byte >> 7;
    auto num_bytes = std::countl_zero(uint8_t(first_byte ^ inverted_sign_mask));
    // Use a buffer to read the serialized bytes, which is in big endian order.
    std::array<int8_t, 9> buffer;
    // The buffer[0] value is used only to read the extra sign byte in case of a 9-byte encoding.
    // The actual value is always read into the buffer starting from buffer[1].
    auto value_ptr = buffer.data() + 1;
    if (num_bytes == 8 && ((static_cast<int8_t>(src[1]) >> 7) == inverted_sign_mask)) {
        // This is a 9-byte encoding. Read one more additional byte into buffer
        // and ignore the extra byte in the beginning during write.
        read_fragmented_checked(src, num_bytes + 1, buffer.data());
        // Flip the 9th inverted sign bit
        buffer[1] ^= BYTE_SIGN_MASK;
    } else {
        int bytes_pos = 0;
        if constexpr (prefix_sign_bytes) {
            // We read the serialized bytes into their positions w.r.to the big endian
            // order and then fill with sign bytes at the front.
            bytes_pos = sizeof(int64_t) - num_bytes;
            std::memset(value_ptr, ~inverted_sign_mask, bytes_pos);
        }
        read_fragmented_checked(src, num_bytes, value_ptr + bytes_pos);
        // Flip the inverted sign bits in first significant byte of the value
        value_ptr[bytes_pos] ^= int8_t((-0x100 >> num_bytes) & 0xFF);
    }

    if constexpr (prefix_sign_bytes) {
        // Write out the entire long value which is in the big endian order.
        out.write(bytes_view(value_ptr, sizeof(int64_t)));
    } else {
        // Remove redundant leading sign bytes, except for the cases where the value
        // requires two bytes for correct representation.
        // For example: 0x0080 and 0xFF01 retain their leading sign byte, as removing it would flip the sign.
        // For all other cases, skip the first sign byte if it is present.
        if (value_ptr[0] == ~inverted_sign_mask && num_bytes > 1 && (value_ptr[1] >> 7 != inverted_sign_mask)) {
            value_ptr++;
            num_bytes--;
        }
        // Write out only the significant bytes of the long.
        out.write(bytes_view(value_ptr, num_bytes));
    }
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
// The encoded length is XORed with the provided sign mask before writing,
// enabling the caller to invert the encoding for negative varint values.
// Note: The encoding does not support lengths greater than `(1 << 63) − 1`,
// but this is okay as the length of a varint in bytes cannot reach that limit.
void encode_varint_length(uint64_t length, int64_t sign_mask, bytes_ostream& out) {
    const auto bits_minus_one = std::bit_width(length | 1) - 1; // 0 to 63 (the | 1 is to make sure 0 maps to 0 (1 bit))
    const auto bytes_minus_one = std::min(8, bits_minus_one / 7);
    std::array<char, 9> buffer;
    if (bytes_minus_one == 8) {
        // 9 byte encoding. Prefix an extra sign byte to the value.
        buffer[0] = sign_mask == 0 ? 0xFF: 0;
    } else {
        // Incorporate the length bits into the first non-zero byte of the value
        int64_t mask = (-0x100 >> bytes_minus_one) & 0xFF;
        length |= (mask << (bytes_minus_one * 8));
    }

    // XOR the length with sign mask and copy the big endian form to the buffer.
    seastar::write_be(buffer.data() + 1, length ^ sign_mask);
    // Write out the non-zero bytes from the big endian form
    out.write(buffer.data() + 8 - bytes_minus_one, bytes_minus_one + 1);
}

// Decode the length of a varint from comparable bytes.
// Refer encode_varint_length() for the encoding details.
uint64_t decode_varint_length(managed_bytes_view& src, int64_t sign_mask) {
    // Count the number of bytes to read
    const uint8_t first_byte = static_cast<uint8_t>(src[0]) ^ uint8_t(sign_mask);
    auto bytes_minus_one = std::countl_one(first_byte);

    // Use a buffer to read the serialized bytes, which is in big endian order.
    std::array<int8_t, 9> buffer;
    int bytes_pos = 8 - bytes_minus_one;
    read_fragmented_checked(src, bytes_minus_one + 1, buffer.data() + bytes_pos);
    // Prefix sign mask to the value
    std::memset(buffer.data(), int8_t(sign_mask & 0xFF), bytes_pos);

    // Read the big endian value as an int64 and XOR with the sign mask.
    // Skip reading buffer[0] as it is used only to read the extra sign byte in case of a 9-byte encoding.
    int64_t length = read_be<int64_t>(reinterpret_cast<const char*>(buffer.data() + 1)) ^ sign_mask;
    if (bytes_minus_one < 8) {
        // Length bits were incorporated into the first non-zero byte
        // of the value for values with bytes_minus_one < 8; Remove them.
        int64_t mask = (-0x100 >> bytes_minus_one) & 0xFF;
        length ^= (mask << (bytes_minus_one * 8));
    }
    return length;
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
static void encode_varint_type(managed_bytes_view& serialized_bytes_view, bytes_ostream& out) {
    // Peek first byte
    uint8_t first_byte = static_cast<uint8_t>(serialized_bytes_view[0]);
    const int64_t sign_mask = first_byte < 0x80 ? 0 : -1;
    // Discard first byte if it is a sign only byte
    if (first_byte == uint8_t(sign_mask & 0xFF)) {
        serialized_bytes_view.remove_prefix(1);
    }

    const size_t serialized_bytes_size = serialized_bytes_view.size_bytes();
    if (serialized_bytes_size < VARINT_FULL_FORM_THRESHOLD) {
        // Length is 6 bytes or less - encode it as a signed long.
        // The serialized bytes are in big endian format with the leading sign only bytes trimmed out.
        // Copy the bytes into a int64_t taking into account the leading trimmed out sign bytes.
        int64_t value = sign_mask;
        read_fragmented_checked(serialized_bytes_view, serialized_bytes_size,
            reinterpret_cast<bytes::value_type*>(&value) + sizeof(int64_t) - serialized_bytes_size);
        // Value is in big endian; flip it to host byte order and encode.
        encode_signed_long_type(seastar::be_to_cpu(value), out);
        return;
    }

    // Full form encoding;
    // Invert and write the sign byte
    write_native_int<uint8_t>(out, uint8_t(~sign_mask & 0xFF));

    // Encode length section (length - 7)
    encode_varint_length(serialized_bytes_view.size_bytes() - VARINT_FULL_FORM_THRESHOLD, sign_mask, out);

    // Encode rest of data as it is
    out.write(serialized_bytes_view);
    // Clear the view after writing
    serialized_bytes_view = managed_bytes_view();
}

// Decode a byte-comparable representation to the varint number.
static void decode_varint_type(managed_bytes_view& comparable_bytes_view, bytes_ostream& out) {
    // Check the first byte to determine if the bytes were encoded using long or full-form encoding
    const uint8_t sign_byte = ~uint8_t(comparable_bytes_view[0]);
    if (sign_byte != 0 && sign_byte != 0xFF) {
        // First byte is in the range 01-FE, extract the varint using signed long decoder
        decode_signed_long_type<false>(comparable_bytes_view, out);
        return;
    }

    // The encoded bytes are in full-form
    // Consume the sign byte
    comparable_bytes_view.remove_prefix(1);

    // Read the length
    uint64_t length = decode_varint_length(comparable_bytes_view, sign_byte ? -1 : 0) + VARINT_FULL_FORM_THRESHOLD;

    // Add a leading sign byte if there is no sign bit in first bit
    if ((comparable_bytes_view[0] & BYTE_SIGN_MASK) != (sign_byte & BYTE_SIGN_MASK)) {
        write_native_int<uint8_t>(out, sign_byte);
    }

    // Consume length bytes from src and write them into out
    out.write(comparable_bytes_view.prefix(length));
    // Remove the bytes that were consumed
    comparable_bytes_view.remove_prefix(length);
}

// Fixed length signed floating point number encode/decode.
// To encode :
//   If positive : invert first bit to make it greater than all negatives
//   If negative : invert all bytes to make negatives with bigger magnitude smaller
// Decoding is identical except the logic to identify positive/negative values
template <std::floating_point T, bool src_is_byte_comparable>
static void convert_fixed_length_float(managed_bytes_view& src, bytes_ostream& out) {
    // Read and write float as uint32_t; double as uint64_t;
    using uint_t = std::conditional_t<std::is_same_v<T, float>, uint32_t, uint64_t>;
    static_assert(sizeof(uint_t) == sizeof(T), "cannot read floating point type as unsigned int as their sizes are not equal");
    uint_t value_be = read_simple_native<uint_t>(src);

    // Deduce sign - the values have their sign bit flipped in byte comparable format.
    // Since value_be is in big endian, use a big endian sign mask to check sign
    static const auto sign_mask_be = seastar::cpu_to_be<uint_t>(uint_t(1) << (sizeof(uint_t) * 8 - 1));
    bool negative_value = value_be & sign_mask_be;
    if constexpr (src_is_byte_comparable) {
        negative_value = !negative_value;
    }

    // Flip all the bits for a negative value or only the sign bit for a positive value.
    write_native_int<uint_t>(out, value_be ^ (negative_value ? uint_t(-1) : sign_mask_be));
}

// Reorder the given timeuuid msb and make it byte comparable
// Scylla and Cassandra use a standard UUID memory layout for MSB:
// 4 bytes    2 bytes    2 bytes
// time_low - time_mid - time_hi_and_version
// It is reordered as the following to make it byte comparable :
// time_hi_and_version - time_mid - time_low
static inline uint64_t timeuuid_msb_to_comparable_bytes(uint64_t msb) {
    return (msb <<  48)
            | ((msb <<  16) & 0xFFFF00000000L)
            |  (msb >> 32);
}

// Reconstruct timeuuid msb from the byte comparable value
static inline uint64_t timeuuid_msb_from_comparable_bytes(uint64_t byte_comparable_msb) {
    return (byte_comparable_msb << 32)
            | ((byte_comparable_msb >> 16) & 0xFFFF0000L)
            | (byte_comparable_msb >> 48);
}

// Encode decode for timeuuids.
// Pull the version digit to the beginning and rearrange the other bits
// to maintain time-based ordering in byte-comparable form.
// Additionally, invert the sign bits of all bytes in the lower bits to
// preserve Cassandra's legacy comparison order, which compared individual
// bytes as signed values.
template <bool perform_encode>
static void convert_timeuuid(managed_bytes_view& src, bytes_ostream& out) {
    // Read the uuid's msb and lsb into a uint64_t array
    auto buffer = read_fragmented_into_array<uint64_t, 2>(src);
    // The values are still in big endian order, so swap the msb to host order before applying any transformations
    if constexpr (perform_encode) {
        buffer[0] = seastar::cpu_to_be(timeuuid_msb_to_comparable_bytes(seastar::be_to_cpu(buffer[0])));
    } else {
        buffer[0] = seastar::cpu_to_be(timeuuid_msb_from_comparable_bytes(seastar::be_to_cpu(buffer[0])));
    }
    // Flip first bit of every byte - doesn't matter that they are still in big endian order
    buffer[1] ^= 0x8080808080808080L;

    // Write out the encoded value
    write_native_int_array(out, buffer);
}

// UUIDs are fixed-length unsigned integers, where the UUID version/type
// has to be compared first, so pull the version digit first for a byte
// comparable representation.
//
// For time-based UUIDs (version 1), additional reordering is required
// to maintain time-based ordering in byte-comparable form.
static void encode_uuid_type(managed_bytes_view& serialized_bytes_view, bytes_ostream& out) {
    // Read the uuid's high and low bits into a uint64_t array
    auto buffer = read_fragmented_into_array<uint64_t, 2>(serialized_bytes_view);
    // The values are still in big endian order, so swap the msb to host order before applying any transformations
    uint64_t high_bits = seastar::be_to_cpu(buffer[0]);
    auto version = ((high_bits >> 12) & 0xf);
    if (version == 1) {
        // This is a time-based UUID and the msb needs to be rearranged to make it byte comparable
        buffer[0] = seastar::cpu_to_be(timeuuid_msb_to_comparable_bytes(high_bits));
    } else {
        // For non-time UUIDs, write the msb after shifting the version bits to the beginning
        buffer[0] = seastar::cpu_to_be((version << 60) | ((high_bits >> 4) & 0x0FFFFFFFFFFFF000L) | (high_bits & 0xFFFL));
    }

    // Write out the encoded value
    write_native_int_array(out, buffer);
}

static void decode_uuid_type(managed_bytes_view& comparable_bytes_view, bytes_ostream& out) {
    // Read the uuid's high and low bits into a uint64_t array
    auto buffer = read_fragmented_into_array<uint64_t, 2>(comparable_bytes_view);
    uint64_t high_bits = seastar::be_to_cpu(buffer[0]);
    auto version = (high_bits >> 60) & 0xF;
    if (version == 1) {
        // This is a time-based UUID and the msb needs to be reshuffled
        buffer[0] = seastar::cpu_to_be(timeuuid_msb_from_comparable_bytes(high_bits));
    } else {
        // For non-time UUIDs, the only thing that's needed is to put the version bits back where they were originally.
        buffer[0] = seastar::cpu_to_be(((high_bits << 4) & 0xFFFFFFFFFFFF0000L) | version << 12 | (high_bits & 0x0000000000000FFFL));
    }

    // Write out the decoded, serialized value
    write_native_int_array(out, buffer);
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

// Constructs a byte-comparable representation of the decimal_type_impl, which is stored as a big_decimal.
//
// To compare, we need a normalized value, i.e. one with a sign, exponent and (0,1) mantissa.
// To avoid loss of precision, both exponent and mantissa need to be base-100.
// We cannot get this directly off the serialized bytes and the unscaled value has to be reconstructed first.
// After calculating such a mantissa and base 100 exponent, the value is encoded as follows :
//     - sign bit inverted + 0x40 + signed exponent length, where exponent is negated if value is negative
//     - zero or more exponent bytes (as given by length)
//     - 0x80 + first pair of decimal digits, negative if the decimal is negative and the digits are rounded to -inf
//     - zero or more 0x80 + pair of decimal digits, always positive
//     - trailing 0x00
// Zero is special-cased as 0x80.
//
// Because the trailing 00 cannot be produced from a pair of decimal digits (positive or not),
// no value can be a prefix of another.
//
// Encoding examples:
//    1.1    as       c1 = 0x80 (positive number) + 0x40 + (positive exponent) 0x01 (exp length 1)
//                    01 = exponent 1 (100^1)
//                    81 = 0x80 + 01 (0.01)
//                    8a = 0x80 + 10 (....10)   0.0110e2
//                    00
//    -1     as       3f = 0x00 (negative number) + 0x40 - (negative exponent) 0x01 (exp length 1)
//                    ff = exponent -1. negative number, thus 100^1
//                    7f = 0x80 - 01 (-0.01)    -0.01e2
//                    00
//    -99.9  as       3f = 0x00 (negative number) + 0x40 - (negative exponent) 0x01 (exp length 1)
//                    ff = exponent -1. negative number, thus 100^1
//                    1c = 0x80 - 100 (-1.00)
//                    8a = 0x80 + 10  (+....10) -0.999e2
//                    00
static void encode_decimal_type(managed_bytes_view& serialized_bytes_view, bytes_ostream& out) {
    const int32_t scale = read_simple<int32_t>(serialized_bytes_view);
    const boost::multiprecision::cpp_int unscaled_value =
            value_cast<utils::multiprecision_int>(varint_type->deserialize_value(serialized_bytes_view));
    if (unscaled_value.is_zero()) {
        // special case for 0
        write_native_int(out, POSITIVE_DECIMAL_HEADER_MASK);
        return;
    }

    // The big_decimal stores decimal of form unscaled_value * pow(10, -scale)
    // A positive scale moves the decimal point to the left and a negative scale moves it to the right.
    // The decimal 123.456 can be stored as unscaled value = 123456 and scale = 3.
    // This needs to be normalized and converted to a mantissa that lies in [0.01, 1) and a base-100 exponent.
    // such that the big_decimal value = mantissa [0.01, 1) * 100 ^ exponent.
    // The decimal 123.456 becomes mantissa = 0.0123456 and base 100 exponent = 2 (i.e.) 0.0123456 * 100 ^ 2.
    // So, first deduce the scale for an adjusted mantissa that lies in [0.01, 1)
    int64_t adjusted_scale = scale - count_digits(unscaled_value);
    // The base 100 exponent is half of the negative scale rounded up to the nearest integer and will fit within in 4 bytes.
    int64_t exponent_base_100 = std::ceil(double(-adjusted_scale) / 2);
    // Flip the exponent sign for negative big_decimals, so that ones with larger magnitudes are ordered before the smaller ones.
    bool value_is_negative = unscaled_value.sign() < 0;
    int32_t modulated_exponent = value_is_negative ? -exponent_base_100 : exponent_base_100;

    // Use an intermediate buffer instead of directly writing every byte into bytes_ostream.
    std::array<uint8_t, 512> buffer;
    size_t buffer_pos = 0;
    // Write the header byte in the byte comparable format, as follows:
    // sign bit inverted + 0x40 + signed exponent length, where exponent is negated if value is negative
    auto num_exponent_bytes_to_write = 4 - (std::countl_zero<uint32_t>(std::abs(modulated_exponent)) / 8);
    buffer[buffer_pos++] = uint8_t((value_is_negative ? NEGATIVE_DECIMAL_HEADER_MASK : POSITIVE_DECIMAL_HEADER_MASK)
            + DECIMAL_EXPONENT_LENGTH_HEADER_MASK
            + (modulated_exponent < 0 ? -num_exponent_bytes_to_write : num_exponent_bytes_to_write));
    // Write out the non zero bytes from modulated exponent
    while (num_exponent_bytes_to_write--) {
        buffer[buffer_pos++] = uint8_t((modulated_exponent >> (num_exponent_bytes_to_write * 8)) & 0xFF);
    }

    // The mantissa is to be written as a sequence of pair of digits.
    // Calculate the base 10 exponent that can convert the unscaled value into the mantissa [0.01, 1).
    // (i.e.) expected mantissa = unscaled_value * 10 ^ unscaled_to_mantissa_exponent.
    // Note that unscaled_to_mantissa_exponent will always be positive as it is intended
    // to convert the unscaled_value, which is an integer to a mantissa that lies in [0.01, 1).
    int64_t unscaled_to_mantissa_exponent = scale + 2 * exponent_base_100;
    // Using the unscaled_to_mantissa_exponent, iterate and split the unscaled value into a sequence of pair of digits.
    static const boost::multiprecision::cpp_int mp_10(10);
    boost::multiprecision::cpp_int mantissa = unscaled_value;
    while (!mantissa.is_zero()) {
        // extract the first two digits
        unscaled_to_mantissa_exponent -= 2;
        int8_t pair_of_digits = 0;
        if (unscaled_to_mantissa_exponent >= 0) {
            boost::multiprecision::cpp_int dividend = boost::multiprecision::pow(mp_10, unscaled_to_mantissa_exponent);
            pair_of_digits = int8_t(mantissa / dividend);
            // after extracting the first pair, remove it from the unscaled value
            mantissa = mantissa % dividend;
            if (mantissa.sign() < 0) {
                // Negative decimal with more than one pair of digits in mantissa.
                // First pair of digits are rounded to -inf and the sign is retained.
                // The remaining digits contain the positive difference between the round'd value and the unscaled value.
                // For example : -1234.56's unscaled value = -123456, and it is treated as as -130000 + 6544 the pairs are : -13, 65, 44.
                // So, round off the current pair (which is the first pair) by decreasing it.
                pair_of_digits--;
                // Update mantissa with the positive difference
                mantissa = unscaled_value - pair_of_digits * dividend;
            }
        } else {
            // unscaled_to_mantissa_exponent is negative as the last pair of digit is a single digit.
            // So, multiply by 10 to get the last pair of digits.
            // example : if mantissa = 0.12345 => 50 is the pair of digits : 12, 34, 50
            // However, if mantissa = 0.012345, the pairs are 01, 23, 45 and the control will never reach here.
            pair_of_digits = int8_t(mantissa * 10);
            mantissa = 0;
        }

        // Add 0x80 to the mantissa digit pairs and then write them.
        //
        // Adding 0x80 ensures:
        // 1. Negative mantissa values are ordered before the positive ones
        //    - Negative pairs (after rounding toward -∞) are shifted into the 0x00–0x7F range.
        //    - Positive pairs are shifted into the 0x80–0xFF range.
        //    Example:
        //       a) -0.12345 (rounded to -13) → -13 + 0x80 = 0x73
        //       b) 0.12345 (rounded to 12)   → 12 + 0x80 = 0x8C
        //       Since 0x73 < 0x8C, the negative value (-0.12345) is ordered first.
        // 2. Mantissa bytes are distinct from the 0x00 terminator.
        buffer[buffer_pos++] = 0x80 + pair_of_digits;

        if (buffer_pos == buffer.size()) {
            write_native_int_array(out, buffer);
            buffer_pos = 0;
        }
    }

    // Finally, write the byte to mark end of decimal and flush any remaining data to out
    buffer[buffer_pos++] = DECIMAL_LAST_BYTE;
    write_native_int_array(out, buffer, buffer_pos);
}

// Decoder for decimal type.
// Refer encode_decimal_type() for the encoding scheme
static void decode_decimal_type(managed_bytes_view& comparable_bytes_view, bytes_ostream& out) {
    uint8_t header_byte = read_simple<uint8_t>(comparable_bytes_view);
    if (header_byte == POSITIVE_DECIMAL_HEADER_MASK) {
        // special cased zero
        static const auto big_decimal_zero_serialized_bytes = data_value(big_decimal()).serialize_nonnull();
        out.write(big_decimal_zero_serialized_bytes);
        return;
    }

    // The header byte is encoded with the sign of the decimal,
    // and the sign and the length (in bytes) of the decimal exponent.
    // Extract sign of the decimal
    const bool value_is_negative = header_byte < POSITIVE_DECIMAL_HEADER_MASK;
    header_byte -= value_is_negative ? NEGATIVE_DECIMAL_HEADER_MASK : POSITIVE_DECIMAL_HEADER_MASK;
    header_byte -= DECIMAL_EXPONENT_LENGTH_HEADER_MASK;
    // Now only the sign and length of the exponent remains in header_byte.
    // The length is encoded as negative if the exponent is negative.
    const bool exponent_is_negative = header_byte > 0x80;
    header_byte = exponent_is_negative ? -header_byte : header_byte;
    // Exponent length (in bytes) is now in header_byte.
    // First, initialise the exponent variable with 1s if the exponent is negative.
    int64_t exponent = exponent_is_negative ? -1 : 0;
    // Extract the exponent.
    while (header_byte--) {
        exponent = (exponent << 8) | read_simple<uint8_t>(comparable_bytes_view);
    }
    // Exponent sign is flipped for negative decimals. Unflip it.
    exponent = value_is_negative ? -exponent : exponent;

    // Extract the mantissa as an multiprecision_int and recosntruct the unscaled value of the big_decimal.
    // The mantissa lies in [0.01, 1) and is encoded as a sequence of pair of digits.
    utils::multiprecision_int unscaled_value(0);
    int8_t curr_byte = read_simple<int8_t>(comparable_bytes_view);
    while (curr_byte != DECIMAL_LAST_BYTE) {
        // Every mantissa byte has an offset 0x80, subtract that from curr byte
        curr_byte -= 0x80;
        // Move the existing digits in unscaled_value to the right by
        // multiplying it by base (100) and then add int the curr digits.
        unscaled_value = (unscaled_value * 100) + curr_byte;
        // Reduce exponent as this iteration multiplies the unscaled_value by 100
        exponent--;
        curr_byte = read_simple<uint8_t>(comparable_bytes_view);
    }

    // Calculate the big_decimal scale from the base-100 exponent.
    // The big_decimal's scale is a base-10 exponent with the sign flipped, stored as a 32-bit integer.
    int64_t scale = exponent * -2;

    // The scale has undergone various transformations along with unscaled_value,
    // so there’s a chance it might be out of bounds relative to the int32 limits.
    if (scale < std::numeric_limits<int32_t>::min()) {
        // The scale is lower than the minimum int32 value.
        // This can happen if unscaled_value had multiple trailing zeros, which got absorbed into an already too-small scale.
        // For example, unscaled_value = 1234000 and scale = std::numeric_limits<int32_t>::min() will become
        //              unscaled_value = 1234 and scale = std::numeric_limits<int32_t>::min() - 3, after encoding/decoding.
        // Append zeros to unscaled_value to bring the scale within limits.
        const int32_t scale_reduction = std::numeric_limits<int32_t>::min() - scale;
        static const boost::multiprecision::cpp_int mp_10(10);
        unscaled_value *= boost::multiprecision::pow(mp_10, scale_reduction);
        scale = std::numeric_limits<int32_t>::min();
    } else if (scale > std::numeric_limits<int32_t>::max()) {
        // The scale is greater than the maximum int32 value.
        // This can happen iff the last pair of digits in the mantissa required multiplication by 10 during encoding.
        // And that would increase the scale by 1 after decoding, pushing it past its maximum limit.
        // For example, a decimal with unscaled_value = 12345 and scale = std::numeric_limits<int32_t>::max()
        //     will be encoded as mantissa = 0.12345, with digit pairs: 12, 34, 50.
        //     After decoding, the unscaled_value becomes 123450 and scale = std::numeric_limits<int32_t>::max() + 1.
        // Trim the zero from the unscaled value and update the scale.
        unscaled_value /= 10;
        scale = std::numeric_limits<int32_t>::max();
    }

    // Write out the scale and unscaled values
    write_be(reinterpret_cast<char*>(out.write_place_holder(sizeof(int32_t))), int32_t(scale));
    out.write(data_value(std::move(unscaled_value)).serialize_nonnull());
}

// Escape zeros to encode variable length natively byte-comparable data types.
// Zeros in the source are escaped as ESCAPE followed by one or more ESCAPED_0_CONT and ending with ESCAPED_0_DONE.
// The encoding terminates in an escaped state (either with ESCAPE or ESCAPED_0_CONT).
// If the source ends in a zero, we use ESCAPED_0_CONT to ensure that the encoding remains smaller than
// the same source with an additional zero at the end.
//
// E.g. "A\0\0B" translates to 4100FEFF4200
//      "A\0B\0"               4100FF4200FE
//      "A\0"                  4100FE
//      "AB"                   414200
//
// In a single-byte source, the bytes could be passed unchanged, but this would prevent combining components.
// This translation preserves order, and since the encoding for 0 is higher than the separator,
// it also ensures that shorter components are treated as smaller.
//
// The encoding is not prefix-free since, for example, the encoding of "A" (4100) is a prefix of the encoding of "A\0" (4100FE).
// However, the byte following the prefix is guaranteed to be FE or FF, making the encoding weakly prefix-free.
// Additionally, any such prefix sequence will compare as smaller than the value it prefixes,
// because any permitted separator byte will be smaller than the byte following the prefix.
static void escape_zeros(managed_bytes_view& serialized_bytes_view, bytes_ostream& out) {
    bool escaped = false;
    // Process the input in multiple fixed-size chunks to allow the use of a fixed-size
    // intermediate buffer for writing the encoded output. The buffer helps reduce the
    // number of writes to the output stream.
    constexpr size_t max_chunk_size = 1024;
    // The encoded bytes will have one extra byte for every zero sequence in the source.
    // So, worst case, it can only be 1.5 times the size of the chunk.
    constexpr size_t buffer_size = (max_chunk_size / 2) * 3;
    std::array<signed char, buffer_size> buffer;

    while (!serialized_bytes_view.empty()) {
        const size_t curr_chunk_size = std::min(max_chunk_size, serialized_bytes_view.current_fragment().size());
        const auto& curr_chunk = serialized_bytes_view.prefix(curr_chunk_size).current_fragment();
        serialized_bytes_view.remove_prefix(curr_chunk_size);
        auto curr_zero_start = curr_chunk.find(ESCAPE);
        if (curr_zero_start == managed_bytes_view::fragment_type::npos) {
            // There are no zeros in current chunk
            if (escaped) {
                // but the previous chunk ended with a zero, so end the escape sequence
                write_native_int(out, ESCAPED_0_DONE);
                escaped = false;
            }
            // write the current chunk as it is; this is the fast path for the common text cases
            out.write(curr_chunk);
        } else {
            // There are zeros that need to be escaped;
            size_t buffer_pos = 0;
            size_t curr_chunk_pos = 0;
            while (curr_chunk_pos != curr_chunk_size) {
                if (escaped) {
                    // Fill one ESCAPED_0_CONT byte for each zero in the input
                    const auto next_non_zero_byte = std::min(curr_chunk.find_first_not_of(ESCAPE, curr_chunk_pos), curr_chunk_size);
                    const auto curr_seq_length = next_non_zero_byte - curr_chunk_pos;
                    if (curr_seq_length > 0) {
                        std::fill_n(buffer.data() + buffer_pos, curr_seq_length, ESCAPED_0_CONT);
                        buffer_pos += curr_seq_length;
                        curr_chunk_pos = next_non_zero_byte;
                    }

                    if (curr_chunk_pos != curr_chunk_size) {
                        // The sequence ends before the end of the fragment
                        escaped = false;
                        buffer[buffer_pos++] = ESCAPED_0_DONE;
                    }

                } else if (curr_chunk[curr_chunk_pos] == ESCAPE) {
                    // Start escape sequence
                    buffer[buffer_pos++] = ESCAPE;
                    curr_chunk_pos++;
                    escaped = true;

                } else {
                    // Write the non zero byte sequence as it is
                    const auto next_zero_byte = std::min(curr_chunk.find_first_of(ESCAPE, curr_chunk_pos), curr_chunk_size);
                    const auto curr_seq_length = next_zero_byte - curr_chunk_pos;
                    std::memcpy(reinterpret_cast<void*>(buffer.data() + buffer_pos), reinterpret_cast<const void*>(curr_chunk.data() + curr_chunk_pos), curr_seq_length);
                    buffer_pos += curr_seq_length;
                    curr_chunk_pos = next_zero_byte;
                }
            }

            // Write the encoded bytes so far to out.
            write_native_int_array(out, buffer, buffer_pos);
        }
    }

    // Terminate encoded bytes in an escaped state
    write_native_int(out, escaped ? ESCAPED_0_CONT : ESCAPE);
}

// Decode variable length natively byte-comparable data types into the original unescaped form
static void unescape_zeros(managed_bytes_view& comparable_bytes_view, bytes_ostream& out) {
    bool escaped = false;
    // Process the input in multiple fixed-size chunks to allow the use of a fixed-size
    // intermediate buffer for writing the decoded output. The buffer helps reduce the
    // number of writes to the output stream.
    constexpr size_t max_chunk_size = 1024;
    // One byte is removed for every escape sequence present in the chunk, so the decoded
    // output for a chunk will never exceed chunk_size.
    std::array<signed char, max_chunk_size> buffer;

    while (!comparable_bytes_view.empty()) {
        const size_t curr_chunk_size = std::min(max_chunk_size, comparable_bytes_view.current_fragment().size());
        const auto& curr_chunk = comparable_bytes_view.prefix(curr_chunk_size).current_fragment();
        size_t buffer_pos = 0;
        size_t curr_chunk_pos = 0;
        while (curr_chunk_pos != curr_chunk_size) {
            if (escaped) {
                // The decoder is in an escaped state.
                switch (uint8_t(curr_chunk[curr_chunk_pos])) {
                case ESCAPED_0_CONT: {
                    // Fill in as many ZEROs as there are ESCAPED_0_CONTs
                    const auto end_of_escape_cont = std::min(curr_chunk.find_first_not_of(ESCAPED_0_CONT, curr_chunk_pos), curr_chunk_size);
                    const auto curr_seq_length = end_of_escape_cont - curr_chunk_pos;
                    if (curr_seq_length > 0) {
                        std::fill_n(buffer.data() + buffer_pos, curr_seq_length, ESCAPE);
                        buffer_pos += curr_seq_length;
                        curr_chunk_pos = end_of_escape_cont;
                    }
                    break;
                }
                case ESCAPED_0_DONE:
                    // End of escape sequence
                    buffer[buffer_pos++] = ESCAPE;
                    curr_chunk_pos++;
                    escaped = false;
                    break;
                default:
                    // The next byte is neither ESCAPED_0_CONT(0xFE) nor ESCAPED_0_DONE(0xFF).
                    // This happens iff the byte-comparable is part of a multi-component sequence,
                    // and we have reached the end of the current encoded component.
                    // The next byte is a separator (between components) or
                    // a terminator (end of sequence) and decoder should consume just until that byte.
                    comparable_bytes_view.remove_prefix(curr_chunk_pos);
                    write_native_int_array(out, buffer, buffer_pos);
                    return;
                }
            } else if (curr_chunk[curr_chunk_pos] == ESCAPE) {
                // Start of escape sequence
                curr_chunk_pos++;
                escaped = true;
            } else {
                // Find the next zero sequence.
                const auto next_zero_byte = std::min(curr_chunk.find(ESCAPE, curr_chunk_pos), curr_chunk_size);
                const auto curr_seq_length = next_zero_byte - curr_chunk_pos;
                std::memcpy(reinterpret_cast<void*>(buffer.data() + buffer_pos), reinterpret_cast<const void*>(curr_chunk.data() + curr_chunk_pos), curr_seq_length);
                buffer_pos += curr_seq_length;
                curr_chunk_pos = next_zero_byte;
            }
        }
        comparable_bytes_view.remove_prefix(curr_chunk_size);
        write_native_int_array(out, buffer, buffer_pos);
    }
}

// Functions are defined later in the file as they depend on to_comparable_bytes_visitor and from_comparable_bytes_visitor.
static void to_comparable_bytes(const abstract_type& type, managed_bytes_view& serialized_bytes_view, bytes_ostream& out);
static void from_comparable_bytes(const abstract_type& type, managed_bytes_view& comparable_bytes_view, bytes_ostream& out);

// Encodes a single non-null element of a multi-component type into a byte-comparable format.
// The element can be an item from a list, set, vector, a key or value from a map, or a field from a tuple.
// The serialized bytes of the element are transformed into a byte-comparable representation,
// prefixed with a `NEXT_COMPONENT` marker to delimit it from other elements, and written to the output stream.
void encode_component(const abstract_type& type, managed_bytes_view serialized_bytes_view, bytes_ostream& out) {
    write_native_int(out, NEXT_COMPONENT);
    to_comparable_bytes(type, serialized_bytes_view, out);
}

// Decodes a single non-null element of a multi-component type from its byte-comparable representation
// into its serialized format. The serialized value is prefixed with its size in bytes.
void decode_component(const abstract_type& type, managed_bytes_view& comparable_bytes_view, bytes_ostream& out) {
    // Placeholder to write the size of the serialized bytes
    auto element_size_ptr = reinterpret_cast<char*>(out.write_place_holder(sizeof(int32_t)));
    auto curr_write_pos = out.pos();
    // Decode the comparable bytes into serialized bytes and write it into out
    from_comparable_bytes(type, comparable_bytes_view, out);
    // Now write the size of the serialized bytes in big endian format
    write_be(element_size_ptr, static_cast<int32_t>(out.written_since(curr_write_pos)));
}

// Encodes a single null element of a multi-component type into a byte-comparable format.
static void encode_null_component(bytes_ostream& out) {
    // Write the NULL component marker
    write_native_int(out, NEXT_COMPONENT_NULL);
}

// Decodes a single null element of a multi-component type.
static void decode_null_component(bytes_ostream& out) {
    // Write -1 as length for null value encoded as 4-byte big endian.
    static const auto null_length = [] {
        std::array<char, 4> arr{};
        write_be(arr.data(), int32_t(-1));
        return arr;
    }();
    out.write(bytes_view(reinterpret_cast<const signed char*>(null_length.data()), null_length.size()));
}

// Decodes the next marker and the component, if available, into serialized format.
template<bool allow_null_component_value>
static stop_iteration decode_marker_and_component(const abstract_type& type, managed_bytes_view& comparable_bytes_view, bytes_ostream& out) {
    switch (read_simple_native<uint8_t>(comparable_bytes_view)) {
    case NEXT_COMPONENT:
        decode_component(type, comparable_bytes_view, out);
        return stop_iteration::no;
    case TERMINATOR:
        // End of the collection, return without writing anything
        return stop_iteration::yes;
    case NEXT_COMPONENT_NULL:
        if constexpr (allow_null_component_value) {
            decode_null_component(out);
            return stop_iteration::no;
        }
        // NEXT_COMPONENT_NULL encountered when allow_null_component_value is false;
        // Fallthrough to throw an exception
        [[fallthrough]];
    default:
        // This should not happen unless the encoding scheme has changed
        throw_with_backtrace<marshal_exception>("decode_next_component - unexpected component marker in collection");
    }
}

// Encode a set or a list type into byte comparable format.
// The collection is encoded as a sequence of components, each preceded by a header.
// The component header is either NEXT_COMPONENT_NULL or NEXT_COMPONENT, depending on whether the element is null or not.
// The collection is terminated with a TERMINATOR byte.
static void encode_set_or_list_type(const listlike_collection_type_impl& type, managed_bytes_view& serialized_bytes_view, bytes_ostream& out) {
    const auto& elements_type = *type.get_elements_type();
    using llpdi = listlike_partial_deserializing_iterator;
    for (auto it = llpdi::begin(serialized_bytes_view); it != llpdi::end(serialized_bytes_view); it++) {
        // Read the serialized bytes from the collection value and write it in byte comparable format.
        if ((*it).has_value()) {
            encode_component(elements_type, (*it).value(), out);
        } else {
            encode_null_component(out);
        }
    }
    write_native_int(out, TERMINATOR);
}

// Decode set or list type from byte comparable format.
static void decode_set_or_list_type(const listlike_collection_type_impl& type, managed_bytes_view& comparable_bytes_view, bytes_ostream& out) {
    const auto& elements_type = *type.get_elements_type();
    // Create a place holder for the size of the collection.
    // The size will be written later after decoding all the elements.
    auto collection_size_ptr = reinterpret_cast<char*>(out.write_place_holder(sizeof(int32_t)));
    int32_t collection_size = 0;
    while (decode_marker_and_component<true>(elements_type, comparable_bytes_view, out) == stop_iteration::no) {
        collection_size++;
    }

    write_be(collection_size_ptr, collection_size);
}

// Encode a map type into byte comparable format.
// The map is encoded as a sequence of key-value pairs, each preceded by a component header similar to sets and lists.
static void encode_map(const map_type_impl& type, managed_bytes_view& serialized_bytes_view, bytes_ostream& out) {
    const auto& key_type = *type.get_keys_type();
    const auto& value_type = *type.get_values_type();
    auto map_size = read_collection_size(serialized_bytes_view);
    while (map_size--) {
        encode_component(key_type, read_collection_key(serialized_bytes_view), out);
        encode_component(value_type, read_collection_value_nonnull(serialized_bytes_view), out);
    }
    write_native_int(out, TERMINATOR);
}

// Decode a map type from byte comparable format.
static void decode_map(const map_type_impl& type, managed_bytes_view& comparable_bytes_view, bytes_ostream& out) {
    const auto& key_type = *type.get_keys_type();
    const auto& value_type = *type.get_values_type();
    // Create a place holder for the size of the map.
    // The size will be written later after decoding all the elements.
    auto map_size_ptr = reinterpret_cast<char*>(out.write_place_holder(sizeof(int32_t)));
    int32_t map_size = 0;
    while (decode_marker_and_component<false>(key_type, comparable_bytes_view, out) == stop_iteration::no) {
        // Decode value
        if (read_simple_native<uint8_t>(comparable_bytes_view) != NEXT_COMPONENT) {
            throw_with_backtrace<marshal_exception>("decode_map - unexpected component marker in map");
        }
        decode_component(value_type, comparable_bytes_view, out);
        map_size++;
    }
    write_be(map_size_ptr, static_cast<int32_t>(map_size));
}

// Encode a tuple type into byte comparable format.
// The tuple is encoded as a sequence of components, each preceded by a component header similar to sets and lists.
// If an element is null, it is encoded with a NEXT_COMPONENT_NULL marker and any trailing nulls are skipped.
// The tuple is terminated with a TERMINATOR byte.
static void encode_tuple(const tuple_type_impl& type, managed_bytes_view& serialized_bytes_view, bytes_ostream& out) {
    int pending_null_writes = 0;
    for (const auto& element_type : type.all_types()) {
        if (serialized_bytes_view.empty()) {
            // End of tuple values: all remaining elements are null and can be omitted from encoding.
            break;
        }
        auto element = read_tuple_element(serialized_bytes_view);
        if (element) {
            // Write any pending null components before writing the current element.
            while (pending_null_writes > 0) {
                encode_null_component(out);
                pending_null_writes--;
            }
            encode_component(*element_type.get(), element.value(), out);
        } else {
            // Null tuple element. Track it but do not write it yet as it maybe a trailing null.
            pending_null_writes++;
        }
    }

    write_native_int(out, TERMINATOR);
}

// Decodes a tuple type from byte comparable format into its serialized representation.
static void decode_tuple(const tuple_type_impl& type, managed_bytes_view& comparable_bytes_view, bytes_ostream& out) {
    for (const auto& element_type : type.all_types()) {
        if (decode_marker_and_component<true>(*element_type.get(), comparable_bytes_view, out) == stop_iteration::yes) {
            break;
        }
    }
}

// Encodes a vector type into byte comparable format.
// The collection is encoded as a sequence of components, each preceded by NEXT_COMPONENT header.
// The collection is terminated with a TERMINATOR byte.
static void encode_vector(const vector_type_impl& type, managed_bytes_view& serialized_bytes_view, bytes_ostream& out) {
    const auto& element_type = *type.get_elements_type();
    auto value_length = element_type.value_length_if_fixed();
    for (size_t i = 0; i < type.get_dimension(); i++) {
        encode_component(element_type, read_vector_element(serialized_bytes_view, value_length), out);
    }
    write_native_int(out, TERMINATOR);
}

// Decodes a vector from byte-comparable format into its serialized representation.
// For vectors with fixed-length elements, each element is decoded directly into the output stream.
// For vectors with variable-length elements, each element is decoded and prefixed with its size serialized as an unsigned_vint.
static void decode_vector(const vector_type_impl& type, managed_bytes_view& comparable_bytes_view, bytes_ostream& out) {
    const auto& element_type = *type.get_elements_type();
    if (element_type.value_length_if_fixed()) {
        // For fixed-length element types, decode each component directly into the output stream, no need to prefix length
        while (read_simple_native<uint8_t>(comparable_bytes_view) == NEXT_COMPONENT) {
            from_comparable_bytes(element_type, comparable_bytes_view, out);
        }
    } else {
        // For variable-length element types, decode every component into a temporary buffer, serialize
        // its size as an unsigned vint, and write both the size and the component to the output stream.
        bytes_ostream decoded_value;
        while (read_simple_native<uint8_t>(comparable_bytes_view) == NEXT_COMPONENT) {
            from_comparable_bytes(element_type, comparable_bytes_view, decoded_value);
            const auto decoded_size = decoded_value.size();
            std::array<int8_t, max_vint_length> serialized_decoded_size;
            unsigned_vint::serialize(decoded_size, serialized_decoded_size.data());
            out.write(bytes_view(serialized_decoded_size.data(), unsigned_vint::serialized_size(decoded_size)));
            out.append(decoded_value);
            decoded_value.clear();
        }
    }
}

// Flip all the bits from the input and write to the flipped stream.
static void flip_all_bits(const bytes_view& input, bytes_ostream& flipped) {
    // Process word by word and write the flipped output to buffer
    uint64_t word;
    // Use an intermediate buffer to store the flipped output to reduce the
    // number of writes to the output stream.
    std::array<uint8_t, sizeof(word) * 128> buffer; // 1K buffer

    // Process input as batches of words
    size_t buffer_pos = 0, input_pos = 0, batch_size = 0;
    while ((batch_size = std::min(align_down(input.size() - input_pos, sizeof(word)), buffer.size())) > 0) {
        while (buffer_pos < batch_size) {
            std::memcpy(&word, input.data() + input_pos, sizeof(word));
            word = ~word;
            std::memcpy(buffer.data() + buffer_pos, &word, sizeof(word));
            input_pos += sizeof(word);
            buffer_pos += sizeof(word);
        }

        // Flush the buffer
        write_native_int_array(flipped, buffer, buffer_pos);
        buffer_pos = 0;
    }

    // Flip the remaining bytes in input, if any, and write to buffer.
    while (input_pos < input.size()) {
        buffer[buffer_pos++] = ~input[input_pos++];
    }

    // Flush remaining data in buffer to out
    if (buffer_pos > 0) {
        // Flush the buffer
        write_native_int_array(flipped, buffer, buffer_pos);
    }
}

// Encodes a serialized value into its reversed type, byte comparable format.
// The serialized bytes of the underlying type is transformed into its standard byte comparable representation.
// Then, all the bits are flipped to ensure that the lexicographical sort order is reversed.
static void encode_reversed(const reversed_type_impl& type, managed_bytes_view& serialized_bytes_view, bytes_ostream& out) {
    bytes_ostream encoded_bytes_bo;
    to_comparable_bytes(*type.underlying_type(), serialized_bytes_view, encoded_bytes_bo);
    for (const auto& fragment : encoded_bytes_bo.fragments()) {
        flip_all_bits(fragment, out);
    }
}

// Decode a reversed type byte comparable representation into its serialized format.
static void decode_reversed(const reversed_type_impl& type, managed_bytes_view& comparable_bytes_view, bytes_ostream& out) {
    bytes_ostream encoded_bytes_bo;
    while (!comparable_bytes_view.empty()) {
        flip_all_bits(comparable_bytes_view.current_fragment(), encoded_bytes_bo);
        comparable_bytes_view.remove_current();;
    }
    auto encoded_bytes_mb = std::move(encoded_bytes_bo).to_managed_bytes();
    auto encoded_bytes_mbv = managed_bytes_view(encoded_bytes_mb);
    from_comparable_bytes(*type.underlying_type(), encoded_bytes_mbv, out);
}

// to_comparable_bytes_visitor provides methods to
// convert serialized bytes into byte comparable format.
struct to_comparable_bytes_visitor {
    managed_bytes_view& serialized_bytes_view;
    bytes_ostream& out;

    void operator()(const boolean_type_impl&) {
        // Any non zero byte value is encoded as 1 else 0
        write_native_int(out, uint8_t(read_simple_native<uint8_t>(serialized_bytes_view) != 0));
    }

    // Fixed length signed integers encoding
    template <std::signed_integral T>
    void operator()(const integer_type_impl<T>& type) {
        convert_signed_fixed_length_integer<T>(serialized_bytes_view, out);
    }

    // The long type uses variable-length encoding, unlike other smaller fixed-length signed integers.
    void operator()(const long_type_impl&) {
        encode_signed_long_type(read_simple<int64_t>(serialized_bytes_view), out);
    }

    // Encode variable length integers
    void operator()(const varint_type_impl&) {
        encode_varint_type(serialized_bytes_view, out);
    }

    // Encoding for float and double
    template <std::floating_point T>
    void operator()(const floating_type_impl<T>&) {
        convert_fixed_length_float<T, false>(serialized_bytes_view, out);
    }

    // Encoding for simple_date_type_impl and time_type_impl
    // They are both fixed length unsigned integers and are already byte comparable in their serialized form
    template <std::integral T>
    void operator()(const simple_type_impl<T>&) {
        out.write(serialized_bytes_view.prefix(sizeof(T)));
        serialized_bytes_view.remove_prefix(sizeof(T));
    }

    // timestamp_type is encoded as fixed length signed integer
    void operator()(const timestamp_type_impl&) {
        convert_signed_fixed_length_integer<db_clock::rep>(serialized_bytes_view, out);
    }

    void operator()(const uuid_type_impl&) {
        encode_uuid_type(serialized_bytes_view, out);
    }

    // Encode timeuuid
    void operator()(const timeuuid_type_impl&) {
        convert_timeuuid<true>(serialized_bytes_view, out);
    }

    void operator()(const decimal_type_impl&) {
        encode_decimal_type(serialized_bytes_view, out);
    }

    void operator()(const bytes_type_impl&) {
        escape_zeros(serialized_bytes_view, out);
    }

    // Encode text and ascii types
    void operator()(const string_type_impl&) {
        escape_zeros(serialized_bytes_view, out);
    }

    void operator()(const duration_type_impl&) {
        escape_zeros(serialized_bytes_view, out);
    }

    void operator()(const inet_addr_type_impl&) {
        escape_zeros(serialized_bytes_view, out);
    }

    // encode sets and lists
    void operator()(const listlike_collection_type_impl& type) {
        encode_set_or_list_type(type, serialized_bytes_view, out);
    }

    void operator()(const map_type_impl& type) {
        encode_map(type, serialized_bytes_view, out);
    }

    // encode tuples and UDTs
    void operator()(const tuple_type_impl& type) {
        encode_tuple(type, serialized_bytes_view, out);
    }

    void operator()(const vector_type_impl& type) {
        encode_vector(type, serialized_bytes_view, out);
    }

    void operator()(const reversed_type_impl& type) {
        encode_reversed(type, serialized_bytes_view, out);
    }

    void operator()(const empty_type_impl&) {}

    void operator()(const abstract_type& type) {
        // Unimplemented
        on_internal_error(cblogger, fmt::format("byte comparable format not supported for type {}", type.name()));
    }
};

void to_comparable_bytes(const abstract_type& type, managed_bytes_view& serialized_bytes_view, bytes_ostream& out) {
    visit(type, to_comparable_bytes_visitor{serialized_bytes_view, out});
}

comparable_bytes::comparable_bytes(const abstract_type& type, managed_bytes_view serialized_bytes_view) {
    bytes_ostream encoded_bytes_ostream;
    to_comparable_bytes(type, serialized_bytes_view, encoded_bytes_ostream);
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
        // return a byte without changing anything
        out.write(comparable_bytes_view.prefix(1));
        comparable_bytes_view.remove_prefix(1);
    }

    template <std::signed_integral T>
    void operator()(const integer_type_impl<T>& type) {
        // First bit (sign bit) is inverted for the fixed length signed integers.
        // Reuse encode logic to unflip the sign bit
        convert_signed_fixed_length_integer<T>(comparable_bytes_view, out);
    }

    void operator()(const long_type_impl&) {
        decode_signed_long_type<true>(comparable_bytes_view, out);
    }

    void operator()(const varint_type_impl&) {
        decode_varint_type(comparable_bytes_view, out);
    }

    // Decoding for float and double
    template <std::floating_point T>
    void operator()(const floating_type_impl<T>&) {
        convert_fixed_length_float<T, true>(comparable_bytes_view, out);
    }

    // Decoder for simple_date_type_impl and time_type_impl; they are written as it is.
    template <std::integral T>
    void operator()(const simple_type_impl<T>&) {
        out.write(comparable_bytes_view.prefix(sizeof(T)));
        comparable_bytes_view.remove_prefix(sizeof(T));
    }

    void operator()(const timestamp_type_impl&) {
        convert_signed_fixed_length_integer<db_clock::rep>(comparable_bytes_view, out);
    }

    void operator()(const uuid_type_impl&) {
        decode_uuid_type(comparable_bytes_view, out);
    }

    void operator()(const timeuuid_type_impl&) {
        convert_timeuuid<false>(comparable_bytes_view, out);
    }

    void operator()(const decimal_type_impl&) {
        decode_decimal_type(comparable_bytes_view, out);
    }

    void operator()(const bytes_type_impl&) {
        unescape_zeros(comparable_bytes_view, out);
    }

    // Decode text and ascii types
    void operator()(const string_type_impl&) {
        unescape_zeros(comparable_bytes_view, out);
    }

    void operator()(const duration_type_impl&) {
        unescape_zeros(comparable_bytes_view, out);
    }

    void operator()(const inet_addr_type_impl&) {
        unescape_zeros(comparable_bytes_view, out);
    }

    // decode sets and lists
    void operator()(const listlike_collection_type_impl& type) {
        decode_set_or_list_type(type, comparable_bytes_view, out);
    }

    void operator()(const map_type_impl& type) {
        decode_map(type, comparable_bytes_view, out);
    }

    // decode tuples and UDTs
    void operator()(const tuple_type_impl& type) {
        decode_tuple(type, comparable_bytes_view, out);
    }

    void operator()(const vector_type_impl& type) {
        decode_vector(type, comparable_bytes_view, out);
    }

    void operator()(const reversed_type_impl& type) {
        decode_reversed(type, comparable_bytes_view, out);
    }

    void operator()(const empty_type_impl&) {}

    void operator()(const abstract_type& type) {
        // Unimplemented
        on_internal_error(cblogger, fmt::format("byte comparable format not supported for type {}", type.name()));
    }
};

void from_comparable_bytes(const abstract_type& type, managed_bytes_view& comparable_bytes_view, bytes_ostream& out) {
    visit(type, from_comparable_bytes_visitor{comparable_bytes_view, out});
}

managed_bytes_opt comparable_bytes::to_serialized_bytes(const abstract_type& type) const {
    if (_encoded_bytes.empty() && type != *empty_type) {
        return managed_bytes_opt();
    }

    managed_bytes_view comparable_bytes_view(_encoded_bytes);
    bytes_ostream serialized_bytes_ostream;
    from_comparable_bytes(type, comparable_bytes_view, serialized_bytes_ostream);
    return std::move(serialized_bytes_ostream).to_managed_bytes();
}

data_value comparable_bytes::to_data_value(const data_type& type) const {
    auto decoded_bytes = to_serialized_bytes(*type);
    if (!decoded_bytes) {
        return data_value::make_null(type);
    }

    return type->deserialize(decoded_bytes.value());
}
