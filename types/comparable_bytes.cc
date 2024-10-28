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

logging::logger cblogger("comparable_bytes");

static constexpr uint8_t BYTE_SIGN_MASK = 0x80;
static constexpr int VARINT_FULL_FORM_THRESHOLD = 7;

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

    // TODO: Handle other types

    void operator()(const abstract_type& type) {
        // Unimplemented
        on_internal_error(cblogger, fmt::format("byte comparable format not supported for type {}", type.name()));
    }
};

comparable_bytes::comparable_bytes(const abstract_type& type, managed_bytes_view serialized_bytes_view) {
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

    // TODO: Handle other types

    void operator()(const abstract_type& type) {
        // Unimplemented
        on_internal_error(cblogger, fmt::format("byte comparable format not supported for type {}", type.name()));
    }
};

managed_bytes_opt comparable_bytes::to_serialized_bytes(const abstract_type& type) const {
    if (_encoded_bytes.empty()) {
        return managed_bytes_opt();
    }

    managed_bytes_view comparable_bytes_view(_encoded_bytes);
    bytes_ostream serialized_bytes_ostream;
    visit(type, from_comparable_bytes_visitor{comparable_bytes_view, serialized_bytes_ostream});
    return std::move(serialized_bytes_ostream).to_managed_bytes();
}

data_value comparable_bytes::to_data_value(const data_type& type) const {
    auto decoded_bytes = to_serialized_bytes(*type);
    if (!decoded_bytes) {
        return data_value::make_null(type);
    }

    return type->deserialize(decoded_bytes.value());
}
