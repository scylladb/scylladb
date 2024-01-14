/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "base64.hh"

#include <seastar/core/print.hh>

// Arrays for quickly converting to and from an integer between 0 and 63,
// and the character used in base64 encoding to represent it.
static class base64_chars {
public:
    static constexpr const char to[] =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    static constexpr uint8_t invalid_char = 255;
    uint8_t from[255];
    base64_chars() {
        static_assert(sizeof(to) == 64 + 1);
        for (int i = 0; i < 255; i++) {
            from[i] = invalid_char; // signal invalid character
        }
        for (int i = 0; i < 64; i++) {
            from[(unsigned) to[i]] = i;
        }
    }
} base64_chars;

std::string base64_encode(bytes_view in) {
    std::string ret;
    ret.reserve(((4 * in.size() / 3) + 3) & ~3);
    int i = 0;
    unsigned char chunk3[3]; // chunk of input
    for (auto byte : in) {
        chunk3[i++] = byte;
        if (i == 3) {
            ret += base64_chars.to[ (chunk3[0] & 0xfc) >> 2 ];
            ret += base64_chars.to[ ((chunk3[0] & 0x03) << 4) + ((chunk3[1] & 0xf0) >> 4) ];
            ret += base64_chars.to[ ((chunk3[1] & 0x0f) << 2) + ((chunk3[2] & 0xc0) >> 6) ];
            ret += base64_chars.to[ chunk3[2] & 0x3f ];
            i = 0;
        }
    }
    if (i) {
        // i can be 1 or 2.
        for(int j = i; j < 3; j++)
            chunk3[j] = '\0';
        ret += base64_chars.to[ ( chunk3[0] & 0xfc) >> 2 ];
        ret += base64_chars.to[ ((chunk3[0] & 0x03) << 4) + ((chunk3[1] & 0xf0) >> 4) ];
        if (i == 2) {
            ret += base64_chars.to[ ((chunk3[1] & 0x0f) << 2) + ((chunk3[2] & 0xc0) >> 6) ];
        } else {
            ret += '=';
        }
        ret += '=';
    }
    return ret;
}

static size_t base64_padding_len(std::string_view str) {
    size_t padding = 0;
    padding += (!str.empty() && str.back() == '=');
    padding += (str.size() > 1 && *(str.end() - 2) == '=');
    return padding;
}

static void base64_trim_padding(std::string_view& str) {
    if (str.size() % 4 != 0) {
        throw std::invalid_argument(format("Base64 encoded length is expected a multiple of 4 bytes but found: {}", str.size()));
    }
    str.remove_suffix(base64_padding_len(str));
}

static std::string base64_decode_string(std::string_view in) {
    base64_trim_padding(in);
    int i = 0;
    int8_t chunk4[4]; // chunk of input, each byte converted to 0..63;
    std::string ret;
    ret.reserve(in.size() * 3 / 4);
    for (unsigned char c : in) {
        uint8_t dc = base64_chars.from[c];
        if (dc == base64_chars::invalid_char) {
           throw std::invalid_argument(format("Invalid Base64 character: '{}'", char(c)));
        }
        chunk4[i++] = dc;
        if (i == 4) {
            ret += (chunk4[0] << 2) + ((chunk4[1] & 0x30) >> 4);
            ret += ((chunk4[1] & 0xf) << 4) + ((chunk4[2] & 0x3c) >> 2);
            ret += ((chunk4[2] & 0x3) << 6) + chunk4[3];
            i = 0;
        }
    }
    if (i) {
        // i can be 2 or 3, meaning 1 or 2 more output characters
        if (i>=2)
            ret += (chunk4[0] << 2) + ((chunk4[1] & 0x30) >> 4);
        if (i==3)
            ret += ((chunk4[1] & 0xf) << 4) + ((chunk4[2] & 0x3c) >> 2);
    }
    return ret;
}

bytes base64_decode(std::string_view in) {
    // FIXME: This copy is sad. The problem is we need back "bytes"
    // but "bytes" doesn't have efficient append and std::string.
    // To fix this we need to use bytes' "uninitialized" feature.
    std::string ret = base64_decode_string(in);
    return bytes(ret.begin(), ret.end());
}

size_t base64_decoded_len(std::string_view str) {
    return str.size() / 4 * 3 - base64_padding_len(str);
}

bool base64_begins_with(std::string_view base, std::string_view operand) {
    if (base.size() < operand.size() || base.size() % 4 != 0 || operand.size() % 4 != 0) {
        return false;
    }
    if (base64_padding_len(operand) == 0) {
        return base.starts_with(operand);
    }
    const std::string_view unpadded_base_prefix = base.substr(0, operand.size() - 4);
    const std::string_view unpadded_operand = operand.substr(0, operand.size() - 4);
    if (unpadded_base_prefix != unpadded_operand) {
        return false;
    }
    // Decode and compare last 4 bytes of base64-encoded strings
    const std::string base_remainder = base64_decode_string(base.substr(operand.size() - 4, operand.size()));
    const std::string operand_remainder = base64_decode_string(operand.substr(operand.size() - 4));
    return base_remainder.starts_with(operand_remainder);
}
