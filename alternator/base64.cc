/*
 * Copyright 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

// The DynamoAPI dictates that "binary" (a.k.a. "bytes" or "blob") values
// be encoded in the JSON API as base64-encoded strings. This is code to
// convert byte arrays to base64-encoded strings, and back.

#include "base64.hh"

#include <ctype.h>


// Arrays for quickly converting to and from an integer between 0 and 63,
// and the character used in base64 encoding to represent it.
static class base64_chars {
public:
    static constexpr const char* to =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    int8_t from[255];
    base64_chars() {
        static_assert(strlen(to) == 64);
        for (int i = 0; i < 255; i++) {
            from[i] = 255; // signal invalid character
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

bytes base64_decode(std::string_view in) {
    int i = 0;
    int8_t chunk4[4]; // chunk of input, each byte converted to 0..63;
    std::string ret;
    ret.reserve(in.size() * 3 / 4);
    for (unsigned char c : in) {
        uint8_t dc = base64_chars.from[c];
        if (dc == 255) {
            // Any unexpected character, include the "=" character usually
            // used for padding, signals the end of the decode.
            break;
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
    // FIXME: This copy is sad. The problem is we need back "bytes"
    // but "bytes" doesn't have efficient append and std::string.
    // To fix this we need to use bytes' "uninitialized" feature.
    return bytes(ret.begin(), ret.end());
}
