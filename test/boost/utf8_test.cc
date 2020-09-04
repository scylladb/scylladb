/*
 * Copyright (c) 2018, Arm Limited and affiliates. All rights reserved.
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

#define BOOST_TEST_MODULE core

#include <cstdint>
#include <vector>
#include <boost/test/unit_test.hpp>

#include "utils/utf8.hh"

struct test_str {
   const void *data;
   size_t len;
};

// Positive strings
static const std::vector<test_str> positive = {
    {"", 0},
    {"\x00", 1},
    {"\x66", 1},
    {"\x7F", 1},
    {"\x00\x7F", 2},
    {"\x7F\x00", 2},
    {"\xC2\x80", 2},
    {"\xDF\xBF", 2},
    {"\xE0\xA0\x80", 3},
    {"\xE0\xA0\xBF", 3},
    {"\xED\x9F\x80", 3},
    {"\xEF\x80\xBF", 3},
    {"\xF0\x90\xBF\x80", 4},
    {"\xF2\x81\xBE\x99", 4},
    {"\xF4\x8F\x88\xAA", 4},
};

// Negative strings
static const std::vector<test_str> negative = {
    {"\x80", 1},
    {"\xBF", 1},
    {"\xC0\x80", 2},
    {"\xC1\x00", 2},
    {"\xC2\x7F", 2},
    {"\xDF\xC0", 2},
    {"\xE0\x9F\x80", 3},
    {"\xE0\xC2\x80", 3},
    {"\xED\xA0\x80", 3},
    {"\xED\x7F\x80", 3},
    {"\xEF\x80\x00", 3},
    {"\xF0\x8F\x80\x80", 4},
    {"\xF0\xEE\x80\x80", 4},
    {"\xF2\x90\x91\x7F", 4},
    {"\xF4\x90\x88\xAA", 4},
    {"\xF4\x00\xBF\xBF", 4},
    {"\x00\x00\x00\x00\x00\xC2\x80\x00\x00\x00\xE1\x80\x80\x00\x00\xC2" \
     "\xC2\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00", 32},
    {"\x00\x00\x00\x00\x00\xC2\xC2\x80\x00\x00\xE1\x80\x80\x00\x00\x00", 16},
    {"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" \
     "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF1\x80", 32},
    {"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" \
     "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF1", 32},
    {"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" \
     "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF1\x80" \
     "\x80", 33},
    {"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" \
     "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF1\x80" \
     "\xC2\x80", 34},
    {"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" \
     "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF0" \
     "\x80\x80\x80", 35},
};

// Round concatenate positive test strings to 1024 bytes
static void prepare_test_buf(uint8_t *buf, size_t pos_idx) {
    int buf_idx = 0;

    while (buf_idx < 1024) {
        size_t buf_len = 1024 - buf_idx;

        if (buf_len >= positive[pos_idx].len) {
            memcpy(buf+buf_idx, positive[pos_idx].data, positive[pos_idx].len);
            buf_idx += positive[pos_idx].len;
        } else {
            // Fill remaining buffer with 0
            memset(buf+buf_idx, 0, buf_len);
            buf_idx += buf_len;
        }

        if (++pos_idx == positive.size()) {
            pos_idx = 0;
        }
    }
}

BOOST_AUTO_TEST_CASE(test_utf8_positive) {
    // Test single positive string
    for (auto &test : positive) {
        BOOST_CHECK(utils::utf8::validate((const uint8_t*)test.data, test.len));
    }

    const int max_size = 1024 + 32;
    uint64_t buf64[max_size/8 + 2];
    // Unalign buffer address: offset 8 bytes boundary by 1 byte
    uint8_t *buf = (reinterpret_cast<uint8_t*>(buf64)) + 1;

    // Test concatenated and shifted positive strings to cover 1k length
    for (size_t i = 0; i < positive.size(); ++i) {
        // Round concatenate strings staring from i-th positive string
        size_t buf_len = 1024;
        prepare_test_buf(buf, i);

        // Shift 16 bytes, validate each shift
        for (int j = 0; j < 16; ++j) {
            BOOST_CHECK(utils::utf8::validate(buf, buf_len));
            for (int k = buf_len; k >= 1; --k)
                buf[k] = buf[k-1];
            buf[0] = '\x55';
            ++buf_len;
        }
    }
}

BOOST_AUTO_TEST_CASE(test_utf8_negative) {
    // Test single negative string
    for (auto &test : negative) {
        BOOST_CHECK(!utils::utf8::validate((const uint8_t*)test.data, test.len));
    }

    // Must be larger than 1024 + 16 + max(negative string length)
    uint8_t buf[1024*2];

    for (size_t i = 0; i < negative.size(); ++i) {
        prepare_test_buf(buf, i % positive.size());

        // Append one error string
        memcpy(buf+1024, negative[i].data, negative[i].len);
        size_t buf_len = 1024 + negative[i].len;

        // Shift 16 bytes, validate each shift
        for (int j = 0; j < 16; ++j) {
            BOOST_CHECK(!utils::utf8::validate(buf, buf_len));
            for (int k = buf_len; k >= 1; --k)
                buf[k] = buf[k-1];
            buf[0] = '\x66';
            ++buf_len;
        }
    }
}

BOOST_AUTO_TEST_CASE(test_utf8_position) {
    auto test_string = [](const char* str, std::optional<size_t> expected) {
        BOOST_CHECK(utils::utf8::validate_with_error_position(reinterpret_cast<const uint8_t*>(str), strlen(str)) == expected);
    };

    test_string("valid string", std::nullopt);
    test_string("ab\xc3\x28 xx", 2);
    test_string("abc\xe2\x82\x28", 3);
    test_string("abcd\xf0\x28\x8c\x28", 4);
    test_string("abcd\xc3\x28", 4);
}
