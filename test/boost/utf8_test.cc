/*
 * Copyright (c) 2018, Arm Limited and affiliates. All rights reserved.
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#define BOOST_TEST_MODULE core

#include <cstdint>
#include <vector>
#include <boost/test/unit_test.hpp>
#include <random>

#include "utils/utf8.hh"
#include "utils/fragmented_temporary_buffer.hh"

struct test_str {
   const void *data;
   size_t len;
   size_t bad_pos = 0;
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
     "\xC2\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00", 32, 15},
    {"\x00\x00\x00\x00\x00\xC2\xC2\x80\x00\x00\xE1\x80\x80\x00\x00\x00", 16, 5},
    {"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" \
     "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF1\x80", 32, 30},
    {"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" \
     "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF1", 32, 31},
    {"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" \
     "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF1\x80" \
     "\x80", 33, 30},
    {"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" \
     "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF1\x80" \
     "\xC2\x80", 34, 30},
    {"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" \
     "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF0" \
     "\x80\x80\x80", 35, 31},
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

BOOST_AUTO_TEST_CASE(test_utf8_fragmented) {
    auto random_engine = std::default_random_engine(std::random_device()());
    std::vector<int8_t> tmp;
    tmp.reserve(20000); // avoid reallocations
    std::optional<size_t> bad_pos;
    for (unsigned i = 0; i < 100000; ++i) {
        auto nr_positive_begin = std::uniform_int_distribution(0, 30)(random_engine);
        auto nr_negative = std::uniform_int_distribution(0, 1)(random_engine);
        auto nr_positive_end = std::uniform_int_distribution(0, 20)(random_engine);
        auto nr_frags = std::uniform_int_distribution(1, 4)(random_engine);
        tmp.clear();
        bad_pos.reset();
        auto random_test_str = [&] (const std::vector<test_str>& test_set) -> test_str {
            auto idx = std::uniform_int_distribution<size_t>(0, test_set.size() - 1)(random_engine);
            return test_set[idx];
        };
        auto add_test_str = [&] (test_str t) {
	    auto data = reinterpret_cast<const int8_t*>(t.data);
            tmp.insert(tmp.end(), data, data + t.len);
        };
        auto add_negative_test_str = [&] (test_str t) {
            if (!bad_pos) {
                bad_pos = tmp.size() + t.bad_pos;
            }
            add_test_str(t);
        };
        auto fragmentize = [&] (int nr_frags) -> fragmented_temporary_buffer {
            std::vector<temporary_buffer<char>> vec;
            std::vector<size_t> breakpoints;
            vec.reserve(nr_frags);
            breakpoints.reserve(nr_frags + 1);
            breakpoints.push_back(0);
            for (int i = 0; i < nr_frags - 1; ++i) {
                 breakpoints.push_back(std::uniform_int_distribution<size_t>(0, tmp.size())(random_engine));
            }
            breakpoints.push_back(tmp.size());
            std::sort(breakpoints.begin(), breakpoints.end());
            auto data = reinterpret_cast<const char*>(tmp.data());
            for (int i = 0; i < nr_frags; ++i) {
                 vec.push_back(temporary_buffer<char>(data + breakpoints[i], breakpoints[i+1] - breakpoints[i]));
            }
            return fragmented_temporary_buffer(std::move(vec), tmp.size());
        };
        for (int j = 0; j != nr_positive_begin; ++j) {
             add_test_str(random_test_str(positive));
        }
        for (int j = 0; j != nr_negative; ++j) {
             add_negative_test_str(random_test_str(negative));
        }
        for (int j = 0; j != nr_positive_end; ++j) {
             add_test_str(random_test_str(positive));
        }
        auto frag_buf = fragmentize(nr_frags);
        auto result = utils::utf8::validate_with_error_position_fragmented(fragmented_temporary_buffer::view(frag_buf));
        BOOST_REQUIRE(result == bad_pos);
    }
}
