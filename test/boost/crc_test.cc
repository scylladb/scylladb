/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include "utils/crc.hh"
#include "utils/clmul.hh"
#include "utils/gz/barrett.hh"
#include <seastar/core/print.hh>

constexpr uint32_t input_32_1_c = 0x12345678;
uint32_t input_32_1 = input_32_1_c; // NOT constexpr

constexpr uint32_t input_32_2_c = 0xabcdef12;
uint32_t input_32_2 = input_32_2_c; // NOT constexpr

constexpr uint64_t input_64_1_c = 0x1234567890abcdef;
uint64_t input_64_1 = input_64_1_c; // NOT constexpr

BOOST_AUTO_TEST_CASE(clmul_u32_constexpr_equals_native) {
    constexpr auto constexpr_result = clmul(input_32_1_c, input_32_2_c);
    BOOST_REQUIRE_EQUAL(clmul(input_32_1, input_32_2), constexpr_result);
}

BOOST_AUTO_TEST_CASE(barrett_fold_constexpr_equals_native) {
    constexpr auto constexpr_result = crc32_fold_barrett_u64(input_64_1_c);
    BOOST_REQUIRE_EQUAL(crc32_fold_barrett_u64(input_64_1), constexpr_result);
}
inline
uint32_t
do_compute_crc(utils::crc32& c) {
    return c.get();
}

template <typename T, typename... Rest>
inline
uint32_t
do_compute_crc(utils::crc32& c, const T& val, const Rest&... rest) {
    c.process_le(val);
    return do_compute_crc(c, rest...);
}

template <typename... T>
inline
uint32_t
compute_crc(const T&... vals) {
    utils::crc32 c;
    return do_compute_crc(c, vals...);
}

BOOST_AUTO_TEST_CASE(crc_1_vs_4) {
    using b = uint8_t;
    BOOST_REQUIRE_EQUAL(compute_crc(0x01020304), compute_crc(b(4), b(3), b(2), b(1)));
}

BOOST_AUTO_TEST_CASE(crc_121_vs_4) {
    using b = uint8_t;
    using w = uint16_t;
    BOOST_REQUIRE_EQUAL(compute_crc(0x01020304), compute_crc(b(4), w(0x0203), b(1)));
}

BOOST_AUTO_TEST_CASE(crc_44_vs_8) {
    using q = uint64_t;
    BOOST_REQUIRE_EQUAL(compute_crc(q(0x0102030405060708)), compute_crc(0x05060708, 0x01020304));
}

// Test crc32::process()
BOOST_AUTO_TEST_CASE(crc_process) {
    const size_t max_size = 7 + 4096 + 31;  // cover all code path
    const size_t test_sizes[] = {
        0, 1, 8, 9, 1023, 1024, 1025, 1032, 1033, 4095, 4096, 4097, max_size
    };

    // Create data buffer offset 8 bytes boundary by 1 byte
    uint64_t data64[max_size/8 + 2];
    uint8_t *data = (reinterpret_cast<uint8_t*>(data64)) + 1;

    // Fill data with test pattern
    for (size_t i = 0; i < max_size; ++i) {
        data[i] = i + 1;
    }

    for (size_t size : test_sizes) {
        utils::crc32 c1, c2;

        // Get correct answer with simplest method
        for (size_t i = 0; i < size; ++i) {
            c1.process_le(data[i]);
        }

        // Calculate crc by optimized routine
        c2.process(data, size);

        BOOST_REQUIRE_EQUAL(c1.get(), c2.get());
    }
}
