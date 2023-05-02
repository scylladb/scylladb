/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#define BOOST_TEST_MODULE alternator
#include <boost/test/included/unit_test.hpp>

#include <seastar/util/defer.hh>
#include <seastar/core/memory.hh>
#include "utils/base64.hh"
#include "utils/rjson.hh"
#include "alternator/serialization.hh"

static std::map<std::string, std::string> strings {
    {"", ""},
    {"a", "YQ=="},
    {"ab", "YWI="},
    {"abc", "YWJj"},
    {"abcd", "YWJjZA=="},
    {"abcde", "YWJjZGU="},
    {"abcdef", "YWJjZGVm"},
    {"abcdefg", "YWJjZGVmZw=="},
    {"abcdefgh", "YWJjZGVmZ2g="},
};

BOOST_AUTO_TEST_CASE(test_base64_encode_decode) {
    for (auto& [str, encoded] : strings) {
        BOOST_REQUIRE_EQUAL(base64_encode(to_bytes_view(str)), encoded);
        auto decoded = base64_decode(encoded);
        BOOST_REQUIRE_EQUAL(to_bytes_view(str), bytes_view(decoded));
    }
}

BOOST_AUTO_TEST_CASE(test_base64_decoded_len) {
    for (auto& [str, encoded] : strings) {
        BOOST_REQUIRE_EQUAL(str.size(), base64_decoded_len(encoded));
    }
}

BOOST_AUTO_TEST_CASE(test_base64_begins_with) {
    for (auto& [str, encoded] : strings) {
        for (size_t i = 0; i < str.size(); ++i) {
            std::string prefix(str.c_str(), i);
            std::string encoded_prefix = base64_encode(to_bytes_view(prefix));
            BOOST_REQUIRE(base64_begins_with(encoded, encoded_prefix));
        }
    }
    std::string str1 = "ABCDEFGHIJKL123456";
    std::string str2 = "ABCDEFGHIJKL1234567";
    std::string str3 = "ABCDEFGHIJKL12345678";
    std::string encoded_str1 = base64_encode(to_bytes_view(str1));
    std::string encoded_str2 = base64_encode(to_bytes_view(str2));
    std::string encoded_str3 = base64_encode(to_bytes_view(str3));
    std::vector<std::string> non_prefixes = {
        "B", "AC", "ABD", "ACD", "ABCE", "ABCEG", "ABCDEFGHIJKLM", "ABCDEFGHIJKL123456789"
    };
    for (auto& non_prefix : non_prefixes) {
        std::string encoded_non_prefix = base64_encode(to_bytes_view(non_prefix));
        BOOST_REQUIRE(!base64_begins_with(encoded_str1, encoded_non_prefix));
        BOOST_REQUIRE(!base64_begins_with(encoded_str2, encoded_non_prefix));
        BOOST_REQUIRE(!base64_begins_with(encoded_str3, encoded_non_prefix));
    }
}

BOOST_AUTO_TEST_CASE(test_allocator_fail_gracefully) {
    // Allocation size is set to a ridiculously high value to ensure
    // that it will immediately fail - trying to lazily allocate just
    // a little more than total memory may still succeed.
    static size_t too_large_alloc_size = memory::stats().total_memory() * 1024 * 1024;
    rjson::allocator allocator;
    // Impossible allocation should throw
    BOOST_REQUIRE_THROW(allocator.Malloc(too_large_alloc_size), rjson::error);
    // So should impossible reallocation
    void* memory = allocator.Malloc(1);
    auto release = defer([memory] { rjson::allocator::Free(memory); });
    BOOST_REQUIRE_THROW(allocator.Realloc(memory, 1, too_large_alloc_size), rjson::error);
    // Internal rapidjson stack should also throw
    // and also be destroyed gracefully later
    rapidjson::internal::Stack stack(&allocator, 0);
    BOOST_REQUIRE_THROW(stack.Push<char>(too_large_alloc_size), rjson::error);
}

// Test the alternator::internal::magnitude_and_precision() function which we
// use to used to check if a number exceeds DynamoDB's limits on magnitude and
// precision (for issue #6794). This just tests the internal implementation -
// we also have end-to-end tests trying to insert various numbers with bad
// magnitude and precision to the database in test/alternator/test_number.py.
BOOST_AUTO_TEST_CASE(test_magnitude_and_precision) {
    struct expected {
        const char* number;
        int magnitude;
        int precision;
    };
    std::vector<expected> tests = {
        // number     magnitude, precision
        {"0",         0, 0},
        {"0e10",      0, 0},
        {"0e-10",     0, 0},
        {"0e+10",     0, 0},
        {"0.0",       0, 0},
        {"0.00e10",   0, 0},
        {"1",         0, 1},
        {"12.",       1, 2},
        {"1.1",       0, 2},
        {"12.3",      1, 3},
        {"12.300",    1, 3},
        {"0.3",       -1, 1},
        {".3",        -1, 1},
        {"3e-1",      -1, 1},
        {"0.00012",   -4, 2},
        {"1.2e-4",    -4, 2},
        {"1.2E-4",    -4, 2},
        {"12.345e50", 51, 5},
        {"12.345e-50",-49, 5},
        {"123000000", 8, 3},
        {"123000000.000e+5", 13, 3},
        {"10.01",     1, 4},
        {"1.001e1",   1, 4},
        {"1e5",       5, 1},
        {"1e+5",      5, 1},
        {"1e-5",      -5, 1},
        {"123e-7",    -5, 3},
        // These are important edge cases: DynamoDB considers 1e126 to be
        // overflowing but 9.9999e125 is considered to have magnitude 125
        // and ok. Conversely, 1e-131 is underflowing and 0.9e-130 is too.
        {"9.99999e125", 125, 6},
        {"0.99999e-130", -131, 5},
        {"0.9e-130", -131, 1},
        // Although 1e1000 is not allowed, 0e0000 is allowed - it's just 0.
        {"0e1000",    0, 0},
    };
    // prefixes that should do nothing to a number
    std::vector<std::string> prefixes = {
        "",
        "0",
        "+",
        "-",
        "+0000",
        "-0000"
    };
    for (expected test : tests) {
        for (std::string prefix : prefixes) {
            std::string number = prefix + test.number;
            auto res = alternator::internal::get_magnitude_and_precision(number);
            BOOST_CHECK_MESSAGE(res.magnitude == test.magnitude,
                format("{}: expected magnitude {}, got {}", number, test.magnitude, res.magnitude));
            BOOST_CHECK_MESSAGE(res.precision == test.precision,
                format("{}: expected precision {}, got {}", number, test.precision, res.precision));
        }
    }
    // Huge exponents like 1e1000000 are not guaranteed to return that
    // specific number as magnitude, but is guaranteed to return some
    // other high magnitude that the caller can complain is excessive.
    auto res = alternator::internal::get_magnitude_and_precision("1e1000000");
    BOOST_CHECK(res.magnitude > 1000);
    res = alternator::internal::get_magnitude_and_precision("1e-1000000");
    BOOST_CHECK(res.magnitude < -1000);
    // Even if an exponent so huge that it doesn't even fit in a 32-bit
    // integer, we shouldn't fail to recognize its excessive magnitude:
    res = alternator::internal::get_magnitude_and_precision("1e1000000000000");
    BOOST_CHECK(res.magnitude > 1000);
    res = alternator::internal::get_magnitude_and_precision("1e-1000000000000");
    BOOST_CHECK(res.magnitude < -1000);
}