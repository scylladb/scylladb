/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#define BOOST_TEST_MODULE utils

#include <iterator>
#include <boost/test/unit_test.hpp>
#include "utils/pretty_printers.hh"

BOOST_AUTO_TEST_CASE(test_print_data_size_SI) {
    struct {
        size_t n;
        std::string_view formatted;
    } sizes[] = {
        {0ULL, "0 bytes"},
        {1ULL, "1 byte"},
        {42ULL, "42 bytes"},
        {9'000ULL, "9000 bytes"},
        {10'000ULL, "10kB"},
        {10'001ULL, "10kB"},
        {10'000'000ULL, "10MB"},
        {10'000'000'000ULL, "10GB"},
        {10'000'000'000'000ULL, "10TB"},
        {10'000'000'000'000'000ULL, "10PB"},
        {10'000'000'000'000'000'000ULL, "10000PB"},
    };
    for (auto [n, expected] : sizes) {
        std::string actual;
        fmt::format_to(std::back_inserter(actual), "{}", utils::pretty_printed_data_size{n});
        BOOST_CHECK_EQUAL(actual, expected);
    }
}

BOOST_AUTO_TEST_CASE(test_print_data_size_IEC) {
    struct {
        size_t n;
        std::string_view formatted;
    } sizes[] = {
        {0ULL, "0 bytes"},
        {1ULL, "1 byte"},
        {42ULL, "42 bytes"},
        {8'191LL, "8191 bytes"},
        {8'192LL, "8KiB"},
        {8'193LL, "8KiB"},
        {10'000ULL, "9KiB"},
    };
    for (auto [n, expected] : sizes) {
        std::string actual;
        fmt::format_to(std::back_inserter(actual), "{:i}", utils::pretty_printed_data_size{n});
        BOOST_CHECK_EQUAL(actual, expected);
    }
}

BOOST_AUTO_TEST_CASE(test_print_data_size_IEC_SANS_I) {
    struct {
        size_t n;
        std::string_view formatted;
    } sizes[] = {
        {0ULL, "0B"},
        {1ULL, "1B"},
        {42ULL, "42B"},
        {8'192LL, "8K"},
    };
    for (auto [n, expected] : sizes) {
        std::string actual;
        fmt::format_to(std::back_inserter(actual), "{:I}", utils::pretty_printed_data_size{n});
        BOOST_CHECK_EQUAL(actual, expected);
    }
}

BOOST_AUTO_TEST_CASE(test_print_throughput) {
    struct {
        size_t n;
        float seconds;
        std::string_view formatted;
    } sizes[] = {
        {0ULL, 0.F, "0 bytes/s"},
        {42ULL, 0.F, "0 bytes/s"},
        {42ULL, 1.F, "42 bytes/s"},
        {42ULL, 0.5F, "84 bytes/s"},
        {10'000'000ULL, 1.F, "10MB/s"},
        {10'000'000ULL, 0.5F, "20MB/s"},
    };
    for (auto [n, seconds, expected] : sizes) {
        std::string actual;
        fmt::format_to(std::back_inserter(actual), "{}", utils::pretty_printed_throughput{n, std::chrono::duration<float>(seconds)});
        BOOST_CHECK_EQUAL(actual, expected);
    }
}
