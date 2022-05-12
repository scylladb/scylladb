/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include "utils/streaming_histogram.hh"
#include <seastar/core/print.hh>
#include <map>
#include <cmath>

BOOST_AUTO_TEST_CASE(basic_streaming_histogram_test) {
    utils::streaming_histogram hist(5);
    long samples[] = {23, 19, 10, 16, 36, 2, 9, 32, 30, 45};

    // add 7 points to histogram of 5 bins
    for (int i = 0; i < 7; i++) {
        hist.update(samples[i]);
    }

    // should end up (2,1),(9.5,2),(17.5,2),(23,1),(36,1)
    std::map<double, uint64_t> expected1;
    expected1.emplace(2.0, 1L);
    expected1.emplace(9.5, 2L);
    expected1.emplace(17.5, 2L);
    expected1.emplace(23.0, 1L);
    expected1.emplace(36.0, 1L);

    auto expected_itr = expected1.begin();
    for (auto& actual : hist.bin) {
        auto entry = expected_itr++;
        // Asserts that two keys are equal to within a positive delta
        BOOST_REQUIRE(std::fabs(entry->first - actual.first) <= 0.01);
        BOOST_REQUIRE_EQUAL(entry->second, actual.second);
    }

    // merge test
    utils::streaming_histogram hist2(3);
    for (int i = 7; i < int(sizeof(samples)/sizeof(samples[0])); i++) {
        hist2.update(samples[i]);
    }
    hist.merge(hist2);
    // should end up (2,1),(9.5,2),(19.33,3),(32.67,3),(45,1)
    std::map<double, uint64_t> expected2;
    expected2.emplace(2.0, 1L);
    expected2.emplace(9.5, 2L);
    expected2.emplace(19.33, 3L);
    expected2.emplace(32.67, 3L);
    expected2.emplace(45.0, 1L);
    expected_itr = expected2.begin();
    for (auto& actual : hist.bin) {
        auto entry = expected_itr++;
        // Asserts that two keys are equal to within a positive delta
        BOOST_REQUIRE(std::fabs(entry->first - actual.first) <= 0.01);
        BOOST_REQUIRE_EQUAL(entry->second, actual.second);
    }


    // sum test
    BOOST_REQUIRE(std::fabs(3.28 - hist.sum(15)) <= 0.01);
    // sum test (b > max(hist))
    BOOST_REQUIRE(std::fabs(10.0 - hist.sum(50)) <= 0.01);
}

