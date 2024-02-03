/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include "utils/histogram.hh"
#include <algorithm>
#include <sstream>

using duration = utils::time_estimated_histogram::duration;

std::string vector_to_string(const std::vector<double>& v) {
    std::stringstream ss;
    ss << "{";
    std::copy(v.begin(), v.end(),
        std::ostream_iterator<uint64_t>(ss, ","));
    ss << "}";
    return ss.str();
}

void fill_range(utils::summary_calculator& summary, size_t count, const duration& from, const duration& to) {
    auto delta = (to-from)/count;
    for (size_t i = 0; i < count; i++) {
        summary.mark(from + i*delta);
    }
}

BOOST_AUTO_TEST_CASE(test_summary_api) {
    utils::summary_calculator summary;
    BOOST_CHECK_EQUAL(vector_to_string(summary.summary()), vector_to_string({0,0,0}));
    BOOST_CHECK_EQUAL(vector_to_string(summary.quantiles()), vector_to_string({0.5,0.95,0.99}));
    summary.set_quantiles({0.7, 0.8});
    BOOST_CHECK_EQUAL(vector_to_string(summary.summary()), vector_to_string({0,0}));
    BOOST_CHECK_EQUAL(vector_to_string(summary.quantiles()), vector_to_string({0.7, 0.8}));
    summary.mark(std::chrono::microseconds(520));
    summary.mark(std::chrono::microseconds(770));
    BOOST_CHECK_EQUAL(summary.histogram().count(), 2);
    BOOST_CHECK_EQUAL(summary.histogram().get(0), 1);
    BOOST_CHECK_EQUAL(summary.histogram().get(2), 1);
}

BOOST_AUTO_TEST_CASE(test_summary_calculation) {
    utils::summary_calculator summary;
    fill_range(summary, 1000, std::chrono::microseconds(520), std::chrono::seconds(1));
    fill_range(summary, 900, std::chrono::seconds(1), std::chrono::seconds(2));
    fill_range(summary, 50, std::chrono::seconds(2), std::chrono::seconds(3));
    fill_range(summary, 40, std::chrono::seconds(3), std::chrono::seconds(4));
    fill_range(summary, 10, std::chrono::seconds(4), std::chrono::seconds(8));
    BOOST_CHECK_EQUAL(summary.histogram().count(), 2000);
    summary.update();
    BOOST_CHECK_EQUAL(vector_to_string(summary.summary()), vector_to_string({1048576, 2097152, 4194304}));
    summary.update();
    BOOST_CHECK_EQUAL(vector_to_string(summary.summary()), vector_to_string({0,0,0}));
}
