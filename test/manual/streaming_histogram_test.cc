/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2017-present ScyllaDB
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

