/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include <boost/range/algorithm/copy.hpp>
#include "cartesian_product.hh"

template<typename Range>
auto to_vec(Range&& r) {
    std::vector<typename Range::iterator::value_type> v;
    std::copy(std::begin(r), std::end(r), std::back_inserter(v));
    return v;
}

BOOST_AUTO_TEST_CASE(test_cartesian_product) {
    using vec = std::vector<std::vector<int>>;
    BOOST_REQUIRE(to_vec(make_cartesian_product(vec({{1, 2}, {3}, {4, 5}}))) == vec({
        {1, 3, 4},
        {1, 3, 5},
        {2, 3, 4},
        {2, 3, 5}
    }));
}
