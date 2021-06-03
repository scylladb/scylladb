/*
 * Copyright (C) 2020-present ScyllaDB
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

#include <seastar/testing/thread_test_case.hh>
#include "utils/stall_free.hh"

SEASTAR_THREAD_TEST_CASE(test_merge1) {
    std::list<int> l1{1, 2, 5, 8};
    std::list<int> l2{3};
    std::list<int> expected{1,2,3,5,8};
    utils::merge_to_gently(l1, l2, std::less<int>());
    BOOST_CHECK(l1 == expected);
}

SEASTAR_THREAD_TEST_CASE(test_merge2) {
    std::list<int> l1{1};
    std::list<int> l2{3, 5, 6};
    std::list<int> expected{1,3,5,6};
    utils::merge_to_gently(l1, l2, std::less<int>());
    BOOST_CHECK(l1 == expected);
}

SEASTAR_THREAD_TEST_CASE(test_merge3) {
    std::list<int> l1{};
    std::list<int> l2{3, 5, 6};
    std::list<int> expected{3,5,6};
    utils::merge_to_gently(l1, l2, std::less<int>());
    BOOST_CHECK(l1 == expected);
}

SEASTAR_THREAD_TEST_CASE(test_merge4) {
    std::list<int> l1{1};
    std::list<int> l2{};
    std::list<int> expected{1};
    utils::merge_to_gently(l1, l2, std::less<int>());
    BOOST_CHECK(l1 == expected);
}
