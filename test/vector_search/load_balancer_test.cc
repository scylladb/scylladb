/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "vector_search/load_balancer.hh"
#include <seastar/testing/test_case.hh>
#include <random>

using namespace seastar;
using namespace vector_search;

BOOST_AUTO_TEST_CASE(next_returns_nullptr_on_empty_container) {
    std::mt19937 seeded_engine(0);
    load_balancer lb{std::vector<lw_shared_ptr<int>>{}, seeded_engine};

    BOOST_CHECK(lb.next() == nullptr);
}

BOOST_AUTO_TEST_CASE(next_returns_all_elements_in_random_order) {
    std::mt19937 seeded_engine(0);
    std::vector<lw_shared_ptr<int>> read;
    load_balancer lb{std::vector<lw_shared_ptr<int>>{make_lw_shared(1), make_lw_shared(2), make_lw_shared(3)}, seeded_engine};

    while (auto n = lb.next()) {
        read.push_back(n);
    }

    BOOST_CHECK_EQUAL(read.size(), 3);
    BOOST_CHECK_EQUAL(*read[0], 2);
    BOOST_CHECK_EQUAL(*read[1], 3);
    BOOST_CHECK_EQUAL(*read[2], 1);
}
