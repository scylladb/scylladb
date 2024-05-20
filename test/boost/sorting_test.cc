/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/testing/test_case.hh>
#include "test/lib/log.hh"
#include "utils/sorting.hh"

struct sorting_test_data {
    std::multimap<int, int> adjacent_map;
    std::vector<int> expected_result;
    bool expect_error = false;
};

SEASTAR_TEST_CASE(test_topological_sorting) {
    return seastar::async([] {
        std::vector<int> all_nodes = {1, 2, 3, 4, 5};
        std::vector<sorting_test_data> tests = {
            // valid directed acyclic graphs
            {
                {}, 
                {1, 2, 3, 4, 5},
            },
            {
                {{1, 2}, {2, 3}, {3, 4}, {4, 5}}, 
                {1, 2, 3, 4, 5},
            },
            {
                {{5, 4}, {4, 3}, {3, 2}, {2, 1}}, 
                {5, 4, 3, 2, 1},
            },
            {
                {{1, 2}, {1, 3}, {1, 4}, {1, 5}, {2, 3}, {2, 4}, {2, 5}, {3, 4}, {3, 5}, {4, 5}}, 
                {1, 2, 3, 4, 5},
            },
            {
                {{1, 2}, {2, 3}, {1, 4}, {4, 5}, {5, 2}}, 
                {1, 4, 5, 2, 3},
            },
            {
                {{1, 2}, {1, 4}, {2, 4}, {4, 3}, {4, 5}, {5, 3}}, 
                {1, 2, 4, 5, 3},
            },
            {
                {{2, 1}, {1, 4}, {3, 5}, {5, 4}}, 
                {2, 3, 1, 5, 4},
            },
            {
                {{4, 1}, {3, 2}}, 
                {3, 4, 5, 2, 1}
            },
            // directed graphs with cycles
            {
                {{1, 2}, {2, 3}, {3, 1}}, 
                {},
                true
            },
            {
                {{1, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 1}}, 
                {},
                true
            },
            {
                {{4, 1}, {3, 2}, {3, 4}, {1, 5}, {5, 3}}, 
                {},
                true
            },
            {
                {{1, 2}, {1, 4}, {2, 4}, {4, 3}, {4, 5}, {5, 3}, {3, 1}}, 
                {},
                true
            },
        };

        for (size_t i = 0; i < tests.size(); i++) {
            testlog.info("Testing case index: {}", i);
            if (tests[i].expect_error) {
                BOOST_REQUIRE_THROW(utils::topological_sort(all_nodes, tests[i].adjacent_map).get(), std::runtime_error);
            } else {
                auto result = utils::topological_sort(all_nodes, tests[i].adjacent_map).get();
                BOOST_REQUIRE_EQUAL_COLLECTIONS(result.begin(), result.end(), tests[i].expected_result.begin(), tests[i].expected_result.end());
            }
        }
    });
}
