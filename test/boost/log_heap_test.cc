/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>

#include "utils/assert.hh"
#include "utils/log_heap.hh"

template<const log_heap_options& opts>
struct node : public log_heap_hook<opts> {
    size_t v;
    node(size_t v) : v(v) {}
};

template<const log_heap_options& opts>
void test_with_options() {
    std::vector<std::unique_ptr<node<opts>>> nodes;

    log_heap<node<opts>, opts> hist;

    BOOST_REQUIRE(hist.empty());

    unsigned i = 0;
    for (; i < opts.min_size; ++i) {
        nodes.push_back(std::make_unique<node<opts>>(i));
        hist.push(*nodes.back());
        BOOST_REQUIRE(!hist.contains_above_min());
        BOOST_REQUIRE(!hist.empty());
    }

    for (; i <= opts.max_size; ++i) {
        nodes.push_back(std::make_unique<node<opts>>(i));
        hist.push(*nodes.back());
        BOOST_REQUIRE(hist.contains_above_min());
        BOOST_REQUIRE(!hist.empty());
    }

    size_t count = 0;

    // Check monotonicity of buckets
    size_t prev_key = 0;
    for (auto&& bucket : hist.buckets()) {
        size_t max_key = 0;
        for (auto&& t : bucket) {
            ++count;
            auto key = t.v;
            if (prev_key) {
                SCYLLA_ASSERT(key > prev_key);
            }
            max_key = std::max(max_key, key);
        }
        if (max_key) {
            prev_key = max_key;
        }
    }

    BOOST_REQUIRE(count == (opts.max_size + 1));
}

extern constexpr log_heap_options opts1{(1 << 4) + 3, 3, (1 << 6) + 2};
extern constexpr log_heap_options opts2{(1 << 4) + 2, 1, (1 << 17) + 2};
extern constexpr log_heap_options opts3{(1 << 4) + 1, 0, (1 << 17) + 2};
extern constexpr log_heap_options opts4{(1 << 4) + 0, 3, (1 << 17)};

template<>
size_t hist_key<node<opts1>>(const node<opts1>& n) { return n.v; }

template<>
size_t hist_key<node<opts2>>(const node<opts2>& n) { return n.v; }

template<>
size_t hist_key<node<opts3>>(const node<opts3>& n) { return n.v; }

template<>
size_t hist_key<node<opts4>>(const node<opts4>& n) { return n.v; }

BOOST_AUTO_TEST_CASE(test_log_heap) {
    test_with_options<opts1>();
    test_with_options<opts2>();
    test_with_options<opts3>();
    test_with_options<opts4>();
}
