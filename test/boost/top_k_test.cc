/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include "utils/top_k.hh"
#include "test/lib/test_utils.hh"
#include <fmt/ranges.h>
#include <vector>
#include <algorithm>
#include <optional>

//---------------------------------------------------------------------------------------------

using top_k_t = utils::space_saving_top_k<unsigned>;
using results_t = top_k_t::results;

// a permutation of 1..17
std::vector<unsigned> freq{13, 8, 12, 4, 11, 2, 15, 1, 5, 3, 16, 7, 6, 9, 14, 10, 17};

// expected results
std::vector<unsigned> exp_results(unsigned k = 10) {
    auto v = freq;
    std::sort(v.begin(), v.end(), std::greater<unsigned>());
    v.resize(k);
    return v;
}


std::vector<unsigned> count(const utils::space_saving_top_k<unsigned>::results& res) {
    std::vector<unsigned> v;
    for (auto& c : res) {
        v.push_back(c.count);
    }
    return v;
}

//---------------------------------------------------------------------------------------------

// items are inserted by their index as key, with frequencies given in freq
BOOST_AUTO_TEST_CASE(test_top_k_straight_insertion) {
    top_k_t top(32);

    for (unsigned i = 0; i < freq.size(); ++i) {
        for (unsigned j = 0; j < freq[i]; ++j) {
            top.append(i);
        }
    }

    std::vector<unsigned> res = count(top.top(10));
    BOOST_REQUIRE_EQUAL(res, exp_results());
}

//---------------------------------------------------------------------------------------------

// items inserted by interleaved index order, with frequencies given in freq
BOOST_AUTO_TEST_CASE(test_top_k_interleaved_insertion) {
    top_k_t top(32);
    auto f1 = freq;

    bool all_0;
    do {
        all_0 = true;
        for (unsigned i = 0; i < f1.size(); ++i) {
            if (f1[i] > 0) {
                --f1[i];
                top.append(i);
                all_0 = false;
            }
        }
    } while (!all_0);

    auto res{count(top.top(10))};
    BOOST_REQUIRE_EQUAL(res, exp_results());
}

//---------------------------------------------------------------------------------------------

// items inserted with their frequencies as weights
BOOST_AUTO_TEST_CASE(test_top_k_bulk_insertion) {
    top_k_t top(32);

    for (unsigned i = 0; i < freq.size(); ++i) {
        top.append(i, freq[i]);
    }

    auto res{count(top.top(10))};
    BOOST_REQUIRE_EQUAL(res, exp_results());
}

//---------------------------------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(test_top_k_single_value) {
    top_k_t top(32);

    for (unsigned i = 0; i < 100; ++i) {
        top.append(1);
    }

    auto res{count(top.top(10))};
    BOOST_REQUIRE_EQUAL(res, (std::vector<unsigned>{100}));
}

//---------------------------------------------------------------------------------------------

struct bad_boy {
    unsigned n;
    bad_boy(unsigned n) : n(n) {}

    bad_boy(const bad_boy& bb) noexcept = default;

    bool operator==(const bad_boy& x) const {
        static unsigned zz = 0;
        if (++zz == 100) {
            throw "explode";
        }
        return n == x.n;
    }

    operator unsigned() const { return n; }
};

namespace std {
template<>
struct hash<bad_boy>
{
    size_t operator()(const bad_boy& x) const { return hash<unsigned>()(x.n); }
};
}

BOOST_AUTO_TEST_CASE(test_top_k_fail) {
    utils::space_saving_top_k<bad_boy> top(32);

    try {
        for (unsigned i = 0; i < 100; ++i) {
            top.append_return_all(bad_boy{1});
        }
        BOOST_FAIL("expected error");
    } catch (...) {
        BOOST_REQUIRE_EQUAL(top.valid(), false);
    }

    BOOST_CHECK_THROW(top.top(10), std::runtime_error);
    BOOST_CHECK_THROW(top.size(), std::runtime_error);
}

//---------------------------------------------------------------------------------------------
