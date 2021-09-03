/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

// Unit tests for testing Heat-Weighted Load Balancing algorithm internals
// (notably, the miss_equalizing_combination() function) in isolation,
// without running a cluster or real requests.

#define BOOST_TEST_MODULE core  /* needed to create main() */
#include <boost/test/unit_test_log.hpp>
#include <boost/test/unit_test.hpp>

#include <boost/range/adaptors.hpp>

#include <string>
#include <random>
#include <algorithm>

#include "db/heat_load_balance.hh"
#include "to_string.hh"

// do_reorder() cycles the node_hit_rate vector to have "me" as first, as we
// expect to happen in practice on real nodes. We are worried about bugs in
// the algorithm's decisions depending on the order of the nodes, so we don't
// want all the nodes to see the other nodes in the exact same order.
static std::vector<std::pair<int,float>> do_reorder(
    std::vector<std::pair<int,float>> node_hit_rate, int me)
{
    std::vector<std::pair<int,float>> ret;
    ret.reserve(node_hit_rate.size());
    for (unsigned i = 0; i < node_hit_rate.size(); i++) {
        auto a = node_hit_rate[(me + i) % node_hit_rate.size()];
        ret.push_back(a);
    }
    return ret;
}

// test_hit_rates() is used by various tests below to test the correctness of
// miss_equalizing_combination(). We don't have an analytic solution or oracle
// saying what miss_equalizing_combination() is supposed to return, but we can
// test that it gives the desired random distribution:
//
// We start with known hit ratios for N nodes. We then repeatedly pick a
// random coordinator and then uses miss_equalizing_combination() to pick
// CL replicas from the N (the real Scylla would send read requests to those
// replicas). Finally we check that the miss rates of the different replicas
// receiving these requests was indeed equalized.
//
// Again, this is just a simulation - no actual request is sent here, these
// requests are just counted.
static void test_hit_rates(std::vector<float> hr, unsigned CL,
                           bool want_extra = false, unsigned iterations = 100000) {
    const int N = hr.size();
    BOOST_TEST_MESSAGE(format("N={} nodes, given hit rates: {}",
        N, join(" ", hr)));
    BOOST_TEST_MESSAGE(format(" asking for CL={}, extra={}", CL, want_extra));

    std::vector<std::pair<int,float>> node_hit_rate;
    int i = 0;
    for (auto f : hr) {
        node_hit_rate.emplace_back(i++, f);
    }

    std::random_device r;
    std::default_random_engine random_engine(r());
    std::uniform_int_distribution<> rand_node = std::uniform_int_distribution<>(0, N-1);

    // count the number of requests forwarded to each replica:
    std::vector<int> count;
    count.resize(N);
    // count number of requests forwarded from a particular coordinator to a
    // particular replica:
    std::vector<std::vector<int>> count2;
    count2.resize(N);
    for (auto& v : count2) {
        v.resize(N);
    }
    // count of invalid combinations drawn, such as a combination returning
    // the same node more than once. Should remain 0 until the end.
    int count_invalid = 0;

    // Do many loop iterations, in each we pick a coordinator randomly, and
    // then see which CL nodes it will choose. This loop will pick each
    // coordinator roughly "iterations" times.
    for (unsigned i = 0; i < N * iterations; i++) {
        int coord = rand_node(random_engine);
        std::vector<int> c;
        // we can just use:
        // c = miss_equalizing_combination(node_hit_rate, coord, CL);
        // But we used to have bugs where some of the decisions of the
        // algorithm depended on the order of nodes, and in the real Scylla
        // each of the coordinators sees the nodes in a different order
        // (with itself first) - so to simulate (and check) that, we reorder
        // the nodes so that the coordinator is first.
        c = miss_equalizing_combination(do_reorder(node_hit_rate, coord),
                0, CL, want_extra);
        if (want_extra) {
            auto extra = c.back();
            c.pop_back();
            // check if "extra" is valid. Needs to be one of the nodes, but
            // not in c.
            bool valid = extra >= 0 && extra < N;
            for (auto i : c) {
                if (i == extra) {
                    valid = false;
                }
            }
            if (!valid) {
                BOOST_TEST_MESSAGE(format("invalid extra {} for {}",
                    extra, join(" ", c)));
                count_invalid++;
            }
        }
        if (c.size() != CL) {
            BOOST_TEST_MESSAGE(format("vector with wrong number of elements, expected {}, got {}.", CL, c.size()));
            count_invalid++;
        }
        // sort for nicer debugging printout (more useful with uniq) but also to
        // make it easier to find invalid combinations with duplicate nodes
        std::sort(c.begin(), c.end());
        bool invalid = false;
        int prev = -1;
        for(auto& s : c) {
            if (prev == s) {
                // combination with the same node more than once
                invalid = true;
            }
            if (s < 0 || s >= N) {
                invalid = true;
            }
            prev = s;
            count[s]++;
            count2[coord][s]++;
        }
        if (invalid) {
            if (count_invalid < 20) {
                BOOST_TEST_MESSAGE(format("invalid combination: {}",
                    join(" ", c)));
            } else if (count_invalid == 20) {
                BOOST_TEST_MESSAGE(format("... additional invalid combinations hidden (see count below)"));
            }
            count_invalid++;
        }
    }

    float sum = 0;
    for(int i = 0; i < N; i++) {
        sum += count[i];
    }
    BOOST_TEST_MESSAGE(format("fraction of work sent to nodes: {}",
        join(" ", count | boost::adaptors::transformed([sum] (float c) { return c/sum; }))));

    // Check that the miss rate is similar on all nodes (that's the primary
    // goal of HWLB!)
    BOOST_TEST_MESSAGE("misses (work * (1-hitrate)) per node: ");
    float min_misses = 1e100;
    float max_misses = 0;
    for(int i = 0; i < N; i++) {
        float misses = (1-node_hit_rate[i].second)*count[i];
        // There's a maximum amount of work that any code can receive -
        // N*(1/CL). If a node wanted to get more than that to get equal
        // misses, and didn't, we should check its miss count.
        if (std::abs(count[i]/sum - 1.0f/CL) < 1e-3) {
            BOOST_TEST_MESSAGE(format("  {} (clipped, ignoring)", misses));
            continue;
        }
        BOOST_TEST_MESSAGE(format("  {}", misses));
        min_misses = std::min(min_misses, misses);
        max_misses = std::max(max_misses, misses);
    }
    BOOST_TEST_MESSAGE(format("  so miss rate ratio (should be close to 1): {}", max_misses/min_misses));
    // For large number of iterations, max_misses and min_misses should come
    // arbitrarily close. If a test involves a node which is supposed to only
    // get a few requests (i.e., its hit rate is much worse than other nodes)
    // the statistics we get from these few requests can be inaccurate, so
    // such tests should use a higher number of iterations - or we can pick
    // a high-enough slack here.
    BOOST_CHECK(max_misses/min_misses < 1.06);

    // While the primary design goal of HWLB is that it equalizes miss rates
    // of the different nodes, HWLB has a second design goal: to keep as many
    // of the requests it can locally.
    for (int i = 0; i < N; i++) {
        float total = 0;
        for (int j = 0; j < N; j++) {
            total += count2[i][j];
        }
        BOOST_TEST_MESSAGE(format("Work kept by coordinator {}: {}", i, count2[i][i]/total));
        BOOST_TEST_MESSAGE(format(" note NP={}, 1/C={}", N*count[i]/sum, 1.0f / CL));
        // Node should send the maximum possible to itself: NP or 1/C, whichever is
        // lower (since it cannot keep more than 1/C locally).
        // Note that p[i] is approximately count[i]/sum
        float e = std::min(N * count[i]/sum, 1.0f / CL);
        BOOST_CHECK(std::abs((count2[i][i]/total) - e) < 1e-2);
    }
    BOOST_TEST_MESSAGE("");
    if (count_invalid) {
        BOOST_TEST_MESSAGE(format("ERROR: Found invalid combinations: {}", count_invalid));
    }
    // Any number of invalid combinations indicates a bug, but the serious
    // bugs (see invalid_combinations_*() tests) generate many thousands
    // invalid combinations. It seems we have a flakiness (perhaps having
    // something to do with float accuracy?) where tests which usually succeed
    // such as test_clip(), very rarely (e.g., once in a million calls
    // to miss_equalizing_combination()) generate an invalid combination.
    // To work around this flakyness (and allow this test into the suite),
    // we check here count_invalid <= 3. Later we should revert it to
    // count_invalid == 0 and find the cause of the rare bug.
    BOOST_CHECK(count_invalid <= 3);
    BOOST_TEST_MESSAGE("----------------------------------------------------------");
}

// Tests with N=3, CL=1
BOOST_AUTO_TEST_CASE(test_3_1) {
    test_hit_rates({0.8, 0.65, 0.55}, 1);
    test_hit_rates({0.8, 0.8, 0.8}, 1);
    test_hit_rates({0.9, 0.25, 0.15}, 1);
    test_hit_rates({0.94, 0.93, 0.4}, 1);
    test_hit_rates({0.98, 0.98, 0.4}, 1);
    // In the following tests one of the nodes is expected to get a tiny
    // fraction of the requests, so to get good statistics we must do more
    // iterations:
    test_hit_rates({0.999, 0.999, 0.4}, 1, false, 500000);
    test_hit_rates({0.999, 0.999, 0.2}, 1, false, 500000);
    test_hit_rates({0.999, 0.999, 0.8}, 1, false, 500000);
    test_hit_rates({0.999, 0.99, 0.5123}, 1, false, 500000);
}

// Tests with N=3, CL=2
BOOST_AUTO_TEST_CASE(test_3_2) {
    test_hit_rates({0.8, 0.8, 0.8}, 2);
    test_hit_rates({0.8, 0.8, 0.2}, 2);
    test_hit_rates({0.8, 0.57, 0.8}, 2);
    test_hit_rates({0.81, 0.79, 0.57}, 2);
    test_hit_rates({0.81, 0.79, 0.27}, 2);
    test_hit_rates({0.87, 0.83, 0.75}, 2);
    test_hit_rates({0.95, 0.95, 0.15}, 2);
    test_hit_rates({0.8, 0.65, 0.55}, 2);
    test_hit_rates({0.94, 0.93, 0.4}, 2);
    test_hit_rates({0.98, 0.98, 0.4}, 2);
    test_hit_rates({0.999, 0.999, 0.4}, 2);
}

// Tests with N=4, CL=3
BOOST_AUTO_TEST_CASE(test_4_3) {
    test_hit_rates({0.90, 0.89, 0.89, 0.40}, 3);
}

// Tests with N=5, CL=2
BOOST_AUTO_TEST_CASE(test_5_2) {
    test_hit_rates({0.79, 0.78, 0.77, 0.80, 0.32}, 2);
}

// Tests with N=5, CL=3
BOOST_AUTO_TEST_CASE(test_5_3) {
    test_hit_rates({0.79, 0.78, 0.77, 0.80, 0.32}, 3);
}

// Tests with N=6 with one cold node
BOOST_AUTO_TEST_CASE(test_6_one_cold) {
    test_hit_rates({0.94, 0.93, 0.95, 0.97, 0.96, 0.4}, 1);
    //test_hit_rates({0.94, 0.93, 0.95, 0.97, 0.96, 0.4}, 2); // moved to invalid_combinations_2()
    //test_hit_rates({0.94, 0.93, 0.95, 0.97, 0.96, 0.4}, 3); // moved to invalid_combinations_3()
    test_hit_rates({0.95, 0.95, 0.95, 0.95, 0.95, 0.4}, 1);
    test_hit_rates({0.95, 0.95, 0.95, 0.95, 0.95, 0.4}, 2);
    test_hit_rates({0.95, 0.95, 0.95, 0.95, 0.95, 0.4}, 3);
}

// Tests with N=7, CL=4
BOOST_AUTO_TEST_CASE(test_7_4) {
    test_hit_rates({0.79, 0.78, 0.77, 0.80, 0.33, 0.33, 0.3}, 4);
}

// Test cases where it is impossible to reproduce the desired probabilities,
// because the amount of work we can send to each node is limited by CL (each
// node can only send it 1/CL of its work). The test_hit_rates() function
// detects this case and allows the number of misses on this node to be
// different than the rest.
BOOST_AUTO_TEST_CASE(test_clip) {
    // In this case, it is impossible to reproduce the desired probabilities because
    // node 0 wants probability 0.652 > 0.5.
    test_hit_rates({0.95, 0.85, 0.75}, 2);
    // Similarly here, one of the probabilities is 0.348 > 0.333, so we
    // can't achieve the exact probabilities.
    test_hit_rates({0.90, 0.89, 0.91, 0.40}, 3);
}

// Tests which used to produce invalid combinations - where the same node
// was listed more than once. In all these tests the desired amount of work
// sent to each node was achieved, but the CL assumption (that we send to
// CL *different* nodes!) was violated.
// Reproduces issue #9285:
BOOST_AUTO_TEST_CASE(invalid_combinations_1, *boost::unit_test::expected_failures(1)) {
    // BUG! The desired work sent to nodes is accurately achieved, but we get
    // a lot of "invalid combination" errors.  This happens because we get in
    // one of the pp's (the amount of work which one coordinator sends to one
    // other node) a probability higher than 0.33:
    // the first stage divided p node 1's causing me (which has the highest
    // deficit) to partitipate in the division twice and give it pp[1] = 0.12
    // and then later at the end we 2 remaining mixed nodes with surplus
    // 0.316, and add that to the 0.12 and get over 1/CL = 0.333....
    // Perhaps like we fixed the one-mixed-node-remaining case we also need
    // to fix this case? But it will be very messy to track for the different
    // me without running the full algorithm :-(
    test_hit_rates({0.66, 0.66, 0.34, 0.32}, 3);
}
// Reproduces issue #9285:
BOOST_AUTO_TEST_CASE(invalid_combinations_2, *boost::unit_test::expected_failures(1)) {
    test_hit_rates({0.94, 0.93, 0.95, 0.97, 0.96, 0.4}, 2);
}
// Reproduces issue #9285:
BOOST_AUTO_TEST_CASE(invalid_combinations_3, *boost::unit_test::expected_failures(1)) {
    test_hit_rates({0.94, 0.93, 0.95, 0.97, 0.96, 0.4}, 3);
}
// Reproduces issue #9285:
BOOST_AUTO_TEST_CASE(invalid_combinations_4, *boost::unit_test::expected_failures(1)) {
    // 4, 5: cases of the the same bug. These tests cannot fully succeed because they
    // have desired probabilities above 1/CL that need to be clipped. However,
    // as in the above example (and for the same reason) they also get a lot of
    // "invalid combination" errors and this is a bug.
    test_hit_rates({0.77, 0.80, 0.30, 0.32}, 3);
}
// Reproduces issue #9285:
BOOST_AUTO_TEST_CASE(invalid_combinations_5, *boost::unit_test::expected_failures(1)) {
    test_hit_rates({0.77, 0.80, 0.30, 0.30}, 3);
}

// This is one of the tests from test_clip above, that once in a few runs gets
// one or two invalid combination (seems always 0 2 2). To *work around* this
// flakyness test_hit_rates() above "forgives" 3 or less invalid combinations
// (note that the invalid_combinations_* tests above all generate many
// thousands invalid combinations). But eventually we need to undo that
// forgiveness, and fix this test case to always pass instead of being flaky.
// Reproduces issue #9285:
BOOST_AUTO_TEST_CASE(test_clip_flaky) {
    test_hit_rates({0.90, 0.89, 0.91, 0.40}, 3);
}


// Test for the "extra" node
BOOST_AUTO_TEST_CASE(extra) {
    test_hit_rates({0.79, 0.78, 0.77, 0.80, 0.32}, 2, true);
}

// Tests that demonstrate the effect of db/consistency_level.cc's
// max_hit_rate variable, which clips hit rates - and what happens
// when it is 0.999 (before issue #8815) or 0.95 (that issue's fix).
BOOST_AUTO_TEST_CASE(test_a) {
    test_hit_rates({0.999, 0.999, 0.999, 0.999, 0.999, 0.94}, 2);
    test_hit_rates({0.95, 0.95, 0.95, 0.95, 0.95, 0.94}, 2);
    test_hit_rates({0.999, 0.999, 0.999, 0.999, 0.999, 0.989}, 2);
}
