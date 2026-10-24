/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/testing/perf_tests.hh>
#include <seastar/testing/random.hh>

#include <algorithm>
#include <limits>
#include <numeric>
#include <random>
#include <vector>

#include "utils/array-search.hh"

namespace {

// B+tree node keys, half-full to full, padded like bplus::maybe_key does
template <int Capacity>
struct array_search_gt_fixture {
    static constexpr size_t count = 1024;
    struct node {
        int64_t keys[Capacity];
        int size;
    };
    std::vector<node> nodes = std::vector<node>(count);
    std::vector<int64_t> probes = std::vector<int64_t>(count);

    array_search_gt_fixture() {
        auto& eng = seastar::testing::local_random_engine;
        std::uniform_int_distribution<int64_t> key(utils::simple_key_unused_value + 1, std::numeric_limits<int64_t>::max());
        std::uniform_int_distribution<int> size(Capacity / 2, Capacity);
        for (auto& n : nodes) {
            n.size = size(eng);
            std::generate_n(n.keys, n.size, [&] { return key(eng); });
            std::sort(n.keys, n.keys + n.size);
            std::fill(n.keys + n.size, n.keys + Capacity, utils::simple_key_unused_value);
        }
        std::generate(probes.begin(), probes.end(), [&] { return key(eng); });
    }

    size_t search() {
        for (size_t i = 0; i < count; i++) {
            perf_tests::do_not_optimize(utils::array_search_gt(probes[i], nodes[i].keys, Capacity, nodes[i].size));
        }
        return count;
    }
};

// Radix node index arrays: distinct bytes, needle always present
struct array_search_eq_fixture {
    static constexpr size_t count = 1024;
    struct node {
        uint8_t idx[32];
    };
    std::vector<node> nodes = std::vector<node>(count);
    std::vector<uint8_t> probes16 = std::vector<uint8_t>(count);
    std::vector<uint8_t> probes32 = std::vector<uint8_t>(count);

    array_search_eq_fixture() {
        auto& eng = seastar::testing::local_random_engine;
        std::uniform_int_distribution<unsigned> pos16(0, 15), pos32(0, 31);
        for (size_t i = 0; i < count; i++) {
            std::iota(std::begin(nodes[i].idx), std::end(nodes[i].idx), 0);
            std::shuffle(std::begin(nodes[i].idx), std::end(nodes[i].idx), eng);
            probes16[i] = nodes[i].idx[pos16(eng)];
            probes32[i] = nodes[i].idx[pos32(eng)];
        }
    }
};

using array_search_gt4 = array_search_gt_fixture<4>;
using array_search_gt16 = array_search_gt_fixture<16>;

}

PERF_TEST_F(array_search_gt4, search) { return search(); }
PERF_TEST_F(array_search_gt16, search) { return search(); }

PERF_TEST_F(array_search_eq_fixture, search_16) {
    for (size_t i = 0; i < count; i++) {
        perf_tests::do_not_optimize(utils::array_search_16_eq(probes16[i], nodes[i].idx));
    }
    return count;
}

PERF_TEST_F(array_search_eq_fixture, search_32) {
    for (size_t i = 0; i < count; i++) {
        perf_tests::do_not_optimize(utils::array_search_32_eq(probes32[i], nodes[i].idx));
    }
    return count;
}
