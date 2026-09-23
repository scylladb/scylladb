/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#define BOOST_TEST_MODULE hyperloglog

#include <boost/test/unit_test.hpp>
#include <cmath>
#include <cstdint>
#include <fmt/format.h>
#include "sstables/hyperloglog.hh"

namespace {

constexpr uint8_t precision = 10;
// 4 standard errors at p=10 (1.04 / sqrt(1024) ~= 3.25%).
constexpr double tolerance = 0.13;

uint64_t splitmix64(uint64_t x) {
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}

hll::HyperLogLog make_sketch(uint64_t begin, uint64_t end, uint8_t p = precision) {
    hll::HyperLogLog h(p);
    for (uint64_t i = begin; i < end; i++) {
        h.offer_hashed(splitmix64(i));
    }
    return h;
}

void check_estimate(const hll::HyperLogLog& h, double expected, double rel_tolerance = tolerance) {
    auto e = h.estimate();
    BOOST_TEST_MESSAGE(fmt::format("expected {} estimated {}", expected, e));
    BOOST_REQUIRE(std::isfinite(e));
    BOOST_REQUIRE_LE(std::abs(e - expected), expected * rel_tolerance);
}

std::vector<uint8_t> registers(hll::HyperLogLog& h) {
    auto bytes = h.get_bytes();
    return std::vector<uint8_t>(bytes.end() - h.registerSize(), bytes.end());
}

// Hash landing in register `index` with rank `rank` at p=10.
uint64_t hash_with_rank(uint64_t index, unsigned rank) {
    return (index << (64 - precision)) | (uint64_t(1) << (64 - precision - rank));
}

}

BOOST_AUTO_TEST_CASE(test_empty_and_tiny) {
    BOOST_REQUIRE_EQUAL(make_sketch(0, 0).estimate(), 0);
    check_estimate(make_sketch(0, 1), 1, 0.01);
    // 4 sigma of linear counting at n=100, m=1024 (~2.25%).
    check_estimate(make_sketch(0, 100), 100, 0.09);
}

BOOST_AUTO_TEST_CASE(test_accuracy) {
    for (uint64_t n : {10'000, 1'000'000, 10'000'000}) {
        check_estimate(make_sketch(0, n), n);
    }
}

BOOST_AUTO_TEST_CASE(test_merge_overlapping) {
    auto a = make_sketch(0, 600'000);
    a.merge(make_sketch(400'000, 1'000'000));
    check_estimate(a, 1'000'000);
}

// Bits below the low 32 of the shifted hash used to be dropped.
BOOST_AUTO_TEST_CASE(test_rank_uses_full_hash) {
    hll::HyperLogLog h(precision);
    h.offer_hashed(hash_with_rank(0, 32));
    h.offer_hashed(hash_with_rank(1, 40));
    h.offer_hashed(0); // all-zero rank is capped at 64 - p + 1
    auto r = registers(h);
    BOOST_REQUIRE_EQUAL(r[0], 55);
    BOOST_REQUIRE_EQUAL(r[1], 40);
    h.clear();
    h.offer_hashed(hash_with_rank(0, 32));
    BOOST_REQUIRE_EQUAL(registers(h)[0], 32);
}

// A raw estimate above 2^32 used to hit the 32-bit large-range correction and yield NaN.
BOOST_AUTO_TEST_CASE(test_large_range_estimate) {
    hll::HyperLogLog h(precision);
    for (uint64_t i = 0; i < h.registerSize(); i++) {
        h.offer_hashed(hash_with_rank(i, 23));
    }
    const double m = h.registerSize();
    const double raw = 0.7213 / (1.0 + 1.079 / m) * m * m / (m * std::ldexp(1.0, -23));
    BOOST_REQUIRE_GT(raw, 4294967296.0);
    BOOST_REQUIRE(std::isfinite(h.estimate()));
    BOOST_REQUIRE_CLOSE(h.estimate(), raw, 1e-9);
}
