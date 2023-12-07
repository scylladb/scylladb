/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include <unordered_map>
#include <boost/functional/hash.hpp>
#include <algorithm>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include "utils/bit_cast.hh"
#include "utils/reservoir_sampling.hh"

BOOST_AUTO_TEST_CASE(test_reservoir_sampling_probability) {
    // We sample K elements from the sequence 1..N.
    // We do this REPEATS times, and check that the number of times
    // we observed each sample is within expectations.

    constexpr int REPEATS = 100000;
    constexpr int K = 3;
    constexpr int N = 5;
    // There are 10 possible 3-element subsets of a 5-element set.
    // Thus the expected count of each sample is REPEATS / 10 = 10000.
    // The probability that the actual count falls outside the below
    // bounds should be smaller than (1 - 1e-23).
    constexpr int MIN_EXPECTED_COUNT = 9000;
    constexpr int MAX_EXPECTED_COUNT = 11000;

    using sample = std::array<int, K>;
    auto results = std::unordered_map<sample, int, boost::hash<sample>>();

    for (int repeat = 0; repeat < REPEATS; ++repeat) {
        sample storage{};
        auto rs = utils::reservoir_sampler(storage.size(), repeat);
        // Sample K elements from the sequence 1..N
        for (int i = rs.next_replace(); i < N; i = rs.next_replace()) {
            storage.at(rs.replace()) = i;
        }
        // Increment the count for this sample.
        std::ranges::sort(storage);
        results.insert({storage, 0}).first->second += 1;
    }

    // The code below iterates over all possible samples.
    sample wksp;
    auto first = wksp.begin();
    auto last = wksp.end();
    // Fill wksp with first possible sample.
    std::iota(first, last, 0);
    size_t n_samples = 0;
    while (true) {
        n_samples += 1;
        const auto& sample_count = results.insert({wksp, 0}).first->second;
        // These comparisons should be almost impossible to fail.
        BOOST_REQUIRE_GE(sample_count, MIN_EXPECTED_COUNT);
        BOOST_REQUIRE_LE(sample_count, MAX_EXPECTED_COUNT);
        // Advance wksp to next possible sample.
        auto mt = last;
        --mt;
        while (mt > first && *mt == N-(last-mt)) {
            --mt;
        }
        if (mt == first && *mt == N-(last-mt)) {
            break;
        }
        ++(*mt);
        while (++mt != last) {
            *mt = *(mt-1) + 1;
        }
    }
    // Check that no invalid samples were generated.
    BOOST_REQUIRE_EQUAL(n_samples, results.size());
}

BOOST_AUTO_TEST_CASE(test_reservoir_sampling_zero_size) {
    // Special case with sample size of 0.
    auto rs = utils::reservoir_sampler(0, std::random_device()());
    BOOST_REQUIRE_EQUAL(rs.next_replace(), -1);
}

BOOST_AUTO_TEST_CASE(test_page_sampling_probability) {
    // We sample K pages from the sequence 1..N.
    // We do this REPEATS times, and check that the number of times
    // we observed each page is within expectations.

    constexpr int REPEATS = 100000;
    constexpr int K = 3;
    constexpr int N = 5;
    // There are 10 possible 3-element subsets of a 5-element set.
    // Thus the expected count of each sample is REPEATS / 10 = 10000.
    // The probability that the actual count falls outside the below
    // bounds should be smaller than (1 - 1e-23).
    constexpr int MIN_EXPECTED_COUNT = 9000;
    constexpr int MAX_EXPECTED_COUNT = 11000;

    using sample = std::array<int, K>;
    auto results = std::unordered_map<sample, int, boost::hash<sample>>();

    constexpr size_t PAGE_SIZE = sizeof(int);
    constexpr size_t BLOCK_SIZE = 3;

    auto data = std::array<int, N>{};
    std::iota(data.begin(), data.end(), 0);

    for (int repeat = 0; repeat < REPEATS; ++repeat) {
        sample storage{};
        auto ps = utils::page_sampler(PAGE_SIZE, storage.size(), repeat);

        // Sample K elements from the sequence 1..N
        auto stream = std::as_bytes(std::span(data)).subspan(0);
        while (stream.size()) {
            auto block = stream.first(std::min(BLOCK_SIZE, stream.size()));
            stream = stream.subspan(block.size());
            while (block.size()) {
                if (auto cmd = ps.ingest_some(block)) {
                    BOOST_REQUIRE_EQUAL(cmd->data.size(), PAGE_SIZE);
                    storage.at(cmd->slot) = read_unaligned<int>(cmd->data.data());
                }
            }
        }
        // Increment the count for this sample.
        std::ranges::sort(storage);
        results.insert({storage, 0}).first->second += 1;
    }

    // The code below iterates over all possible samples.
    sample wksp;
    auto first = wksp.begin();
    auto last = wksp.end();
    // Fill wksp with first possible sample.
    std::iota(first, last, 0);
    size_t n_samples = 0;
    while (true) {
        n_samples += 1;
        const auto& sample_count = results.insert({wksp, 0}).first->second;
        // These comparisons should be almost impossible to fail.
        BOOST_REQUIRE_GE(sample_count, MIN_EXPECTED_COUNT);
        BOOST_REQUIRE_LE(sample_count, MAX_EXPECTED_COUNT);
        // Advance wksp to next possible sample.
        auto mt = last;
        --mt;
        while (mt > first && *mt == N-(last-mt)) {
            --mt;
        }
        if (mt == first && *mt == N-(last-mt)) {
            break;
        }
        ++(*mt);
        while (++mt != last) {
            *mt = *(mt-1) + 1;
        }
    }
    // Check that no invalid samples were generated.
    BOOST_REQUIRE_EQUAL(n_samples, results.size());
}

BOOST_AUTO_TEST_CASE(test_page_sampling_zero_size) {
    // Special case with sample size of 0.
    auto ps = utils::page_sampler(4, 0, std::random_device()());
    auto data = std::array<std::byte, 1025>();
    auto stream = std::as_bytes(std::span(data)).subspan(0);
    while (stream.size()) {
        BOOST_REQUIRE(!ps.ingest_some(stream));
    }
}
