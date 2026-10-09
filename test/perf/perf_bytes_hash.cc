/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "bytes.hh"
#include "utils/managed_bytes.hh"
#include "test/lib/random_utils.hh"

#include <seastar/testing/perf_tests.hh>

// Hashes a batch of keys per iteration; results are per key.
template <size_t KeySize>
struct keys {
    static constexpr size_t count = 1000;
    std::vector<bytes> data;
    std::vector<managed_bytes> managed;
    keys() {
        for (size_t i = 0; i < count; ++i) {
            data.push_back(tests::random::get_bytes(KeySize));
            managed.emplace_back(data.back());
        }
    }

    // The pre-one-shot std::hash<bytes_view> and std::hash<managed_bytes_view>, kept as fixed reference points.
    size_t streaming_xxh64() {
        size_t sink = 0;
        for (const auto& b : data) {
            simple_xx_hasher h;
            appending_hash<bytes_view>{}(h, b);
            sink ^= h.finalize();
        }
        perf_tests::do_not_optimize(sink);
        return count;
    }
    size_t streaming_xxh64_managed() {
        size_t sink = 0;
        for (const auto& m : managed) {
            simple_xx_hasher h;
            appending_hash<managed_bytes_view>{}(h, m);
            sink ^= h.finalize();
        }
        perf_tests::do_not_optimize(sink);
        return count;
    }
    size_t oneshot_xxh64() {
        size_t sink = 0;
        for (const auto& b : data) {
            sink ^= XXH64(b.data(), b.size(), 0);
        }
        perf_tests::do_not_optimize(sink);
        return count;
    }
    size_t std_hash() {
        size_t sink = 0;
        for (const auto& b : data) {
            sink ^= std::hash<bytes_view>{}(b);
        }
        perf_tests::do_not_optimize(sink);
        return count;
    }
    size_t std_hash_managed() {
        size_t sink = 0;
        for (const auto& m : managed) {
            sink ^= std::hash<managed_bytes_view>{}(m);
        }
        perf_tests::do_not_optimize(sink);
        return count;
    }
};

#define BYTES_HASH_TESTS(N)                                                                                                                                    \
    using keys_##N = keys<N>;                                                                                                                                  \
    PERF_TEST_F(keys_##N, streaming_xxh64_##N) {                                                                                                               \
        return streaming_xxh64();                                                                                                                              \
    }                                                                                                                                                          \
    PERF_TEST_F(keys_##N, streaming_xxh64_managed_##N) {                                                                                                       \
        return streaming_xxh64_managed();                                                                                                                      \
    }                                                                                                                                                          \
    PERF_TEST_F(keys_##N, oneshot_xxh64_##N) {                                                                                                                 \
        return oneshot_xxh64();                                                                                                                                \
    }                                                                                                                                                          \
    PERF_TEST_F(keys_##N, std_hash_##N) {                                                                                                                      \
        return std_hash();                                                                                                                                     \
    }                                                                                                                                                          \
    PERF_TEST_F(keys_##N, std_hash_managed_##N) {                                                                                                              \
        return std_hash_managed();                                                                                                                             \
    }

BYTES_HASH_TESTS(8)
BYTES_HASH_TESTS(16)
BYTES_HASH_TESTS(64)
BYTES_HASH_TESTS(256)
