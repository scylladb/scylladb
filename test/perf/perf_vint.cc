/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/testing/perf_tests.hh>
#include <seastar/testing/random.hh>
#include <seastar/testing/test_runner.hh>

#include <random>

#include "vint-serialization.hh"

class vint {
public:
    static constexpr size_t count = 1000;
private:
    std::vector<uint64_t> _integers;
    bytes _serialized;
public:
    vint()
        : _integers(count)
        , _serialized(bytes::initialized_later{}, count * max_vint_length)
    {
        auto eng = seastar::testing::local_random_engine;
        auto dist = std::uniform_int_distribution<uint64_t>{};
        std::generate_n(_integers.begin(), count, [&] { return dist(eng); });

        auto dst = _serialized.data();
        for (auto v : _integers) {
            auto len = unsigned_vint::serialize(v, dst);
            dst += len;
        }
    }

    const std::vector<uint64_t>& integers() const { return _integers; }
    bytes_view serialized() const { return bytes_view(_serialized.data()); }
};

PERF_TEST_F(vint, serialize) {
    std::array<int8_t, max_vint_length> output;
    auto dst = output.data();
    for (auto v : integers()) {
        perf_tests::do_not_optimize(unsigned_vint::serialize(v, dst));
        perf_tests::do_not_optimize(dst);
    }
    return count;
}

PERF_TEST_F(vint, deserialize) {
    auto src = serialized();
    for (auto i = 0u; i < count; i++) {
        auto len = unsigned_vint::serialized_size_from_first_byte(src.front());
        perf_tests::do_not_optimize(unsigned_vint::deserialize(src));
        src.remove_prefix(len);
    }
    return count;
}
