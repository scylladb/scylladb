/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "seastar/include/seastar/testing/perf_tests.hh"
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
