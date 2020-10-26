/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
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

#include <seastar/core/seastar.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/random_utils.hh"
#include "utils/managed_bytes.hh"

static constexpr size_t max_size = 512 * 1024;

static bytes random_bytes(size_t min_len = 0, size_t max_len = 0) {
    size_t size = max_len ? tests::random::get_int(min_len, max_len) : min_len;
    return tests::random::get_bytes(size);
}

SEASTAR_THREAD_TEST_CASE(test_managed_bytes_from_bytes) {
    auto b = random_bytes(0, max_size);
    auto m = managed_bytes(b);
    m.with_linearized([&] (bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, b);
    });
    BOOST_REQUIRE_EQUAL(to_bytes(m), b);
}

SEASTAR_THREAD_TEST_CASE(test_managed_bytes_equality) {
    auto b1 = random_bytes(0, max_size);
    auto m1 = managed_bytes(b1);
    auto m2 = managed_bytes(b1);
    BOOST_REQUIRE_EQUAL(m1, m2);

    // test inequality of same size managed_bytes
    b1 = random_bytes(1, max_size);
    bytes b2;
    do {
        b2 = random_bytes(b1.size());
    } while (b2 == b1);
    m2 = managed_bytes(b2);
    BOOST_REQUIRE_NE(m1, m2);

    // test inequality of different-size managed_bytes
    auto pfx = bytes_view(b1.data(), tests::random::get_int(size_t(0), b1.size()));
    m2 = managed_bytes(pfx);
    BOOST_REQUIRE_NE(m1, m2);
    BOOST_REQUIRE_NE(m2, m1);
}

SEASTAR_THREAD_TEST_CASE(test_managed_bytes_view_from_bytes) {
    auto b = random_bytes(0, max_size);
    auto m = managed_bytes(b);
    auto mv = managed_bytes_view(m);
    mv.with_linearized([&] (bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, b);
    });
    BOOST_REQUIRE_EQUAL(to_bytes(mv), b);
}
