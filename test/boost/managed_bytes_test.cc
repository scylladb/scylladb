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

static bytes random_bytes(size_t min_len = 0, size_t max_len = 0) {
    size_t size = max_len ? tests::random::get_int(min_len, max_len) : min_len;
    return tests::random::get_bytes(size);
}

SEASTAR_THREAD_TEST_CASE(test_managed_bytes_from_bytes) {
    bytes b;
    managed_bytes m;

    m.with_linearized([&] (bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, bytes_view());
    });

    b = random_bytes(1, 16);
    m = managed_bytes(b);
    m.with_linearized([&] (bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, b);
    });

    b = random_bytes(16, 256);
    m = managed_bytes(b);
    m.with_linearized([&] (bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, b);
    });

    b = random_bytes(256, 4096);
    m = managed_bytes(b);
    m.with_linearized([&] (bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, b);
    });

    b = random_bytes(4096, 131072);
    m = managed_bytes(b);
    m.with_linearized([&] (bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, b);
    });
}

SEASTAR_THREAD_TEST_CASE(test_managed_bytes_equality) {
    bytes b, b2;
    managed_bytes m1, m2;

    BOOST_REQUIRE_EQUAL(m1, m2);

    b = random_bytes(1, 131072);
    m1 = managed_bytes(b);
    m2 = managed_bytes(b);
    BOOST_REQUIRE_EQUAL(m1, m2);
    do {
        b2 = random_bytes(b.size());
    } while (b2 == b);
    m2 = managed_bytes(b2);
    BOOST_REQUIRE_NE(m1, m2);
}
