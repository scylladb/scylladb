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
#include "utils/fragment_range.hh"

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
    BOOST_REQUIRE_EQUAL(to_bytes(m), bytes_view());

    b = random_bytes(1, 16);
    m = managed_bytes(b);
    m.with_linearized([&] (bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, b);
    });
    BOOST_REQUIRE_EQUAL(to_bytes(m), b);

    b = random_bytes(16, 256);
    m = managed_bytes(b);
    m.with_linearized([&] (bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, b);
    });
    BOOST_REQUIRE_EQUAL(to_bytes(m), b);

    b = random_bytes(256, 4096);
    m = managed_bytes(b);
    m.with_linearized([&] (bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, b);
    });
    BOOST_REQUIRE_EQUAL(to_bytes(m), b);

    b = random_bytes(4096, 131072);
    m = managed_bytes(b);
    m.with_linearized([&] (bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, b);
    });
    BOOST_REQUIRE_EQUAL(to_bytes(m), b);
}

SEASTAR_THREAD_TEST_CASE(test_managed_bytes_from_manged_bytes_view) {
    bytes b;
    managed_bytes m;
    managed_bytes_view mv;

    m = managed_bytes(mv);
    BOOST_REQUIRE_EQUAL(managed_bytes_view(m), mv);

    b = random_bytes(1, 16);
    mv = managed_bytes_view(b);
    m = managed_bytes(mv);
    BOOST_REQUIRE_EQUAL(managed_bytes_view(m), mv);

    b = random_bytes(16, 256);
    mv = managed_bytes_view(b);
    m = managed_bytes(mv);
    BOOST_REQUIRE_EQUAL(managed_bytes_view(m), mv);

    b = random_bytes(256, 4096);
    mv = managed_bytes_view(b);
    m = managed_bytes(mv);
    BOOST_REQUIRE_EQUAL(managed_bytes_view(m), mv);

    b = random_bytes(4096, 131072);
    mv = managed_bytes_view(b);
    m = managed_bytes(mv);
    BOOST_REQUIRE_EQUAL(managed_bytes_view(m), mv);
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

SEASTAR_THREAD_TEST_CASE(test_managed_bytes_view_from_bytes) {
    bytes b;
    managed_bytes_view m;

    m.with_linearized([&] (bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, bytes_view());
    });
    BOOST_REQUIRE_EQUAL(to_bytes(m), bytes_view());

    b = random_bytes(1, 16);
    m = managed_bytes_view(b);
    m.with_linearized([&] (bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, b);
    });
    BOOST_REQUIRE_EQUAL(to_bytes(m), b);

    b = random_bytes(16, 256);
    m = managed_bytes_view(b);
    m.with_linearized([&] (bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, b);
    });
    BOOST_REQUIRE_EQUAL(to_bytes(m), b);

    b = random_bytes(256, 4096);
    m = managed_bytes_view(b);
    m.with_linearized([&] (bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, b);
    });
    BOOST_REQUIRE_EQUAL(to_bytes(m), b);

    b = random_bytes(4096, 131072);
    m = managed_bytes_view(b);
    m.with_linearized([&] (bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, b);
    });
    BOOST_REQUIRE_EQUAL(to_bytes(m), b);
}

SEASTAR_THREAD_TEST_CASE(test_managed_bytes_view_from_managed_bytes) {
    bytes b;
    managed_bytes m;
    managed_bytes_view mv;

    m = managed_bytes();
    m.with_linearized([&] (bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, bytes_view());
    });
    BOOST_REQUIRE_EQUAL(to_bytes(m), bytes_view());

    b = random_bytes(1, 131072);
    m = managed_bytes(b);
    mv = managed_bytes_view(m);
    mv.with_linearized([&] (bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, b);
    });
    BOOST_REQUIRE_EQUAL(to_bytes(m), b);
}

SEASTAR_THREAD_TEST_CASE(test_managed_bytes_view_from_bytes_view) {
    bytes b;
    managed_bytes_view m;

    m = bytes_view();
    m.with_linearized([&] (bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, bytes_view());
    });
    BOOST_REQUIRE_EQUAL(to_bytes(m), bytes_view());

    b = random_bytes(1, 131072);
    m = managed_bytes_view(bytes_view(b));
    m.with_linearized([&] (bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, b);
    });
    BOOST_REQUIRE_EQUAL(to_bytes(m), b);
}

SEASTAR_THREAD_TEST_CASE(test_managed_bytes_view_remove_prefix) {
    bytes b;
    managed_bytes_view m;
    size_t n;

    m.remove_prefix(0);
    BOOST_REQUIRE_EQUAL(to_bytes(m), bytes_view());
    BOOST_REQUIRE_THROW(m.remove_prefix(1), std::runtime_error);

    b = random_bytes(1, 16);
    m = managed_bytes_view(b);
    n = tests::random::get_int(size_t(0), b.size());
    BOOST_TEST_MESSAGE(format("size={} remove_prefix={}", b.size(), n));
    m.remove_prefix(n);
    BOOST_REQUIRE_EQUAL(to_bytes(m), bytes_view(b.data() + n, b.size() - n));
    BOOST_REQUIRE_THROW(m.remove_prefix(b.size() + 1), std::runtime_error);

    b = random_bytes(16, 131072);
    m = managed_bytes_view(b);
    n = tests::random::get_int(size_t(0), b.size());
    BOOST_TEST_MESSAGE(format("size={} remove_prefix={}", b.size(), n));
    m.remove_prefix(n);
    BOOST_REQUIRE_EQUAL(to_bytes(m), bytes_view(b.data() + n, b.size() - n));
    BOOST_REQUIRE_THROW(m.remove_prefix(b.size() + 1), std::runtime_error);
}

SEASTAR_THREAD_TEST_CASE(test_managed_bytes_range) {
    bytes b;
    managed_bytes m;

    b = random_bytes(0, 131072);
    m = managed_bytes(b);
    auto v = fragment_range_view<managed_bytes>(m);
    BOOST_REQUIRE_EQUAL(v.size_bytes(), b.size());
    BOOST_REQUIRE_EQUAL(v.empty(), !b.size());

    size_t size = 0;
    for (auto f : m) {
        size += f.size();
    }
    BOOST_REQUIRE_EQUAL(size, b.size());
}

SEASTAR_THREAD_TEST_CASE(test_managed_bytes_view_range) {
    bytes b;
    managed_bytes_view m;
    size_t measured_size;

    b = random_bytes(0, 131072);
    m = managed_bytes_view(b);
    auto v = fragment_range_view<managed_bytes_view>(m);
    BOOST_REQUIRE_EQUAL(v.size_bytes(), b.size());
    BOOST_REQUIRE_EQUAL(v.empty(), !b.size());

    size_t size = 0;
    for (auto f : m) {
        size += f.size();
    }
    BOOST_REQUIRE_EQUAL(size, b.size());

    m.remove_prefix(b.size());
    BOOST_REQUIRE(v.empty());

    size = 0;
    for (auto f : m) {
        size += f.size();
    }
    BOOST_REQUIRE_EQUAL(size, 0);
}

SEASTAR_THREAD_TEST_CASE(test_managed_bytes_view_equality) {
    bytes b, b2;
    managed_bytes_view m1, m2;

    BOOST_REQUIRE_EQUAL(m1, m2);

    b = random_bytes(1, 131072);
    m1 = managed_bytes_view(b);
    m2 = managed_bytes_view(b);
    BOOST_REQUIRE_EQUAL(m1, m2);
    BOOST_REQUIRE_EQUAL(compare_unsigned(m1, m2), 0);
    do {
        b2 = random_bytes(b.size());
    } while (b2 == b);
    m2 = managed_bytes_view(b2);
    BOOST_REQUIRE_NE(m1, m2);
    if (b < b2) {
        BOOST_REQUIRE(compare_unsigned(m1, m2) < 0);
    } else {
        BOOST_REQUIRE(compare_unsigned(m1, m2) > 0);
    }
}

SEASTAR_THREAD_TEST_CASE(test_managed_bytes_view_equality_matrix) {
    bytes b1, b2;
    bytes_view bv1, bv2;
    managed_bytes mb1, mb2;
    managed_bytes_view mbv1, mbv2;

    b1 = random_bytes(1, 131072);
    do {
        b2 = random_bytes(b1.size());
    } while (b2 == b1);
    bv1 = b1;
    bv2 = b2;
    mb1 = managed_bytes(b1);
    mb2 = managed_bytes(b2);
    mbv1 = managed_bytes_view(b1);
    mbv2 = managed_bytes_view(b2);
    BOOST_REQUIRE_EQUAL(mb1, mbv1);
    BOOST_REQUIRE_NE(mb1, mbv2);
    BOOST_REQUIRE_EQUAL(mbv1, mb1);
    BOOST_REQUIRE_NE(mbv1, mb2);
    BOOST_REQUIRE_EQUAL(bv1, mbv1);
    BOOST_REQUIRE_NE(bv1, mbv2);
    BOOST_REQUIRE_EQUAL(mbv1, bv1);
    BOOST_REQUIRE_NE(mbv1, bv2);
}
