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

SEASTAR_THREAD_TEST_CASE(test_managed_bytes_from_manged_bytes_view) {
    auto b = random_bytes(0, max_size);
    auto m1 = managed_bytes(b);
    auto mv = managed_bytes_view(m1);
    auto m2 = managed_bytes(mv);
    BOOST_REQUIRE_EQUAL(to_bytes(m2), b);
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

SEASTAR_THREAD_TEST_CASE(test_managed_bytes_view_from_managed_bytes) {
    auto b = random_bytes(0, max_size);
    auto m = managed_bytes(b);
    auto mv = managed_bytes_view(m);
    mv.with_linearized([&] (bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, b);
    });
    BOOST_REQUIRE_EQUAL(to_bytes(m), b);
}

SEASTAR_THREAD_TEST_CASE(test_managed_bytes_view_from_bytes_view) {
    auto b = random_bytes(0, max_size);
    auto mv = managed_bytes_view(bytes_view(b));
    mv.with_linearized([&] (bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, b);
    });
    BOOST_REQUIRE_EQUAL(to_bytes(mv), b);
}

SEASTAR_THREAD_TEST_CASE(test_managed_bytes_view_remove_prefix) {
    auto b = random_bytes(0, max_size);
    auto m = managed_bytes(b);
    auto mv = managed_bytes_view(m);
    size_t n = b.size() ? tests::random::get_int(size_t(0), b.size()) : 0;
    BOOST_TEST_MESSAGE(format("size={} remove_prefix={}", b.size(), n));
    mv.remove_prefix(n);
    BOOST_REQUIRE_EQUAL(to_bytes(mv), bytes_view(b.data() + n, b.size() - n));
    BOOST_REQUIRE_THROW(mv.remove_prefix(b.size() + 1), std::runtime_error);
}

SEASTAR_THREAD_TEST_CASE(test_managed_bytes_fragment_range_view) {
    auto b = random_bytes(0, max_size);
    auto m = managed_bytes(b);
    auto fr = m.as_fragment_range();
    auto v = fragment_range_view<managed_bytes_fragment_range_view>(fr);
    BOOST_REQUIRE_EQUAL(v.size_bytes(), b.size());
    BOOST_REQUIRE_EQUAL(v.empty(), !b.size());

    size_t size = 0;
    for (auto f : v) {
        size += f.size();
    }
    BOOST_REQUIRE_EQUAL(size, b.size());

    auto mv = managed_bytes_view(m);
    size_t n = b.size() ? tests::random::get_int(size_t(0), b.size()) : 0;
    mv.remove_prefix(n);
    fr = mv.as_fragment_range();
    v = fragment_range_view<managed_bytes_fragment_range_view>(fr);
    BOOST_REQUIRE_EQUAL(v.empty(), !mv.size());

    size = 0;
    for (auto f : v) {
        size += f.size();
    }
    BOOST_REQUIRE_EQUAL(size, mv.size());
}

SEASTAR_THREAD_TEST_CASE(test_managed_bytes_view_equality) {
    // test equality
    auto b1 = random_bytes(0, max_size);
    auto m1 = managed_bytes(b1);
    auto mv1 = managed_bytes_view(m1);
    auto m2 = managed_bytes(b1);
    auto mv2 = managed_bytes_view(m2);
    BOOST_REQUIRE_EQUAL(mv1, mv2);
    BOOST_REQUIRE_EQUAL(compare_unsigned(mv1, mv2), 0);

    // test inequality
    bytes b2;
    do {
        b2 = random_bytes(b1.size());
    } while (b2 == b1);
    m2 = managed_bytes(b2);
    mv2 = managed_bytes_view(m2);
    BOOST_REQUIRE_NE(mv1, mv2);
    if (compare_unsigned(b1, b2) < 0) {
        BOOST_REQUIRE(compare_unsigned(mv1, mv2) < 0);
    } else {
        BOOST_REQUIRE(compare_unsigned(mv1, mv2) > 0);
    }
}

SEASTAR_THREAD_TEST_CASE(test_managed_bytes_view_equality_matrix) {
    bytes b1, b2;
    bytes_view bv1, bv2;
    managed_bytes mb1, mb2;
    managed_bytes_view mbv1, mbv2;

    b1 = random_bytes(1, max_size);
    do {
        b2 = random_bytes(b1.size());
    } while (b2 == b1);
    bv1 = b1;
    bv2 = b2;
    mb1 = managed_bytes(b1);
    mb2 = managed_bytes(b2);
    mbv1 = managed_bytes_view(mb1);
    mbv2 = managed_bytes_view(mb2);
    BOOST_REQUIRE_EQUAL(mb1, mbv1);
    BOOST_REQUIRE_NE(mb1, mbv2);
    BOOST_REQUIRE_EQUAL(mbv1, mb1);
    BOOST_REQUIRE_NE(mbv1, mb2);
    BOOST_REQUIRE_EQUAL(bv1, mbv1);
    BOOST_REQUIRE_NE(bv1, mbv2);
    BOOST_REQUIRE_EQUAL(mbv1, bv1);
    BOOST_REQUIRE_NE(mbv1, bv2);
}

SEASTAR_THREAD_TEST_CASE(test_managed_bytes_view_substr) {
    auto b = random_bytes(1, max_size);
    auto m = managed_bytes(b);
    auto mv = managed_bytes_view(m);
    auto offset = tests::random::get_int(size_t(0), b.size());
    auto len = tests::random::get_int(size_t(0), b.size());
    BOOST_TEST_MESSAGE(format("size={} offset={} len={}", b.size(), offset, len));
    auto bv = bytes_view(b.data() + offset, std::min(b.size() - offset, len));
    auto mvs = mv.substr(offset, len);
    BOOST_REQUIRE_EQUAL(mvs, bv);
    if (mvs.size()) {
        offset = tests::random::get_int(size_t(0), mvs.size());
        bv = bytes_view(bv.data() + offset, bv.size() - offset);
        mvs = mvs.substr(offset, managed_bytes_view::npos);
        BOOST_REQUIRE_EQUAL(mvs, bv);
    }
}
