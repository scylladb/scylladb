/*
 * Copyright (C) 2015 ScyllaDB
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

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>

#include "utils/managed_vector.hh"
#include "utils/logalloc.hh"

static constexpr unsigned count = 125;

template<typename Vector>
void fill(Vector& vec) {
    BOOST_CHECK(vec.empty());
    for (unsigned i = 0; i < count; i++) {
        vec.emplace_back(i);
    }
    BOOST_CHECK(!vec.empty());
    BOOST_CHECK_EQUAL(vec.size(), count);
    BOOST_CHECK_GE(vec.capacity(), vec.size());
}

template<typename Vector>
void verify_filled(const Vector& vec) {
    unsigned idx = 0;
    for (auto&& v : vec) {
        BOOST_CHECK_EQUAL(v, idx);
        BOOST_CHECK_EQUAL(vec[idx], idx);
        idx++;
    }
    BOOST_CHECK_EQUAL(idx, count);
}

template<typename Vector>
void verify_empty(const Vector& vec) {
    BOOST_CHECK_EQUAL(vec.size(), 0);
    BOOST_CHECK(vec.empty());
    for (auto&& v : vec) {
        (void)v;
        BOOST_FAIL("vec should be empty");
    }
}

BOOST_AUTO_TEST_CASE(test_emplace) {
    managed_vector<unsigned> vec;
    fill(vec);
    verify_filled(vec);

    vec.clear();
    verify_empty(vec);
}

BOOST_AUTO_TEST_CASE(test_copy) {
    managed_vector<unsigned> vec;

    managed_vector<unsigned> vec2(vec);
    verify_empty(vec2);

    fill(vec);

    managed_vector<unsigned> vec3(vec);
    verify_filled(vec);
    verify_filled(vec3);
}

BOOST_AUTO_TEST_CASE(test_move) {
    managed_vector<unsigned> vec;

    managed_vector<unsigned> vec2(std::move(vec));
    verify_empty(vec2);

    fill(vec);

    managed_vector<unsigned> vec3(std::move(vec));
    verify_empty(vec);
    verify_filled(vec3);
}

BOOST_AUTO_TEST_CASE(test_erase) {
    managed_vector<unsigned> vec;
    fill(vec);

    vec.erase(vec.begin());

    unsigned idx = 1;
    for (auto&& v : vec) {
        BOOST_CHECK_EQUAL(v, idx);
        BOOST_CHECK_EQUAL(vec[idx - 1], idx);
        idx++;
    }
    BOOST_CHECK_EQUAL(idx, count);
}

BOOST_AUTO_TEST_CASE(test_resize_up) {
    managed_vector<unsigned> vec;
    fill(vec);
    vec.resize(count + 5);

    unsigned idx = 0;
    for (auto&& v : vec) {
        if (idx < count) {
            BOOST_CHECK_EQUAL(v, idx);
            BOOST_CHECK_EQUAL(vec[idx], idx);
        } else {
            BOOST_CHECK_EQUAL(v, 0);
            BOOST_CHECK_EQUAL(vec[idx], 0);
        }
        idx++;
    }
    BOOST_CHECK_EQUAL(idx, count + 5);
}

BOOST_AUTO_TEST_CASE(test_resize_down) {
    managed_vector<unsigned> vec;
    fill(vec);
    vec.resize(5);

    unsigned idx = 0;
    for (auto&& v : vec) {
        BOOST_CHECK_EQUAL(v, idx);
        BOOST_CHECK_EQUAL(vec[idx], idx);
        idx++;
    }
    BOOST_CHECK_EQUAL(idx, 5);
}

BOOST_AUTO_TEST_CASE(test_compaction) {
    logalloc::region reg;
    with_allocator(reg.allocator(), [&] {
        managed_vector<unsigned> vec;
        fill(vec);

        verify_filled(vec);

        reg.full_compaction();

        verify_filled(vec);
    });
}
