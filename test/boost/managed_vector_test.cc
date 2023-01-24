/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/test/unit_test.hpp>
#include "test/lib/scylla_test_case.hh"

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

SEASTAR_THREAD_TEST_CASE(test_emplace) {
    managed_vector<unsigned> vec;
    fill(vec);
    verify_filled(vec);

    vec.clear();
    verify_empty(vec);
}

SEASTAR_THREAD_TEST_CASE(test_copy) {
    managed_vector<unsigned> vec;

    managed_vector<unsigned> vec2(vec);
    verify_empty(vec2);

    fill(vec);

    managed_vector<unsigned> vec3(vec);
    verify_filled(vec);
    verify_filled(vec3);
}

SEASTAR_THREAD_TEST_CASE(test_move) {
    managed_vector<unsigned> vec;

    managed_vector<unsigned> vec2(std::move(vec));
    verify_empty(vec2);

    fill(vec);

    managed_vector<unsigned> vec3(std::move(vec));
    verify_empty(vec);
    verify_filled(vec3);
}

SEASTAR_THREAD_TEST_CASE(test_erase) {
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

SEASTAR_THREAD_TEST_CASE(test_resize_up) {
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

SEASTAR_THREAD_TEST_CASE(test_resize_down) {
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

SEASTAR_THREAD_TEST_CASE(test_compaction) {
    logalloc::region reg;
    with_allocator(reg.allocator(), [&] {
        managed_vector<unsigned> vec;
        fill(vec);

        verify_filled(vec);

        reg.full_compaction();

        verify_filled(vec);
    });
}
