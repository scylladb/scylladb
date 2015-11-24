/*
 * Copyright 2015 ScyllaDB
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

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>

#include "utils/dynamic_bitset.hh"

BOOST_AUTO_TEST_CASE(test_set_clear_test) {
    utils::dynamic_bitset bits;
    bits.resize(178);
    for (size_t i = 0; i < 178; i++) {
        BOOST_REQUIRE(!bits.test(i));
    }

    for (size_t i = 0; i < 178; i += 2) {
        bits.set(i);
    }

    for (size_t i = 0; i < 178; i++) {
        if (i % 2) {
            BOOST_REQUIRE(!bits.test(i));
        } else {
            BOOST_REQUIRE(bits.test(i));
        }
    }

    for (size_t i = 0; i < 178; i += 4) {
        bits.clear(i);
    }

    for (size_t i = 0; i < 178; i++) {
        if (i % 2 || i % 4 == 0) {
            BOOST_REQUIRE(!bits.test(i));
        } else {
            BOOST_REQUIRE(bits.test(i));
        }
    }
}

BOOST_AUTO_TEST_CASE(test_find_first_next) {
    utils::dynamic_bitset bits;
    bits.resize(178);
    for (size_t i = 0; i < 178; i++) {
        BOOST_REQUIRE(!bits.test(i));
    }
    BOOST_REQUIRE_EQUAL(bits.find_first_set(), utils::dynamic_bitset::npos);

    for (size_t i = 0; i < 178; i += 2) {
        bits.set(i);
    }

    size_t i = bits.find_first_set();
    BOOST_REQUIRE_EQUAL(i, 0);
    do {
        auto j = bits.find_next_set(i);
        BOOST_REQUIRE_EQUAL(i + 2, j);
        i = j;
    } while (i < 176);
    BOOST_REQUIRE_EQUAL(bits.find_next_set(i), utils::dynamic_bitset::npos);

    i = bits.find_first_clear();
    BOOST_REQUIRE_EQUAL(i, 1);
    do {
        auto j = bits.find_next_clear(i);
        BOOST_REQUIRE_EQUAL(i + 2, j);
        i = j;
    } while (i < 177);
    BOOST_REQUIRE_EQUAL(bits.find_next_clear(i), utils::dynamic_bitset::npos);

    for (size_t i = 0; i < 178; i += 4) {
        bits.clear(i);
    }

    i = bits.find_first_set();
    BOOST_REQUIRE_EQUAL(i, 2);
    do {
        auto j = bits.find_next_set(i);
        BOOST_REQUIRE_EQUAL(i + 4, j);
        i = j;
    } while (i < 174);
    BOOST_REQUIRE_EQUAL(bits.find_next_set(i), utils::dynamic_bitset::npos);

    i = bits.find_first_clear();
    BOOST_REQUIRE_EQUAL(i, 0);
    do {
        auto j = bits.find_next_clear(i);
        if (i % 4 == 1) {
            BOOST_REQUIRE_EQUAL(i + 2, j);
        } else {
            BOOST_REQUIRE_EQUAL(i + 1, j);
        }
        i = j;
    } while (i < 177);
    BOOST_REQUIRE_EQUAL(bits.find_next_clear(i), utils::dynamic_bitset::npos);

    bits.resize(0);
    bits.resize(222);
    bits.set(4);
    bits.set(201);

    BOOST_REQUIRE_EQUAL(bits.find_first_set(), 4);
    BOOST_REQUIRE_EQUAL(bits.find_next_set(3), 4);
    BOOST_REQUIRE_EQUAL(bits.find_next_set(4), 201);
    BOOST_REQUIRE_EQUAL(bits.find_next_set(200), 201);
    BOOST_REQUIRE_EQUAL(bits.find_next_set(201), utils::dynamic_bitset::npos);
}

BOOST_AUTO_TEST_CASE(test_find_last_prev) {
    utils::dynamic_bitset bits;
    bits.resize(178);
    for (size_t i = 0; i < 178; i++) {
        BOOST_REQUIRE(!bits.test(i));
    }
    BOOST_REQUIRE_EQUAL(bits.find_last_set(), utils::dynamic_bitset::npos);

    for (size_t i = 0; i < 178; i += 2) {
        bits.set(i);
    }

    size_t i = bits.find_last_set();
    BOOST_REQUIRE_EQUAL(i, 176);
    do {
        auto j = bits.find_previous_set(i);
        BOOST_REQUIRE_EQUAL(i - 2, j);
        i = j;
    } while (i > 0);
    BOOST_REQUIRE_EQUAL(bits.find_previous_set(i), utils::dynamic_bitset::npos);

    i = bits.find_last_clear();
    BOOST_REQUIRE_EQUAL(i, 177);
    do {
        auto j = bits.find_previous_clear(i);
        BOOST_REQUIRE_EQUAL(i - 2, j);
        i = j;
    } while (i > 1);
    BOOST_REQUIRE_EQUAL(bits.find_previous_clear(i), utils::dynamic_bitset::npos);

    for (size_t i = 0; i < 178; i += 4) {
        bits.clear(i);
    }

    i = bits.find_last_set();
    BOOST_REQUIRE_EQUAL(i, 174);
    do {
        auto j = bits.find_previous_set(i);
        BOOST_REQUIRE_EQUAL(i - 4, j);
        i = j;
    } while (i > 4);
    BOOST_REQUIRE_EQUAL(bits.find_previous_set(i), utils::dynamic_bitset::npos);

    i = bits.find_last_clear();
    BOOST_REQUIRE_EQUAL(i, 177);
    do {
        auto j = bits.find_previous_clear(i);
        if (i % 4 == 3) {
            BOOST_REQUIRE_EQUAL(i - 2, j);
        } else {
            BOOST_REQUIRE_EQUAL(i - 1, j);
        }
        i = j;
    } while (i > 0);
    BOOST_REQUIRE_EQUAL(bits.find_previous_clear(i), utils::dynamic_bitset::npos);

    bits.resize(0);
    bits.resize(222);
    bits.set(4);
    bits.set(201);

    BOOST_REQUIRE_EQUAL(bits.find_last_set(), 201);
    BOOST_REQUIRE_EQUAL(bits.find_previous_set(202), 201);
    BOOST_REQUIRE_EQUAL(bits.find_previous_set(201), 4);
    BOOST_REQUIRE_EQUAL(bits.find_previous_set(5), 4);
    BOOST_REQUIRE_EQUAL(bits.find_previous_set(4), utils::dynamic_bitset::npos);
}

BOOST_AUTO_TEST_CASE(test_resize_grow) {
    utils::dynamic_bitset bits;

    bits.resize(1);
    BOOST_REQUIRE(!bits.test(0));

    bits.resize(2, true);
    BOOST_REQUIRE(!bits.test(0));
    BOOST_REQUIRE(bits.test(1));

    bits.resize(3);
    BOOST_REQUIRE(!bits.test(0));
    BOOST_REQUIRE(bits.test(1));
    BOOST_REQUIRE(!bits.test(2));

    bits.resize(4, true);
    BOOST_REQUIRE(!bits.test(0));
    BOOST_REQUIRE(bits.test(1));
    BOOST_REQUIRE(!bits.test(2));
    BOOST_REQUIRE(bits.test(3));

    bits.resize(124, true);
    BOOST_REQUIRE_EQUAL(bits.find_next_clear(3), utils::dynamic_bitset::npos);
    BOOST_REQUIRE_EQUAL(bits.find_last_set(), 123);
}