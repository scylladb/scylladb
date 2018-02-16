/*
 * Copyright (C) 2018 ScyllaDB
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

#include "enum_set.hh"

#include <iostream>
#include <iterator>
#include <unordered_set>

#include <boost/test/unit_test.hpp>

#include "to_string.hh"

enum class fruit { apple = 3, pear = 7, banana = 8 };

static std::ostream& operator<<(std::ostream& os, fruit f) {
    switch (f) {
        case fruit::apple: os << "apple"; break;
        case fruit::pear: os << "pear"; break;
        case fruit::banana: os << "banana"; break;
    }

    return os;
}

using fruit_enum = super_enum<fruit, fruit::apple, fruit::pear, fruit::banana>;

//
// `super_enum`
//

BOOST_AUTO_TEST_CASE(enum_max_sequence) {
    BOOST_REQUIRE_EQUAL(fruit_enum::max_sequence, 8);
}

BOOST_AUTO_TEST_CASE(enum_min_sequence) {
    BOOST_REQUIRE_EQUAL(fruit_enum::min_sequence, 3);
}

BOOST_AUTO_TEST_CASE(enum_valid_sequence) {
    BOOST_REQUIRE(fruit_enum::is_valid_sequence(3));
    BOOST_REQUIRE(fruit_enum::is_valid_sequence(7));
    BOOST_REQUIRE(fruit_enum::is_valid_sequence(8));

    BOOST_REQUIRE(!fruit_enum::is_valid_sequence(0));
    BOOST_REQUIRE(!fruit_enum::is_valid_sequence(4));
    BOOST_REQUIRE(!fruit_enum::is_valid_sequence(9));
}

//
// `enum_set`
//

using fruit_set = enum_set<fruit_enum>;

BOOST_AUTO_TEST_CASE(set_contains) {
    const auto fs = fruit_set::of<fruit::apple, fruit::banana>();
    BOOST_REQUIRE(fs.contains(fruit::apple));
    BOOST_REQUIRE(!fs.contains(fruit::pear));
}

BOOST_AUTO_TEST_CASE(set_from_mask) {
    const auto fs = fruit_set::of<fruit::apple, fruit::banana>();
    BOOST_REQUIRE_EQUAL(fs.mask(), fruit_set::from_mask(fs.mask()).mask());

    BOOST_REQUIRE_THROW(fruit_set::from_mask(0xdead), bad_enum_set_mask);
}

BOOST_AUTO_TEST_CASE(set_enable) {
    auto fs = fruit_set();
    fs.set(fruit::apple);
    BOOST_REQUIRE(fs.contains(fruit::apple));
}

BOOST_AUTO_TEST_CASE(set_enable_if) {
    auto fs = fruit_set();

    fs.set_if<fruit::apple>(false);
    BOOST_REQUIRE(!fs.contains(fruit::apple));

    fs.set_if<fruit::apple>(true);
    BOOST_REQUIRE(fs.contains(fruit::apple));
}

BOOST_AUTO_TEST_CASE(set_remove) {
    auto fs = fruit_set::of<fruit::pear>();
    fs.remove(fruit::pear);
    BOOST_REQUIRE(!fs.contains(fruit::pear));
}

BOOST_AUTO_TEST_CASE(set_iterator) {
    const auto fs1 = fruit_set::of<fruit::pear, fruit::banana>();

    std::unordered_set<fruit> us1;
    std::copy(fs1.begin(), fs1.end(), std::inserter(us1, us1.begin()));

    BOOST_REQUIRE_EQUAL(us1, (std::unordered_set<fruit>{fruit::pear, fruit::banana}));

    //
    // Empty set.
    //

    const auto fs2 = fruit_set();
    std::unordered_set<fruit> us2;
    std::copy(fs2.begin(), fs2.end(), std::inserter(us2, us2.begin()));

    BOOST_REQUIRE(us2.empty());
}
