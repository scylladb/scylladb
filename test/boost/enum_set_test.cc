/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#define BOOST_TEST_MODULE core

#include "enum_set.hh"

#include <iterator>
#include <unordered_set>

#include <boost/test/unit_test.hpp>
#include <fmt/ostream.h>
#include <fmt/ranges.h>


enum class fruit { apple = 3, pear = 7, banana = 8 };

template <> struct fmt::formatter<fruit> : fmt::formatter<string_view> {
    auto format(fruit f, fmt::format_context& ctx) const {
        std::string_view name;
        using enum fruit;
        switch (f) {
            case apple: name = "apple"; break;
            case pear: name = "pear"; break;
            case banana: name = "banana"; break;
        }
        return formatter<string_view>::format(name, ctx);
    }
};

namespace std {
std::ostream& boost_test_print_type(std::ostream& os, const std::unordered_set<fruit>& fruits) {
    fmt::print(os, "{}", fruits);
    return os;
}
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

BOOST_AUTO_TEST_CASE(full_set) {
    const auto fs = fruit_set::full();
    BOOST_REQUIRE(fs.contains(fruit::apple));
    BOOST_REQUIRE(fs.contains(fruit::pear));
    BOOST_REQUIRE(fs.contains(fruit::banana));
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

BOOST_AUTO_TEST_CASE(set_add) {
    auto fs0 = fruit_set();
    auto fs1 = fruit_set::of<fruit::pear>();
    auto fs2 = fruit_set::of<fruit::apple, fruit::banana>();
    auto fs3 = fruit_set::of<fruit::apple>();
    fs1.add(fs2);
    fs3.add(fs2);
    BOOST_REQUIRE(fs1.contains(fruit::pear) && fs1.contains(fruit::apple) && fs1.contains(fruit::banana));
    BOOST_REQUIRE(!fs3.contains(fruit::pear) && fs3.contains(fruit::apple) && fs3.contains(fruit::banana));
    fs1.add(fs0);
    fs3.add(fs0);
    BOOST_REQUIRE(fs1.contains(fruit::pear) && fs1.contains(fruit::apple) && fs1.contains(fruit::banana));
    BOOST_REQUIRE(!fs3.contains(fruit::pear) && fs3.contains(fruit::apple) && fs3.contains(fruit::banana));
    fs0.add(fruit_set::of<fruit::apple>());
    BOOST_REQUIRE(!fs0.contains(fruit::pear) && fs0.contains(fruit::apple) && !fs0.contains(fruit::banana));
}

BOOST_AUTO_TEST_CASE(set_toggle) {
    auto fs = fruit_set();
    fs.set<fruit::pear>();

    BOOST_REQUIRE(!fs.contains<fruit::apple>());
    BOOST_REQUIRE(fs.contains<fruit::pear>());
    BOOST_REQUIRE(!fs.contains<fruit::banana>());

    fs.toggle<fruit::pear>();

    BOOST_REQUIRE(!fs.contains<fruit::apple>());
    BOOST_REQUIRE(!fs.contains<fruit::pear>());
    BOOST_REQUIRE(!fs.contains<fruit::banana>());

    fs.toggle<fruit::pear>();

    BOOST_REQUIRE(!fs.contains<fruit::apple>());
    BOOST_REQUIRE(fs.contains<fruit::pear>());
    BOOST_REQUIRE(!fs.contains<fruit::banana>());

    fs.toggle(fruit::pear);

    BOOST_REQUIRE(!fs.contains<fruit::apple>());
    BOOST_REQUIRE(!fs.contains<fruit::pear>());
    BOOST_REQUIRE(!fs.contains<fruit::banana>());

    fs.toggle(fruit::pear);

    BOOST_REQUIRE(!fs.contains<fruit::apple>());
    BOOST_REQUIRE(fs.contains<fruit::pear>());
    BOOST_REQUIRE(!fs.contains<fruit::banana>());

    fs.toggle<fruit::banana>();

    BOOST_REQUIRE(!fs.contains<fruit::apple>());
    BOOST_REQUIRE(fs.contains<fruit::pear>());
    BOOST_REQUIRE(fs.contains<fruit::banana>());

    fs.toggle<fruit::apple>();

    BOOST_REQUIRE(fs.contains<fruit::apple>());
    BOOST_REQUIRE(fs.contains<fruit::pear>());
    BOOST_REQUIRE(fs.contains<fruit::banana>());

    fs.toggle(fruit::banana);

    BOOST_REQUIRE(fs.contains<fruit::apple>());
    BOOST_REQUIRE(fs.contains<fruit::pear>());
    BOOST_REQUIRE(!fs.contains<fruit::banana>());

    fs.toggle(fruit::apple);

    BOOST_REQUIRE(!fs.contains<fruit::apple>());
    BOOST_REQUIRE(fs.contains<fruit::pear>());
    BOOST_REQUIRE(!fs.contains<fruit::banana>());
}
