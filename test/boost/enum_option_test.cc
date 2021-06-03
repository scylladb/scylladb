/*
 * Copyright (C) 2019-present ScyllaDB
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

#include <array>
#include <boost/program_options.hpp>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>

#include "utils/enum_option.hh"

namespace po = boost::program_options;

namespace {

struct days {
    enum enumeration : uint8_t { Mo, Tu, We, Th, Fr, Sa, Su };
    static std::unordered_map<std::string, enumeration> map() {
        return {{"Mon", Mo}, {"Tue", Tu}, {"Wed", We}, {"Thu", Th}, {"Fri", Fr}, {"Sat", Sa}, {"Sun", Su}};
    }
};

template <typename T>
enum_option<T> parse(const char* value) {
    po::options_description desc("Allowed options");
    desc.add_options()("opt", po::value<enum_option<T>>(), "Option");
    po::variables_map vm;
    const char* argv[] = {"$0", "--opt", value};
    po::store(po::parse_command_line(3, argv, desc), vm);
    return vm["opt"].as<enum_option<T>>();
}

template <typename T>
std::string format(typename T::enumeration d) {
    std::ostringstream os;
    os << enum_option<T>(d);
    return os.str();
}

} // anonymous namespace

BOOST_AUTO_TEST_CASE(test_parsing) {
    BOOST_CHECK_EQUAL(parse<days>("Sun"), days::Su);
    BOOST_CHECK_EQUAL(parse<days>("Mon"), days::Mo);
    BOOST_CHECK_EQUAL(parse<days>("Tue"), days::Tu);
    BOOST_CHECK_EQUAL(parse<days>("Wed"), days::We);
    BOOST_CHECK_EQUAL(parse<days>("Thu"), days::Th);
    BOOST_CHECK_EQUAL(parse<days>("Fri"), days::Fr);
    BOOST_CHECK_EQUAL(parse<days>("Sat"), days::Sa);
}

BOOST_AUTO_TEST_CASE(test_parsing_error) {
    BOOST_REQUIRE_THROW(parse<days>("Sunday"), po::invalid_option_value);
    BOOST_REQUIRE_THROW(parse<days>(""), po::invalid_option_value);
    BOOST_REQUIRE_THROW(parse<days>(" "), po::invalid_option_value);
    BOOST_REQUIRE_THROW(parse<days>(" Sun"), po::invalid_option_value);
}

BOOST_AUTO_TEST_CASE(test_formatting) {
    BOOST_CHECK_EQUAL(format<days>(days::Mo), "Mon");
    BOOST_CHECK_EQUAL(format<days>(days::Tu), "Tue");
    BOOST_CHECK_EQUAL(format<days>(days::We), "Wed");
    BOOST_CHECK_EQUAL(format<days>(days::Th), "Thu");
    BOOST_CHECK_EQUAL(format<days>(days::Fr), "Fri");
    BOOST_CHECK_EQUAL(format<days>(days::Sa), "Sat");
    BOOST_CHECK_EQUAL(format<days>(days::Su), "Sun");
}

BOOST_AUTO_TEST_CASE(test_formatting_unknown) {
    BOOST_CHECK_EQUAL(format<days>(static_cast<days::enumeration>(77)), "?unknown");
}

namespace {

struct names {
    enum enumeration : uint8_t { John, Jane, Jim };
    static std::map<std::string, enumeration> map() {
        return {{"John", John}, {"Jane", Jane}, {"James", Jim}};
    }
};

} // anonymous namespace

BOOST_AUTO_TEST_CASE(test_ordered_map) {
    BOOST_CHECK_EQUAL(parse<names>("James"), names::Jim);
    BOOST_CHECK_EQUAL(format<names>(names::Jim), "James");
    BOOST_CHECK_EQUAL(parse<names>("John"), names::John);
    BOOST_CHECK_EQUAL(format<names>(names::John), "John");
    BOOST_CHECK_EQUAL(parse<names>("Jane"), names::Jane);
    BOOST_CHECK_EQUAL(format<names>(names::Jane), "Jane");
    BOOST_CHECK_THROW(parse<names>("Jimbo"), po::invalid_option_value);
    BOOST_CHECK_EQUAL(format<names>(static_cast<names::enumeration>(77)), "?unknown");
}

namespace {

struct cities {
    enum enumeration { SF, TO, NY };
    static std::unordered_map<std::string, enumeration> map() {
        return {
            {"SanFrancisco", SF}, {"SF", SF}, {"SFO", SF}, {"Frisco", SF},
            {"Toronto", TO}, {"TO", TO}, {"YYZ", TO}, {"TheSix", TO},
            {"NewYork", NY}, {"NY", NY}, {"NYC", NY}, {"BigApple", NY},
        };
    }
};

} // anonymous namespace

BOOST_AUTO_TEST_CASE(test_multiple_parse) {
    BOOST_CHECK_EQUAL(parse<cities>("SanFrancisco"), cities::SF);
    BOOST_CHECK_EQUAL(parse<cities>("SF"), cities::SF);
    BOOST_CHECK_EQUAL(parse<cities>("SFO"), cities::SF);
    BOOST_CHECK_EQUAL(parse<cities>("Frisco"), cities::SF);
    BOOST_CHECK_EQUAL(parse<cities>("Toronto"), cities::TO);
    BOOST_CHECK_EQUAL(parse<cities>("TO"), cities::TO);
    BOOST_CHECK_EQUAL(parse<cities>("YYZ"), cities::TO);
    BOOST_CHECK_EQUAL(parse<cities>("TheSix"), cities::TO);
    BOOST_CHECK_EQUAL(parse<cities>("NewYork"), cities::NY);
    BOOST_CHECK_EQUAL(parse<cities>("NY"), cities::NY);
    BOOST_CHECK_EQUAL(parse<cities>("NYC"), cities::NY);
    BOOST_CHECK_EQUAL(parse<cities>("BigApple"), cities::NY);
}

BOOST_AUTO_TEST_CASE(test_multiple_format) {
    BOOST_CHECK((std::set<std::string>{"SanFrancisco", "SF", "SFO", "Frisco"}).contains(format<cities>(cities::SF)));
    BOOST_CHECK((std::set<std::string>{"Toronto", "TO", "YYZ", "TheSix"}).contains(format<cities>(cities::TO)));
    BOOST_CHECK((std::set<std::string>{"NewYork", "NY", "NYC", "BigApple"}).contains(format<cities>(cities::NY)));
}

namespace {

struct numbers {
    enum enumeration : uint8_t { ONE, TWO };
    static std::unordered_map<int, enumeration> map() {
        return {{1, ONE}, {2, TWO}};
    }
};

} // anonymous namespace

BOOST_AUTO_TEST_CASE(test_non_string) {
    BOOST_CHECK_EQUAL(parse<numbers>("1"), numbers::ONE);
    BOOST_CHECK_EQUAL(parse<numbers>("2"), numbers::TWO);
    BOOST_CHECK_THROW(parse<numbers>("3"), po::invalid_option_value);
    BOOST_CHECK_THROW(parse<numbers>("xx"), po::invalid_option_value);
    BOOST_CHECK_THROW(parse<numbers>(""), po::invalid_option_value);
    BOOST_CHECK_EQUAL(format<numbers>(numbers::ONE), "1");
    BOOST_CHECK_EQUAL(format<numbers>(numbers::TWO), "2");
    BOOST_CHECK_EQUAL(format<numbers>(static_cast<numbers::enumeration>(77)), "?unknown");
}
