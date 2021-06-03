
/*
 * Copyright (C) 2015-present ScyllaDB
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

#define BOOST_TEST_MODULE json

#include <boost/test/unit_test.hpp>

#include <seastar/core/sstring.hh>

#include "utils/rjson.hh"

using namespace seastar;

BOOST_AUTO_TEST_CASE(test_value_to_quoted_string) {
    std::vector<sstring> input = {
            "\"\\\b\f\n\r\t",
            sstring(1, '\0') + "\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f",
            "regular string",
            "mixed\t\t\t\ba\f \007 string \002 fgh",
            "chào mọi người 123!",
            "ყველას მოგესალმებით 456?;",
            "всем привет",
            "大家好",
            ""
    };

    std::vector<sstring> expected = {
            "\"\\\"\\\\\\b\\f\\n\\r\\t\"",
            "\"\\u0000\\u0001\\u0002\\u0003\\u0004\\u0005\\u0006\\u0007\\b\\t\\n\\u000B\\f\\r\\u000E\\u000F\\u0010\\u0011\\u0012\\u0013\\u0014\\u0015\\u0016\\u0017\\u0018\\u0019\\u001A\\u001B\\u001C\\u001D\\u001E\\u001F\"",
            "\"regular string\"",
            "\"mixed\\t\\t\\t\\ba\\f \\u0007 string \\u0002 fgh\"",
            "\"chào mọi người 123!\"",
            "\"ყველას მოგესალმებით 456?;\"",
            "\"всем привет\"",
            "\"大家好\"",
            "\"\""
    };

    for (size_t i = 0; i < input.size(); ++i) {
        BOOST_CHECK_EQUAL(rjson::quote_json_string(input[i]), expected[i]);
    }
}

BOOST_AUTO_TEST_CASE(test_parsing_map_from_null) {
    std::map<sstring, sstring> empty_map;
    auto map1 = rjson::parse_to_map<std::map<sstring, sstring>>("null");
    auto map2 = rjson::parse_to_map<std::map<sstring, sstring>>("{}");
    BOOST_REQUIRE(map1 == map2);
    BOOST_REQUIRE(map1 == empty_map);
}
