/*
 * Copyright (C) 2017-present ScyllaDB
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
#include <utility>
#include "caching_options.hh"

BOOST_AUTO_TEST_CASE(test_caching_options) {
    using string_map = std::map<sstring, sstring>;
    {
        string_map in_map = { {"keys", "ALL"}, {"rows_per_partition", "NONE"}};
        caching_options co = caching_options::from_map(in_map);
        auto out_map = co.to_map();
        BOOST_REQUIRE(in_map == out_map);
    }
    {
        sstring in_str = "{\"keys\":\"NONE\",\"rows_per_partition\":\"10\"}";
        caching_options co = caching_options::from_sstring(in_str);
        sstring out_str = co.to_sstring();
        BOOST_REQUIRE_EQUAL(in_str, out_str);
    }
    {
        sstring in_str = "{\"keys\": \"SOME\", \"rows_per_partition\": \"ALL\"}";
        BOOST_REQUIRE_THROW(caching_options::from_sstring(in_str), std::exception);
    }
    {
        sstring in_str = "{\"keys\": \"NONE, }";
        BOOST_REQUIRE_THROW(caching_options::from_sstring(in_str), std::exception);
    }
}
