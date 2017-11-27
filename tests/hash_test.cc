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


#include <boost/test/unit_test.hpp>
#include <seastar/core/future.hh>

#include "seastarx.hh"
#include "tests/test-utils.hh"
#include "utils/hash.hh"

SEASTAR_TEST_CASE(test_pair_hash){
    auto hash_compare = [](auto p) {
        auto h1 = utils::tuple_hash()(p);
        auto h2 = utils::hash_combine(std::hash<decltype(p.first)>()(p.first), std::hash<decltype(p.second)>()(p.second));

        BOOST_CHECK_EQUAL(h1, h2);
    };

    hash_compare(std::make_pair(1, 2));
    hash_compare(std::make_pair(1.5, 2.1));
    hash_compare(std::make_pair("gris", "bacon"));
    hash_compare(std::make_pair(666, "bacon"));

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_tuple_hash){
    auto hash_compare = [](auto p, auto h2) {
        auto h1 = utils::tuple_hash()(p);

        BOOST_CHECK_EQUAL(h1, h2);
    };

    hash_compare(std::make_tuple(1, 2), utils::hash_combine(
            std::hash<int32_t>()(1)
            , std::hash<int32_t>()(2)
            ));
    hash_compare(std::make_tuple(1, 2, 3), utils::hash_combine(
            std::hash<int32_t>()(1)
            , utils::hash_combine(std::hash<int32_t>()(2)
                    , std::hash<int32_t>()(3))
            ));

    hash_compare(std::make_tuple("apa", "ko"), utils::hash_combine(
            std::hash<const char *>()("apa")
            , std::hash<const char *>()("ko")
            ));
    hash_compare(std::make_tuple("olof", 2, "kung"), utils::hash_combine(
            std::hash<const char *>()("olof")
            , utils::hash_combine(std::hash<int32_t>()(2)
                    , std::hash<const char *>()("kung"))
            ));

    return make_ready_future<>();
}
