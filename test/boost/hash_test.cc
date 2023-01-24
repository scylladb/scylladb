/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <boost/test/unit_test.hpp>
#include <seastar/core/future.hh>

#include "seastarx.hh"
#include "test/lib/scylla_test_case.hh"
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
