/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <seastar/testing/test_case.hh>
#include "test/lib/scylla_test_case.hh"

#include "utils/tagged_integer.hh"

using namespace seastar;

using test_tagged_int = utils::tagged_integer<struct test_int_tag, int>;

SEASTAR_THREAD_TEST_CASE(test_tagged_integer_ops) {
    int i = 17;
    auto t = test_tagged_int(i);

    BOOST_REQUIRE_EQUAL(t.value(), i);
    BOOST_REQUIRE_EQUAL((++t).value(), ++i);
    BOOST_REQUIRE_EQUAL((--t).value(), --i);
    BOOST_REQUIRE_EQUAL((t++).value(), i++);
    BOOST_REQUIRE_EQUAL((t--).value(), i--);
    BOOST_REQUIRE_EQUAL((t + test_tagged_int(42)).value(), i + 42);
    BOOST_REQUIRE_EQUAL((t - test_tagged_int(42)).value(), i - 42);
    BOOST_REQUIRE_EQUAL((t += test_tagged_int(42)).value(), i += 42);
    BOOST_REQUIRE_EQUAL((t -= test_tagged_int(42)).value(), i -= 42);
}
