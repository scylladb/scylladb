/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>

#include "types/types.hh"
#include "types/comparable_bytes.hh"

BOOST_AUTO_TEST_CASE(test_comparable_bytes_opt) {
    BOOST_REQUIRE(comparable_bytes::from_data_value(data_value::make_null(int32_type)) == comparable_bytes_opt());
    BOOST_REQUIRE(comparable_bytes::from_serialized_bytes(*int32_type, managed_bytes_opt()) == comparable_bytes_opt());
}

BOOST_AUTO_TEST_CASE(test_bool) {
    auto test_bool_value = [] (comparable_bytes_opt& comparable_bytes, bool value) {
        BOOST_REQUIRE_EQUAL(comparable_bytes->size(), 1);
        BOOST_REQUIRE_MESSAGE(comparable_bytes->as_managed_bytes_view().front() == uint8_t(value),
                              fmt::format("comparable bytes encode failed for bool value : {}", value));
        BOOST_REQUIRE_MESSAGE(value == comparable_bytes->to_data_value(boolean_type),
                              fmt::format("comparable bytes decode failed for bool value : {}", value));
    };

    auto cb_false = comparable_bytes::from_data_value(false);
    test_bool_value(cb_false, false);
    auto cb_true = comparable_bytes::from_data_value(true);
    test_bool_value(cb_true, true);
    // Verify order
    BOOST_REQUIRE(cb_false < cb_true);
}
