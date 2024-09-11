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
