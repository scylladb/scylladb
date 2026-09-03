/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>
#include <seastar/core/shared_ptr.hh>

#include "db/config.hh"
#include "exceptions/exceptions.hh"
#include "schema/speculative_retry_initializer.hh"

BOOST_AUTO_TEST_SUITE(speculative_retry_config_test)

// Test that registering the initializer with an invalid option value throws.
// (In a full ScyllaDB instance this makes startup fail; see test/cluster.)
BOOST_AUTO_TEST_CASE(test_invalid_speculative_retry_config) {
    auto cfg = seastar::make_shared<db::config>();
    cfg->read_from_yaml(R"foo(
        speculative_retry_user_table_default: FOO
        )foo");

    BOOST_REQUIRE_THROW(register_speculative_retry_initializer(*cfg), exceptions::configuration_exception);
}

BOOST_AUTO_TEST_SUITE_END()
