/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>

#include <exception>
#include <stdexcept>
#include <type_traits>
#include <cxxabi.h>
#include <boost/test/unit_test_log.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include "seastarx.hh"
#include <seastar/core/future.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/log.hh>

#include "utils/assert.hh"
#include "utils/exceptions.hh"

BOOST_AUTO_TEST_SUITE(exceptions_optimized_test)

#if defined(NO_OPTIMIZED_EXCEPTION_HANDLING)
#undef NO_OPTIMIZED_EXCEPTION_HANDLING
#endif

#include "utils/exceptions.hh"

#if defined(OPTIMIZED_EXCEPTION_HANDLING_AVAILABLE)

#include "exceptions_test.inc.cc"

#else

#include <boost/test/unit_test_log.hpp>

SEASTAR_TEST_CASE(test_noop) {
    BOOST_TEST_MESSAGE("Optimized implementation of handling exceptions "
            "without throwing is not available. Skipping tests in this file.");
    return seastar::make_ready_future<>();
}

#endif

BOOST_AUTO_TEST_SUITE_END()
