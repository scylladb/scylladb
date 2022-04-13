/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#if defined(NO_OPTIMIZED_EXCEPTION_HANDLING)
#undef NO_OPTIMIZED_EXCEPTION_HANDLING
#endif

#include "utils/exceptions.hh"

#if defined(OPTIMIZED_EXCEPTION_HANDLING_AVAILABLE)

#include "exceptions_test.inc.cc"

#else

#include <boost/test/unit_test_log.hpp>
#include <seastar/testing/test_case.hh>

SEASTAR_TEST_CASE(test_noop) {
    BOOST_TEST_MESSAGE("Optimized implementation of handling exceptions "
            "without throwing is not available. Skipping tests in this file.");
    return seastar::make_ready_future<>();
}

#endif
