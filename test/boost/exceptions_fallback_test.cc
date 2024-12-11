/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>

#define NO_OPTIMIZED_EXCEPTION_HANDLING

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

BOOST_AUTO_TEST_SUITE(exceptions_fallback_test)

#include "exceptions_test.inc.cc"

BOOST_AUTO_TEST_SUITE_END()
