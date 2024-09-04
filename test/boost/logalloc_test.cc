/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <boost/test/unit_test.hpp>
#include <boost/intrusive/parent_from_member.hpp>
#include <algorithm>

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/format.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread_cputime_clock.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/with_timeout.hh>
#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/random.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include "utils/assert.hh"
#include "utils/logalloc.hh"
#include "utils/managed_ref.hh"
#include "utils/managed_bytes.hh"
#include "test/lib/log.hh"
#ifndef SEASTAR_DEFAULT_ALLOCATOR
#include "utils/chunked_vector.hh"
#include "utils/logalloc.hh"
#include "utils/lsa/weak_ptr.hh"
#include "test/lib/make_random_string.hh"
#endif
#include "utils/log.hh"

BOOST_AUTO_TEST_SUITE(logalloc_test)

#include "./logalloc_test.inc.cc"

BOOST_AUTO_TEST_SUITE_END()
