/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/testing/on_internal_error.hh>

#include "sstables/exceptions.hh"
#include "test/lib/exception_utils.hh"
#include "test/lib/scylla_test_case.hh"

using namespace sstables;

SEASTAR_THREAD_TEST_CASE(parse_assert_without_message_is_null_safe) {
    seastar::testing::scoped_no_abort_on_internal_error no_abort;

    BOOST_REQUIRE_EXCEPTION(
            [] {
                parse_assert(false);
            }(),
            malformed_sstable_exception, exception_predicate::message_equals("parse_assert() failed"));
}

SEASTAR_THREAD_TEST_CASE(on_parse_error_null_message_is_null_safe) {
    seastar::testing::scoped_no_abort_on_internal_error no_abort;

    BOOST_REQUIRE_EXCEPTION(
            [] {
                on_parse_error(static_cast<const char*>(nullptr), {});
            }(),
            malformed_sstable_exception, exception_predicate::message_equals("parse_assert() failed"));
}
