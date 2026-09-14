/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <memory>

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

/*
    Lends a test a bucket of its own on the run's S3 server, and takes it away
    again afterwards, so that a test neither inherits the objects of every test
    before it nor leaves its own behind.  This also keeps the S3 mock's
    per-bucket key listing -- a single metadata file it rewrites on every
    mutation -- from growing over the run and serialising unrelated tests.

    The bucket's name is published as S3_BUCKET_FOR_TEST for the lifetime of the
    fixture, which is where make_test_object_storage_options("S3") picks it up.
    With no S3 server configured (no S3_SERVER_ADDRESS_FOR_TEST) the fixture
    does nothing, leaving such a test to be skipped by its own precondition.

    Use it as a decorator, so that only the tests that need a bucket get one:

        SEASTAR_TEST_CASE(my_test, *tests::has_scylla_test_env,
                                   *seastar::testing::async_fixture<s3_fixture>()) {
 */
class s3_fixture {
    class impl;
    std::unique_ptr<impl> _impl;
public:
    s3_fixture();
    ~s3_fixture();

    // Empty when there is no S3 server for a bucket to live on.
    const seastar::sstring& bucket() const;

    seastar::future<> setup();
    seastar::future<> teardown();
};
