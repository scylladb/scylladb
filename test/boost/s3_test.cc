/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <boost/test/unit_test.hpp>
#include <seastar/core/thread.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/log.hh"
#include "utils/s3/client.hh"

/*
 * Tests below expect minio server to be running on localhost
 * with the bucket named "testbucket" created with unrestricted
 * anonymous read-write access
 */

SEASTAR_THREAD_TEST_CASE(test_client_put_get_object) {
    const ipv4_addr s3_server(::getenv("MINIO_SERVER_ADDRESS"), 9000);
    const sstring name(fmt::format("/testbucket/testobject-{}", ::getpid()));

    testlog.info("Make client\n");
    auto cln = s3::client::make(s3_server);

    testlog.info("Put object {}\n", name);
    temporary_buffer<char> data = sstring("1234567890").release();
    cln->put_object(name, std::move(data)).get();

    testlog.info("Get object size\n");
    size_t sz = cln->get_object_size(name).get0();
    BOOST_REQUIRE_EQUAL(sz, 10);

    testlog.info("Get object content\n");
    temporary_buffer<char> res = cln->get_object_contiguous(name).get0();
    BOOST_REQUIRE_EQUAL(to_sstring(std::move(res)), sstring("1234567890"));

    testlog.info("Get object part\n");
    res = cln->get_object_contiguous(name, s3::range{ 1, 3 }).get0();
    BOOST_REQUIRE_EQUAL(to_sstring(std::move(res)), sstring("234"));

    testlog.info("Delete object\n");
    cln->delete_object(name).get();

    testlog.info("Verify it's gone\n");
    BOOST_REQUIRE_EXCEPTION(cln->get_object_size(name).get(), std::runtime_error, seastar::testing::exception_predicate::message_contains("404 Not Found"));

    testlog.info("Closing\n");
    cln->close().get();
}
