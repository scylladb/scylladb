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
#include <seastar/util/closeable.hh>
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/log.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/test_utils.hh"
#include "utils/s3/client.hh"
#include "utils/s3/creds.hh"

s3::endpoint_config_ptr make_minio_config() {
    s3::endpoint_config cfg = {
        .port = std::stoul(tests::getenv_safe("S3_SERVER_PORT_FOR_TEST")),
    };
    return make_lw_shared<s3::endpoint_config>(std::move(cfg));
}

/*
 * Tests below expect minio server to be running on localhost
 * with the bucket named env['S3_PUBLIC_BUCKET_FOR_TEST'] created with
 * unrestricted anonymous read-write access
 */

SEASTAR_THREAD_TEST_CASE(test_client_put_get_object) {
    const sstring name(fmt::format("/{}/testobject-{}", tests::getenv_safe("S3_PUBLIC_BUCKET_FOR_TEST"), ::getpid()));

    testlog.info("Make client\n");
    auto cln = s3::client::make(tests::getenv_safe("S3_SERVER_ADDRESS_FOR_TEST"), make_minio_config());

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

SEASTAR_THREAD_TEST_CASE(test_client_multipart_upload) {
    const sstring name(fmt::format("/{}/testlargeobject-{}", tests::getenv_safe("S3_PUBLIC_BUCKET_FOR_TEST"), ::getpid()));

    testlog.info("Make client\n");
    auto cln = s3::client::make(tests::getenv_safe("S3_SERVER_ADDRESS_FOR_TEST"), make_minio_config());

    testlog.info("Upload object\n");
    auto out = output_stream<char>(cln->make_upload_sink(name));
    auto close = seastar::deferred_close(out);

    static constexpr unsigned chunk_size = 1024;
    auto rnd = tests::random::get_bytes(chunk_size);
    uint64_t object_size = 0;
    for (unsigned ch = 0; ch < 128 * 1024; ch++) {
        out.write(reinterpret_cast<char*>(rnd.begin()), rnd.size()).get();
        object_size += rnd.size();
    }

    testlog.info("Flush multipart upload\n");
    out.flush().get();

    testlog.info("Closing\n");
    close.close_now();

    testlog.info("Checking file size\n");
    size_t sz = cln->get_object_size(name).get0();
    BOOST_REQUIRE_EQUAL(sz, object_size);

    testlog.info("Checking correctness\n");
    for (int samples = 0; samples < 7; samples++) {
        uint64_t len = tests::random::get_int(1u, chunk_size);
        uint64_t off = tests::random::get_int(object_size) - len;

        auto s_buf = cln->get_object_contiguous(name, s3::range{ off, len }).get0();
        unsigned align = off % chunk_size;
        testlog.info("Got [{}:{}) chunk\n", off, len);
        testlog.info("Checking {} vs {} len {}\n", align, 0, std::min<uint64_t>(chunk_size - align, len));
        BOOST_REQUIRE_EQUAL(memcmp(rnd.begin() + align, s_buf.get(), std::min<uint64_t>(chunk_size - align, len)), 0);
        if (len > chunk_size - align) {
            testlog.info("Checking {} vs {} len {}\n", 0, chunk_size - align, len - (chunk_size - align));
            BOOST_REQUIRE_EQUAL(memcmp(rnd.begin(), s_buf.get() + (chunk_size - align), len - (chunk_size - align)), 0);
        }
    }

    testlog.info("Delete object\n");
    cln->delete_object(name).get();

    cln->close().get();
}

SEASTAR_THREAD_TEST_CASE(test_client_readable_file) {
    const sstring name(fmt::format("/{}/testroobject-{}", tests::getenv_safe("S3_PUBLIC_BUCKET_FOR_TEST"), ::getpid()));

    testlog.info("Make client\n");
    auto cln = s3::client::make(tests::getenv_safe("S3_SERVER_ADDRESS_FOR_TEST"), make_minio_config());

    testlog.info("Put object {}\n", name);
    temporary_buffer<char> data = sstring("1234567890ABCDEF").release();
    cln->put_object(name, std::move(data)).get();

    auto f = cln->make_readable_file(name);

    testlog.info("Check file size\n");
    size_t sz = f.size().get0();
    BOOST_REQUIRE_EQUAL(sz, 16);

    testlog.info("Check buffer read\n");
    char buffer[16];
    sz = f.dma_read(4, buffer, 7).get0();
    BOOST_REQUIRE_EQUAL(sz, 7);
    BOOST_REQUIRE_EQUAL(sstring(buffer, 7), sstring("567890A"));

    testlog.info("Check iovec read\n");
    std::vector<iovec> iovs;
    iovs.push_back({buffer, 3});
    iovs.push_back({buffer + 3, 2});
    iovs.push_back({buffer + 5, 4});
    sz = f.dma_read(3, std::move(iovs)).get0();
    BOOST_REQUIRE_EQUAL(sz, 9);
    BOOST_REQUIRE_EQUAL(sstring(buffer, 3), sstring("456"));
    BOOST_REQUIRE_EQUAL(sstring(buffer + 3, 2), sstring("78"));
    BOOST_REQUIRE_EQUAL(sstring(buffer + 5, 4), sstring("90AB"));

    testlog.info("Check bulk read\n");
    auto buf = f.dma_read_bulk<char>(5, 8).get0();
    BOOST_REQUIRE_EQUAL(to_sstring(std::move(buf)), sstring("67890ABC"));

    testlog.info("Delete object\n");
    cln->delete_object(name).get();
    testlog.info("Closing\n");
    f.close().get();
    cln->close().get();
}
