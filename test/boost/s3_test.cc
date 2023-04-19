/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <boost/test/unit_test.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
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
#include "gc_clock.hh"

// The test can be run on real AWS-S3 bucket. For that, create a bucket with
// permissive enough policy and then run the test with AWS_S3_EXTRA env set
// to key:secret:region string. E.g. like this
//
//   export S3_SERVER_ADDRESS_FOR_TEST=s3.us-east-2.amazonaws.com
//   export S3_SERVER_PORT_FOR_TEST=443
//   export S3_PUBLIC_BUCKET_FOR_TEST=xemul
//   export AWS_S3_EXTRA="${aws_key}:${aws_secret}:us-east-2"

s3::endpoint_config_ptr make_minio_config() {
    s3::endpoint_config cfg = {
        .port = std::stoul(tests::getenv_safe("S3_SERVER_PORT_FOR_TEST")),
    };
    auto extra = ::getenv("AWS_S3_EXTRA");
    if (extra) {
        std::vector<std::string> items;
        boost::split(items, extra, boost::is_any_of(":"));
        if (items.size() != 3) {
            throw std::runtime_error("Invalid endpoint format, expected host:port");
        }
        testlog.info("Adding AWS configuration to endpoint");
        cfg.use_https = true;
        cfg.aws.emplace();
        cfg.aws->key = items[0];
        cfg.aws->secret = items[1];
        cfg.aws->region = items[2];
    }
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

    testlog.info("Get object stats\n");
    s3::client::stats st = cln->get_object_stats(name).get0();
    BOOST_REQUIRE_EQUAL(st.size, 10);
    // forgive timezone difference as minio server is GMT by default
    BOOST_REQUIRE(std::difftime(st.last_modified, gc_clock::to_time_t(gc_clock::now())) < 24*3600);

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

void do_test_client_multipart_upload(bool with_copy_upload) {
    const sstring name(fmt::format("/{}/test{}object-{}", tests::getenv_safe("S3_PUBLIC_BUCKET_FOR_TEST"), with_copy_upload ? "jumbo" : "large", ::getpid()));

    testlog.info("Make client\n");
    auto cln = s3::client::make(tests::getenv_safe("S3_SERVER_ADDRESS_FOR_TEST"), make_minio_config());

    testlog.info("Upload object (with copy = {})\n", with_copy_upload);
    auto out = output_stream<char>(
        // Make it 3 parts per piece, so that 128Mb buffer below
        // would be split into several 15Mb pieces
        with_copy_upload ? cln->make_upload_jumbo_sink(name, 3) : cln->make_upload_sink(name)
    );
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

SEASTAR_THREAD_TEST_CASE(test_client_multipart_upload) {
    do_test_client_multipart_upload(false);
}

SEASTAR_THREAD_TEST_CASE(test_client_multipart_copy_upload) {
    do_test_client_multipart_upload(true);
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
