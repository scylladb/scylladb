/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <unordered_set>
#include <boost/test/unit_test.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <seastar/core/thread.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/http/exception.hh>
#include <seastar/util/closeable.hh>
#include <seastar/util/short_streams.hh>
#include <seastar/core/units.hh>
#include "test/lib/scylla_test_case.hh"
#include "test/lib/log.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/test_utils.hh"
#include "test/lib/tmpdir.hh"
#include "utils/assert.hh"
#include "utils/s3/client.hh"
#include "utils/s3/creds.hh"
#include "utils/exceptions.hh"
#include "sstables/checksum_utils.hh"
#include "gc_clock.hh"

// The test can be run on real AWS-S3 bucket. For that, create a bucket with
// permissive enough policy and then run the test with env set respectively
// E.g. like this
//
//   export S3_SERVER_ADDRESS_FOR_TEST=s3.us-east-2.amazonaws.com
//   export S3_SERVER_PORT_FOR_TEST=443
//   export S3_BUCKET_FOR_TEST=xemul
//   export AWS_ACCESS_KEY_ID=${aws_access_key_id}
//   export AWS_SECRET_ACCESS_KEY=${aws_secret_access_key}
//   export AWS_SESSION_TOKEN=${aws_session_token}
//   export AWS_DEFAULT_REGION="us-east-2"

s3::endpoint_config_ptr make_minio_config() {
    s3::endpoint_config cfg = {
        .port = std::stoul(tests::getenv_safe("S3_SERVER_PORT_FOR_TEST")),
        .use_https = ::getenv("AWS_DEFAULT_REGION") != nullptr,
        .aws = {{
            .access_key_id = tests::getenv_safe("AWS_ACCESS_KEY_ID"),
            .secret_access_key = tests::getenv_safe("AWS_SECRET_ACCESS_KEY"),
            .session_token = ::getenv("AWS_SESSION_TOKEN") ? : "",
            .region = ::getenv("AWS_DEFAULT_REGION") ? : "local",
        }},
    };
    return make_lw_shared<s3::endpoint_config>(std::move(cfg));
}

/*
 * Tests below expect minio server to be running on localhost
 * with the bucket named env['S3_BUCKET_FOR_TEST'] created with
 * unrestricted anonymous read-write access
 */

SEASTAR_THREAD_TEST_CASE(test_client_put_get_object) {
    const sstring name(fmt::format("/{}/testobject-{}", tests::getenv_safe("S3_BUCKET_FOR_TEST"), ::getpid()));

    testlog.info("Make client\n");
    semaphore mem(16<<20);
    auto cln = s3::client::make(tests::getenv_safe("S3_SERVER_ADDRESS_FOR_TEST"), make_minio_config(), mem);
    auto close_client = deferred_close(*cln);

    testlog.info("Put object {}\n", name);
    temporary_buffer<char> data = sstring("1234567890").release();
    cln->put_object(name, std::move(data)).get();

    testlog.info("Get object size\n");
    size_t sz = cln->get_object_size(name).get();
    BOOST_REQUIRE_EQUAL(sz, 10);

    testlog.info("Get object stats\n");
    s3::stats st = cln->get_object_stats(name).get();
    BOOST_REQUIRE_EQUAL(st.size, 10);
    // forgive timezone difference as minio server is GMT by default
    BOOST_REQUIRE(std::difftime(st.last_modified, gc_clock::to_time_t(gc_clock::now())) < 24*3600);

    testlog.info("Get object content\n");
    temporary_buffer<char> res = cln->get_object_contiguous(name).get();
    BOOST_REQUIRE_EQUAL(to_sstring(std::move(res)), sstring("1234567890"));

    testlog.info("Get object part\n");
    res = cln->get_object_contiguous(name, s3::range{ 1, 3 }).get();
    BOOST_REQUIRE_EQUAL(to_sstring(std::move(res)), sstring("234"));

    testlog.info("Delete object\n");
    cln->delete_object(name).get();

    testlog.info("Verify it's gone\n");
    BOOST_REQUIRE_EXCEPTION(cln->get_object_size(name).get(), storage_io_error, [] (const storage_io_error& ex) {
        return ex.code().value() == ENOENT;
    });
}

static auto deferred_delete_object(shared_ptr<s3::client> client, sstring name) {
    return seastar::defer([client, name] {
        testlog.info("Delete object: {}\n", name);
        client->delete_object(name).get();
    });
}

void do_test_client_multipart_upload(bool with_copy_upload) {
    const sstring name(fmt::format("/{}/test{}object-{}", tests::getenv_safe("S3_BUCKET_FOR_TEST"), with_copy_upload ? "jumbo" : "large", ::getpid()));

    testlog.info("Make client\n");
    semaphore mem(16<<20);
    auto cln = s3::client::make(tests::getenv_safe("S3_SERVER_ADDRESS_FOR_TEST"), make_minio_config(), mem);
    auto close_client = deferred_close(*cln);

    testlog.info("Upload object (with copy = {})\n", with_copy_upload);
    auto out = output_stream<char>(
        // Make it 3 parts per piece, so that 128Mb buffer below
        // would be split into several 15Mb pieces
        with_copy_upload ? cln->make_upload_jumbo_sink(name, 3) : cln->make_upload_sink(name)
    );
    auto close = seastar::deferred_close(out);

    static constexpr unsigned chunk_size = 1000;
    auto rnd = tests::random::get_bytes(chunk_size);
    uint64_t object_size = 0;
    for (unsigned ch = 0; ch < 128 * 1024; ch++) {
        out.write(reinterpret_cast<char*>(rnd.begin()), rnd.size()).get();
        object_size += rnd.size();
    }

    testlog.info("Flush multipart upload\n");
    out.flush().get();
    auto delete_object = deferred_delete_object(cln, name);

    testlog.info("Closing\n");
    close.close_now();

    testlog.info("Checking file size\n");
    size_t sz = cln->get_object_size(name).get();
    BOOST_REQUIRE_EQUAL(sz, object_size);

    testlog.info("Checking correctness\n");
    for (int samples = 0; samples < 7; samples++) {
        uint64_t len = tests::random::get_int(1u, chunk_size);
        uint64_t off = tests::random::get_int(object_size - len);

        auto s_buf = cln->get_object_contiguous(name, s3::range{ off, len }).get();
        unsigned align = off % chunk_size;
        testlog.info("Got [{}:{}) chunk\n", off, len);
        testlog.info("Checking {} vs {} len {}\n", align, 0, std::min<uint64_t>(chunk_size - align, len));
        BOOST_REQUIRE_EQUAL(memcmp(rnd.begin() + align, s_buf.get(), std::min<uint64_t>(chunk_size - align, len)), 0);
        if (len > chunk_size - align) {
            testlog.info("Checking {} vs {} len {}\n", 0, chunk_size - align, len - (chunk_size - align));
            BOOST_REQUIRE_EQUAL(memcmp(rnd.begin(), s_buf.get() + (chunk_size - align), len - (chunk_size - align)), 0);
        }
    }
}

SEASTAR_THREAD_TEST_CASE(test_client_multipart_upload) {
    do_test_client_multipart_upload(false);
}

SEASTAR_THREAD_TEST_CASE(test_client_multipart_copy_upload) {
    do_test_client_multipart_upload(true);
}

SEASTAR_THREAD_TEST_CASE(test_client_multipart_upload_fallback) {
    const sstring name(fmt::format("/{}/testfbobject-{}", tests::getenv_safe("S3_BUCKET_FOR_TEST"), ::getpid()));

    testlog.info("Make client");
    semaphore mem(0);
    mem.broken(); // so that any attempt to use it throws
    auto cln = s3::client::make(tests::getenv_safe("S3_SERVER_ADDRESS_FOR_TEST"), make_minio_config(), mem);
    auto close_client = deferred_close(*cln);

    testlog.info("Upload object");
    auto out = output_stream<char>(cln->make_upload_sink(name));
    auto close = seastar::deferred_close(out);

    temporary_buffer<char> data = sstring("1A3B5C7890").release();
    out.write(reinterpret_cast<const char*>(data.begin()), data.size()).get();

    testlog.info("Flush upload");
    out.flush().get(); // if it tries to do regular flush, memory claim would throw
    auto delete_object = deferred_delete_object(cln, name);

    testlog.info("Closing");
    close.close_now();

    testlog.info("Get object content");
    temporary_buffer<char> res = cln->get_object_contiguous(name).get();
    BOOST_REQUIRE_EQUAL(to_sstring(std::move(res)), to_sstring(std::move(data)));
}

using with_remainder_t = bool_class<class with_remainder_tag>;

future<> test_client_upload_file(std::string_view test_name, size_t total_size, size_t memory_size) {
    tmpdir tmp;
    const auto file_path = tmp.path() / "test";

    uint32_t expected_checksum = crc32_utils::init_checksum();

    // 1. prefill the data file to be uploaded
    {
        file f = co_await open_file_dma(file_path.native(), open_flags::create | open_flags::wo);
        auto output = co_await make_file_output_stream(std::move(f));
        std::string_view data = "1234567890ABCDEF";
        // so we can test !with_remainder case properly with multiple writes
        SCYLLA_ASSERT(total_size % data.size() == 0);

        for (size_t bytes_written = 0;
             bytes_written < total_size;
             bytes_written += data.size()) {
            co_await output.write(data.data(), data.size());
            uint32_t chunk_checksum = crc32_utils::checksum(data.data(), data.size());
            expected_checksum = checksum_combine_or_feed<crc32_utils>(
                expected_checksum, chunk_checksum, data.data(), data.size());
        }
        co_await output.close();
    }

    const auto object_name = fmt::format("/{}/{}-{}",
                                         tests::getenv_safe("S3_BUCKET_FOR_TEST"),
                                         test_name,
                                         ::getpid());

    // 2. upload the file to s3
    semaphore mem{memory_size};
    auto client = s3::client::make(tests::getenv_safe("S3_SERVER_ADDRESS_FOR_TEST"),
                                   make_minio_config(),
                                   mem);
    co_await client->upload_file(file_path, object_name);
    // 3. retrieve the object from s3 and retrieve the object from S3 and
    //    compare it with the pattern
    uint32_t actual_checksum = crc32_utils::init_checksum();
    auto readable_file = client->make_readable_file(object_name);
    auto input = make_file_input_stream(readable_file);
    size_t actual_size = 0;
    for (;;) {
        auto buf = co_await input.read();
        if (buf.empty()) {
            // empty() signifies the end of stream
            break;
        }
        actual_size += buf.size();
        bytes_view bv{reinterpret_cast<const int8_t*>(buf.get()), buf.size()};
        //fmt::print("{}", fmt_hex(bv));
        uint32_t chunk_checksum = crc32_utils::checksum(buf.get(), buf.size());
        actual_checksum = checksum_combine_or_feed<crc32_utils>(
            actual_checksum, chunk_checksum, buf.get(), buf.size());
    }
    BOOST_CHECK_EQUAL(total_size, actual_size);
    BOOST_CHECK_EQUAL(expected_checksum, actual_checksum);

    co_await readable_file.close();
    co_await input.close();
    co_await client->close();
}

SEASTAR_TEST_CASE(test_client_upload_file_multi_part_without_remainder) {
    const size_t part_size = 5_MiB;
    const size_t total_size = 4 * part_size;
    const size_t memory_size = part_size;
    co_await test_client_upload_file(seastar_test::get_name(), total_size, memory_size);
}

SEASTAR_TEST_CASE(test_client_upload_file_multi_part_with_remainder) {
    const size_t part_size = 5_MiB;
    const size_t remainder_size = part_size / 2;
    const size_t total_size = 4 * part_size + remainder_size;
    const size_t memory_size = part_size;
    co_await test_client_upload_file(seastar_test::get_name(), total_size, memory_size);
}

SEASTAR_TEST_CASE(test_client_upload_file_single_part) {
    const size_t part_size = 5_MiB;
    const size_t total_size = part_size / 2;
    const size_t memory_size = part_size;
    co_await test_client_upload_file(seastar_test::get_name(), total_size, memory_size);
}

SEASTAR_THREAD_TEST_CASE(test_client_readable_file) {
    const sstring name(fmt::format("/{}/testroobject-{}", tests::getenv_safe("S3_BUCKET_FOR_TEST"), ::getpid()));

    testlog.info("Make client\n");
    semaphore mem(16<<20);
    auto cln = s3::client::make(tests::getenv_safe("S3_SERVER_ADDRESS_FOR_TEST"), make_minio_config(), mem);
    auto close_client = deferred_close(*cln);

    testlog.info("Put object {}\n", name);
    temporary_buffer<char> data = sstring("1234567890ABCDEF").release();
    cln->put_object(name, std::move(data)).get();
    auto delete_object = deferred_delete_object(cln, name);

    auto f = cln->make_readable_file(name);
    auto close_readable_file = deferred_close(f);

    testlog.info("Check file size\n");
    size_t sz = f.size().get();
    BOOST_REQUIRE_EQUAL(sz, 16);

    testlog.info("Check buffer read\n");
    char buffer[16];
    sz = f.dma_read(4, buffer, 7).get();
    BOOST_REQUIRE_EQUAL(sz, 7);
    BOOST_REQUIRE_EQUAL(sstring(buffer, 7), sstring("567890A"));

    testlog.info("Check iovec read\n");
    std::vector<iovec> iovs;
    iovs.push_back({buffer, 3});
    iovs.push_back({buffer + 3, 2});
    iovs.push_back({buffer + 5, 4});
    sz = f.dma_read(3, std::move(iovs)).get();
    BOOST_REQUIRE_EQUAL(sz, 9);
    BOOST_REQUIRE_EQUAL(sstring(buffer, 3), sstring("456"));
    BOOST_REQUIRE_EQUAL(sstring(buffer + 3, 2), sstring("78"));
    BOOST_REQUIRE_EQUAL(sstring(buffer + 5, 4), sstring("90AB"));

    testlog.info("Check bulk read\n");
    auto buf = f.dma_read_bulk<char>(5, 8).get();
    BOOST_REQUIRE_EQUAL(to_sstring(std::move(buf)), sstring("67890ABC"));
}

SEASTAR_THREAD_TEST_CASE(test_client_readable_file_stream) {
    const sstring name(fmt::format("/{}/teststreamobject-{}", tests::getenv_safe("S3_BUCKET_FOR_TEST"), ::getpid()));

    testlog.info("Make client\n");
    semaphore mem(16<<20);
    auto cln = s3::client::make(tests::getenv_safe("S3_SERVER_ADDRESS_FOR_TEST"), make_minio_config(), mem);
    auto close_client = deferred_close(*cln);

    testlog.info("Put object {}\n", name);
    sstring sample("1F2E3D4C5B6A70899807A6B5C4D3E2F1");
    temporary_buffer<char> data(sample.c_str(), sample.size());
    cln->put_object(name, std::move(data)).get();
    auto delete_object = deferred_delete_object(cln, name);

    auto f = cln->make_readable_file(name);
    auto close_readable_file = deferred_close(f);
    auto in = make_file_input_stream(f);
    auto close_stream = deferred_close(in);

    testlog.info("Check input stream read\n");
    auto res = seastar::util::read_entire_stream_contiguous(in).get();
    BOOST_REQUIRE_EQUAL(res, sample);
}

SEASTAR_THREAD_TEST_CASE(test_client_put_get_tagging) {
    const sstring name(fmt::format("/{}/testobject-{}",
                                   tests::getenv_safe("S3_BUCKET_FOR_TEST"), ::getpid()));
    semaphore mem(16<<20);
    auto client = s3::client::make(tests::getenv_safe("S3_SERVER_ADDRESS_FOR_TEST"), make_minio_config(), mem);
    auto close_client = deferred_close(*client);
    auto data = sstring("1234567890ABCDEF").release();
    client->put_object(name, std::move(data)).get();
    auto delete_object = deferred_delete_object(client, name);

    {
        auto tagset = client->get_object_tagging(name).get();
        BOOST_CHECK(tagset.empty());
    }
    {
        s3::tag_set expected_tagset{{"1", "one"}, {"2", "two"}};
        client->put_object_tagging(name, expected_tagset).get();
        auto actual_tagset = client->get_object_tagging(name).get();
        std::ranges::sort(actual_tagset);
        std::ranges::sort(expected_tagset);
        BOOST_CHECK(actual_tagset == expected_tagset);
    }
    {
        client->delete_object_tagging(name).get();
        auto tagset = client->get_object_tagging(name).get();
        BOOST_CHECK(tagset.empty());
    }
}

static std::unordered_set<sstring> populate_bucket(shared_ptr<s3::client> client, sstring bucket, sstring prefix, int nr_objects) {
    std::unordered_set<sstring> names;

    for (int i = 0; i < nr_objects; i++) {
        temporary_buffer<char> data = sstring("1234567890").release();
        auto name = format("obj.{}", i);
        client->put_object(format("/{}/{}{}", bucket, prefix, name), std::move(data)).get();
        names.insert(name);
    }

    return names;
}

SEASTAR_THREAD_TEST_CASE(test_client_list_objects) {
    const sstring bucket = tests::getenv_safe("S3_BUCKET_FOR_TEST");
    const sstring prefix(fmt::format("testprefix-{}/", ::getpid()));
    semaphore mem(16<<20);
    auto client = s3::client::make(tests::getenv_safe("S3_SERVER_ADDRESS_FOR_TEST"), make_minio_config(), mem);
    auto close_client = deferred_close(*client);

    // Put extra object to check list-by-prefix filters it out
    temporary_buffer<char> data = sstring("1234567890").release();
    client->put_object(format("/{}/extra-{}", bucket, ::getpid()), std::move(data)).get();
    auto names = populate_bucket(client, bucket, prefix, 12);

    s3::client::bucket_lister lister(client, bucket, prefix, 5);
    auto close_lister = deferred_close(lister);

    while (auto de = lister.get().get()) {
        testlog.info("-> [{}]", de->name);
        auto it = names.find(de->name);
        BOOST_REQUIRE(it != names.end());
        names.erase(it);
    }
    BOOST_REQUIRE(names.empty());
}

SEASTAR_THREAD_TEST_CASE(test_client_list_objects_incomplete) {
    const sstring bucket = tests::getenv_safe("S3_BUCKET_FOR_TEST");
    const sstring prefix(fmt::format("testprefix-{}/", ::getpid()));
    semaphore mem(16<<20);
    auto client = s3::client::make(tests::getenv_safe("S3_SERVER_ADDRESS_FOR_TEST"), make_minio_config(), mem);
    auto close_client = deferred_close(*client);

    populate_bucket(client, bucket, prefix, 8);

    s3::client::bucket_lister lister(client, bucket, prefix, 9, 2);
    auto close_lister = deferred_close(lister);

    // Peek one entry to start lister work
    auto de = lister.get().get();
    close_lister.close_now();
}
