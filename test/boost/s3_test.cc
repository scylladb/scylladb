/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */


#include <unordered_set>
#include <boost/test/unit_test.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <seastar/core/thread.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/sleep.hh>
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
#include "utils/error_injection.hh"
#include "utils/s3/aws_error.hh"
#include "utils/s3/client.hh"
#include "utils/s3/creds.hh"
#include "utils/s3/utils/manip_s3.hh"
#include "utils/exceptions.hh"
#include "utils/s3/credentials_providers/aws_credentials_provider_chain.hh"
#include "utils/s3/credentials_providers/instance_profile_credentials_provider.hh"
#include "utils/s3/credentials_providers/sts_assume_role_credentials_provider.hh"
#include "sstables/checksum_utils.hh"
#include "gc_clock.hh"

using namespace std::string_view_literals;
using namespace std::chrono_literals;

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

static shared_ptr<s3::client> make_proxy_client(semaphore& mem) {
    s3::endpoint_config cfg = {
        .port = std::stoul(tests::getenv_safe("PROXY_S3_SERVER_PORT")),
        .use_https = false,
        .region = ::getenv("AWS_DEFAULT_REGION") ? : "local",
    };
    return s3::client::make(tests::getenv_safe("PROXY_S3_SERVER_HOST"), make_lw_shared<s3::endpoint_config>(std::move(cfg)), mem);
}

static shared_ptr<s3::client> make_minio_client(semaphore& mem) {
    s3::endpoint_config cfg = {
        .port = std::stoul(tests::getenv_safe("S3_SERVER_PORT_FOR_TEST")),
        .use_https = ::getenv("AWS_DEFAULT_REGION") != nullptr,
        .region = ::getenv("AWS_DEFAULT_REGION") ? : "local",
    };
    return s3::client::make(tests::getenv_safe("S3_SERVER_ADDRESS_FOR_TEST"), make_lw_shared<s3::endpoint_config>(std::move(cfg)), mem);
}

using client_maker_function = std::function<shared_ptr<s3::client>(semaphore&)>;

/*
 * Tests below expect minio server to be running on localhost
 * with the bucket named env['S3_BUCKET_FOR_TEST'] created with
 * unrestricted anonymous read-write access
 */

void client_put_get_object(const client_maker_function& client_maker) {
    const sstring name(fmt::format("/{}/testobject-{}", tests::getenv_safe("S3_BUCKET_FOR_TEST"), ::getpid()));

    testlog.info("Make client\n");
    semaphore mem(16 << 20);
    auto cln = client_maker(mem);
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

SEASTAR_THREAD_TEST_CASE(test_client_put_get_object_minio) {
    client_put_get_object(make_minio_client);
}

SEASTAR_THREAD_TEST_CASE(test_client_put_get_object_proxy) {
    client_put_get_object(make_proxy_client);
}

static auto deferred_delete_object(shared_ptr<s3::client> client, sstring name) {
    return seastar::defer([client, name] {
        testlog.info("Delete object: {}\n", name);
        client->delete_object(name).get();
    });
}

void do_test_client_multipart_upload(const client_maker_function& client_maker, bool with_copy_upload) {
    const sstring name(fmt::format("/{}/test{}object-{}", tests::getenv_safe("S3_BUCKET_FOR_TEST"), with_copy_upload ? "jumbo" : "large", ::getpid()));

    testlog.info("Make client\n");
    semaphore mem(16<<20);
    auto cln = client_maker(mem);
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

SEASTAR_THREAD_TEST_CASE(test_client_multipart_upload_minio) {
    do_test_client_multipart_upload(make_minio_client, false);
}

SEASTAR_THREAD_TEST_CASE(test_client_multipart_upload_proxy) {
    do_test_client_multipart_upload(make_proxy_client, false);
}

SEASTAR_THREAD_TEST_CASE(test_client_multipart_copy_upload_minio) {
    do_test_client_multipart_upload(make_minio_client, true);
}

SEASTAR_THREAD_TEST_CASE(test_client_multipart_copy_upload_proxy) {
    do_test_client_multipart_upload(make_proxy_client, true);
}

void client_multipart_upload_fallback(const client_maker_function& client_maker) {
    const sstring name(fmt::format("/{}/testfbobject-{}", tests::getenv_safe("S3_BUCKET_FOR_TEST"), ::getpid()));

    testlog.info("Make client");
    semaphore mem(0);
    mem.broken(); // so that any attempt to use it throws
    auto cln = client_maker(mem);
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

SEASTAR_THREAD_TEST_CASE(test_client_multipart_upload_fallback_minio) {
    client_multipart_upload_fallback(make_minio_client);
}

SEASTAR_THREAD_TEST_CASE(test_client_multipart_upload_fallback_proxy) {
    client_multipart_upload_fallback(make_proxy_client);
}

using with_remainder_t = bool_class<class with_remainder_tag>;

future<> test_client_upload_file(const client_maker_function& client_maker, std::string_view test_name, size_t total_size, size_t memory_size) {
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
    auto client = client_maker(mem);
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
    co_await client->delete_object(object_name);
    co_await client->close();
}

SEASTAR_TEST_CASE(test_client_upload_file_multi_part_without_remainder_minio) {
    const size_t part_size = 5_MiB;
    const size_t total_size = 4 * part_size;
    const size_t memory_size = part_size;
    co_await test_client_upload_file(make_minio_client, seastar_test::get_name(), total_size, memory_size);
}

SEASTAR_TEST_CASE(test_client_upload_file_multi_part_without_remainder_proxy) {
    const size_t part_size = 5_MiB;
    const size_t total_size = 4 * part_size;
    const size_t memory_size = part_size;
    co_await test_client_upload_file(make_proxy_client, seastar_test::get_name(), total_size, memory_size);
}

SEASTAR_TEST_CASE(test_client_upload_file_multi_part_with_remainder_minio) {
    const size_t part_size = 5_MiB;
    const size_t remainder_size = part_size / 2;
    const size_t total_size = 4 * part_size + remainder_size;
    const size_t memory_size = part_size;
    co_await test_client_upload_file(make_minio_client, seastar_test::get_name(), total_size, memory_size);
}

SEASTAR_TEST_CASE(test_client_upload_file_multi_part_with_remainder_proxy) {
    const size_t part_size = 5_MiB;
    const size_t remainder_size = part_size / 2;
    const size_t total_size = 4 * part_size + remainder_size;
    const size_t memory_size = part_size;
    co_await test_client_upload_file(make_proxy_client, seastar_test::get_name(), total_size, memory_size);
}

SEASTAR_TEST_CASE(test_client_upload_file_single_part_minio) {
    const size_t part_size = 5_MiB;
    const size_t total_size = part_size / 2;
    const size_t memory_size = part_size;
    co_await test_client_upload_file(make_minio_client, seastar_test::get_name(), total_size, memory_size);
}

SEASTAR_TEST_CASE(test_client_upload_file_single_part_proxy) {
    const size_t part_size = 5_MiB;
    const size_t total_size = part_size / 2;
    const size_t memory_size = part_size;
    co_await test_client_upload_file(make_proxy_client, seastar_test::get_name(), total_size, memory_size);
}


SEASTAR_THREAD_TEST_CASE(test_client_abort_stuck_semaphore) {
    const sstring base_name(fmt::format("test_object-{}", ::getpid()));
    constexpr size_t object_size = 128_KiB;

    tmpdir tmp;
    const auto file_path = tmp.path() / base_name;

    file f = open_file_dma(file_path.native(), open_flags::create | open_flags::wo).get();
    auto output = make_file_output_stream(std::move(f)).get();

    for (size_t bytes_written = 0; bytes_written < object_size;) {
        auto rnd = tests::random::get_bytes(std::min(object_size - bytes_written, 1024ul));
        output.write(reinterpret_cast<char*>(rnd.data()), rnd.size()).get();
        bytes_written += rnd.size();
    }
    output.close().get();

    testlog.info("Make client\n");
    semaphore mem(32_KiB);
    auto cln = make_minio_client(mem);
    auto close_client = deferred_close(*cln);
    const auto object_name = fmt::format("/{}/{}", tests::getenv_safe("S3_BUCKET_FOR_TEST"), base_name);
    auto delete_object = deferred_delete_object(cln, object_name);
    abort_source as;
    auto upload = cln->upload_file(file_path, object_name, {}, {}, &as);
    auto ex = std::make_exception_ptr(std::runtime_error("Cancelling!"));
    as.request_abort_ex(ex);
    upload.wait();
    BOOST_REQUIRE(upload.failed());
    BOOST_REQUIRE_EXCEPTION(std::rethrow_exception(upload.get_exception()), semaphore_aborted, [](const semaphore_aborted& e) {
        return e.what() == "Semaphore aborted"sv;
    });
}

void client_readable_file(const client_maker_function& client_maker) {
    const sstring name(fmt::format("/{}/testroobject-{}", tests::getenv_safe("S3_BUCKET_FOR_TEST"), ::getpid()));

    testlog.info("Make client\n");
    semaphore mem(16<<20);
    auto cln = client_maker(mem);
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

SEASTAR_THREAD_TEST_CASE(test_client_readable_file_minio) {
    client_readable_file(make_minio_client);
}

SEASTAR_THREAD_TEST_CASE(test_client_readable_file_proxy) {
    client_readable_file(make_proxy_client);
}

void client_readable_file_stream(const client_maker_function& client_maker) {
    const sstring name(fmt::format("/{}/teststreamobject-{}", tests::getenv_safe("S3_BUCKET_FOR_TEST"), ::getpid()));

    testlog.info("Make client\n");
    semaphore mem(16<<20);
    auto cln = client_maker(mem);
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

SEASTAR_THREAD_TEST_CASE(test_client_readable_file_stream_minio) {
    client_readable_file_stream(make_minio_client);
}

SEASTAR_THREAD_TEST_CASE(test_client_readable_file_stream_proxy) {
    client_readable_file_stream(make_proxy_client);
}

void client_put_get_tagging(const client_maker_function& client_maker) {
    const sstring name(fmt::format("/{}/testobject-{}",
                                   tests::getenv_safe("S3_BUCKET_FOR_TEST"), ::getpid()));
    semaphore mem(16<<20);
    auto client = client_maker(mem);

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

SEASTAR_THREAD_TEST_CASE(test_client_put_get_tagging_minio) {
    client_put_get_tagging(make_minio_client);
}

SEASTAR_THREAD_TEST_CASE(test_client_put_get_tagging_proxy) {
    client_put_get_tagging(make_proxy_client);
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

void client_list_objects(const client_maker_function& client_maker) {
    const sstring bucket = tests::getenv_safe("S3_BUCKET_FOR_TEST");
    const sstring prefix(fmt::format("testprefix-{}/", ::getpid()));
    semaphore mem(16<<20);
    auto client = client_maker(mem);
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

SEASTAR_THREAD_TEST_CASE(test_client_list_objects_minio) {
    client_list_objects(make_minio_client);
}

SEASTAR_THREAD_TEST_CASE(test_client_list_objects_proxy) {
    client_list_objects(make_proxy_client);
}

void client_list_objects_incomplete(const client_maker_function& client_maker) {
    const sstring bucket = tests::getenv_safe("S3_BUCKET_FOR_TEST");
    const sstring prefix(fmt::format("testprefix-{}/", ::getpid()));
    semaphore mem(16<<20);
    auto client = client_maker(mem);
    auto close_client = deferred_close(*client);

    populate_bucket(client, bucket, prefix, 8);

    s3::client::bucket_lister lister(client, bucket, prefix, 9, 2);
    auto close_lister = deferred_close(lister);

    // Peek one entry to start lister work
    auto de = lister.get().get();
    close_lister.close_now();
}

SEASTAR_THREAD_TEST_CASE(test_client_list_objects_incomplete_minio) {
    client_list_objects_incomplete(make_minio_client);
}

SEASTAR_THREAD_TEST_CASE(test_client_list_objects_incomplete_proxy) {
    client_list_objects_incomplete(make_proxy_client);
}

void client_broken_bucket(const client_maker_function& client_maker) {
    const sstring name(fmt::format("/{}/testobject-{}", "NO_BUCKET", ::getpid()));
    semaphore mem(16 << 20);
    auto client = client_maker(mem);

    auto close_client = deferred_close(*client);
    auto data = sstring("1234567890ABCDEF").release();
    BOOST_REQUIRE_EXCEPTION(client->put_object(name, std::move(data)).get(), storage_io_error, [](const storage_io_error& e) {
        return e.code().value() == EIO && e.what() == "S3 request failed. Code: 100. Reason: The specified bucket is not valid."sv;
    });
}

SEASTAR_THREAD_TEST_CASE(test_client_broken_bucket_minio) {
    client_broken_bucket(make_minio_client);
}

void client_missing_prefix(const client_maker_function& client_maker) {
    const sstring name(fmt::format("/{}/testobject-{}", tests::getenv_safe("S3_BUCKET_FOR_TEST"), ::getpid()));
    semaphore mem(16 << 20);
    auto client = client_maker(mem);

    auto close_client = deferred_close(*client);
    BOOST_REQUIRE_EXCEPTION(client->get_object_size(name).get(), storage_io_error, [](const storage_io_error& e) {
        return e.code().value() == ENOENT && e.what() == "S3 request failed. Code: 117. Reason:  HTTP code: 404 Not Found"sv;
    });
}

SEASTAR_THREAD_TEST_CASE(test_client_missing_prefix_minio) {
    client_missing_prefix(make_minio_client);
}

void client_access_missing_object(const client_maker_function& client_maker) {
    const sstring name(fmt::format("/{}/testobject-{}", tests::getenv_safe("S3_BUCKET_FOR_TEST"), ::getpid()));
    semaphore mem(16 << 20);
    auto client = client_maker(mem);

    auto close_client = deferred_close(*client);
    BOOST_REQUIRE_EXCEPTION(client->get_object_tagging(name).get(), storage_io_error, [](const storage_io_error& e) {
        return e.code().value() == ENOENT && e.what() == "S3 request failed. Code: 133. Reason: The specified key does not exist."sv;
    });
}

SEASTAR_THREAD_TEST_CASE(test_client_access_missing_object_minio) {
    client_access_missing_object(make_minio_client);
}

SEASTAR_THREAD_TEST_CASE(test_object_reupload) {
    // Pay attention, we are reuploading the same file during the test
    const sstring name(fmt::format("/{}/testobject-{}", tests::getenv_safe("S3_BUCKET_FOR_TEST"), ::getpid()));

    semaphore mem(16 << 20);
    auto cln = make_minio_client(mem);
    auto close_client = deferred_close(*cln);
    auto delete_object = deferred_delete_object(cln, name);
    constexpr std::string_view content{"1234567890"};
    for (auto i : {1, 2}) {
        testlog.info("Put object {}, iteration {}", name, i);
        temporary_buffer<char> data = sstring(content).release();
        cln->put_object(name, std::move(data)).get();

        size_t sz = cln->get_object_size(name).get();
        BOOST_REQUIRE_EQUAL(sz, content.length());

        s3::stats st = cln->get_object_stats(name).get();
        BOOST_REQUIRE_EQUAL(st.size, content.length());
    }

    for (auto jumbo : {true, false}) {
        for (auto i : {1, 2}) {
            testlog.info("Upload object {}, iteration {} (with copy = {})", name, i, jumbo);
            auto out = output_stream<char>(
                // Make it 3 parts per piece, so that 128Mb buffer below
                // would be split into several 15Mb pieces
                jumbo ? cln->make_upload_jumbo_sink(name, 3) : cln->make_upload_sink(name));

            constexpr unsigned chunk_size = 1000;
            constexpr unsigned writes = 128 * 1024;
            auto rnd = tests::random::get_bytes(chunk_size);
            uint64_t object_size = 0;
            for (unsigned ch = 0; ch < writes; ch++) {
                out.write(reinterpret_cast<char*>(rnd.begin()), rnd.size()).get();
                object_size += rnd.size();
            }

            out.flush().get();
            out.close().get();

            size_t sz = cln->get_object_size(name).get();
            BOOST_REQUIRE_EQUAL(sz, object_size);

            s3::stats st = cln->get_object_stats(name).get();
            BOOST_REQUIRE_EQUAL(st.size, object_size);
        }
    }
}

void test_download_data_source(const client_maker_function& client_maker, bool is_chunked, unsigned chunks) {
    const sstring name(fmt::format("/{}/testdatasourceobject-{}", tests::getenv_safe("S3_BUCKET_FOR_TEST"), ::getpid()));

    testlog.info("Make client\n");
    semaphore mem(16<<20);
    auto cln = client_maker(mem);
    auto close_client = deferred_close(*cln);
    auto delete_object = deferred_delete_object(cln, name);

    static constexpr unsigned chunk_size = 1000;
    testlog.info("Preparation: Upload object");
    auto rnd = tests::random::get_bytes(chunk_size);
    {
        auto out = output_stream<char>(cln->make_upload_sink(name));
        auto close = seastar::deferred_close(out);

        for (unsigned ch = 0; ch < chunks; ch++) {
            out.write(reinterpret_cast<char*>(rnd.begin()), rnd.size()).get();
        }
        out.flush().get();
    }

    testlog.info("Download object");
    auto in = is_chunked ? input_stream<char>(cln->make_chunked_download_source(name, s3::full_range)) : input_stream<char>(cln->make_download_source(name, s3::full_range));
    auto close = seastar::deferred_close(in);
    for (unsigned ch = 0; ch < chunks; ch++) {
        auto buf = in.read_exactly(chunk_size).get();
        BOOST_REQUIRE_EQUAL(memcmp(buf.begin(), rnd.begin(), 1000), 0);
    }
}

SEASTAR_THREAD_TEST_CASE(test_download_data_source_minio) {
    test_download_data_source(make_minio_client, false, 128 * 1024);
}

SEASTAR_THREAD_TEST_CASE(test_download_data_source_proxy) {
    test_download_data_source(make_proxy_client, false, 3 * 1024);
}

SEASTAR_THREAD_TEST_CASE(test_chunked_download_data_source_minio) {
    test_download_data_source(make_minio_client, true, 128 * 1024);
}

SEASTAR_THREAD_TEST_CASE(test_chunked_download_data_source_proxy) {
    test_download_data_source(make_proxy_client, true, 3 * 1024);
}

void test_chunked_download_data_source(const client_maker_function& client_maker, size_t object_size) {
    const sstring base_name(fmt::format("test_object-{}", ::getpid()));

    tmpdir tmp;
    const auto file_path = tmp.path() / base_name;

    file f = open_file_dma(file_path.native(), open_flags::create | open_flags::wo).get();
    auto output = make_file_output_stream(std::move(f)).get();

    for (size_t bytes_written = 0; bytes_written < object_size;) {
        auto rnd = tests::random::get_bytes(std::min(object_size - bytes_written, 1024ul));
        output.write(reinterpret_cast<char*>(rnd.data()), rnd.size()).get();
        bytes_written += rnd.size();
    }
    output.close().get();

    testlog.info("Make client\n");
    semaphore mem(16 << 20);
    auto cln = client_maker(mem);
    auto close_client = deferred_close(*cln);
    const auto object_name = fmt::format("/{}/{}", tests::getenv_safe("S3_BUCKET_FOR_TEST"), base_name);
    auto delete_object = deferred_delete_object(cln, object_name);
    cln->upload_file(file_path, object_name).get();

    testlog.info("Download object");
    auto in = input_stream<char>(cln->make_chunked_download_source(object_name, s3::full_range));
    auto close = seastar::deferred_close(in);

    file rf = open_file_dma(file_path.native(), open_flags::ro).get();
    auto file_input = make_file_input_stream(std::move(rf));
    auto close_file = seastar::deferred_close(file_input);

    size_t total_size = 0;
    size_t trigger_counter = 0;
    while (true) {
        // We want the background fiber to fill the buffer queue and start waiting to drain it
        seastar::sleep(100us).get();
        auto buf = in.read().get();
        total_size += buf.size();
        if (buf.empty()) {
            break;
        }
        ++trigger_counter;
        if (trigger_counter % 10 == 0) {
            utils::get_local_injector().enable("break_s3_inflight_req", true);
        }

        auto file_buf = file_input.read_exactly(buf.size()).get();
        BOOST_REQUIRE_EQUAL(memcmp(buf.begin(), file_buf.begin(), buf.size()), 0);
    }

    BOOST_REQUIRE_EQUAL(total_size, object_size);
#ifdef SCYLLA_ENABLE_ERROR_INJECTION
    utils::get_local_injector().enable("kill_s3_inflight_req");
    auto in_throw = input_stream<char>(cln->make_chunked_download_source(object_name, s3::full_range));
    auto close_throw = seastar::deferred_close(in_throw);

    auto reader = [&in_throw] {
        while (true) {
            auto buf = in_throw.read().get();
            if (buf.empty()) {
                break;
            }
        }
    };
    BOOST_REQUIRE_EXCEPTION(
        reader(), storage_io_error, [](const storage_io_error& e) {
            return e.what() == "S3 request failed. Code: 16. Reason: "sv;
        });
#else
    testlog.info("Skipping error injection test, as it requires SCYLLA_ENABLE_ERROR_INJECTION to be enabled");
#endif

    cln->delete_object(object_name).get();
    cln->close().get();
}

SEASTAR_THREAD_TEST_CASE(test_chunked_download_data_source_with_delays_minio) {
    test_chunked_download_data_source(make_minio_client, 20_MiB);
}

SEASTAR_THREAD_TEST_CASE(test_chunked_download_data_source_with_delays_proxy) {
    test_chunked_download_data_source(make_proxy_client, 20_MiB);
}

void test_object_copy(const client_maker_function& client_maker, size_t chunk_size, size_t chunks) {
    const sstring name(fmt::format("/{}/testobject-{}", tests::getenv_safe("S3_BUCKET_FOR_TEST"), ::getpid()));
    const sstring name_copy(fmt::format("/{}/testobject-{}-copy", tests::getenv_safe("S3_BUCKET_FOR_TEST"), ::getpid()));

    semaphore mem(16 << 20);
    auto cln = client_maker(mem);;
    auto close_client = deferred_close(*cln);
    auto delete_object = deferred_delete_object(cln, name);
    auto delete_copy_object = deferred_delete_object(cln, name_copy);

    auto out = output_stream<char>(cln->make_upload_sink(name));
    auto rnd = tests::random::get_bytes(chunk_size);

    for (unsigned ch = 0; ch < chunks; ch++) {
        out.write(reinterpret_cast<char*>(rnd.begin()), rnd.size()).get();
    }

    out.flush().get();
    out.close().get();
    cln->copy_object(name, name_copy, 5_MiB).get();

    auto sz = cln->get_object_size(name_copy).get();
    BOOST_REQUIRE_EQUAL(sz, chunk_size * chunks);

    for (size_t off = 0; off < sz; off += chunk_size) {
        auto len = std::min(chunk_size, sz - off);
        auto range = s3::range{off, len};
        auto orig_buf = cln->get_object_contiguous(name, range).get();
        auto copy_buf = cln->get_object_contiguous(name_copy, range).get();
        testlog.info("Got [{}:{}) chunk", off, len);
        BOOST_REQUIRE_EQUAL(memcmp(copy_buf.get(), orig_buf.get(), len), 0);
    }
}

SEASTAR_THREAD_TEST_CASE(test_small_object_copy) {
    test_object_copy(make_minio_client, 1000, 2);
}

SEASTAR_THREAD_TEST_CASE(test_large_object_copy) {
    test_object_copy(make_minio_client, 1_MiB, 6);
}

SEASTAR_THREAD_TEST_CASE(test_small_object_copy_proxy) {
    test_object_copy(make_proxy_client, 1000, 2);
}

SEASTAR_THREAD_TEST_CASE(test_large_object_copy_proxy) {
    test_object_copy(make_proxy_client, 1_MiB, 6);
}

SEASTAR_THREAD_TEST_CASE(test_creds) {
    /*
     * Note: This test does not cover the functionality of the `aws::environment_aws_credentials_provider` class because proper testing requires altering
     * environment variables. Therefore, running such a test in parallel is not feasible. However, all S3-based tests will use
     * `aws::environment_aws_credentials_provider` to obtain credentials. As a result, if this functionality fails, all S3-related tests will fail immediately.
     */

    auto host = tests::getenv_safe("MOCK_S3_SERVER_HOST");
    auto port = std::stoul(tests::getenv_safe("MOCK_S3_SERVER_PORT"));
    tmpdir tmp;

    auto sts_provider = std::make_unique<aws::sts_assume_role_credentials_provider>(host, port, false);
    auto creds = sts_provider->get_aws_credentials().get();
    BOOST_REQUIRE_EQUAL(creds.access_key_id, "STS_EXAMPLE_ACCESS_KEY_ID");
    BOOST_REQUIRE_EQUAL(creds.secret_access_key, "STS_EXAMPLE_SECRET_ACCESS_KEY");
    BOOST_REQUIRE_EQUAL(creds.session_token.contains("STS_SESSIONTOKEN"), true);

    auto md_provider = std::make_unique<aws::instance_profile_credentials_provider>(host, port);
    creds = md_provider->get_aws_credentials().get();
    BOOST_REQUIRE_EQUAL(creds.access_key_id, "INSTANCE_FROFILE_EXAMPLE_ACCESS_KEY_ID");
    BOOST_REQUIRE_EQUAL(creds.secret_access_key, "INSTANCE_FROFILE_EXAMPLE_SECRET_ACCESS_KEY");
    BOOST_REQUIRE_EQUAL(creds.session_token.contains("INSTANCE_FROFILE_SESSIONTOKEN"), true);

    aws::aws_credentials_provider_chain provider_chain;
    provider_chain.add_credentials_provider(std::make_unique<aws::sts_assume_role_credentials_provider>(host, port, false))
        .add_credentials_provider(std::make_unique<aws::instance_profile_credentials_provider>(host, port));
    creds = provider_chain.get_aws_credentials().get();
    BOOST_REQUIRE_EQUAL(creds.access_key_id, "STS_EXAMPLE_ACCESS_KEY_ID");
    BOOST_REQUIRE_EQUAL(creds.secret_access_key, "STS_EXAMPLE_SECRET_ACCESS_KEY");
    BOOST_REQUIRE_EQUAL(creds.session_token.contains("STS_SESSIONTOKEN"), true);
    seastar::sleep(1050ms).get();
    auto creds1 = provider_chain.get_aws_credentials().get();
    BOOST_REQUIRE(creds1.expires_at - creds.expires_at >= 1s);

    provider_chain = {};
    provider_chain.add_credentials_provider(std::make_unique<aws::sts_assume_role_credentials_provider>("0.0.0.0", 0, false))
        .add_credentials_provider(std::make_unique<aws::instance_profile_credentials_provider>(host, port));
    creds = provider_chain.get_aws_credentials().get();
    BOOST_REQUIRE_EQUAL(creds.access_key_id, "INSTANCE_FROFILE_EXAMPLE_ACCESS_KEY_ID");
    BOOST_REQUIRE_EQUAL(creds.secret_access_key, "INSTANCE_FROFILE_EXAMPLE_SECRET_ACCESS_KEY");
    BOOST_REQUIRE_EQUAL(creds.session_token.contains("INSTANCE_FROFILE_SESSIONTOKEN"), true);
    seastar::sleep(1050ms).get();
    creds1 = provider_chain.get_aws_credentials().get();
    BOOST_REQUIRE(creds1.expires_at - creds.expires_at >= 1s);

    provider_chain = {};
    provider_chain.add_credentials_provider(std::make_unique<aws::sts_assume_role_credentials_provider>("0.0.0.0", 0, false))
        .add_credentials_provider(std::make_unique<aws::instance_profile_credentials_provider>("0.0.0.0", 0));
    creds = provider_chain.get_aws_credentials().get();
    BOOST_REQUIRE_EQUAL(creds.access_key_id, "");
    BOOST_REQUIRE_EQUAL(creds.secret_access_key, "");
    BOOST_REQUIRE_EQUAL(creds.session_token, "");
}

BOOST_AUTO_TEST_CASE(s3_fqn_manipulation) {
    std::string bucket_name, object_name;
    // Empty input
    BOOST_REQUIRE_EQUAL(s3::s3fqn_to_parts("", bucket_name, object_name), false);
    BOOST_REQUIRE(bucket_name.empty());
    BOOST_REQUIRE(object_name.empty());
    // Incorrect scheme
    BOOST_REQUIRE_EQUAL(s3::s3fqn_to_parts("http://bucket/object", bucket_name, object_name), false);
    BOOST_REQUIRE(bucket_name.empty());
    BOOST_REQUIRE(object_name.empty());
    // No scheme
    BOOST_REQUIRE_EQUAL(s3::s3fqn_to_parts("/bucket/object", bucket_name, object_name), false);
    BOOST_REQUIRE_EQUAL(bucket_name.empty(), true);
    BOOST_REQUIRE_EQUAL(object_name.empty(), true);
    // Scheme without slashes
    BOOST_REQUIRE_EQUAL(s3::s3fqn_to_parts("s3:", bucket_name, object_name), false);
    BOOST_REQUIRE_EQUAL(bucket_name.empty(), true);
    BOOST_REQUIRE_EQUAL(object_name.empty(), true);
    // Scheme with single slash
    BOOST_REQUIRE_EQUAL(s3::s3fqn_to_parts("s3:/", bucket_name, object_name), true);
    BOOST_REQUIRE_EQUAL(bucket_name.empty(), true);
    BOOST_REQUIRE_EQUAL(object_name, "/");
    // Without bucket
    BOOST_REQUIRE_EQUAL(s3::s3fqn_to_parts("s3://", bucket_name, object_name), true);
    BOOST_REQUIRE_EQUAL(bucket_name.empty(), true);
    BOOST_REQUIRE_EQUAL(object_name, "/");
    // Bucket only, no prefix/object
    BOOST_REQUIRE_EQUAL(s3::s3fqn_to_parts("s3://bucket/", bucket_name, object_name), true);
    BOOST_REQUIRE_EQUAL(bucket_name, "bucket");
    BOOST_REQUIRE_EQUAL(object_name, "/");
    // Same as above, but without trailing slash
    BOOST_REQUIRE_EQUAL(s3::s3fqn_to_parts("s3://bucket", bucket_name, object_name), true);
    BOOST_REQUIRE_EQUAL(bucket_name, "bucket");
    BOOST_REQUIRE_EQUAL(object_name, "/");
    // Bucket and prefix without object
    BOOST_REQUIRE_EQUAL(s3::s3fqn_to_parts("s3://bucket/object", bucket_name, object_name), true);
    BOOST_REQUIRE_EQUAL(bucket_name, "bucket");
    BOOST_REQUIRE_EQUAL(object_name, "object");
    // Bucket and object with object
    BOOST_REQUIRE_EQUAL(s3::s3fqn_to_parts("s3://bucket/prefix1/prefix2/foo.bar", bucket_name, object_name), true);
    BOOST_REQUIRE_EQUAL(bucket_name, "bucket");
    BOOST_REQUIRE_EQUAL(object_name, "prefix1/prefix2/foo.bar");
    // Bucket and object with object and extra slashes
    BOOST_REQUIRE_EQUAL(s3::s3fqn_to_parts("s3://bucket///prefix1/prefix2//foo.bar", bucket_name, object_name), true);
    BOOST_REQUIRE_EQUAL(bucket_name, "bucket");
    BOOST_REQUIRE_EQUAL(object_name, "prefix1/prefix2/foo.bar");
}
