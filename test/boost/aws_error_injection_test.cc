/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "test/lib/log.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/scylla_test_case.hh"
#include "test/lib/test_utils.hh"
#include "test/lib/tmpdir.hh"
#include "utils/exceptions.hh"
#include "utils/s3/client.hh"
#include <cstdlib>
#include <seastar/core/fstream.hh>
#include <seastar/core/units.hh>
#include <seastar/http/httpd.hh>
#include <seastar/util/closeable.hh>

using namespace seastar;
using namespace std::string_view_literals;

enum class failure_policy : uint8_t {
    SUCCESS = 0,
    RETRYABLE_FAILURE = 1,
    NONRETRYABLE_FAILURE = 2,
    NEVERENDING_RETRYABLE_FAILURE = 3,
};

static uint16_t get_port() {
    return std::stoi(tests::getenv_safe("MOCK_S3_SERVER_PORT"));
}

static std::string get_address() {
    return tests::getenv_safe("MOCK_S3_SERVER_HOST");
}

static s3::endpoint_config_ptr make_minio_config() {
    s3::endpoint_config cfg = {
        .port = get_port(),
        .use_https = false,
        .region = "us-east-1",
    };
    return make_lw_shared<s3::endpoint_config>(std::move(cfg));
}

static void register_policy(const std::string& key, failure_policy policy) {
    auto cln = http::experimental::client(socket_address(net::inet_address(get_address()), get_port()));
    auto close_client = deferred_close(cln);
    auto req = http::request::make("PUT", get_address(), "/");
    req._headers["Content-Length"] = "0";
    req.query_parameters["Key"] = key;
    req.query_parameters["Policy"] = std::to_string(std::to_underlying(policy));
    cln.make_request(std::move(req), [](const http::reply&, input_stream<char>&&) -> future<> { return seastar::make_ready_future(); }).get();
}

void test_client_upload_file(std::string_view test_name, failure_policy policy, size_t total_size, size_t memory_size) {
    tmpdir tmp;
    const auto file_path = tmp.path() / fmt::format("test-{}", ::getpid());
    {
        file f = open_file_dma(file_path.native(), open_flags::create | open_flags::wo).get();
        auto output = make_file_output_stream(std::move(f)).get();
        auto close_file = deferred_close(output);
        std::string_view data = "1234567890ABCDEF";
        // so we can test !with_remainder case properly with multiple writes
        SCYLLA_ASSERT(total_size % data.size() == 0);

        for (size_t bytes_written = 0; bytes_written < total_size; bytes_written += data.size()) {
            output.write(data.data(), data.size()).get();
        }
    }

    const auto object_name = fmt::format("/{}/{}-{}", "test", test_name, ::getpid());
    register_policy(object_name, policy);

    semaphore mem{memory_size};
    auto client = s3::client::make(get_address(), make_minio_config(), mem);
    auto client_shutdown = deferred_close(*client);
    client->upload_file(file_path, object_name).get();
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_file_success) {
    const size_t part_size = 5_MiB;
    const size_t remainder_size = part_size / 2;
    const size_t total_size = 4 * part_size + remainder_size;
    const size_t memory_size = part_size;
    BOOST_REQUIRE_NO_THROW(test_client_upload_file(seastar_test::get_name(), failure_policy::SUCCESS, total_size, memory_size));
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_file_retryable_success) {
    const size_t part_size = 5_MiB;
    const size_t remainder_size = part_size / 2;
    const size_t total_size = 4 * part_size + remainder_size;
    const size_t memory_size = part_size;
    BOOST_REQUIRE_NO_THROW(test_client_upload_file(seastar_test::get_name(), failure_policy::RETRYABLE_FAILURE, total_size, memory_size));
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_file_failure_1) {
    const size_t part_size = 5_MiB;
    const size_t remainder_size = part_size / 2;
    const size_t total_size = 4 * part_size + remainder_size;
    const size_t memory_size = part_size;
    BOOST_REQUIRE_EXCEPTION(test_client_upload_file(seastar_test::get_name(), failure_policy::NEVERENDING_RETRYABLE_FAILURE, total_size, memory_size),
            storage_io_error, [](const storage_io_error& e) {
                return e.code().value() == EIO && e.what() == "S3 request failed. Code: 1. Reason: We encountered an internal error. Please try again."sv;
            });
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_file_failure_2) {
    const size_t part_size = 5_MiB;
    const size_t remainder_size = part_size / 2;
    const size_t total_size = 4 * part_size + remainder_size;
    const size_t memory_size = part_size;
    BOOST_REQUIRE_EXCEPTION(test_client_upload_file(seastar_test::get_name(), failure_policy::NONRETRYABLE_FAILURE, total_size, memory_size), storage_io_error,
            [](const storage_io_error& e) {
                return e.code().value() == EIO && e.what() == "S3 request failed. Code: 2. Reason: Something went terribly wrong"sv;
            });
}

void do_test_client_multipart_upload(failure_policy policy, bool is_jumbo = false) {
    const sstring name(fmt::format("/{}/testobject-{}-{}", "test", is_jumbo ? "jumbo" : "large", ::getpid()));

    register_policy(name, policy);
    testlog.info("Make client");
    semaphore mem(16 << 20);
    auto cln = s3::client::make(get_address(), make_minio_config(), mem);
    auto close_client = deferred_close(*cln);

    testlog.info("Upload object");
    auto out = output_stream<char>(is_jumbo ? cln->make_upload_jumbo_sink(name, 3) : cln->make_upload_sink(name));
    auto close_stream = deferred_close(out);

    static constexpr unsigned chunk_size = 1000;
    auto rnd = tests::random::get_bytes(chunk_size);
    for (unsigned ch = 0; ch < 128_KiB; ch++) {
        out.write(reinterpret_cast<char*>(rnd.begin()), rnd.size()).get();
    }

    testlog.info("Flush multipart upload");
    out.flush().get();
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_sink_success) {
    BOOST_REQUIRE_NO_THROW(do_test_client_multipart_upload(failure_policy::SUCCESS));
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_sink_retryable_success) {
    BOOST_REQUIRE_NO_THROW(do_test_client_multipart_upload(failure_policy::RETRYABLE_FAILURE));
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_sink_failure_1) {
    BOOST_REQUIRE_EXCEPTION(do_test_client_multipart_upload(failure_policy::NEVERENDING_RETRYABLE_FAILURE), storage_io_error, [](const storage_io_error& e) {
        return e.code().value() == EIO && e.what() == "S3 request failed. Code: 1. Reason: We encountered an internal error. Please try again."sv;
    });
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_sink_failure_2) {
    BOOST_REQUIRE_EXCEPTION(do_test_client_multipart_upload(failure_policy::NONRETRYABLE_FAILURE), storage_io_error, [](const storage_io_error& e) {
        return e.code().value() == EIO && e.what() == "S3 request failed. Code: 2. Reason: Something went terribly wrong"sv;
    });
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_jumbo_sink_success) {
    BOOST_REQUIRE_NO_THROW(do_test_client_multipart_upload(failure_policy::SUCCESS, true));
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_jumbo_sink_retryable_success) {
    BOOST_REQUIRE_NO_THROW(do_test_client_multipart_upload(failure_policy::RETRYABLE_FAILURE, true));
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_jumbo_sink_failure_1) {
    BOOST_REQUIRE_EXCEPTION(
            do_test_client_multipart_upload(failure_policy::NEVERENDING_RETRYABLE_FAILURE, true), storage_io_error, [](const storage_io_error& e) {
                return e.code().value() == EIO && e.what() == "S3 request failed. Code: 1. Reason: We encountered an internal error. Please try again."sv;
            });
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_jumbo_sink_failure_2) {
    BOOST_REQUIRE_EXCEPTION(do_test_client_multipart_upload(failure_policy::NONRETRYABLE_FAILURE, true), storage_io_error, [](const storage_io_error& e) {
        return e.code().value() == EIO && e.what() == "S3 request failed. Code: 2. Reason: Something went terribly wrong"sv;
    });
}
