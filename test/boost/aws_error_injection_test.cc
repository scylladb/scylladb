/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
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
#include <seastar/core/metrics_api.hh>
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
    COPY_SOURCE_REFUSED_LARGE = 5,
    COPY_SOURCE_REFUSED_SMALL = 6,
    COPY_SOURCE_NOT_FOUND = 7,
    COPY_SOURCE_PAYLOAD_TOO_LARGE = 8,
    COPY_SOURCE_FORBIDDEN = 9,
};

static uint16_t get_port() {
    return std::stoi(tests::getenv_safe("MOCK_S3_SERVER_PORT"));
}

static std::string get_address() {
    return tests::getenv_safe("MOCK_S3_SERVER_HOST");
}

static s3::endpoint_config_ptr make_mock_config() {
    s3::endpoint_config cfg = {
        .port = get_port(),
        .use_https = false,
        .region = "us-east-1",
    };
    return make_lw_shared<s3::endpoint_config>(std::move(cfg));
}

static void register_policy(const std::string& key, failure_policy policy) {
    auto cln = http::client(socket_address(net::inet_address(get_address()), get_port()));
    auto close_client = deferred_close(cln);
    auto req = http::request::make("PUT", get_address(), "/");
    req._headers["Content-Length"] = "0";
    req.set_query_param("Key", key);
    req.set_query_param("Policy", std::to_string(std::to_underlying(policy)));
    cln.make_request(std::move(req), [](const http::reply&, input_stream<char>&&) -> future<> { return seastar::make_ready_future(); }).get();
}

void test_client_upload_file(std::string_view test_name, failure_policy policy, size_t total_size) {
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

    auto client = s3::client::make(get_address(), make_mock_config());
    auto client_shutdown = deferred_close(*client);
    client->upload_file(file_path, object_name).get();
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_file_success) {
    const size_t part_size = 5_MiB;
    const size_t remainder_size = part_size / 2;
    const size_t total_size = 4 * part_size + remainder_size;
    BOOST_REQUIRE_NO_THROW(test_client_upload_file(seastar_test::get_name(), failure_policy::SUCCESS, total_size));
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_file_retryable_success) {
    const size_t part_size = 5_MiB;
    const size_t remainder_size = part_size / 2;
    const size_t total_size = 4 * part_size + remainder_size;
    BOOST_REQUIRE_NO_THROW(test_client_upload_file(seastar_test::get_name(), failure_policy::RETRYABLE_FAILURE, total_size));
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_file_failure_1) {
    const size_t part_size = 5_MiB;
    const size_t remainder_size = part_size / 2;
    const size_t total_size = 4 * part_size + remainder_size;
    BOOST_REQUIRE_EXCEPTION(test_client_upload_file(seastar_test::get_name(), failure_policy::NEVERENDING_RETRYABLE_FAILURE, total_size),
            storage_io_error, [](const storage_io_error& e) {
                return e.code().value() == EIO && e.what() == "S3 request failed. Code: 1. Reason: We encountered an internal error. Please try again."sv;
            });
}

SEASTAR_THREAD_TEST_CASE(test_multipart_upload_file_failure_2) {
    const size_t part_size = 5_MiB;
    const size_t remainder_size = part_size / 2;
    const size_t total_size = 4 * part_size + remainder_size;
    BOOST_REQUIRE_EXCEPTION(test_client_upload_file(seastar_test::get_name(), failure_policy::NONRETRYABLE_FAILURE, total_size), storage_io_error,
            [](const storage_io_error& e) {
                return e.code().value() == EIO && e.what() == "S3 request failed. Code: 2. Reason: Something went terribly wrong"sv;
            });
}

void do_test_client_multipart_upload(failure_policy policy, bool is_jumbo = false) {
    const sstring name(fmt::format("/{}/testobject-{}-{}", "test", is_jumbo ? "jumbo" : "large", ::getpid()));

    register_policy(name, policy);
    testlog.info("Make client");
    auto cln = s3::client::make(get_address(), make_mock_config());
    auto close_client = deferred_close(*cln);

    testlog.info("Upload object");
    auto [sink, etag] = is_jumbo ? cln->make_upload_jumbo_sink_and_get_etag(name, s3::object_metadata{}, 3)
                                 : cln->make_upload_sink_and_get_etag(name);
    // The callers decide whether the upload must fail; the etag has to agree either way.
    std::exception_ptr ex;
    {
        auto out = output_stream<char>(std::move(sink));
        auto close_stream = deferred_close(out);

        static constexpr unsigned chunk_size = 1000;
        auto rnd = tests::random::get_bytes(chunk_size);
        try {
            for (unsigned ch = 0; ch < 128_KiB; ch++) {
                out.write(reinterpret_cast<char*>(rnd.begin()), rnd.size()).get();
            }
            testlog.info("Flush multipart upload");
            out.flush().get();
        } catch (...) {
            ex = std::current_exception();
        }
    }
    // The sink is closed and gone by now, so the etag future is resolved whatever happened
    // above, and can be inspected without hanging.
    if (ex) {
        // A failed upload fails the etag future too. The only failure the mock lets through to
        // the sink is the CompleteMultipartUpload one, which the sink sends from its own
        // flush(), so the etag is resolved by that flush and carries the very exception caught
        // above. (The mock also answers the abort-upload request with 404 under the
        // NONRETRYABLE_FAILURE policy, but abort_upload() swallows that one.) The close() the
        // scope above ended with flushed the sink a second time, but that flush had nothing
        // left to do: the first one took the etag promise out of the sink, so there was no
        // second resolve, and finalize_upload() had set _object_produced before sending its
        // request, which keeps this flush out of the plain-PUT fallback that would otherwise
        // overwrite the object with an empty one.
        BOOST_REQUIRE(etag.failed());
        etag.ignore_ready_future();
        std::rethrow_exception(ex);
    }
    // A successful upload resolves it with the tag of the completed upload.
    BOOST_REQUIRE(etag.available());
    BOOST_REQUIRE(!etag.get().empty());
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

static std::optional<double> sum_s3_metric(const sstring& metric_name) {
    const auto& value_map = seastar::metrics::impl::get_value_map();
    auto fam_it = value_map.find(metric_name);
    if (fam_it == value_map.end()) {
        return std::nullopt;
    }
    std::optional<double> total;
    for (const auto& [holder, reg] : fam_it->second) {
        if (reg && reg->is_enabled()) {
            total = total.value_or(0.0) + (*reg)().d();
        }
    }
    return total;
}

static double s3_requests(const sstring& metric_name) {
    return sum_s3_metric(metric_name).value_or(0);
}

struct copy_requests {
    double heads, posts, puts;
};

// Copies a source with the policy as a single request and returns the requests it took, or the failure.
static std::pair<copy_requests, std::exception_ptr> single_request_copy(const char* test_name, failure_policy policy) {
    const auto source = fmt::format("/test/copysource-{}-{}", test_name, ::getpid());
    const auto target = fmt::format("/test/copytarget-{}-{}", test_name, ::getpid());
    register_policy(source, policy);

    auto cln = s3::client::make(get_address(), make_mock_config());
    auto close_client = deferred_close(*cln);
    const copy_requests before{s3_requests("s3_total_head_requests"), s3_requests("s3_total_post_requests"), s3_requests("s3_total_put_requests")};
    std::exception_ptr ex;
    try {
        cln->copy_object(source, target, {}, {}, {}, nullptr, true).get();
    } catch (...) {
        ex = std::current_exception();
    }
    return {{s3_requests("s3_total_head_requests") - before.heads, s3_requests("s3_total_post_requests") - before.posts,
             s3_requests("s3_total_put_requests") - before.puts}, ex};
}

// A source above what one CopyObject takes is refused with a 400, whatever its error code is
// (this one is unknown to the client). The client then asks its size and copies it in parts.
SEASTAR_THREAD_TEST_CASE(test_copy_object_single_request_falls_back_to_multipart_on_400) {
    auto [used, ex] = single_request_copy("refused-large", failure_policy::COPY_SOURCE_REFUSED_LARGE);
    BOOST_REQUIRE(!ex);
    BOOST_REQUIRE_EQUAL(used.heads, 1);
    // Create and complete of the multipart upload.
    BOOST_REQUIRE_EQUAL(used.posts, 2);
    // The refused CopyObject and the two UploadPartCopy of the 6 GiB source.
    BOOST_REQUIRE_EQUAL(used.puts, 3);
}

// So is a 413 EntityTooLarge, which S3-compatible servers may answer.
SEASTAR_THREAD_TEST_CASE(test_copy_object_single_request_falls_back_to_multipart_on_413) {
    auto [used, ex] = single_request_copy("payload-too-large", failure_policy::COPY_SOURCE_PAYLOAD_TOO_LARGE);
    BOOST_REQUIRE(!ex);
    BOOST_REQUIRE_EQUAL(used.heads, 1);
    BOOST_REQUIRE_EQUAL(used.posts, 2);
    BOOST_REQUIRE_EQUAL(used.puts, 3);
}

// A 403 is no refusal of a large source: the copy fails with its own error, without a size HEAD.
SEASTAR_THREAD_TEST_CASE(test_copy_object_single_request_does_not_ask_size_on_403) {
    auto [used, ex] = single_request_copy("forbidden", failure_policy::COPY_SOURCE_FORBIDDEN);
    BOOST_REQUIRE(ex);
    BOOST_REQUIRE_EXCEPTION(std::rethrow_exception(ex), storage_io_error, [] (const storage_io_error& e) {
        return e.code().value() == EACCES && std::string_view(e.what()).contains("Cannot copy the source");
    });
    BOOST_REQUIRE_EQUAL(used.heads, 0);
    BOOST_REQUIRE_EQUAL(used.posts, 0);
}

// A 400 of a source that is not large is a real error, and is reported as it was.
SEASTAR_THREAD_TEST_CASE(test_copy_object_single_request_rethrows_400_of_small_source) {
    auto [used, ex] = single_request_copy("refused-small", failure_policy::COPY_SOURCE_REFUSED_SMALL);
    BOOST_REQUIRE(ex);
    BOOST_REQUIRE_EXCEPTION(std::rethrow_exception(ex), storage_io_error, [] (const storage_io_error& e) {
        return std::string_view(e.what()).contains("Cannot copy the source");
    });
    BOOST_REQUIRE_EQUAL(used.heads, 1);
    BOOST_REQUIRE_EQUAL(used.posts, 0);
}

// Anything but a 400 is not a refusal: the copy fails without asking the size.
SEASTAR_THREAD_TEST_CASE(test_copy_object_single_request_does_not_ask_size_on_404) {
    auto [used, ex] = single_request_copy("not-found", failure_policy::COPY_SOURCE_NOT_FOUND);
    BOOST_REQUIRE(ex);
    BOOST_REQUIRE_EXCEPTION(std::rethrow_exception(ex), storage_io_error, [] (const storage_io_error& e) {
        return e.code().value() == ENOENT;
    });
    BOOST_REQUIRE_EQUAL(used.heads, 0);
}
