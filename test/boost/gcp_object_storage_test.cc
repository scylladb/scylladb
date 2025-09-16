/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "utils/gcp/object_storage.hh"
#include "utils/gcp/gcp_credentials.hh"

#include <unordered_set>
#include <filesystem>
#include <boost/test/unit_test.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

#include <seastar/core/thread.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/file.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/with_timeout.hh>

#include "test/lib/scylla_test_case.hh"
#include "test/lib/log.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/test_utils.hh"
#include "test/lib/tmpdir.hh"
#include "test/lib/gcs_fixture.hh"
#include "test/lib/test_utils.hh"
#include "utils/assert.hh"
#include "utils/UUID_gen.hh"
#include "utils/io-wrappers.hh"

#include <seastar/testing/test_fixture.hh>

using namespace std::string_view_literals;
using namespace std::chrono_literals;
using namespace utils::gcp;

/*
    Simple test of GCP object storage provider. Uses either real or local, fake, endpoint.

    Note: the above text blobs are service account credentials, including private keys. 
    _Never_ give any real priviledges to these accounts, as we are obviously exposing them here.

    User1 is assumed to have permissions to read/write the bucket
    User2 is assumed to only have permissions to read the bucket, but permission to 
    impersonate User1.

    Note: fake gcp storage does not have any credentials or permissions, so
    for testing with such, leave them unset to skip those tests.

    This test is parameterized with env vars:
    * ENABLE_GCP_STORAGE_TEST - set to non-zero (1/true) to run
    * GCP_STORAGE_ENDPOINT - set to endpoint host. default is https://storage.googleapis.com
    * GCP_STORAGE_PROJECT - project in which to create bucket (if not specified)
    * GCP_STORAGE_USER_1_CREDENTIALS - set to credentials file for user1
    * GCP_STORAGE_USER_2_CREDENTIALS - set to credentials file for user2
    * GCP_STORAGE_BUCKET - set to test bucket
*/

static auto check_gcp_storage_test_enabled() {
    return tests::check_run_test_decorator("ENABLE_GCP_STORAGE_TEST", true);
}

static future<> create_object_of_size(storage::client& c, std::string_view bucket, std::string_view name, size_t dest_size, std::vector<temporary_buffer<char>>* buffer_store = nullptr) {
    auto sink = c.create_upload_sink(bucket, name);
    seastar::output_stream<char> os(std::move(sink));
    size_t done = 0;
    while (done < dest_size) {
        auto rem = dest_size - done;
        auto len = std::min(rem, tests::random::get_int(size_t(1), size_t(4*1024*1024)));
        auto rnd = tests::random::get_bytes(len);
        auto data = reinterpret_cast<char*>(rnd.data());
        co_await os.write(data, len);
        if (buffer_store) {
            buffer_store->emplace_back(data, len, make_object_deleter(std::move(rnd)));
        }
        done += len;
    }
    co_await os.flush();
    co_await os.close();
}

static future<> test_read_write_helper(const local_gcs_wrapper& env, size_t dest_size) {
    auto& c = env.client();
    {
        auto uuid = fmt::format("{}", utils::UUID_gen::get_time_UUID());
        std::vector<temporary_buffer<char>> written;

        // ensure we remove the object
        env.objects_to_delete.emplace_back(uuid);
        co_await create_object_of_size(c, env.bucket, uuid, dest_size, &written);

        auto source = c.create_download_source(env.bucket, uuid);
        auto is1 = seastar::input_stream<char>(std::move(source));
        auto is2 = seastar::input_stream<char>(create_memory_source(std::move(written)));

        uint64_t read = 0;
        while (!is1.eof()) {
            auto buf = co_await is1.read();
            if (buf.empty()) {
                break;
            }
            auto buf2 = co_await is2.read_exactly(buf.size());
            BOOST_REQUIRE_EQUAL(buf, buf2);
            read += buf.size();
        }

        BOOST_REQUIRE_EQUAL(read, dest_size);
    }
}

BOOST_AUTO_TEST_SUITE(gcs_tests, *seastar::testing::async_fixture<gcs_fixture>())

SEASTAR_FIXTURE_TEST_CASE(test_gcp_storage_create_small_object, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    co_await test_read_write_helper(*this, 8*4);
}

SEASTAR_FIXTURE_TEST_CASE(test_gcp_storage_create_large_object, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    co_await test_read_write_helper(*this, 32*1024*1024 + 357 + 1022*67);
}

SEASTAR_FIXTURE_TEST_CASE(test_gcp_storage_list_objects, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    auto& env = *this;
    auto& c = env.client();
    {
        std::unordered_map<std::string, uint64_t> names;
        for (size_t i = 0; i < 10; ++i) {
            auto name = fmt::format("{}", utils::UUID_gen::get_time_UUID());
            auto size = tests::random::get_int(size_t(1), size_t(2*1024*1024));
            env.objects_to_delete.emplace_back(name);
            co_await create_object_of_size(c, env.bucket, name, size);
            names.emplace(name, size);
        }

        auto infos = co_await c.list_objects(env.bucket);
        size_t n_found = 0;

        for (auto& info : infos) {
            auto i = names.find(info.name);
            if (i != names.end()) {
                BOOST_REQUIRE_EQUAL(info.size, i->second);
                ++n_found;
            }
        }
        BOOST_REQUIRE_EQUAL(n_found, names.size());
    }
}

SEASTAR_FIXTURE_TEST_CASE(test_gcp_storage_delete_object, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    auto& env = *this;
    auto& c = env.client();
    {
        auto name = fmt::format("{}", utils::UUID_gen::get_time_UUID());
        env.objects_to_delete.emplace_back(name);
        co_await create_object_of_size(c, env.bucket, name, 128);
        {
            // validate object was created.
            auto infos = co_await c.list_objects(env.bucket, name);
            BOOST_REQUIRE(std::find_if(infos.begin(), infos.end(), [&](auto& info) {
                return info.name == name;
            }) != infos.end());
        }

        co_await c.delete_object(env.bucket, name);

        auto infos = co_await c.list_objects(env.bucket, name);
        BOOST_REQUIRE(infos.empty());
    }
}

SEASTAR_FIXTURE_TEST_CASE(test_gcp_storage_skip_read, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    auto& env = *this;
    auto& c = env.client();
    {
        auto name = fmt::format("{}", utils::UUID_gen::get_time_UUID());
        std::vector<temporary_buffer<char>> bufs;
        constexpr size_t file_size = 12*1024*1024 + 384*7 + 31;

        co_await create_object_of_size(c, env.bucket, name, 12*1024*1024, &bufs);
        for (size_t i = 0; i < 20; ++i) {
            auto source = c.create_download_source(env.bucket, name);
            auto copy = bufs | std::views::transform([](auto& buf) { return buf.share(); });
            auto is1 = seastar::input_stream<char>(std::move(source));
            auto is2 = seastar::input_stream<char>(create_memory_source(std::vector<temporary_buffer<char>>(copy.begin(), copy.end())));

            std::exception_ptr p;
            try {

                size_t pos = 0; 
                while (pos < file_size) {
                    auto rem = file_size - pos;
                    auto skip = tests::random::get_int(std::min(rem, size_t(100)), rem);
                    auto read = std::min(rem - skip, size_t(tests::random::get_int(31, 2048)));
                    co_await is1.skip(skip);
                    co_await is2.skip(skip);

                    auto b1 = co_await is1.read_exactly(read);
                    auto b2 = co_await is2.read_exactly(read);

                    BOOST_REQUIRE_EQUAL(b1.size(), b2.size());
                    if (b1 != b2) {
                        BOOST_TEST_MESSAGE(fmt::format("diff at {} ({} bytes)", pos + skip, read));
                        auto i = std::mismatch(b1.begin(), b1.end(), b2.begin());
                        BOOST_TEST_MESSAGE(fmt::format("offset {}", std::distance(b1.begin(), i.first)));
                    }
                    BOOST_REQUIRE_EQUAL(b1, b2);
                    pos += (skip + read);
                }
            } catch (...) {
                p = std::current_exception();
            }
            co_await is1.close();
            co_await is2.close();
            if (p) {
                std::rethrow_exception(p);
            }
        }
    }
}

BOOST_AUTO_TEST_SUITE_END()
