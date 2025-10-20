/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "utils/assert.hh"
#include <fmt/ranges.h>

#include <seastar/core/future.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/test_fixture.hh>

#include "db/config.hh"
#include "sstables/object_storage_client.hh"
#include "sstables/storage.hh"
#include "utils/upload_progress.hh"

#include "test/lib/test_utils.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/sstable_test_env.hh"
#include "test/lib/gcs_fixture.hh"

namespace fs = std::filesystem;

using namespace sstables;
using namespace tests;
#if 1
static future<> create_file_of_size(fs::path file, size_t dest_size) {
    auto f = co_await seastar::open_file_dma(file.string(), open_flags::wo|open_flags::create);
    auto os = co_await make_file_output_stream(std::move(f));
    size_t done = 0;
    while (done < dest_size) {
        auto rem = dest_size - done;
        auto len = std::min(rem, size_t(8*1024));
        auto rnd = tests::random::get_bytes(len);
        for (size_t i = 0; i < rnd.size(); ++i) {
            //rnd[i] = "ABCDEFGHIJKLMNO"[i % 16];
        }
        auto data = reinterpret_cast<char*>(rnd.data());
        co_await os.write(data, len);
        done += len;
    }
    co_await os.flush();
    co_await os.close();
}
#endif
static future<> compare_streams(input_stream<char>& is1, input_stream<char>& is2, size_t total) {
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
    BOOST_REQUIRE((co_await is2.read()).empty());
    BOOST_REQUIRE_EQUAL(read, total);
}

future<> test_file_upload(test_env_config cfg, size_t size) {
    auto bucket = cfg.storage.to_map()["bucket"];

    return test_env::do_with_async([size, bucket] (test_env& env) {
        auto ep = env.db_config().object_storage_endpoints().front().key();
        auto client = env.manager().get_endpoint_client(ep);

        tmpdir tmp;
        auto path = tmp.path() / "testfile";
        object_name name(bucket, "testfile");
        utils::upload_progress up;
        create_file_of_size(path, size).get();

        client->upload_file(path, name, up).get();

        auto source = client->make_download_source(name);
        auto is1 = make_file_input_stream(seastar::open_file_dma(path.string(), open_flags::ro).get());
        auto is2 = input_stream<char>(std::move(source));

        compare_streams(is1, is2, size).get();

        is1.close().get();
        is2.close().get();

    }, std::move(cfg));
}

constexpr auto large_size = 256 * 1024 * 1024 + 351;

SEASTAR_TEST_CASE(test_large_file_upload_s3, *boost::unit_test::precondition(tests::has_scylla_test_env)) {
    return test_file_upload(test_env_config{ .storage = make_test_object_storage_options("S3") }, large_size);
}

SEASTAR_FIXTURE_TEST_CASE(test_large_file_upload_gs, gcs_fixture, *check_run_test_decorator("ENABLE_GCP_STORAGE_TEST", true)) {
    return test_file_upload(test_env_config{ .storage = make_test_object_storage_options("GS") }, large_size);
}
