/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "utils/assert.hh"
#include <seastar/core/sstring.hh>
#include <fmt/ranges.h>
#include <fmt/format.h>

#include <seastar/core/future.hh>
#include <seastar/core/metrics_api.hh>
#include <seastar/util/defer.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/test_fixture.hh>

#include "db/config.hh"
#include "sstables/object_storage_client.hh"
#include "sstables/storage.hh"
#include "sstables/sstable_version.hh"
#include "utils/upload_progress.hh"

#include "test/lib/log.hh"
#include "test/lib/test_utils.hh"
#include "test/lib/s3_fixture.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/sstable_test_env.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/key_utils.hh"
#include "test/lib/gcs_fixture.hh"

namespace fs = std::filesystem;

using namespace sstables;
using namespace tests;

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
    BOOST_REQUIRE((co_await is1.read()).empty());
    BOOST_REQUIRE((co_await is2.read()).empty());
    BOOST_REQUIRE_EQUAL(read, total);
}

future<> test_file_upload(test_env_config cfg, size_t size) {
    auto bucket = cfg.storage.to_map()["bucket"];

    return test_env::do_with_async([size, bucket] (test_env& env) {
        auto ep = env.db_config().object_storage_endpoints().front().key();
        auto client = env.manager().get_endpoint_client(ep);

        tmpdir tmp;
        //for multiple tests that may run in CI in the same time we want different files to be used in each test
        sstring test_file_name = fmt::format("testfile-{}-{}", std::chrono::system_clock::now().time_since_epoch().count(), tests::random::get_int(0, 1000000));
        auto path = tmp.path() / test_file_name;
        object_name name(bucket, test_file_name);
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

future<> test_sstable_attributes(test_env_config cfg) {
    auto cfg_map = cfg.storage.to_map();
    auto bucket = cfg_map["bucket"];
    auto prefix = cfg_map.contains("prefix") ? cfg_map["prefix"] : "sstables";

    return test_env::do_with_async([bucket, prefix] (test_env& env) {
        auto ep = env.db_config().object_storage_endpoints().front().key();
        auto client = env.manager().get_endpoint_client(ep);
        simple_schema ss;
        auto schema = ss.schema();
        auto pk = tests::generate_partition_key(schema).key();
        auto mut = mutation(schema, pk);
        mut.partition().apply_insert(*schema, ss.make_ckey(0), ss.new_timestamp());

        auto sst = make_sstable_containing(env.make_sstable(schema), {std::move(mut)}).get();
        auto sid = sst->sstable_identifier();
        BOOST_REQUIRE(sid);
        auto object = sstables::object_name(bucket, prefix, *sid, sstable_version_constants::TOC_SUFFIX);
        auto attributes = client->get_object_metadata(object).get();
        BOOST_REQUIRE_EQUAL(attributes.at(sstring(object_storage_sstable_version_attribute)), fmt::format("{}", sst->get_version()));
        BOOST_REQUIRE_EQUAL(attributes.at(sstring(object_storage_sstable_format_attribute)), fmt::format("{}", sst->get_format()));
    }, std::move(cfg));
}

// The number of read requests that the object storage clients of the given
// type ("s3" or "gs") have sent.
static uint64_t read_requests(std::string_view type) {
    auto all_metrics = seastar::metrics::impl::get_values();
    const auto& all_metadata = *all_metrics->metadata;
    auto family = std::ranges::find_if(all_metadata, [] (const auto& x) { return x.mf.name == "object_storage_total_read_requests"; });
    BOOST_REQUIRE(family != all_metadata.end());
    const auto& values = all_metrics->values[std::distance(all_metadata.begin(), family)];
    uint64_t total = 0;
    for (size_t i = 0; i < family->metrics.size(); ++i) {
        auto label = family->metrics[i].labels().find("type");
        if (label != family->metrics[i].labels().end() && label->second.value() == type) {
            total += values[i].ui();
        }
    }
    return total;
}

// make_ranged_download_source() reads exactly the requested range, in as few
// requests as the backend allows: on S3, a single request; on GCS, a request
// per 8 MiB, after a request for the object's metadata (see the FIXME in
// gs_client_wrapper::make_ranged_download_source()).
future<> test_ranged_download(test_env_config cfg, sstring type, uint64_t request_size, uint64_t metadata_requests) {
    auto bucket = cfg.storage.to_map()["bucket"];

    return test_env::do_with_async([bucket, type, request_size, metadata_requests] (test_env& env) {
        auto ep = env.db_config().object_storage_endpoints().front().key();
        auto client = env.manager().get_endpoint_client(ep);
        object_name name(bucket, fmt::format("ranged-download-{}", tests::random::get_int(0, 1000000)));
        // A different byte at every offset (mod 251), so data read from the
        // wrong offset is noticed.
        constexpr size_t size = 20 * 1024 * 1024 + 123;
        sstring content(sstring::initialized_later(), size);
        for (size_t i = 0; i < size; i++) {
            content[i] = char(i % 251);
        }
        auto out = output_stream<char>(client->make_upload_sink(name, {}));
        out.write(content.data(), content.size()).get();
        out.flush().get();
        out.close().get();
        auto delete_object = defer([&] () noexcept {
            try {
                client->delete_object(name).get();
            } catch (...) {
                testlog.warn("Failed to delete {}: {}", name, std::current_exception());
            }
        });

        auto check_range = [&] (uint64_t offset, uint64_t length) {
            auto requests_before = read_requests(type);
            auto in = input_stream<char>(client->make_ranged_download_source(name, offset, length));
            sstring result;
            while (auto buf = in.read().get()) {
                result += sstring(buf.get(), buf.size());
            }
            in.close().get();
            BOOST_REQUIRE_EQUAL(result, content.substr(offset, length));
            uint64_t expected_requests = length ? metadata_requests + (length - 1) / request_size + 1 : 0;
            BOOST_REQUIRE_EQUAL(read_requests(type) - requests_before, expected_requests);
        };
        // Check a short range, in the middle of the object.
        check_range(1000, 5000);
        // Check a range at the end of the object.
        check_range(size - 300, 300);
        // Check a long range, more than one GCS request.
        check_range(3 * 1024 * 1024 + 7, 12 * 1024 * 1024);
        // Check that an empty range sends no request.
        check_range(5000, 0);
    }, std::move(cfg));
}

SEASTAR_TEST_CASE(test_ranged_download_s3, *boost::unit_test::precondition(tests::has_scylla_test_env)
        *seastar::testing::async_fixture<s3_fixture>()) {
    // On S3, any range is a single request.
    return test_ranged_download(test_env_config{ .storage = make_test_object_storage_options("S3") }, "s3",
            std::numeric_limits<uint64_t>::max(), 0);
}

SEASTAR_FIXTURE_TEST_CASE(test_ranged_download_gs, gcs_fixture, *check_run_test_decorator("ENABLE_GCP_STORAGE_TEST", true)) {
    return test_ranged_download(test_env_config{ .storage = make_test_object_storage_options("GS") }, "gs",
            8 * 1024 * 1024, 1);
}

constexpr auto large_size = 256 * 1024 * 1024 + 351;

SEASTAR_TEST_CASE(test_large_file_upload_s3, *boost::unit_test::precondition(tests::has_scylla_test_env)
        *seastar::testing::async_fixture<s3_fixture>()) {
    return test_file_upload(test_env_config{ .storage = make_test_object_storage_options("S3") }, large_size);
}

SEASTAR_TEST_CASE(test_sstable_object_attributes_s3, *boost::unit_test::precondition(tests::has_scylla_test_env)
        *seastar::testing::async_fixture<s3_fixture>()) {
    return test_sstable_attributes(test_env_config{ .storage = make_test_object_storage_options("S3") });
}

SEASTAR_FIXTURE_TEST_CASE(test_large_file_upload_gs, gcs_fixture, *check_run_test_decorator("ENABLE_GCP_STORAGE_TEST", true)) {
    return test_file_upload(test_env_config{ .storage = make_test_object_storage_options("GS") }, large_size);
}

SEASTAR_FIXTURE_TEST_CASE(test_sstable_object_attributes_gs, gcs_fixture, *check_run_test_decorator("ENABLE_GCP_STORAGE_TEST", true)) {
    return test_sstable_attributes(test_env_config{ .storage = make_test_object_storage_options("GS") });
}
