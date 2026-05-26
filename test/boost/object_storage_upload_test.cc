/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "utils/assert.hh"
#include "utils/overloaded_functor.hh"
#include <seastar/core/sstring.hh>
#include <fmt/ranges.h>
#include <fmt/format.h>

#include <seastar/core/future.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/test_fixture.hh>

#include "db/config.hh"
#include "sstables/sstables.hh"
#include "sstables/object_storage_client.hh"
#include "sstables/storage.hh"
#include "schema/schema_builder.hh"
#include "utils/upload_progress.hh"

#include "test/lib/test_utils.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/sstable_test_env.hh"
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

constexpr auto large_size = 256 * 1024 * 1024 + 351;

SEASTAR_TEST_CASE(test_large_file_upload_s3, *boost::unit_test::precondition(tests::has_scylla_test_env)) {
    return test_file_upload(test_env_config{ .storage = make_test_object_storage_options("S3") }, large_size);
}

SEASTAR_FIXTURE_TEST_CASE(test_large_file_upload_gs, gcs_fixture, *check_run_test_decorator("ENABLE_GCP_STORAGE_TEST", true)) {
    return test_file_upload(test_env_config{ .storage = make_test_object_storage_options("GS") }, large_size);
}

// Exercises the sstable_stream_sink_impl::output() path with object storage.
// Before the fix, output() used open_file() which for S3 returns a readable_file
// (read-only). Writing through it threw std::logic_error("unsupported operation
// on s3 readable file"), breaking tablet migration streaming entirely.
static future<> test_stream_sink_write(test_env_config cfg) {
    return test_env::do_with_async([](test_env& env) {
        auto s = schema_builder("ks", "cf")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("v", utf8_type)
            .build();

        auto version = get_highest_sstable_version();
        auto format = sstable::format_types::big;
        auto generation = env.new_generation();
        auto& mgr = env.manager();
        auto s_opts = env.get_storage_options();

        // Populate the storage-specific location fields — get_storage_options()
        // returns a raw copy without dir (local) or location (object storage),
        // mirroring what make_sstable() does in test_services.cc.
        std::visit(overloaded_functor {
            [&env] (data_dictionary::storage_options::local& o) { o.dir = env.tempdir().path().native(); },
            [&s] (data_dictionary::storage_options::object_storage& o) { o.location = s->id(); },
        }, s_opts.value);

        // Build a TOC component filename — create_stream_sink expects a component
        // filename and parses generation/version/format/component from it.
        auto toc_basename = sstable::component_basename(
                s->ks_name(), s->cf_name(), version, generation, format, component_type::TOC);

        // Create the stream sink for the TOC component (which becomes TemporaryTOC internally).
        // The TOC path is simplest — it doesn't require loading/saving scylla metadata.
        sstable_stream_sink_cfg sink_cfg { .last_component = false, .leave_unsealed = true };
        auto sink = create_stream_sink(s, mgr, s_opts, sstable_state::normal, toc_basename, sink_cfg);

        // This is the call that threw std::logic_error before the fix.
        auto out = sink->output(file_open_options{}, file_output_stream_options{}).get();

        // Write some data to verify the stream is actually writable.
        auto data = tests::random::get_bytes(4096);
        out.write(reinterpret_cast<const char*>(data.data()), data.size()).get();
        out.flush().get();
        out.close().get();
    }, std::move(cfg));
}

SEASTAR_TEST_CASE(test_stream_sink_write_local) {
    return test_stream_sink_write(test_env_config{});
}

SEASTAR_TEST_CASE(test_stream_sink_write_s3, *boost::unit_test::precondition(tests::has_scylla_test_env)) {
    return test_stream_sink_write(test_env_config{ .storage = make_test_object_storage_options("S3") });
}

SEASTAR_FIXTURE_TEST_CASE(test_stream_sink_write_gs, gcs_fixture, *check_run_test_decorator("ENABLE_GCP_STORAGE_TEST", true)) {
    return test_stream_sink_write(test_env_config{ .storage = make_test_object_storage_options("GS") });
}

