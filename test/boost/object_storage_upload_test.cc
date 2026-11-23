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
#include <seastar/testing/test_case.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/testing/test_fixture.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include "db/config.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/object_storage_client.hh"
#include "utils/error_injection.hh"
#include "sstables/storage.hh"
#include "sstables/sstable_version.hh"
#include "utils/upload_progress.hh"
#include "utils/lister.hh"
#include "utils/error_injection.hh"
#include <seastar/util/closeable.hh>
#include <set>

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

static std::set<sstring> list_objects(object_storage_client& client, sstring bucket, sstring prefix) {
    std::set<sstring> names;
    auto lister = client.make_object_lister(std::move(bucket), std::move(prefix), [] (const fs::path&, const directory_entry&) { return true; });
    // Not with_closeable() with a coroutine lambda: its closure dies when that call returns.
    auto close = deferred_close(lister);
    while (auto entry = lister.get().get()) {
        names.insert(entry->name);
    }
    return names;
}

// repaired_at persisted on the node reference is restored on load, without
// writing any other object, and is carried over by clones.
future<> test_persist_repaired_at(test_env_config cfg) {
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

        // Statistics says 7, so that persisting 0 below is observable.
        auto creator = [&env, schema] (shared_sstable sst) { return env.make_sstable(schema, sst->get_version()); };
        auto orig = make_sstable_containing(env.make_sstable(schema), {std::move(mut)}).get();
        auto sst = orig->link_with_rewritten_component(creator, component_type::Statistics,
                [] (sstable& s) { s.update_repaired_at(7); }, update_sstable_id::yes).get();
        BOOST_REQUIRE_EQUAL(env.reusable_sst(sst).get()->get_stats_metadata().repaired_at, 7);

        auto sid = *sst->sstable_identifier();
        auto sid_prefix = fmt::format("{}/{}/", prefix, sid);
        auto objects = list_objects(*client, bucket, sid_prefix);
        BOOST_REQUIRE(!objects.empty());
        auto ref = object_name(bucket, prefix, sid, fmt::format("refs/nodes/{}/{}", env.manager().get_local_host_id(), sst->generation()));

        for (int64_t repaired_at : {42, 0}) {
            sst->persist_repaired_at(repaired_at).get();
            BOOST_REQUIRE_EQUAL(sst->get_stats_metadata().repaired_at, repaired_at);
            BOOST_REQUIRE(sst->sstable_identifier() == sid);
            BOOST_REQUIRE(list_objects(*client, bucket, sid_prefix) == objects);
            BOOST_REQUIRE_EQUAL(client->get_object_metadata(ref).get().at(sstring(object_storage_repaired_at_attribute)), fmt::to_string(repaired_at));
            BOOST_REQUIRE_EQUAL(env.reusable_sst(sst).get()->get_stats_metadata().repaired_at, repaired_at);
        }

#ifdef SCYLLA_ENABLE_ERROR_INJECTION
        // A put that is applied but reported failed: memory adopts the stored value.
        utils::get_local_injector().enable("persist_repaired_at_fail_after_put", true /* one_shot */);
        BOOST_REQUIRE_THROW(sst->persist_repaired_at(13).get(), std::runtime_error);
        BOOST_REQUIRE_EQUAL(sst->get_stats_metadata().repaired_at, 13);
        BOOST_REQUIRE_EQUAL(env.reusable_sst(sst).get()->get_stats_metadata().repaired_at, 13);
        BOOST_REQUIRE(!sst->should_update_repaired_at(13));

        // The re-read fails too: the stored value is unknown, so the same value is written again.
        utils::get_local_injector().enable("persist_repaired_at_fail_after_put", true /* one_shot */);
        utils::get_local_injector().enable("persist_repaired_at_fail_reread", true /* one_shot */);
        BOOST_REQUIRE_THROW(sst->persist_repaired_at(14).get(), std::runtime_error);
        BOOST_REQUIRE_EQUAL(sst->get_stats_metadata().repaired_at, 14);
        BOOST_REQUIRE(sst->should_update_repaired_at(14));
        sst->persist_repaired_at(14).get();
        BOOST_REQUIRE(!sst->should_update_repaired_at(14));
#endif

        sst->persist_repaired_at(42).get();
        for (bool share : {true, false}) {
            auto desc = sst->clone(env.new_generation(), false, share, sst->get_stats_metadata().repaired_at).get();
            BOOST_REQUIRE(desc.sid);
            BOOST_REQUIRE_EQUAL(*desc.sid == sid, share);
            auto clone = env.manager().make_sstable(schema, env.get_storage_options(), desc.generation, desc.sid, sstable_state::normal, desc.version, desc.format);
            clone->load(schema->get_sharder()).get();
            BOOST_REQUIRE_EQUAL(clone->get_stats_metadata().repaired_at, 42);
        }

        // A restored sstable is unrepaired, whatever its Statistics (7) or source reference (42) say.
        auto desc = env.make_storage(schema)->clone_from(*sst, env.new_generation(), false, "").get();
        auto restored = env.manager().make_sstable(schema, env.get_storage_options(), desc.generation, desc.sid, sstable_state::normal, desc.version, desc.format);
        restored->load(schema->get_sharder()).get();
        BOOST_REQUIRE_EQUAL(restored->get_stats_metadata().repaired_at, 0);

        // An sstable marked for deletion is left alone.
        auto doomed = env.reusable_sst(sst).get();
        doomed->mark_for_deletion();
        doomed->persist_repaired_at(5).get();
        BOOST_REQUIRE_EQUAL(doomed->get_stats_metadata().repaired_at, 42);
        BOOST_REQUIRE_EQUAL(client->get_object_metadata(ref).get().at(sstring(object_storage_repaired_at_attribute)), "42");
    }, std::move(cfg));
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

SEASTAR_TEST_CASE(test_persist_repaired_at_s3, *boost::unit_test::precondition(tests::has_scylla_test_env)
        *seastar::testing::async_fixture<s3_fixture>()) {
    return test_persist_repaired_at(test_env_config{ .storage = make_test_object_storage_options("S3") });
}

SEASTAR_FIXTURE_TEST_CASE(test_persist_repaired_at_gs, gcs_fixture, *check_run_test_decorator("ENABLE_GCP_STORAGE_TEST", true)) {
    return test_persist_repaired_at(test_env_config{ .storage = make_test_object_storage_options("GS") });
}

SEASTAR_FIXTURE_TEST_CASE(test_large_file_upload_gs, gcs_fixture, *check_run_test_decorator("ENABLE_GCP_STORAGE_TEST", true)) {
    return test_file_upload(test_env_config{ .storage = make_test_object_storage_options("GS") }, large_size);
}

SEASTAR_FIXTURE_TEST_CASE(test_sstable_object_attributes_gs, gcs_fixture, *check_run_test_decorator("ENABLE_GCP_STORAGE_TEST", true)) {
    return test_sstable_attributes(test_env_config{ .storage = make_test_object_storage_options("GS") });
}

#ifdef SCYLLA_ENABLE_ERROR_INJECTION
// Dropping an endpoint from the config erases its client without close(), while the
// client's background credentials refresh may still be in flight.
SEASTAR_THREAD_TEST_CASE(test_dropped_s3_endpoint_with_inflight_credentials_refresh) {
    // Static credentials keep the provider chain off IMDS/STS.
    tests::tmp_set_env access_key("AWS_ACCESS_KEY_ID", "test");
    tests::tmp_set_env secret_key("AWS_SECRET_ACCESS_KEY", "test");
    constexpr auto injection = "s3_client_update_credentials_wait";
    auto& injector = utils::get_local_injector();
    injector.enable(injection, true);
    auto disable = defer([&] () noexcept { injector.disable(injection); });

    db::config cfg;
    const db::object_storage_endpoint_param ep(db::object_storage_endpoint_param::s3_storage{
            .endpoint = "http://dropped.s3.test:9000", .region = "us-east-1", .iam_role_arn = "", .legacy_format = false});
    cfg.object_storage_endpoints({ep});

    sharded<sstables::storage_manager> sstm;
    sstm.start(std::ref(cfg), sstables::storage_manager::config{.skip_metrics_registration = true}).get();
    auto stop_sstm = deferred_stop(sstm);

    // Instantiate the client; its constructor arms the credentials refresh to fire immediately.
    sstm.local().get_endpoint_client(ep.key());
    for (int tries = 0; injector.waiters(injection) == 0; ++tries) {
        if (tries >= 3000) {
            BOOST_FAIL("the credentials refresh did not reach the injection point within 30s");
        }
        seastar::sleep(std::chrono::milliseconds(10)).get();
    }

    // The storage manager held the only reference: this destroys the client.
    cfg.object_storage_endpoints({});
    BOOST_REQUIRE(!sstm.local().is_known_endpoint(ep.key()));

    injector.receive_message(injection);
    seastar::sleep(std::chrono::seconds(1)).get();
}
#endif
