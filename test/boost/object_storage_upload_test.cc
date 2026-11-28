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
#include <seastar/core/metrics_api.hh>

#include "db/config.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/object_storage_client.hh"
#include "utils/error_injection.hh"
#include "sstables/storage.hh"
#include "sstables/sstable_version.hh"
#include "utils/upload_progress.hh"

#include "test/lib/test_utils.hh"
#include "test/lib/s3_fixture.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/sstable_test_env.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/key_utils.hh"
#include "test/lib/gcs_fixture.hh"
#include "test/lib/eventually.hh"
#include "utils/memory_data_sink.hh"

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

// The http client counts a request once in total_*_requests; its retries are a separate counter.
static double object_storage_delete_requests() {
    double total = 0;
    const auto& value_map = seastar::metrics::impl::get_value_map();
    if (auto it = value_map.find("object_storage_total_delete_requests"); it != value_map.end()) {
        for (const auto& [holder, reg] : it->second) {
            if (reg && reg->is_enabled()) {
                total += (*reg)().d();
            }
        }
    }
    return total;
}

// Every object under "{prefix}/{sid}/", by name relative to it.
static std::vector<sstring> list_sstable_objects(sstables::object_storage_client& client, const sstring& bucket, const sstring& prefix, sstable_id sid) {
    auto lister = client.make_object_lister(bucket, fmt::format("{}/{}/", prefix, sid), [] (const fs::path&, const directory_entry&) { return true; });
    std::vector<sstring> names;
    while (auto de = lister.get().get()) {
        names.push_back(de->name);
    }
    lister.close().get();
    return names;
}

static size_t count_components(const std::vector<sstring>& objects) {
    return std::ranges::count_if(objects, [] (const sstring& name) { return !name.starts_with("refs/"); });
}

static shared_sstable make_object_storage_sstable(test_env& env) {
    simple_schema ss;
    auto schema = ss.schema();
    auto mut = mutation(schema, tests::generate_partition_key(schema).key());
    mut.partition().apply_insert(*schema, ss.make_ckey(0), ss.new_timestamp());
    return make_sstable_containing(env.make_sstable(schema), {std::move(mut)}).get();
}

enum class removal { release_snapshot_ref, remove_by_registry_entry, destroy };

// Removes the last reference of a freshly written sstable in the given way and
// checks that only the objects actually there are deleted, and all of them.
static future<> test_removal_deletes_existing_objects(test_env_config cfg, removal how) {
    auto os = std::get<data_dictionary::storage_options::object_storage>(cfg.storage.value);
    return test_env::do_with_async([os, how] (test_env& env) {
        auto bucket = sstring(os.bucket);
        auto prefix = sstables::object_storage_prefix(os);
        auto client = env.manager().get_endpoint_client(env.db_config().object_storage_endpoints().front().key());
        auto sst = make_object_storage_sstable(env);
        auto sid = *sst->sstable_identifier();
        entry_descriptor desc(sst->generation(), sid, sst->get_version(), sst->get_format(), component_type::TOC);

        auto objects = list_sstable_objects(*client, bucket, prefix, sid);
        auto components = count_components(objects);
        BOOST_REQUIRE_GT(components, 0);
        // The node reference written with the sstable.
        BOOST_REQUIRE_EQUAL(objects.size() - components, 1);

        auto deletes = object_storage_delete_requests();
        switch (how) {
        case removal::release_snapshot_ref:
            // Trade the node reference for a snapshot one, which is then the last.
            for (auto& name : objects) {
                if (name.starts_with("refs/")) {
                    client->delete_object(sstables::object_name(bucket, prefix, sid, name)).get();
                }
            }
            client->put_object(sstables::object_name(bucket, prefix, sid, object_storage_snapshot_ref_name("snap", desc.generation)),
                    memory_data_sink_buffers(), {}).get();
            deletes = object_storage_delete_requests();
            release_object_storage_snapshot_ref(*client, os, "snap", desc).get();
            break;
        case removal::remove_by_registry_entry:
            sst->get_storage().remove_by_registry_entry(desc, env.manager().get_local_host_id()).get();
            break;
        case removal::destroy:
            sst->unlink().get();
            sst = {};
            // destroy() runs in the background once the last reference is dropped.
            BOOST_REQUIRE(eventually_true([&] { return list_sstable_objects(*client, bucket, prefix, sid).empty(); }));
            break;
        }
        BOOST_REQUIRE(list_sstable_objects(*client, bucket, prefix, sid).empty());
        // S3 batches the DELETEs; elsewhere it's one per object there: the components and the last reference.
        if (os.type != "S3") {
            BOOST_REQUIRE_EQUAL(object_storage_delete_requests() - deletes, components + 1);
        }
    }, std::move(cfg));
}

SEASTAR_TEST_CASE(test_release_snapshot_ref_deletes_existing_objects_s3, *boost::unit_test::precondition(tests::has_scylla_test_env)
        *seastar::testing::async_fixture<s3_fixture>()) {
    return test_removal_deletes_existing_objects(test_env_config{ .storage = make_test_object_storage_options("S3") }, removal::release_snapshot_ref);
}

SEASTAR_TEST_CASE(test_remove_by_registry_entry_deletes_existing_objects_s3, *boost::unit_test::precondition(tests::has_scylla_test_env)
        *seastar::testing::async_fixture<s3_fixture>()) {
    return test_removal_deletes_existing_objects(test_env_config{ .storage = make_test_object_storage_options("S3") }, removal::remove_by_registry_entry);
}

SEASTAR_TEST_CASE(test_destroy_deletes_existing_objects_s3, *boost::unit_test::precondition(tests::has_scylla_test_env)
        *seastar::testing::async_fixture<s3_fixture>()) {
    return test_removal_deletes_existing_objects(test_env_config{ .storage = make_test_object_storage_options("S3") }, removal::destroy);
}

// Another reference left keeps the components, and only the removed reference is deleted.
static future<> test_removal_keeps_referenced_objects(test_env_config cfg, removal how) {
    auto os = std::get<data_dictionary::storage_options::object_storage>(cfg.storage.value);
    return test_env::do_with_async([os, how] (test_env& env) {
        auto bucket = sstring(os.bucket);
        auto prefix = sstables::object_storage_prefix(os);
        auto client = env.manager().get_endpoint_client(env.db_config().object_storage_endpoints().front().key());
        auto sst = make_object_storage_sstable(env);
        auto sid = *sst->sstable_identifier();
        entry_descriptor desc(sst->generation(), sid, sst->get_version(), sst->get_format(), component_type::TOC);
        client->put_object(sstables::object_name(bucket, prefix, sid, object_storage_snapshot_ref_name("snap", desc.generation)),
                memory_data_sink_buffers(), {}).get();
        auto components = count_components(list_sstable_objects(*client, bucket, prefix, sid));
        auto tid = sst->get_schema()->id();
        auto gen = sst->generation();

        auto deletes = object_storage_delete_requests();
        switch (how) {
        case removal::release_snapshot_ref:
            // The node reference stays.
            release_object_storage_snapshot_ref(*client, os, "snap", desc).get();
            break;
        case removal::remove_by_registry_entry:
            sst->get_storage().remove_by_registry_entry(desc, env.manager().get_local_host_id()).get();
            break;
        case removal::destroy:
            sst->unlink().get();
            sst = {};
            // destroy() deletes the registry entry last, after any component DELETEs.
            BOOST_REQUIRE(eventually_true([&] {
                bool found = false;
                env.manager().sstables_registry().sstables_registry_list(tid, env.manager().get_local_host_id(),
                        [&] (sstring, sstable_state, entry_descriptor e) { found |= e.generation == gen; return make_ready_future<>(); }).get();
                return !found;
            }));
            break;
        }
        BOOST_REQUIRE_EQUAL(object_storage_delete_requests() - deletes, 1);
        auto left = list_sstable_objects(*client, bucket, prefix, sid);
        BOOST_REQUIRE_EQUAL(count_components(left), components);
        BOOST_REQUIRE_EQUAL(left.size(), components + 1);
    }, std::move(cfg));
}

SEASTAR_TEST_CASE(test_release_snapshot_ref_keeps_referenced_objects_s3, *boost::unit_test::precondition(tests::has_scylla_test_env)
        *seastar::testing::async_fixture<s3_fixture>()) {
    return test_removal_keeps_referenced_objects(test_env_config{ .storage = make_test_object_storage_options("S3") }, removal::release_snapshot_ref);
}

SEASTAR_TEST_CASE(test_remove_by_registry_entry_keeps_referenced_objects_s3, *boost::unit_test::precondition(tests::has_scylla_test_env)
        *seastar::testing::async_fixture<s3_fixture>()) {
    return test_removal_keeps_referenced_objects(test_env_config{ .storage = make_test_object_storage_options("S3") }, removal::remove_by_registry_entry);
}

SEASTAR_TEST_CASE(test_destroy_keeps_referenced_objects_s3, *boost::unit_test::precondition(tests::has_scylla_test_env)
        *seastar::testing::async_fixture<s3_fixture>()) {
    return test_removal_keeps_referenced_objects(test_env_config{ .storage = make_test_object_storage_options("S3") }, removal::destroy);
}

SEASTAR_FIXTURE_TEST_CASE(test_release_snapshot_ref_deletes_existing_objects_gs, gcs_fixture, *check_run_test_decorator("ENABLE_GCP_STORAGE_TEST", true)) {
    return test_removal_deletes_existing_objects(test_env_config{ .storage = make_test_object_storage_options("GS") }, removal::release_snapshot_ref);
}

SEASTAR_FIXTURE_TEST_CASE(test_remove_by_registry_entry_deletes_existing_objects_gs, gcs_fixture, *check_run_test_decorator("ENABLE_GCP_STORAGE_TEST", true)) {
    return test_removal_deletes_existing_objects(test_env_config{ .storage = make_test_object_storage_options("GS") }, removal::remove_by_registry_entry);
}

SEASTAR_FIXTURE_TEST_CASE(test_destroy_deletes_existing_objects_gs, gcs_fixture, *check_run_test_decorator("ENABLE_GCP_STORAGE_TEST", true)) {
    return test_removal_deletes_existing_objects(test_env_config{ .storage = make_test_object_storage_options("GS") }, removal::destroy);
}

SEASTAR_FIXTURE_TEST_CASE(test_release_snapshot_ref_keeps_referenced_objects_gs, gcs_fixture, *check_run_test_decorator("ENABLE_GCP_STORAGE_TEST", true)) {
    return test_removal_keeps_referenced_objects(test_env_config{ .storage = make_test_object_storage_options("GS") }, removal::release_snapshot_ref);
}

SEASTAR_FIXTURE_TEST_CASE(test_remove_by_registry_entry_keeps_referenced_objects_gs, gcs_fixture, *check_run_test_decorator("ENABLE_GCP_STORAGE_TEST", true)) {
    return test_removal_keeps_referenced_objects(test_env_config{ .storage = make_test_object_storage_options("GS") }, removal::remove_by_registry_entry);
}

SEASTAR_FIXTURE_TEST_CASE(test_destroy_keeps_referenced_objects_gs, gcs_fixture, *check_run_test_decorator("ENABLE_GCP_STORAGE_TEST", true)) {
    return test_removal_keeps_referenced_objects(test_env_config{ .storage = make_test_object_storage_options("GS") }, removal::destroy);
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
