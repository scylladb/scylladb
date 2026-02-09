/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */



#include "test/lib/cql_test_env.hh"
#include "utils/assert.hh"
#include <seastar/core/sstring.hh>
#include <fmt/ranges.h>
#include <fmt/format.h>

#include <seastar/core/future.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/test_fixture.hh>

#include "db/config.hh"
#include "sstables/object_storage_client.hh"
#include "sstables/storage.hh"

#include "test/lib/test_utils.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/sstable_test_env.hh"
#include "sstables_loader.hh"
#include "replica/database_fwd.hh"
#include "tasks/task_handler.hh"
#include "db/system_distributed_keyspace.hh"
#include "service/storage_proxy.hh"
#include "test/boost/database_test.hh"

using namespace sstables;

future<> backup(cql_test_env& env, sstring endpoint, sstring bucket) {
    sharded<db::snapshot_ctl> ctl;
    co_await ctl.start(std::ref(env.db()), std::ref(env.get_task_manager()), std::ref(env.get_sstorage_manager()), db::snapshot_ctl::config{});
    auto prefix = "/backup";

    auto task_id = co_await ctl.local().start_backup(endpoint, bucket, prefix, "ks", "cf", "snapshot", false);
    auto task = tasks::task_handler{env.get_task_manager().local(), task_id};
    auto status = co_await task.wait_for_task(30s);
    BOOST_REQUIRE(status.state == tasks::task_manager::task_state::done);

    co_await ctl.stop();
}

future<> check_snapshot_sstables(cql_test_env& env) {
    auto& topology = env.get_storage_proxy().local().get_token_metadata_ptr()->get_topology();
    auto dc = topology.get_datacenter();
    auto rack = topology.get_rack();
    auto sstables = co_await env.get_system_distributed_keyspace().local().get_snapshot_sstables("snapshot", "ks", "cf", dc, rack, db::consistency_level::ONE);

    // Check that the sstables in system_distributed.snapshot_sstables match the sstables in the snapshot directory on disk
    auto& cf = env.local_db().find_column_family("ks", "cf");
    auto tabledir = tests::table_dir(cf);
    auto snapshot_dir = tabledir / sstables::snapshots_dir / "snapshot";
    std::set<sstring> expected_sstables;
    directory_lister lister(snapshot_dir, lister::dir_entry_types::of<directory_entry_type::regular>());
    while (auto de = co_await lister.get()) {
        if (!de->name.ends_with("-TOC.txt")) {
            continue;
        }
        expected_sstables.insert(de->name);
    }

    BOOST_CHECK_EQUAL(sstables.size(), expected_sstables.size());

    for (const auto& sstable : sstables) {
        BOOST_CHECK(expected_sstables.contains(sstable.toc_name));
    }
}

SEASTAR_TEST_CASE(test_populate_snapshot_sstables_from_manifests, *boost::unit_test::precondition(tests::has_scylla_test_env)) {
    using namespace sstables;

    auto db_cfg_ptr = make_shared<db::config>();
    db_cfg_ptr->tablets_mode_for_new_keyspaces(db::tablets_mode_t::mode::enabled);
    db_cfg_ptr->experimental_features({db::experimental_features_t::feature::KEYSPACE_STORAGE_OPTIONS});
    auto storage_options = make_test_object_storage_options("S3");
    db_cfg_ptr->object_storage_endpoints(make_storage_options_config(storage_options));

    return do_with_some_data_in_thread({"cf"}, [storage_options = std::move(storage_options)] (cql_test_env& env) {
            take_snapshot(env, "ks", "cf", "snapshot").get();

            auto ep = storage_options.to_map()["endpoint"];
            auto bucket = storage_options.to_map()["bucket"];
            backup(env, ep, bucket).get();

            BOOST_REQUIRE_THROW(populate_snapshot_sstables_from_manifests(env.get_sstorage_manager().local(), env.get_system_distributed_keyspace().local(), ep, bucket, "unexpected_snapshot", {"/backup/manifest.json"}, db::consistency_level::ONE).get(), std::runtime_error);;

            // populate system_distributed.snapshot_sstables with the content of the snapshot manifest
            populate_snapshot_sstables_from_manifests(env.get_sstorage_manager().local(), env.get_system_distributed_keyspace().local(), ep, bucket, "snapshot", {"/backup/manifest.json"}, db::consistency_level::ONE).get();

            check_snapshot_sstables(env).get();
    }, false, db_cfg_ptr, 10);
}
