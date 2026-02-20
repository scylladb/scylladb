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
#include "service/topology_coordinator.hh"
#include "replica/database_fwd.hh"
#include "tasks/task_handler.hh"
#include "db/system_distributed_keyspace.hh"
#include "service/storage_proxy.hh"

using namespace sstables;


future<> create_dataset(sstring cf, cql_test_env& env) {
    co_await env.create_table([cf] (std::string_view ks_name) {
        return *schema_builder(ks_name, cf)
            .with_column("a", utf8_type, column_kind::partition_key)
            .with_column("b", int32_type, column_kind::clustering_key)
            .build();
    });
    auto stmt = co_await env.prepare(fmt::format("insert into {} (a, b) values (?, ?)", cf));
    auto make_key = [] (int64_t k) {
        std::string s = fmt::format("key{}", k);
        return cql3::raw_value::make_value(utf8_type->decompose(s));
    };
    auto make_val = [] (int64_t x) {
        return cql3::raw_value::make_value(int32_type->decompose(int32_t{x}));
    };
    
    co_await env.execute_prepared(stmt, {make_key(1), make_val(2)});
    co_await env.execute_prepared(stmt, {make_key(2), make_val(3)});
    co_await env.execute_prepared(stmt, {make_key(3), make_val(4)});
}

future<> take_snapshot(cql_test_env& env, sstring cf, sstring snapshot_name) {
    auto uuid = env.db().local().find_uuid("ks", cf);
    co_await replica::database::snapshot_table_on_all_shards(env.db(), uuid, snapshot_name, {});
}

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

    return do_with_cql_env([storage_options = std::move(storage_options)] (cql_test_env& env) -> future<> {
            co_await create_dataset("cf", env);
            co_await take_snapshot(env, "cf", "snapshot");

            auto ep = storage_options.to_map()["endpoint"];
            auto bucket = storage_options.to_map()["bucket"];
            co_await backup(env, ep, bucket);

            // populate system_distributed.snapshot_sstables with the content of the snapshot manifest
            co_await service::populate_snapshot_sstables_from_manifests(env.get_sstorage_manager().local(), env.get_system_distributed_keyspace().local(), ep, bucket, {"/backup/manifest.json"}, db::consistency_level::ONE);

            co_await check_snapshot_sstables(env);
    }, db_cfg_ptr);
}
