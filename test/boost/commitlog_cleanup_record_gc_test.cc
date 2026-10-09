/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <fmt/ranges.h>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "db/commitlog/commitlog.hh"
#include "db/config.hh"
#include "db/system_keyspace.hh"

// Pinned to one shard (test_config.yaml): cf1's live mutation must pin the
// segment holding cf2's stale cleanup records, so both need the same shard.
BOOST_AUTO_TEST_SUITE(commitlog_cleanup_record_gc_test)

// Test that commitlog cleanup records are deleted when they become irrelevant.
SEASTAR_TEST_CASE(test_commitlog_cleanup_record_gc) {
    BOOST_REQUIRE_EQUAL(this_smp_shard_count(), 1);
    auto cfg = cql_test_config();
    cfg.db_config->auto_snapshot.set(false);
    cfg.db_config->commitlog_sync.set("batch");
    cfg.db_config->tablets_mode_for_new_keyspaces.set(db::tablets_mode_t::mode::enabled);
    cfg.initial_tablets = 1;

    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("create table ks.cf1 (pk int, ck int, primary key (pk, ck))").get();
        e.execute_cql("create table ks.cf2 (pk int, ck int, primary key (pk, ck))").get();

        auto insert_mutation = [&] (std::string cf) {
            e.execute_cql(fmt::format("insert into ks.{} (pk,ck) values (0, 0)", cf)).get();
        };
        auto cleanup_tablet = [&] (std::string cf) {
            auto& db = e.local_db();
            db.find_column_family("ks", cf).cleanup_tablet_without_deallocation(db, e.get_system_keyspace().local(), locator::tablet_id(0)).get();
        };
        auto get_num_records = [&] {
            auto res = e.execute_cql("select * from system.commitlog_cleanups;").get();
            auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(res);
            BOOST_REQUIRE(rows);
            return rows->rs().result_set().size();
        };
        auto step_cf = [&] (std::string cf) {
            e.local_db().commitlog()->force_new_active_segment().get();
            e.local_db().commitlog()->wait_for_pending_deletes().get();
            insert_mutation(cf);
            cleanup_tablet(cf);
        };

        // Insert a mutation to cf1 to pin a commitlog segment.
        insert_mutation("cf1");

        // Run some insertions and cleanups on cf2.
        // Commitlog is pinned by cf1, so all cleanup records are relevant, and they
        // keep accumulating.
        step_cf("cf2");
        BOOST_REQUIRE_EQUAL(get_num_records(), 1);
        step_cf("cf2");
        BOOST_REQUIRE_EQUAL(get_num_records(), 2);
        step_cf("cf2");
        BOOST_REQUIRE_EQUAL(get_num_records(), 3);

        // Flush all tables and wait for the released commitlog segments to disappear.
        e.local_db().flush_all_tables().get();
        e.local_db().commitlog()->wait_for_pending_deletes().get();

        // Since the old cleanup records refer to commitlog segments which are now gone,
        // the next cleanup should delete them, leaving only a single new cleanup entry.
        step_cf("cf2");
        BOOST_REQUIRE_EQUAL(get_num_records(), 1);
    }, cfg);
}

BOOST_AUTO_TEST_SUITE_END()
