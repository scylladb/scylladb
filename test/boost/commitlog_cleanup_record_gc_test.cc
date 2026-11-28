/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <fmt/ranges.h>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/core/sleep.hh>
#include "test/lib/cql_test_env.hh"
#include "replica/database.hh"
#include "db/commitlog/commitlog.hh"
#include "db/config.hh"
#include "db/system_keyspace.hh"
#include "cql3/query_processor.hh"

// Pinned to one shard (test_config.yaml): every test here asserts
// this_smp_shard_count() == 1.
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

// Test that system_keyspace::drop_all_commitlog_cleanup_records() deletes all records,
// including those of shards which don't exist in the current boot cycle, and that it
// issues one DELETE per partition (shard), not one per record.
// Reproduces SCYLLADB-3522.
SEASTAR_TEST_CASE(test_drop_all_commitlog_cleanup_records) {
    BOOST_REQUIRE_EQUAL(this_smp_shard_count(), 1);
    return do_with_cql_env_thread([](cql_test_env& e) {
        auto& sys_ks = e.get_system_keyspace().local();
        auto tableid = table_id(utils::UUID_gen::get_time_UUID());

        // Shards >= smp::count model a previous boot cycle with more shards.
        const std::vector<unsigned> shards = {0, 1, 7, 42};
        const int records_per_shard = 1000;
        // Use a segment id far ahead of the commitlog's, so the records are never
        // obsolete (no commitlog segment precedes them).
        const db::segment_id_type segment_id = db::replay_position::ts_mask;
        for (auto shard : shards) {
            for (int i = 0; i < records_per_shard; ++i) {
                auto tr = dht::token_range::make({dht::token::from_int64(i), false}, {dht::token::from_int64(i + 1), true});
                sys_ks.save_commitlog_cleanup_record(tableid, tr, db::replay_position(shard, segment_id, i + 1)).get();
            }
        }

        auto get_num_records = [&] {
            auto res = e.execute_cql("select * from system.commitlog_cleanups;").get();
            auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(res);
            BOOST_REQUIRE(rows);
            return rows->rs().result_set().size();
        };
        BOOST_REQUIRE_EQUAL(get_num_records(), shards.size() * records_per_shard);

        auto get_num_system_deletes = [&] {
            return e.local_qp().get_cql_stats().query_cnt(cql3::source_selector::INTERNAL, cql3::ks_selector::SYSTEM,
                    cql3::cond_selector::NO_CONDITIONS, cql3::statements::statement_type::DELETE);
        };
        const auto deletes_before = get_num_system_deletes();

        sys_ks.drop_all_commitlog_cleanup_records().get();

        // One DELETE per shard (partition), not one per record.
        BOOST_REQUIRE_EQUAL(get_num_system_deletes() - deletes_before, shards.size());
        BOOST_REQUIRE_EQUAL(get_num_records(), 0);
    });
}

// Test that saving a commitlog cleanup record drops the records which became obsolete,
// because they apply only to commitlog segments which are gone (they are below the
// commitlog's minimum position), and that it keeps the records at or above it.
// Also test that a record which is obsolete already is not saved, and that each save
// is a single write to the table (the record and the dropping are one mutation).
// Reproduces SCYLLADB-3522.
SEASTAR_TEST_CASE(test_obsolete_commitlog_cleanup_records_dropped_on_save) {
    BOOST_REQUIRE_EQUAL(this_smp_shard_count(), 1);
    return do_with_cql_env_thread([](cql_test_env& e) {
        auto& sys_ks = e.get_system_keyspace().local();
        auto& cl = *e.local_db().commitlog();
        auto& t = e.local_db().find_column_family(db::system_keyspace::NAME, db::system_keyspace::COMMITLOG_CLEANUPS);
        auto tableid = table_id(utils::UUID_gen::get_time_UUID());

        auto save_record = [&] (int64_t token, db::replay_position rp) {
            auto tr = dht::token_range::make({dht::token::from_int64(token), false}, {dht::token::from_int64(token + 1), true});
            const auto writes_before = t.get_stats().writes().count();
            sys_ks.save_commitlog_cleanup_record(tableid, tr, rp).get();
            BOOST_REQUIRE_EQUAL(t.get_stats().writes().count() - writes_before, 1);
        };
        auto get_num_records = [&] {
            auto res = e.execute_cql("select * from system.commitlog_cleanups;").get();
            auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(res);
            BOOST_REQUIRE(rows);
            return rows->rs().result_set().size();
        };

        // A record which is never obsolete (no commitlog segment precedes it).
        const auto never_obsolete_rp = db::replay_position(this_shard_id(), db::replay_position::ts_mask, 1);
        save_record(0, never_obsolete_rp);

        // The commitlog's minimum position is the start ({id, 0}) of its oldest segment.
        // Make replay positions in that segment, which are at/above the minimum position.
        auto oldest_segment_rp = [&] (db::position_type pos) {
            const auto min_position = cl.min_position();
            return db::replay_position(min_position.shard_id(), min_position.base_id(), pos);
        };

        // Records in the oldest segment, which are not obsolete.
        const auto rp_1 = oldest_segment_rp(1);
        const auto rp_2 = oldest_segment_rp(10);
        save_record(1, rp_1);
        save_record(2, rp_2);
        BOOST_REQUIRE_EQUAL(get_num_records(), 3);

        // Flush all tables and wait for the released commitlog segments to disappear.
        cl.force_new_active_segment().get();
        e.local_db().flush_all_tables().get();
        cl.wait_for_pending_deletes().get();
        BOOST_REQUIRE(cl.min_position() > rp_2);

        // The records below the commitlog's new minimum position are obsolete now, so the
        // next save drops them. The record in the new oldest segment is not obsolete.
        const auto rp_3 = oldest_segment_rp(1);
        save_record(3, rp_3);
        BOOST_REQUIRE_EQUAL(get_num_records(), 2);

        // A record below the commitlog's minimum position is obsolete already, so it
        // is not saved.
        save_record(4, rp_2);
        BOOST_REQUIRE_EQUAL(get_num_records(), 2);

        auto map = sys_ks.get_commitlog_cleanup_records().get();
        auto it = map.find({tableid, this_shard_id()});
        BOOST_REQUIRE(it != map.end());
        BOOST_REQUIRE(it->second.get(1) == never_obsolete_rp);
        BOOST_REQUIRE(it->second.get(2) == std::nullopt);
        BOOST_REQUIRE(it->second.get(3) == std::nullopt);
        BOOST_REQUIRE(it->second.get(4) == rp_3);
        BOOST_REQUIRE(it->second.get(5) == std::nullopt);
    });
}

// Test that saving a commitlog cleanup record with an invalid replay position
// (which would filter nothing on replay) leaves the table unchanged.
SEASTAR_TEST_CASE(test_invalid_replay_position_commitlog_cleanup_record) {
    BOOST_REQUIRE_EQUAL(this_smp_shard_count(), 1);
    return do_with_cql_env_thread([](cql_test_env& e) {
        auto& sys_ks = e.get_system_keyspace().local();
        auto tableid = table_id(utils::UUID_gen::get_time_UUID());
        auto tr = dht::token_range::make({dht::token::from_int64(0), false}, {dht::token::from_int64(1), true});

        auto get_num_records = [&] {
            auto res = e.execute_cql("select * from system.commitlog_cleanups;").get();
            auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(res);
            BOOST_REQUIRE(rows);
            return rows->rs().result_set().size();
        };
        // On a single shard, a record with an invalid replay position would not be written
        // anyway, it would be covered by the pruning done by the same save (it is below the
        // commitlog's minimum position), so also check that the save doesn't write anything.
        auto& t = e.local_db().find_column_family(db::system_keyspace::NAME, db::system_keyspace::COMMITLOG_CLEANUPS);

        BOOST_REQUIRE_EQUAL(get_num_records(), 0);
        const auto writes_before = t.get_stats().writes().count();
        sys_ks.save_commitlog_cleanup_record(tableid, tr, db::replay_position()).get();
        BOOST_REQUIRE_EQUAL(t.get_stats().writes().count(), writes_before);
        BOOST_REQUIRE_EQUAL(get_num_records(), 0);
    });
}

// Test that the tombstones of deleted commitlog cleanup records don't linger
// on disk: once compacted, the deleted records are gone for good.
SEASTAR_TEST_CASE(test_deleted_commitlog_cleanup_records_purged) {
    BOOST_REQUIRE_EQUAL(this_smp_shard_count(), 1);
    return do_with_cql_env_thread([](cql_test_env& e) {
        auto& sys_ks = e.get_system_keyspace().local();
        auto& t = e.local_db().find_column_family(db::system_keyspace::NAME, db::system_keyspace::COMMITLOG_CLEANUPS);
        BOOST_REQUIRE_EQUAL(t.schema()->gc_grace_seconds().count(), 0);

        auto tableid = table_id(utils::UUID_gen::get_time_UUID());
        // Use a segment id far ahead of the commitlog's, so the records are never
        // obsolete (no commitlog segment precedes them).
        const db::segment_id_type segment_id = db::replay_position::ts_mask;
        for (unsigned shard : {0, 1}) {
            for (int i = 0; i < 10; ++i) {
                auto tr = dht::token_range::make({dht::token::from_int64(i), false}, {dht::token::from_int64(i + 1), true});
                sys_ks.save_commitlog_cleanup_record(tableid, tr, db::replay_position(shard, segment_id, i + 1)).get();
            }
        }
        t.flush().get();
        BOOST_REQUIRE_GT(t.sstables_count(), 0);

        sys_ks.drop_all_commitlog_cleanup_records().get();

        // Tombstones are not purged while commitlog segments holding (possibly shadowed)
        // writes of the table are around, so flush all tables and wait for the released
        // commitlog segments to disappear. The table uses the schema commitlog.
        BOOST_REQUIRE(t.schema()->static_props().use_schema_commitlog);
        auto& cl = *e.local_db().schema_commitlog();
        cl.force_new_active_segment().get();
        e.local_db().flush_all_tables().get();
        cl.wait_for_pending_deletes().get();

        // Tombstones are purgeable only if their deletion time is strictly before
        // gc_before (now - gc_grace_seconds), which has a granularity of seconds.
        seastar::sleep(std::chrono::seconds(1)).get();
        t.compact_all_sstables(tasks::make_empty_task_info()).get();
        BOOST_REQUIRE_EQUAL(t.sstables_count(), 0);
    });
}

BOOST_AUTO_TEST_SUITE_END()
