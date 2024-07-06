/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <fmt/ranges.h>
#include "test/lib/scylla_test_case.hh"
#include "test/lib/cql_test_env.hh"
#include "db/commitlog/commitlog_replayer.hh"
#include "db/commitlog/commitlog.hh"
#include "db/config.hh"

// Test that `canonical_token_range(tr)` contains the same tokens as `tr`.
SEASTAR_TEST_CASE(test_canonical_token_range) {
    const int64_t arbitrary_token = -42;
    // Define some "interesting tokens":
    // an arbitrary integer, integers which are offset by 1 from the arbitrary one,
    // minimum and maximum finite tokens, infinite tokens.
    //
    // The idea is that combinations of these should be enough to cover all logic.
    std::vector<dht::token> interesting_tokens = {
        dht::minimum_token(),
        dht::first_token(),
        dht::token::from_int64(arbitrary_token - 1),
        dht::token::from_int64(arbitrary_token),
        dht::token::from_int64(arbitrary_token + 1),
        dht::token::from_int64(std::numeric_limits<int64_t>::max()),
        dht::maximum_token(),
    };
    // Define interesting bounds:
    // inclusive/exclusive bound for each interesting token,
    // and empty (infinite) bound.
    std::vector<std::optional<dht::token_range::bound>> interesting_bounds = {
        std::nullopt,
    };
    for (const auto& t : interesting_tokens) {
        interesting_bounds.push_back(dht::token_range::bound{t, false});
        interesting_bounds.push_back(dht::token_range::bound{t, true});
    }
    // For every interesting range `tr` (every valid pair of interesting bounds),
    // check that `tr` it is semantically equal to `canonical_token_range(tr)`,
    // by testing that it contains the same subset of finite interesting tokens.
    for (const auto& a : interesting_bounds) {
        for (const auto& b : interesting_bounds) {
            auto wrapping_orig = wrapping_interval<dht::token>(a, b);
            if (wrapping_orig.is_wrap_around(dht::token_comparator())) {
                continue;
            }
            auto orig = dht::token_range(wrapping_orig);
            auto canon = db::system_keyspace::canonical_token_range(orig);
            for (auto t : {
                std::numeric_limits<int64_t>::min() + 1,
                arbitrary_token - 1,
                arbitrary_token,
                arbitrary_token + 1,
                std::numeric_limits<int64_t>::max(),
            }) {
                bool in_tr = orig.contains(dht::token::from_int64(t), dht::token_comparator());
                bool in_canon = t > canon.first && t <= canon.second;
                BOOST_REQUIRE_EQUAL(in_tr, in_canon);
            }
        }
    }
    return make_ready_future<>();
}

// Test basic functionality of system_keyspace::save_range_cleanup_record()
// and system_keyspace::get_range_cleanup_records().
SEASTAR_TEST_CASE(test_commitlog_cleanup_records) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        auto tableid = table_id(utils::UUID_gen::get_time_UUID());

        auto insert_record = [&] (table_id tid, std::pair<int64_t, bool> lo, std::pair<int64_t, bool> hi, db::replay_position rp) {
            auto tr = dht::token_range::make({dht::token::from_int64(lo.first), lo.second}, {dht::token::from_int64(hi.first), hi.second});
            e.get_system_keyspace().local().save_commitlog_cleanup_record(tid, tr, rp).get();
        };

        // Insert an interval.
        insert_record(tableid, {0, true}, {1, true}, {42, 1337, 1});
        // Overlap it with a range with higher replay position.
        insert_record(tableid, {1, true}, {2, true}, {42, 1337, 2});
        // Overlap both with a range with lower replay position.
        insert_record(tableid, {-1, true}, {3, true}, {42, 1337, 0});
        // Insert an empty range, it shouldn't affect the result at all.
        insert_record(tableid, {0, false}, {1, false}, {42, 1337, 3});
        // Insert a record for a different table.
        insert_record(table_id(utils::UUID_gen::get_time_UUID()), {0, false}, {1, false}, {42, 1337, 5});
        // Insert a record for a different shard.
        insert_record(tableid, {0, false}, {1, false}, {1, 1337, 5});

        // Read the records.
        auto map = e.get_system_keyspace().local().get_commitlog_cleanup_records().get();
        BOOST_REQUIRE_EQUAL(map.size(), 3); // 3 <shard, table> combinations
        auto local_map = map.find({tableid, 42});
        BOOST_REQUIRE(local_map != map.end());

        auto get_rp = [&] (int64_t t) -> std::optional<db::replay_position> {
            return local_map->second.get(t);
        };
        // Check that the resulting map is as we expect.
        BOOST_REQUIRE(get_rp(-2) == std::nullopt);
        BOOST_REQUIRE(get_rp(-1) == db::replay_position(42, 1337, 0));
        BOOST_REQUIRE(get_rp(0) == db::replay_position(42, 1337, 1));
        BOOST_REQUIRE(get_rp(1) == db::replay_position(42, 1337, 2));
        BOOST_REQUIRE(get_rp(2) == db::replay_position(42, 1337, 2));
        BOOST_REQUIRE(get_rp(3) == db::replay_position(42, 1337, 0));
        BOOST_REQUIRE(get_rp(4) == std::nullopt);
    });
}

// Test that commitlog doesn't resurrect data after table cleanup.
SEASTAR_TEST_CASE(test_commitlog_cleanups) {
    auto cfg = cql_test_config();
    cfg.db_config->auto_snapshot.set(false);
    cfg.db_config->commitlog_sync.set("batch");
    cfg.db_config->enable_tablets.set(true);
    cfg.initial_tablets = 1;

    return do_with_cql_env_thread([](cql_test_env& e) {
        // Create a table.
        e.execute_cql("create table ks.cf (pk int, ck int, primary key (pk, ck))").get();
        auto get_num_rows = [&] {
            auto res = e.execute_cql("select * from ks.cf;").get();
            auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(res);
            BOOST_REQUIRE(rows);
            return rows->rs().result_set().size();
        };

        // Insert a row into the table.
        e.execute_cql("insert into ks.cf (pk,ck) values (0, 0)").get();
        BOOST_REQUIRE_EQUAL(get_num_rows(), 1);

        // Cleanup the tablet.
        e.db().invoke_on_all([&] (replica::database& db) {
            return db.find_column_family("ks", "cf").cleanup_tablet_without_deallocation(db, e.get_system_keyspace().local(), locator::tablet_id(0));
        }).get();
        BOOST_REQUIRE_EQUAL(get_num_rows(), 0);

        // Insert more rows into the table.
        e.execute_cql("insert into ks.cf (pk,ck) values (0, 1)").get();
        e.execute_cql("insert into ks.cf (pk,ck) values (0, 2)").get();
        BOOST_REQUIRE_EQUAL(get_num_rows(), 2);

        // Drop memtables.
        e.db().invoke_on_all([&] (replica::database& db) -> future<> {
            return db.find_column_family("ks", "cf").clear();
        }).get();
        BOOST_REQUIRE_EQUAL(get_num_rows(), 0);

        // Commitlog replay should resurrect the 2 dropped rows,
        // but shouldn't resurrect the 1 cleaned row.
        e.db().invoke_on_all([&] (replica::database& db) -> future<> {
            auto cl = db.commitlog();
            auto rp = co_await db::commitlog_replayer::create_replayer(e.db(), e.get_system_keyspace());
            auto paths = co_await cl->list_existing_segments();
            co_await rp.recover(paths, db::commitlog::descriptor::FILENAME_PREFIX);
        }).get();
        BOOST_REQUIRE_EQUAL(get_num_rows(), 2);
    }, cfg);
}

// Test that commitlog cleanup records are deleted when they become irrelevant.
SEASTAR_TEST_CASE(test_commitlog_cleanup_record_gc) {
    BOOST_REQUIRE_EQUAL(smp::count, 1);
    auto cfg = cql_test_config();
    cfg.db_config->auto_snapshot.set(false);
    cfg.db_config->commitlog_sync.set("batch");
    cfg.db_config->enable_tablets.set(true);
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
