/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */



#include "test/lib/cql_test_env.hh"
#include "utils/assert.hh"
#include <seastar/core/sstring.hh>

#include <seastar/core/future.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/test_fixture.hh>

#include "db/config.hh"
#include "db/consistency_level_type.hh"
#include "db/system_distributed_keyspace.hh"

#include "test/lib/test_utils.hh"


SEASTAR_TEST_CASE(test_snapshot_manifests_table_api_works, *boost::unit_test::precondition(tests::has_scylla_test_env)) {
    auto db_cfg_ptr = make_shared<db::config>();
    
    return do_with_cql_env([] (cql_test_env& env) -> future<> {
            auto snapshot_name = "snapshot";
            auto ks = "ks";
            auto table = "cf";
            auto dc = "dc1";
            auto rack = "r1";
            auto sstable_id = utils::make_random_uuid();
            auto last_token = dht::token::from_int64(100);
            auto toc_name = "me-1-big-TOC.txt";
            auto prefix = "some/prefix";
            auto num_iter = 5;

            for (int i = num_iter - 1; i >= 0; --i) {
                // insert some test data into snapshot_sstables table
                auto first_token = dht::token::from_int64(i);
                co_await env.get_system_distributed_keyspace().local().insert_snapshot_sstable(snapshot_name, ks, table, dc, rack, sstables::sstable_id(sstable_id), first_token, last_token, toc_name, prefix, db::consistency_level::ONE);
            }
            // read it back and check if it is correct
            auto sstables = co_await env.get_system_distributed_keyspace().local().get_snapshot_sstables(snapshot_name, ks, table, dc, rack, db::consistency_level::ONE);

            BOOST_CHECK_EQUAL(sstables.size(), num_iter);

            for (int i = 0; i < num_iter; ++i) {
                const auto& sstable = sstables[i];
                BOOST_CHECK_EQUAL(sstable.toc_name, toc_name);
                BOOST_CHECK_EQUAL(sstable.prefix, prefix);
                BOOST_CHECK_EQUAL(sstable.first_token, dht::token::from_int64(i));
                BOOST_CHECK_EQUAL(sstable.last_token, last_token);
                BOOST_CHECK_EQUAL(sstable.sstable_id.uuid(), sstable_id);
            }

            // test token range filtering: matching range should return the sstable
            auto filtered = co_await env.get_system_distributed_keyspace().local().get_snapshot_sstables(
                    snapshot_name, ks, table, dc, rack, db::consistency_level::ONE,
                    dht::token::from_int64(-10), dht::token::from_int64(num_iter + 1));
            BOOST_CHECK_EQUAL(filtered.size(), num_iter);

            // test token range filtering: non-matching range should return nothing
            auto empty = co_await env.get_system_distributed_keyspace().local().get_snapshot_sstables(
                    snapshot_name, ks, table, dc, rack, db::consistency_level::ONE,
                    dht::token::from_int64(num_iter + 10), dht::token::from_int64(num_iter + 20));
            BOOST_CHECK_EQUAL(empty.size(), 0);
    }, db_cfg_ptr);
}
