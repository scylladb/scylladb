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
            auto id = utils::make_random_uuid();
            auto first_token = dht::token::from_int64(0);
            auto last_token = dht::token::from_int64(100);
            auto toc_name = "me-1-big-TOC.txt";
            auto prefix = "some/prefix";
            uint64_t ttl_seconds = 3600;

            // insert some test data into snapshot_sstables table
            co_await env.get_system_distributed_keyspace().local().insert_snapshot_sstable(snapshot_name, ks, table, dc, rack, id, first_token, last_token, toc_name, prefix, ttl_seconds);

            // read it back and check if it is correct
            auto sstables = co_await env.get_system_distributed_keyspace().local().get_snapshot_sstables(snapshot_name, ks, table, dc, rack);

            BOOST_CHECK_EQUAL(sstables.size(), 1);

            const auto& sstable = sstables[0];
            BOOST_CHECK_EQUAL(sstable.toc_name, toc_name);
            BOOST_CHECK_EQUAL(sstable.prefix, prefix);
            BOOST_CHECK_EQUAL(sstable.first_token, first_token);
            BOOST_CHECK_EQUAL(sstable.last_token, last_token);
            BOOST_CHECK_EQUAL(sstable.id, id);
    }, db_cfg_ptr);
}
