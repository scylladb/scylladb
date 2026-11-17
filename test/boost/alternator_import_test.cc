/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests of ImportTable's tables in system_distributed, which db::alternator_import_table_helper
// reads and writes.

#include "test/lib/scylla_test_case.hh"
#include "test/lib/cql_test_env.hh"

#include "db/system_distributed_keyspace.hh"

#include <chrono>

namespace {

cql_test_config import_test_config() {
    cql_test_config cfg;
    // The import metadata is written with LWT.
    cfg.need_remote_proxy = true;
    return cfg;
}

} // anonymous namespace

// The garbage collector takes an import only if its row did not change since it was read: a
// progress write of an import which still runs, which keeps its status and node, is not undone.
SEASTAR_TEST_CASE(test_import_take_import_checks_counters) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        db::alternator_import_table_helper helper(e.qp().local());
        const db::alternator_import_row row{
            .import_arn = "arn:scylla:alternator:alternator_take:scylla:table/take/import/01700000000000-00000001",
            .client_token = "take",
            .request = "{}",
            .import_status = "IN_PROGRESS",
            .processed_item_count = 5,
            .imported_item_count = 4,
            .error_count = 1,
            .accepted_at = db_clock::now(),
            .node_id = "running-node",
        };
        BOOST_REQUIRE(!helper.insert_import(row, std::chrono::hours(1)).get());
        auto taken = row;
        taken.import_status = "FAILING";
        taken.node_id = "collector";
        auto read_before_progress = row;
        read_before_progress.processed_item_count = 3;
        BOOST_REQUIRE(!helper.take_import(taken, read_before_progress, std::chrono::hours(1)).get());
        BOOST_REQUIRE_EQUAL(helper.get_import(row.import_arn).get()->node_id, "running-node");
        auto read_with_other_bytes = row;
        read_with_other_bytes.processed_size_bytes = 10;
        BOOST_REQUIRE(!helper.take_import(taken, read_with_other_bytes, std::chrono::hours(1)).get());
        BOOST_REQUIRE_EQUAL(helper.get_import(row.import_arn).get()->node_id, "running-node");
        BOOST_REQUIRE(helper.take_import(taken, row, std::chrono::hours(1)).get());
        auto after = helper.get_import(row.import_arn).get();
        BOOST_REQUIRE_EQUAL(after->node_id, "collector");
        BOOST_REQUIRE_EQUAL(after->import_status, "FAILING");
        BOOST_REQUIRE_EQUAL(after->processed_item_count, 5);
    }, import_test_config());
}
