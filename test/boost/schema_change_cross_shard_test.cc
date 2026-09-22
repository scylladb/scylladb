/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#undef SEASTAR_TESTING_MAIN
#include <boost/test/unit_test.hpp>
#include <seastar/testing/test_case.hh>
#include <seastar/core/smp.hh>

#include "dht/i_partitioner.hh"
#include "replica/database.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/eventually.hh"
#include "test/lib/log.hh"
#include "types/types.hh"
#include "utils/error_injection.hh"

// A schema change is committed shard by shard, shard 0 first (see schema_applier::commit()).
// The tests below pause the applier in between and write to, or read from, shard 1 in that
// window, reproducing scylladb/scylladb#23831, scylladb/scylladb#14146 and SCYLLADB-3847.

BOOST_AUTO_TEST_SUITE(schema_change_cross_shard_test)

namespace {

constexpr auto pause_injection = "schema_applier_pause_before_commit_on_other_shards";

bool can_run() {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
    testlog.info("Skipping test: requires error injection (dev or debug build)");
    return false;
#endif
    if (this_smp_shard_count() < 2) {
        testlog.info("Skipping test: requires at least 2 shards");
        return false;
    }
    return true;
}

// An int partition key of `s` whose token is owned by `shard`.
int32_t key_on_shard(cql_test_env& e, const schema_ptr& s, shard_id shard) {
    auto erm = e.local_db().find_column_family(s->id()).get_effective_replication_map();
    const auto& sharder = erm->get_sharder(*s);
    for (int32_t i = 0; i < 100000; ++i) {
        auto pk = partition_key::from_single_value(*s, int32_type->decompose(i));
        if (sharder.shard_for_reads(dht::get_token(*s, pk)) == shard) {
            return i;
        }
    }
    BOOST_FAIL(format("no key of {}.{} found on shard {}", s->ks_name(), s->cf_name(), shard));
    return 0;
}

uint64_t writes_on_shard(cql_test_env& e, shard_id shard) {
    return e.db().invoke_on(shard, [] (replica::database& db) {
        return db.get_stats().total_writes;
    }).get();
}

// Requests which have waited for a schema change commit, summed over all shards.
uint64_t schema_change_commit_waits(cql_test_env& e) {
    return e.db().map_reduce0([] (replica::database& db) {
        return db.get_stats().schema_change_commit_waits;
    }, uint64_t(0), std::plus<uint64_t>()).get();
}

// A schema change paused between its commit on shard 0 and on the other shards, see
// start_paused_schema_change(). Owns the paused statement and the injection; the destructor
// releases both, so a failing assertion doesn't leave the applier blocked for a minute.
class paused_schema_change {
    std::optional<future<::shared_ptr<cql_transport::messages::result_message>>> _statement;
    bool _released = false;
public:
    paused_schema_change(cql_test_env& e, const sstring& schema_change) {
        utils::get_local_injector().enable(pause_injection);
        _statement = e.execute_cql(schema_change);
    }
    paused_schema_change(paused_schema_change&& o) noexcept
            : _statement(std::move(o._statement)), _released(o._released) {
        o._statement.reset();
        o._released = true;
    }
    ~paused_schema_change() {
        release();
        if (_statement) {
            // Called from a seastar thread, both on the normal path and while unwinding.
            _statement->wait();
            _statement->ignore_ready_future();
        }
    }
    // Lets the change be committed on the remaining shards.
    void release() noexcept {
        if (!std::exchange(_released, true)) {
            utils::get_local_injector().receive_message(pause_injection);
            utils::get_local_injector().disable(pause_injection);
        }
    }
    // Releases the change and waits for the statement to complete.
    ::shared_ptr<cql_transport::messages::result_message> get() {
        release();
        auto statement = std::move(*_statement);
        _statement.reset();
        return statement.get();
    }
};

// Starts `schema_change` and returns once shard 0 has committed it, with the applier paused
// before committing on the other shards. `committed` tells whether a shard has the change.
paused_schema_change start_paused_schema_change(cql_test_env& e, const sstring& schema_change,
        std::function<bool(replica::database&)> committed) {
    paused_schema_change change(e, schema_change);
    BOOST_REQUIRE(eventually_true([&] { return committed(e.local_db()); }));
    auto committed_on_shard_1 = e.db().invoke_on(1, [&committed] (replica::database& db) {
        return committed(db);
    }).get();
    BOOST_REQUIRE(!committed_on_shard_1);
    return change;
}

// Issues `write` from shard 0 and returns once it has reached the write path on shard 1.
future<::shared_ptr<cql_transport::messages::result_message>> write_to_shard_1(cql_test_env& e, const sstring& write) {
    auto writes_before = writes_on_shard(e, 1);
    auto f = e.execute_cql(write);
    BOOST_REQUIRE(eventually_true([&] { return writes_on_shard(e, 1) > writes_before; }));
    return f;
}

// Issues `read` from shard 0 and returns once some shard has started waiting for the paused commit.
future<::shared_ptr<cql_transport::messages::result_message>> read_waiting_for_commit(cql_test_env& e, const sstring& read) {
    auto waits_before = schema_change_commit_waits(e);
    auto f = e.execute_cql(read);
    BOOST_REQUIRE(eventually_true([&] { return schema_change_commit_waits(e) > waits_before; }));
    return f;
}

// A full scan of a newly created table reads from every shard, including ones which haven't
// committed the CREATE yet. The coordinating shard waits for the commit before fanning out.
void test_create_table_scan_during_shard_commit(cql_test_env& e) {
    auto create = start_paused_schema_change(e, "CREATE TABLE ks.t (pk int PRIMARY KEY, v int)", [] (replica::database& db) {
        return db.has_schema("ks", "t");
    });
    auto scan = read_waiting_for_commit(e, "SELECT * FROM ks.t");

    create.get();

    assert_that(scan.get()).is_rows().is_empty();
}

} // anonymous namespace

// A single-partition read of a newly created table is forwarded to a shard which hasn't
// committed the CREATE yet. Without waiting for the commit, it fails with no_such_column_family.
SEASTAR_TEST_CASE(test_create_table_read_during_shard_commit) {
    if (!can_run()) {
        return make_ready_future<>();
    }
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto create = start_paused_schema_change(e, "CREATE TABLE ks.t (pk int PRIMARY KEY, v int)", [] (replica::database& db) {
            return db.has_schema("ks", "t");
        });
        auto pk = key_on_shard(e, e.local_db().find_schema("ks", "t"), 1);
        auto read = read_waiting_for_commit(e, format("SELECT * FROM ks.t WHERE pk = {}", pk));

        create.get();

        assert_that(read.get()).is_rows().is_empty();
    });
}

SEASTAR_TEST_CASE(test_create_table_scan_during_shard_commit_vnodes) {
    if (!can_run()) {
        return make_ready_future<>();
    }
    return do_with_cql_env_thread(test_create_table_scan_during_shard_commit);
}

SEASTAR_TEST_CASE(test_create_table_scan_during_shard_commit_tablets) {
    if (!can_run()) {
        return make_ready_future<>();
    }
    cql_test_config cfg;
    cfg.initial_tablets = 8;
    return do_with_cql_env_thread(test_create_table_scan_during_shard_commit, std::move(cfg));
}

// A write with a column added by ALTER reaches a shard which hasn't committed the ALTER yet.
// Without the wait the mutation is downgraded to the old schema and the new value is lost.
SEASTAR_TEST_CASE(test_alter_table_add_column_write_during_shard_commit) {
    if (!can_run()) {
        return make_ready_future<>();
    }
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)").get();
        auto pk = key_on_shard(e, e.local_db().find_schema("ks", "t"), 1);

        auto alter = start_paused_schema_change(e, "ALTER TABLE ks.t ADD c int", [] (replica::database& db) {
            return db.find_schema("ks", "t")->get_column_definition("c") != nullptr;
        });
        auto write = write_to_shard_1(e, format("INSERT INTO ks.t (pk, v, c) VALUES ({}, 1, 2)", pk));

        alter.get();
        write.get();

        assert_that(e.execute_cql(format("SELECT c FROM ks.t WHERE pk = {}", pk)).get())
            .is_rows().with_rows({{int32_type->decompose(2)}});
    });
}

// A write to a newly created table reaches a shard which hasn't committed the CREATE yet.
// Without waiting for the commit, the write fails with no_such_column_family.
SEASTAR_TEST_CASE(test_create_table_write_during_shard_commit) {
    if (!can_run()) {
        return make_ready_future<>();
    }
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto create = start_paused_schema_change(e, "CREATE TABLE ks.t (pk int PRIMARY KEY, v int)", [] (replica::database& db) {
            return db.has_schema("ks", "t");
        });
        auto pk = key_on_shard(e, e.local_db().find_schema("ks", "t"), 1);
        auto write = write_to_shard_1(e, format("INSERT INTO ks.t (pk, v) VALUES ({}, 1)", pk));

        create.get();
        write.get();

        assert_that(e.execute_cql(format("SELECT v FROM ks.t WHERE pk = {}", pk)).get())
            .is_rows().with_rows({{int32_type->decompose(1)}});
    });
}

// A base write on shard 0 generates a view update for shard 1, which doesn't have the view yet.
// Without the wait the update fails and the view row is missing once the write returns.
SEASTAR_TEST_CASE(test_create_view_update_during_shard_commit) {
    if (!can_run()) {
        return make_ready_future<>();
    }
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)").get();
        auto pk = key_on_shard(e, e.local_db().find_schema("ks", "t"), 0);

        auto create = start_paused_schema_change(e,
                "CREATE MATERIALIZED VIEW ks.tv AS SELECT * FROM ks.t WHERE v IS NOT NULL PRIMARY KEY (v, pk) WITH synchronous_updates = true",
                [] (replica::database& db) {
            return db.has_schema("ks", "tv");
        });
        auto v = key_on_shard(e, e.local_db().find_schema("ks", "tv"), 1);
        auto write = write_to_shard_1(e, format("INSERT INTO ks.t (pk, v) VALUES ({}, {})", pk, v));

        create.get();
        write.get();

        assert_that(e.execute_cql(format("SELECT pk FROM ks.tv WHERE v = {}", v)).get())
            .is_rows().with_rows({{int32_type->decompose(pk)}});
    });
}

// Destroying the commit guard, which the applier does on every exit path, releases the waiting
// requests.
SEASTAR_TEST_CASE(test_schema_change_commit_guard_releases_waiters) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto& db = e.local_db();
        auto created = table_id::create_random_id();
        auto timeout = db::timeout_clock::now() + std::chrono::seconds(30);

        BOOST_REQUIRE(db.wait_for_schema_change_commit(created, timeout).available());

        auto guard = db.begin_schema_change_commit({created});
        auto waiter = db.wait_for_schema_change_commit(created, timeout);
        BOOST_REQUIRE(!waiter.available());

        guard.reset();
        waiter.get();
        BOOST_REQUIRE(db.wait_for_schema_change_commit(created, timeout).available());
    });
}

BOOST_AUTO_TEST_SUITE_END()
