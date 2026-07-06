/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/on_internal_error.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>
#include <sstream>

#include "audit/audit.hh"
#include "test/lib/cql_test_env.hh"

#include "transport/messages/result_message.hh"
#include "utils/error_injection.hh"

BOOST_AUTO_TEST_SUITE(test_audit)

#ifdef SCYLLA_ENABLE_ERROR_INJECTION

// Storage stop must drain audit writes that are already in flight. The audit
// write is parked mid-flight via the injection, then stop_storage() is called:
// the close of the pending-writes gate cannot complete until the parked write
// is released, and the record must still be persisted rather than lost.
SEASTAR_TEST_CASE(test_audit_storage_stop_drains_in_flight_write) {
    cql_test_config cfg;
    cfg.db_config->audit("table");
    cfg.db_config->audit_categories("DML");
    cfg.db_config->audit_keyspaces("ks");

    return do_with_cql_env_thread([] (cql_test_env& env) {
        audit::audit::start_audit(env.db_config(), env.get_shared_token_metadata(), env.qp(), env.migration_manager()).get();
        auto audit_stop = defer([] noexcept {
            audit::audit::stop_audit().get();
        });

        audit::audit::start_storage(env.db_config()).get();

        env.execute_cql("CREATE TABLE ks.audit_gate_test (id int PRIMARY KEY)").get();

        utils::get_local_injector().enable("table_helper_insert_before_execute", true /*one_shot*/);
        auto write_fut = env.execute_cql("INSERT INTO ks.audit_gate_test (id) VALUES (1)");

        while (utils::get_local_injector().waiters("table_helper_insert_before_execute") == 0) {
            seastar::yield().get();
        }

        auto stop_fut = audit::audit::stop_storage();
        // The write is parked inside the gate on this shard, where stop_storage()'s
        // invoke_on_all runs the gate close inline; the close cannot complete until
        // the parked write leaves the gate, so stop must not be ready here.
        BOOST_REQUIRE(!stop_fut.available());
        BOOST_REQUIRE(utils::get_local_injector().waiters("table_helper_insert_before_execute") > 0);

        utils::get_local_injector().receive_message("table_helper_insert_before_execute");
        // The write entered the gate before stop closed it, so it must drain to
        // completion (no gate_closed_exception) rather than being lost.
        write_fut.get();
        stop_fut.get();

        auto msg = env.execute_cql("SELECT * FROM audit.audit_log").get();
        auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
        BOOST_REQUIRE(rows);
        // Exactly one record: the in-flight write was drained, neither dropped
        // nor duplicated by the racing shutdown.
        BOOST_REQUIRE_EQUAL(rows->rs().result_set().rows().size(), 1u);
    }, std::move(cfg));
}

#endif // SCYLLA_ENABLE_ERROR_INJECTION

// A write that arrives after storage stop has closed the gate must be rejected
// as a closed-gate condition ("storage stopped"), not mistaken for the startup
// "storage not ready" window. The ready flag stays set across stop, so the late
// write reaches the gate and is rejected there; the distinguishing signal is the
// internal-error reason, which is asserted by capturing the audit log output.
SEASTAR_TEST_CASE(test_audit_storage_stop_rejects_late_write) {
    cql_test_config cfg;
    cfg.db_config->audit("table");
    cfg.db_config->audit_categories("DML");
    cfg.db_config->audit_keyspaces("ks");

    return do_with_cql_env_thread([] (cql_test_env& env) {
        audit::audit::start_audit(env.db_config(), env.get_shared_token_metadata(), env.qp(), env.migration_manager()).get();
        auto audit_stop = defer([] noexcept {
            audit::audit::stop_audit().get();
        });

        audit::audit::start_storage(env.db_config()).get();

        env.execute_cql("CREATE TABLE ks.audit_gate_test (id int PRIMARY KEY)").get();

        // Stop storage with no write in flight, so the gate is fully closed and
        // the storage helper stopped before the late write below is issued.
        audit::audit::stop_storage().get();

        // The late write hits an on_internal_error_noexcept path; keep it logging
        // without aborting so the reason can be inspected.
        seastar::testing::scoped_no_abort_on_internal_error no_abort;

        std::ostringstream captured;
        seastar::logger::set_ostream(captured);
        auto restore_ostream = defer([] noexcept {
            seastar::logger::set_ostream(std::cerr);
        });

        env.execute_cql("INSERT INTO ks.audit_gate_test (id) VALUES (2)").get();

        auto log_output = captured.str();
        // The closed gate must surface as "storage stopped", not the startup-only
        // "storage not ready" condition that would mean the ready flag was reset.
        BOOST_REQUIRE(log_output.find("Audit log dropped (storage stopped)") != std::string::npos);
        BOOST_REQUIRE(log_output.find("storage not ready") == std::string::npos);
    }, std::move(cfg));
}

// A table audit write to a missing table takes the group0-dependent recovery
// path (table_helper::cache_table_info -> setup_table), which must be awaited
// by the write rather than left running detached. If it were detached, the
// recovery could outlive group0 at shutdown and the metadata-recovery ordering
// guarantee would be violated. Because the write awaits recovery, the table is
// guaranteed recreated by the time the write returns, so a later write can
// persist and the audit subsystem stops cleanly afterwards.
SEASTAR_TEST_CASE(test_audit_write_awaits_table_recovery) {
    cql_test_config cfg;
    cfg.db_config->audit("table");
    cfg.db_config->audit_categories("DML");
    cfg.db_config->audit_keyspaces("ks");

    return do_with_cql_env_thread([] (cql_test_env& env) {
        audit::audit::start_audit(env.db_config(), env.get_shared_token_metadata(), env.qp(), env.migration_manager()).get();
        auto audit_stop = defer([] noexcept {
            audit::audit::stop_audit().get();
        });

        audit::audit::start_storage(env.db_config()).get();

        env.execute_cql("CREATE TABLE ks.audit_gate_test (id int PRIMARY KEY)").get();

        // Drop the audit table so the next audited write misses on prepare and
        // enters the recovery path. The drop is DDL on the audit keyspace, which
        // is not audited (categories DML, keyspaces ks), so it triggers no write.
        env.execute_cql("DROP TABLE audit.audit_log").get();

        // This write takes the recovery path. It awaits setup_table, so when the
        // statement returns the table has been recreated; the triggering record
        // itself is dropped best-effort (the recovery rethrows for the caller).
        env.execute_cql("INSERT INTO ks.audit_gate_test (id) VALUES (1)").get();

        auto recovered = env.execute_cql("SELECT * FROM audit.audit_log").get();
        auto recovered_rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(recovered);
        BOOST_REQUIRE(recovered_rows);
        BOOST_REQUIRE_EQUAL(recovered_rows->rs().result_set().rows().size(), 0u);

        // With the table back in place this write prepares and persists normally.
        env.execute_cql("INSERT INTO ks.audit_gate_test (id) VALUES (2)").get();

        audit::audit::stop_storage().get();

        auto msg = env.execute_cql("SELECT * FROM audit.audit_log").get();
        auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
        BOOST_REQUIRE(rows);
        BOOST_REQUIRE_EQUAL(rows->rs().result_set().rows().size(), 1u);
    }, std::move(cfg));
}

BOOST_AUTO_TEST_SUITE_END()
