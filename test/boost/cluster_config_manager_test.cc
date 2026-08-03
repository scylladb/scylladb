/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>

#include <memory>
#include <optional>
#include <tuple>
#include <variant>
#include <vector>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/util/closeable.hh>
#include <seastar/util/defer.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sharded.hh>

#include "db/cluster_config_manager.hh"
#include "db/cluster_config_registry.hh"
#include "exceptions/exceptions.hh"
#include "replica/database.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/eventually.hh"

BOOST_AUTO_TEST_SUITE(cluster_config_manager_test)

namespace {

void with_config_manager(cql_test_env& e, std::function<void(sharded<db::cluster_config_manager>&)> fn) {
    sharded<db::cluster_config_manager> mgr;
    mgr.start(std::ref(mgr), std::ref(e.db()), std::ref(e.qp())).get();
    auto stop_mgr = seastar::defer([&mgr] noexcept {
        mgr.stop().get();
    });
    mgr.local().refresh().get();
    fn(mgr);
}

// Records the values passed by the manager for a table-oriented option, so tests can
// assert which tables received an effective value and which resolved to absence.
struct callback_recorder {
    std::vector<std::tuple<sstring, sstring, sstring>> applies; // (keyspace, table, value)
    std::vector<std::pair<sstring, sstring>> clears;            // (keyspace, table)

    size_t apply_count(const sstring& ks, const sstring& tbl) const {
        size_t n = 0;
        for (const auto& a : applies) {
            if (std::get<0>(a) == ks && std::get<1>(a) == tbl) {
                ++n;
            }
        }
        return n;
    }

    std::optional<sstring> last_apply(const sstring& ks, const sstring& tbl) const {
        std::optional<sstring> v;
        for (const auto& a : applies) {
            if (std::get<0>(a) == ks && std::get<1>(a) == tbl) {
                v = std::get<2>(a);
            }
        }
        return v;
    }

    size_t clear_count(const sstring& ks, const sstring& tbl) const {
        size_t n = 0;
        for (const auto& c : clears) {
            if (c.first == ks && c.second == tbl) {
                ++n;
            }
        }
        return n;
    }
};

// Starts the manager, registers an "auto_repair_enabled" callback that records into rec,
// runs the first authoritative refresh, then invokes fn. The callback is registered on
// shard 0 only (via local()), and the manager invokes a callback only on the shard it was
// registered on, so the shared recorder stays single-threaded and the per-table assertions
// are deterministic. The per-shard register=invoke contract itself is covered by
// test_callback_registered_on_all_shards_fires_on_all_shards and
// test_callback_registered_on_shard0_fires_only_on_shard0.
void with_config_callback(cql_test_env& e, std::shared_ptr<callback_recorder> rec,
    std::function<void(sharded<db::cluster_config_manager>&)> fn) {
    sharded<db::cluster_config_manager> mgr;
    mgr.start(std::ref(mgr), std::ref(e.db()), std::ref(e.qp())).get();
    auto stop_mgr = seastar::defer([&mgr] noexcept {
        mgr.stop().get();
    });
    auto registration = mgr.local().register_config_callback(
        "auto_repair_enabled",
        [rec] (const db::cluster_config_manager::lookup_context& ctx, std::optional<sstring> value) {
            if (value) {
                rec->applies.emplace_back(ctx.keyspace_name.value_or(""), ctx.table_name.value_or(""), *value);
            } else {
                rec->clears.emplace_back(ctx.keyspace_name.value_or(""), ctx.table_name.value_or(""));
            }
            return make_ready_future<>();
        }).get();
    mgr.local().refresh().get();
    fn(mgr);
}

}

SEASTAR_TEST_CASE(test_cluster_config_manager_resolves_schema_waterfall) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE KEYSPACE ks_cfg WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}").get();
        e.execute_cql("CREATE TABLE ks_cfg.tbl (pk int PRIMARY KEY, v int)").get();
        e.execute_cql("ALTER CLUSTER WITH auto_repair_enabled = true").get();
        e.execute_cql("ALTER KEYSPACE ks_cfg WITH auto_repair_enabled = false").get();
        e.execute_cql("ALTER TABLE ks_cfg.tbl WITH auto_repair_enabled = true").get();

        with_config_manager(e, [&] (sharded<db::cluster_config_manager>& mgr) {
            auto& manager = mgr.local();
            db::cluster_config_manager::lookup_context ctx;
            ctx.keyspace_name = "ks_cfg";
            ctx.table_name = "tbl";
            BOOST_REQUIRE_EQUAL(manager.resolve_config("auto_repair_enabled", ctx).value_or(""), "true");

            ctx.table_name = std::nullopt;
            BOOST_REQUIRE_EQUAL(manager.resolve_config("auto_repair_enabled", ctx).value_or(""), "false");

            ctx.keyspace_name = std::nullopt;
            BOOST_REQUIRE_EQUAL(manager.resolve_config("auto_repair_enabled", ctx).value_or(""), "true");
        });
    });
}

// The typed accessors resolve the same chain as resolve_config(), but return a usable value
// instead of absence: with no override stored anywhere they yield the default declared on the
// registry entry, and once an override exists they yield it in the option's native type.
SEASTAR_TEST_CASE(test_cluster_config_manager_typed_accessor_falls_back_to_registered_default) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE KEYSPACE ks_cfg WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}").get();
        e.execute_cql("CREATE TABLE ks_cfg.tbl (pk int PRIMARY KEY, v int)").get();

        const auto* opt = db::cluster_config_registry::find("auto_repair_enabled");
        BOOST_REQUIRE(opt != nullptr);
        const bool registered_default = std::get<bool>(opt->default_value);

        with_config_manager(e, [&] (sharded<db::cluster_config_manager>& mgr) {
            auto& manager = mgr.local();
            db::cluster_config_manager::lookup_context ctx;
            ctx.keyspace_name = "ks_cfg";
            ctx.table_name = "tbl";

            // No scope stores an override: resolve_config() reports absence, while the typed
            // accessor turns that absence into the registered default.
            BOOST_REQUIRE(!manager.resolve_config("auto_repair_enabled", ctx));
            BOOST_REQUIRE_EQUAL(manager.resolve_boolean_config("auto_repair_enabled", ctx), registered_default);
        });

        e.execute_cql("ALTER TABLE ks_cfg.tbl WITH auto_repair_enabled = true").get();

        with_config_manager(e, [&] (sharded<db::cluster_config_manager>& mgr) {
            auto& manager = mgr.local();
            db::cluster_config_manager::lookup_context ctx;
            ctx.keyspace_name = "ks_cfg";
            ctx.table_name = "tbl";
            BOOST_REQUIRE_EQUAL(manager.resolve_boolean_config("auto_repair_enabled", ctx), true);

            // The override is on the table, so the keyspace-only context still falls back to
            // the default rather than inheriting the table's value.
            ctx.table_name = std::nullopt;
            BOOST_REQUIRE_EQUAL(manager.resolve_boolean_config("auto_repair_enabled", ctx), registered_default);
        });
    });
}

namespace {

// No shipped option is node-oriented yet, so the node-oriented tests inject a test-only
// one (mirroring the ALTER DATACENTER/RACK/NODE tests in cql_query_test). Returns the
// deferred cleanup that removes the injected option from every shard.
auto inject_node_oriented_test_option() {
    constexpr uint32_t node_oriented_mask =
        static_cast<uint32_t>(db::cluster_config_registry::scope::cluster)
        | static_cast<uint32_t>(db::cluster_config_registry::scope::datacenter)
        | static_cast<uint32_t>(db::cluster_config_registry::scope::rack)
        | static_cast<uint32_t>(db::cluster_config_registry::scope::node);
    seastar::smp::invoke_on_all([] {
        db::cluster_config_registry::add_test_only_option(db::cluster_config_registry::option{
            .name = "test_node_only_option",
            .type = db::cluster_config_registry::value_type::uint32,
            .scope_mask = node_oriented_mask,
            .min_version = db::cluster_config_registry::version::v0,
            .default_value = uint32_t(0),
        });
    }).get();
    return seastar::defer([] noexcept {
        seastar::smp::invoke_on_all([] {
            db::cluster_config_registry::clear_test_only_options();
        }).get();
    });
}

// No shipped option is text-typed yet either, so the text-value tests inject one.
auto inject_text_test_option() {
    seastar::smp::invoke_on_all([] {
        db::cluster_config_registry::add_test_only_option(db::cluster_config_registry::option{
            .name = "test_text_option",
            .type = db::cluster_config_registry::value_type::text,
            .scope_mask = static_cast<uint32_t>(db::cluster_config_registry::scope::cluster),
            .min_version = db::cluster_config_registry::version::v0,
            .default_value = std::string_view(""),
        });
    }).get();
    return seastar::defer([] noexcept {
        seastar::smp::invoke_on_all([] {
            db::cluster_config_registry::clear_test_only_options();
        }).get();
    });
}

}

// DESCRIBE echoes every effective value back both as a property right-hand side and inside
// a "-- from <scope> (...)" provenance comment, and it wraps a described CDC log or paxos
// table in a /* ... */ block. A CQL string literal has no escape for a line break or for
// "*/", so a text value containing either would turn the rest of the dump into something
// that no longer parses (or no longer means the same thing). The registry rejects them at
// the door, which is what makes the "the trailer is always one valid inline comment"
// contract in describe_statement.cc true.
SEASTAR_TEST_CASE(test_cluster_config_registry_rejects_text_value_that_would_break_a_comment) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto clear_test_options = inject_text_test_option();

        const auto* opt = db::cluster_config_registry::find("test_text_option");
        BOOST_REQUIRE(opt != nullptr);
        BOOST_REQUIRE(!db::cluster_config_registry::validate_value(*opt, "a plain value"));
        BOOST_REQUIRE(!db::cluster_config_registry::validate_value(*opt, "a lone * and / are fine"));
        BOOST_REQUIRE(db::cluster_config_registry::validate_value(*opt, "two\nlines"));
        BOOST_REQUIRE(db::cluster_config_registry::validate_value(*opt, "a\rreturn"));
        BOOST_REQUIRE(db::cluster_config_registry::validate_value(*opt, "ends the block */ here"));

        BOOST_REQUIRE_NO_THROW(e.execute_cql("ALTER CLUSTER WITH test_text_option = 'a plain value'").get());
        BOOST_REQUIRE_THROW(
            e.execute_cql("ALTER CLUSTER WITH test_text_option = 'two\nlines'").get(),
            exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(
            e.execute_cql("ALTER CLUSTER WITH test_text_option = 'ends the block */ here'").get(),
            exceptions::invalid_request_exception);
    });
}

// The node-oriented resolution domain: an override on the node shadows the rack's, the
// rack's shadows the datacenter's, and the datacenter's shadows the cluster's. Verified
// by stacking overrides on every scope and peeling them off one at a time.
SEASTAR_TEST_CASE(test_cluster_config_manager_resolves_node_oriented_waterfall) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto clear_test_options = inject_node_oriented_test_option();

        const auto& topo = e.shared_token_metadata().local().get()->get_topology();
        const sstring dc = topo.get_datacenter();
        const sstring rack = topo.get_rack();
        const auto node_uuid = topo.my_host_id().uuid();

        e.execute_cql("ALTER CLUSTER WITH test_node_only_option = 1").get();
        e.execute_cql(seastar::format("ALTER DATACENTER \"{}\" WITH test_node_only_option = 2", dc)).get();
        e.execute_cql(seastar::format("ALTER RACK \"{}\" \"{}\" WITH test_node_only_option = 3", dc, rack)).get();
        e.execute_cql(seastar::format("ALTER NODE {} WITH test_node_only_option = 4", node_uuid)).get();

        with_config_manager(e, [&] (sharded<db::cluster_config_manager>& mgr) {
            auto& manager = mgr.local();
            db::cluster_config_manager::lookup_context ctx;
            ctx.dc_name = dc;
            ctx.rack_name = rack;
            ctx.node_uuid = node_uuid;

            // Every scope stores an override: the node's wins, in text and typed form.
            BOOST_REQUIRE_EQUAL(manager.resolve_config("test_node_only_option", ctx).value_or(""), "4");
            BOOST_REQUIRE_EQUAL(manager.resolve_uint32_config("test_node_only_option", ctx), 4u);

            // Peeling the overrides off one scope at a time falls through the chain in order.
            e.execute_cql(seastar::format("ALTER NODE {} WITH test_node_only_option = null", node_uuid)).get();
            mgr.local().refresh().get();
            BOOST_REQUIRE_EQUAL(manager.resolve_config("test_node_only_option", ctx).value_or(""), "3");

            e.execute_cql(seastar::format("ALTER RACK \"{}\" \"{}\" WITH test_node_only_option = null", dc, rack)).get();
            mgr.local().refresh().get();
            BOOST_REQUIRE_EQUAL(manager.resolve_config("test_node_only_option", ctx).value_or(""), "2");

            e.execute_cql(seastar::format("ALTER DATACENTER \"{}\" WITH test_node_only_option = null", dc)).get();
            mgr.local().refresh().get();
            BOOST_REQUIRE_EQUAL(manager.resolve_config("test_node_only_option", ctx).value_or(""), "1");

            // With no override anywhere, resolution reports absence and the typed accessor
            // falls back to the registered default.
            e.execute_cql("ALTER CLUSTER WITH test_node_only_option = null").get();
            mgr.local().refresh().get();
            BOOST_REQUIRE(!manager.resolve_config("test_node_only_option", ctx).has_value());
            BOOST_REQUIRE_EQUAL(manager.resolve_uint32_config("test_node_only_option", ctx), 0u);
        });
    });
}

// A node-oriented option has a single callback target: the local node's context, filled
// in from the local topology. The callback is invoked exactly once per change with the
// effective value, and an unchanged refresh does not re-invoke it.
SEASTAR_TEST_CASE(test_callback_node_oriented_option_fires_for_local_node) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto clear_test_options = inject_node_oriented_test_option();

        const auto& topo = e.shared_token_metadata().local().get()->get_topology();
        const sstring dc = topo.get_datacenter();
        const sstring rack = topo.get_rack();
        const auto node_uuid = topo.my_host_id().uuid();

        e.execute_cql(seastar::format("ALTER DATACENTER \"{}\" WITH test_node_only_option = 7", dc)).get();

        sharded<db::cluster_config_manager> mgr;
        mgr.start(std::ref(mgr), std::ref(e.db()), std::ref(e.qp())).get();
        auto stop_mgr = seastar::defer([&mgr] noexcept {
            mgr.stop().get();
        });

        unsigned invocations = 0;
        std::optional<sstring> last_value;
        bool ctx_is_local_node = false;
        auto registration = mgr.local().register_config_callback(
            "test_node_only_option",
            [&] (const db::cluster_config_manager::lookup_context& ctx, std::optional<sstring> value) {
                ++invocations;
                last_value = std::move(value);
                ctx_is_local_node = ctx.dc_name == dc && ctx.rack_name == rack && ctx.node_uuid == node_uuid
                        && !ctx.keyspace_name && !ctx.table_name;
                return make_ready_future<>();
            }).get();

        // The first pass fires once, for the local node, with the value inherited from the
        // datacenter scope through the node-oriented chain.
        mgr.local().refresh().get();
        BOOST_REQUIRE_EQUAL(invocations, 1u);
        BOOST_REQUIRE(ctx_is_local_node);
        BOOST_REQUIRE_EQUAL(last_value.value_or(""), "7");

        // Unchanged refresh: change detection also applies to the node target.
        mgr.local().refresh().get();
        BOOST_REQUIRE_EQUAL(invocations, 1u);

        // A node-scope override shadows the datacenter's and is delivered exactly once,
        // however many passes the write itself triggered.
        e.execute_cql(seastar::format("ALTER NODE {} WITH test_node_only_option = 9", node_uuid)).get();
        mgr.local().refresh().get();
        BOOST_REQUIRE_EQUAL(invocations, 2u);
        BOOST_REQUIRE_EQUAL(last_value.value_or(""), "9");
    });
}

// ALTER TABLE has no restriction on mixing registry-backed config properties (e.g.
// auto_repair_enabled) with legacy schema properties (e.g. gc_grace_seconds) in the
// same statement: the config write and the legacy schema rebuild target disjoint
// columns of the same system_schema.scylla_tables row (see
// db::schema_tables::make_scylla_table_configs_mutation vs make_scylla_tables_mutation),
// so both are generated and applied together. See
// alter_table_statement::prepare_schema_mutations.
SEASTAR_TEST_CASE(test_alter_table_mixing_config_and_legacy_properties_succeeds) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE KEYSPACE ks_cfg WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}").get();
        e.execute_cql("CREATE TABLE ks_cfg.tbl (pk int PRIMARY KEY, v int)").get();

        e.execute_cql("ALTER TABLE ks_cfg.tbl WITH gc_grace_seconds = 12345 AND auto_repair_enabled = true").get();

        // The legacy property was applied to the table's own schema...
        BOOST_REQUIRE_EQUAL(e.local_db().find_schema("ks_cfg", "tbl")->gc_grace_seconds().count(), 12345);

        // ...and the config property was applied too, not silently dropped.
        with_config_manager(e, [&] (sharded<db::cluster_config_manager>& mgr) {
            db::cluster_config_manager::lookup_context ctx;
            ctx.keyspace_name = "ks_cfg";
            ctx.table_name = "tbl";
            BOOST_REQUIRE_EQUAL(mgr.local().resolve_config("auto_repair_enabled", ctx).value_or(""), "true");
        });
    });
}

// A materialized view has no cluster-config write path yet: neither create_view_statement nor
// alter_view_statement writes the out-of-band `configs` column of
// system_schema.scylla_tables. But view_prop_defs::validate_raw delegates keyword validation
// to cf_prop_defs::validate(), which accepts registry-backed scope::table property names, so
// without an explicit check the view paths would report success and silently discard the
// setting. view_prop_defs::validate_raw rejects them as unsupported instead, matching the error
// ALTER TABLE already raises when it is pointed at a view. If per-view config overrides are
// added later, this test is expected to change along with that check.
SEASTAR_TEST_CASE(test_config_properties_are_rejected_on_materialized_views) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE KEYSPACE ks_cfg WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}").get();
        e.execute_cql("CREATE TABLE ks_cfg.tbl (pk int PRIMARY KEY, v int)").get();

        // Match on the message, not just the exception type: invalid_request_exception is what
        // almost every other CQL rejection throws too, so a type-only assertion would still pass
        // if the statement failed for an unrelated reason and the silent-drop hole were open.
        auto rejected_as_config_property = [] (const exceptions::invalid_request_exception& ex) {
            return sstring(ex.what()).find("Cluster-config properties are not supported on materialized views") != sstring::npos
                && sstring(ex.what()).find("auto_repair_enabled") != sstring::npos;
        };

        BOOST_REQUIRE_EXCEPTION(
            e.execute_cql("CREATE MATERIALIZED VIEW ks_cfg.mv AS SELECT * FROM ks_cfg.tbl "
                          "WHERE v IS NOT NULL PRIMARY KEY (v, pk) WITH auto_repair_enabled = true").get(),
            exceptions::invalid_request_exception, rejected_as_config_property);

        e.execute_cql("CREATE MATERIALIZED VIEW ks_cfg.mv AS SELECT * FROM ks_cfg.tbl "
                      "WHERE v IS NOT NULL PRIMARY KEY (v, pk)").get();

        BOOST_REQUIRE_EXCEPTION(
            e.execute_cql("ALTER MATERIALIZED VIEW ks_cfg.mv WITH auto_repair_enabled = true").get(),
            exceptions::invalid_request_exception, rejected_as_config_property);

        // Pointing ALTER TABLE at the view keeps failing as it did before (the config branch of
        // alter_table_statement::prepare_schema_mutations checks is_view()).
        BOOST_REQUIRE_THROW(
            e.execute_cql("ALTER TABLE ks_cfg.mv WITH auto_repair_enabled = true").get(),
            exceptions::invalid_request_exception);

        // A legacy-only ALTER MATERIALIZED VIEW is unaffected by the new check.
        e.execute_cql("ALTER MATERIALIZED VIEW ks_cfg.mv WITH gc_grace_seconds = 12345").get();
        BOOST_REQUIRE_EQUAL(e.local_db().find_schema("ks_cfg", "mv")->gc_grace_seconds().count(), 12345);

        // Rejecting the write must not affect reads: with nothing stored at the view's own table
        // scope, a lookup for it still resolves through the keyspace/cluster scopes.
        e.execute_cql("ALTER KEYSPACE ks_cfg WITH auto_repair_enabled = true").get();
        with_config_manager(e, [&] (sharded<db::cluster_config_manager>& mgr) {
            db::cluster_config_manager::lookup_context ctx;
            ctx.keyspace_name = "ks_cfg";
            ctx.table_name = "mv";
            BOOST_REQUIRE_EQUAL(mgr.local().resolve_config("auto_repair_enabled", ctx).value_or(""), "true");
        });
    });
}

// A non-tablets-affecting legacy keyspace property (durable_writes) can be freely mixed
// with a registry-backed config property in a single ALTER KEYSPACE: the general
// keyspace-metadata-update path (ks_prop_defs::as_ks_metadata_update) already folds
// config option updates into the resulting keyspace_metadata alongside the legacy
// property change. See alter_keyspace_statement::prepare_schema_mutations.
SEASTAR_TEST_CASE(test_alter_keyspace_mixing_config_and_non_tablets_legacy_properties_succeeds) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE KEYSPACE ks_cfg WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
                      "AND tablets = {'enabled': 'false'}").get();

        e.execute_cql("ALTER KEYSPACE ks_cfg WITH durable_writes = false AND auto_repair_enabled = true").get();

        BOOST_REQUIRE(!e.local_db().find_keyspace("ks_cfg").metadata()->durable_writes());

        with_config_manager(e, [&] (sharded<db::cluster_config_manager>& mgr) {
            db::cluster_config_manager::lookup_context ctx;
            ctx.keyspace_name = "ks_cfg";
            BOOST_REQUIRE_EQUAL(mgr.local().resolve_config("auto_repair_enabled", ctx).value_or(""), "true");
        });
    });
}

// The one combination that remains rejected: a registry-backed config property combined
// with a tablets replication-factor change in the same ALTER KEYSPACE statement. Such a
// change is completed asynchronously through the global topology-request machinery (see
// alter_keyspace_statement::changes_tablets), and the request payload
// (ks_prop_defs::flattened()) does not carry registry-backed config keys, so they would
// be silently lost by the time the deferred completion runs.
SEASTAR_TEST_CASE(test_alter_keyspace_mixing_config_and_tablets_rf_change_is_rejected) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE KEYSPACE ks_tablets WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1} "
                      "AND tablets = {'enabled': 'true'}").get();

        BOOST_REQUIRE_THROW(
            e.execute_cql("ALTER KEYSPACE ks_tablets WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1} "
                          "AND auto_repair_enabled = true").get(),
            exceptions::invalid_request_exception);

        // A config-only ALTER on the same tablets-enabled keyspace still succeeds (no
        // replication-options change means changes_tablets(qp) is false).
        e.execute_cql("ALTER KEYSPACE ks_tablets WITH auto_repair_enabled = true").get();

        // A replication-only ALTER (no config properties) still succeeds too, taking the
        // deferred tablets/RF-change path unaffected by this restriction.
        e.execute_cql("ALTER KEYSPACE ks_tablets WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1}").get();
    });
}


SEASTAR_TEST_CASE(test_cluster_config_manager_wait_until_ready_observes_first_runtime_apply) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        sharded<db::cluster_config_manager> mgr;
        mgr.start(std::ref(mgr), std::ref(e.db()), std::ref(e.qp())).get();
        auto stop_mgr = seastar::defer([&mgr] noexcept {
            mgr.stop().get();
        });

        auto ready_future = mgr.local().wait_until_ready();
        BOOST_REQUIRE(!ready_future.available());

        e.execute_cql("ALTER CLUSTER WITH auto_repair_enabled = true").get();
        mgr.local().refresh().get();
        ready_future.get();

        db::cluster_config_manager::lookup_context ctx;
        BOOST_REQUIRE_EQUAL(mgr.local().resolve_config("auto_repair_enabled", ctx).value_or(""), "true");
    });
}

// stop() must wake any wait_until_ready() waiter that is still parked because no successful
// refresh has opened the barrier yet. Without this, the parked promise is destroyed
// unsatisfied at shutdown and the waiter sees an implicit broken_promise; instead it should
// observe a clean gate_closed_exception. The barrier is never opened by stop() itself.
SEASTAR_TEST_CASE(test_cluster_config_manager_wait_until_ready_aborts_on_stop) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        sharded<db::cluster_config_manager> mgr;
        mgr.start(std::ref(mgr), std::ref(e.db()), std::ref(e.qp())).get();
        // Deferred so a failing BOOST_REQUIRE below still stops the sharded service
        // instead of aborting in ~sharded() with live instances.
        auto stop_mgr = deferred_stop(mgr);

        // Never call refresh(), so the barrier stays closed and the waiter parks.
        auto ready_future = mgr.local().wait_until_ready();
        BOOST_REQUIRE(!ready_future.available());

        // The test needs the stop to happen before the assertion; stop_now() also
        // disarms the deferred stop, since stop() must not run twice.
        stop_mgr.stop_now();

        BOOST_REQUIRE_THROW(ready_future.get(), seastar::gate_closed_exception);
    });
}

SEASTAR_TEST_CASE(test_cluster_config_manager_refreshes_after_config_table_write) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        with_config_manager(e, [&] (sharded<db::cluster_config_manager>& mgr) {
            db::cluster_config_manager::lookup_context ctx;
            BOOST_REQUIRE(!mgr.local().resolve_config("auto_repair_enabled", ctx).has_value());

            e.execute_cql("ALTER CLUSTER WITH auto_repair_enabled = true").get();

            CHECK_EVENTUALLY_EQUAL(std::function<sstring()>([&] {
                db::cluster_config_manager::lookup_context local_ctx;
                return mgr.local().resolve_config("auto_repair_enabled", local_ctx).value_or("");
            }), sstring("true"));
        });
    });
}

// A table-oriented callback applies a value only to tables for which the option resolves
// to an effective value. A table with no override is never applied a value (it is only
// ever cleared).
SEASTAR_TEST_CASE(test_callback_applies_only_to_affected_table) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE KEYSPACE ks_cb WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}").get();
        e.execute_cql("CREATE TABLE ks_cb.t1 (pk int PRIMARY KEY)").get();
        e.execute_cql("CREATE TABLE ks_cb.t2 (pk int PRIMARY KEY)").get();

        auto rec = std::make_shared<callback_recorder>();
        with_config_callback(e, rec, [&] (sharded<db::cluster_config_manager>& mgr) {
            // No overrides yet: no value is applied for our user tables.
            BOOST_REQUIRE_EQUAL(rec->apply_count("ks_cb", "t1"), 0u);
            BOOST_REQUIRE_EQUAL(rec->apply_count("ks_cb", "t2"), 0u);

            // A table-scope override gives only that table an effective value.
            e.execute_cql("ALTER TABLE ks_cb.t1 WITH auto_repair_enabled = true").get();
            eventually([&] {
                BOOST_REQUIRE_EQUAL(rec->last_apply("ks_cb", "t1").value_or(""), "true");
            });
            // t2 has no override, so it is never applied a value (it resolves to absent).
            BOOST_REQUIRE_EQUAL(rec->apply_count("ks_cb", "t2"), 0u);

            // A refresh that observes no change does not re-invoke the callback: the manager
            // remembers the value it last delivered per target and skips unchanged ones.
            const auto t1_applies = rec->apply_count("ks_cb", "t1");
            mgr.local().refresh().get();
            mgr.local().refresh().get();
            BOOST_REQUIRE_EQUAL(rec->apply_count("ks_cb", "t1"), t1_applies);
            BOOST_REQUIRE_EQUAL(rec->last_apply("ks_cb", "t1").value_or(""), "true");
            BOOST_REQUIRE_EQUAL(rec->apply_count("ks_cb", "t2"), 0u);
        });
    });
}

// A keyspace-scope value is inherited only by tables that do not have their own
// table-scope override; a table with its own override is shadowed.
SEASTAR_TEST_CASE(test_callback_keyspace_change_respects_table_override) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE KEYSPACE ks_sh WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}").get();
        e.execute_cql("CREATE TABLE ks_sh.t1 (pk int PRIMARY KEY)").get();
        e.execute_cql("CREATE TABLE ks_sh.t2 (pk int PRIMARY KEY)").get();
        e.execute_cql("ALTER TABLE ks_sh.t1 WITH auto_repair_enabled = false").get();
        e.execute_cql("ALTER KEYSPACE ks_sh WITH auto_repair_enabled = true").get();

        auto rec = std::make_shared<callback_recorder>();
        with_config_callback(e, rec, [&] (sharded<db::cluster_config_manager>& mgr) {
            // Each table is applied its effective value: t1 from its own override (false),
            // t2 inherited from the keyspace (true).
            BOOST_REQUIRE_EQUAL(rec->last_apply("ks_sh", "t1").value_or(""), "false");
            BOOST_REQUIRE_EQUAL(rec->last_apply("ks_sh", "t2").value_or(""), "true");

            // Changing the keyspace value flows to t2; t1 keeps resolving to its own override.
            e.execute_cql("ALTER KEYSPACE ks_sh WITH auto_repair_enabled = false").get();
            eventually([&] {
                BOOST_REQUIRE_EQUAL(rec->last_apply("ks_sh", "t2").value_or(""), "false");
            });
            BOOST_REQUIRE_EQUAL(rec->last_apply("ks_sh", "t1").value_or(""), "false");
        });
    });
}

// Removing the only override for a table leaves no effective value, so the callback is
// invoked with std::nullopt for that table.
SEASTAR_TEST_CASE(test_callback_clears_on_override_removal) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE KEYSPACE ks_clr WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}").get();
        e.execute_cql("CREATE TABLE ks_clr.t1 (pk int PRIMARY KEY)").get();
        e.execute_cql("ALTER TABLE ks_clr.t1 WITH auto_repair_enabled = true").get();

        auto rec = std::make_shared<callback_recorder>();
        with_config_callback(e, rec, [&] (sharded<db::cluster_config_manager>& mgr) {
            BOOST_REQUIRE_EQUAL(rec->last_apply("ks_clr", "t1").value_or(""), "true");

            e.execute_cql("ALTER TABLE ks_clr.t1 WITH auto_repair_enabled = null").get();
            // Once the override is gone the table resolves to absent, so std::nullopt is passed.
            eventually([&] {
                BOOST_REQUIRE_GE(rec->clear_count("ks_clr", "t1"), 1u);
            });
        });
    });
}

// A table created after an inherited override already exists receives the resolved value
// on the next refresh (the per-table scan picks up newly created tables).
SEASTAR_TEST_CASE(test_callback_applies_to_table_created_after_override) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE KEYSPACE ks_new WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}").get();
        e.execute_cql("ALTER KEYSPACE ks_new WITH auto_repair_enabled = true").get();

        auto rec = std::make_shared<callback_recorder>();
        with_config_callback(e, rec, [&] (sharded<db::cluster_config_manager>& mgr) {
            e.execute_cql("CREATE TABLE ks_new.t1 (pk int PRIMARY KEY)").get();
            // Refresh inside the retry loop so the per-table scan is re-run until it observes
            // the newly created table (table creation and the config refresh race otherwise).
            eventually([&] {
                mgr.local().refresh().get();
                BOOST_REQUIRE_EQUAL(rec->last_apply("ks_new", "t1").value_or(""), "true");
            });
        });
    });
}

// Dropping a table simply removes it from the per-table scan: the callback stops being
// invoked for it and the refresh that observes the drop does not crash.
SEASTAR_TEST_CASE(test_callback_handles_table_drop) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE KEYSPACE ks_drop WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}").get();
        e.execute_cql("CREATE TABLE ks_drop.t1 (pk int PRIMARY KEY)").get();
        e.execute_cql("ALTER TABLE ks_drop.t1 WITH auto_repair_enabled = true").get();

        auto rec = std::make_shared<callback_recorder>();
        with_config_callback(e, rec, [&] (sharded<db::cluster_config_manager>& mgr) {
            BOOST_REQUIRE_EQUAL(rec->last_apply("ks_drop", "t1").value_or(""), "true");

            e.execute_cql("DROP TABLE ks_drop.t1").get();
            mgr.local().refresh().get();
            auto applies_after_drop = rec->apply_count("ks_drop", "t1");

            // Further refreshes do not apply a value to the dropped table.
            mgr.local().refresh().get();
            BOOST_REQUIRE_EQUAL(rec->apply_count("ks_drop", "t1"), applies_after_drop);
        });
    });
}

// The manager invokes a callback only on the shard it was registered on. Registering on
// every shard (via invoke_on_all) therefore fires the callback on every shard.
SEASTAR_TEST_CASE(test_callback_registered_on_all_shards_fires_on_all_shards) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE KEYSPACE ks_ms WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}").get();
        e.execute_cql("CREATE TABLE ks_ms.t1 (pk int PRIMARY KEY)").get();
        e.execute_cql("ALTER TABLE ks_ms.t1 WITH auto_repair_enabled = true").get();

        sharded<db::cluster_config_manager> mgr;
        mgr.start(std::ref(mgr), std::ref(e.db()), std::ref(e.qp())).get();
        auto stop_mgr = seastar::defer([&mgr] noexcept {
            mgr.stop().get();
        });

        // Each shard counts its own applies for ks_ms.t1 in its own instance, so there is no
        // cross-shard write race. The callback's lambda captures only &applies (a sharded<>,
        // safe to access on any shard via local()), and is registered separately on each shard.
        sharded<unsigned> applies;
        applies.start(0u).get();
        auto stop_applies = seastar::defer([&applies] noexcept {
            applies.stop().get();
        });

        sharded<std::optional<db::cluster_config_manager::config_callback_registration>> registrations;
        registrations.start(std::nullopt).get();
        auto stop_registrations = seastar::defer([&registrations] noexcept {
            registrations.stop().get();
        });

        mgr.invoke_on_all([&applies, &registrations] (db::cluster_config_manager& manager) {
            return manager.register_config_callback(
                "auto_repair_enabled",
                [&applies] (const db::cluster_config_manager::lookup_context& ctx, std::optional<sstring> value) {
                    if (ctx.keyspace_name == "ks_ms" && ctx.table_name == "t1" && value && *value == "true") {
                        applies.local()++;
                    }
                    return make_ready_future<>();
                }).then([&registrations] (db::cluster_config_manager::config_callback_registration registration) {
                    registrations.local() = std::move(registration);
                });
        }).get();

        mgr.local().refresh().get();

        // Every shard must have observed the apply for ks_ms.t1 at least once.
        auto shards_that_applied = applies.map_reduce0(
            [] (unsigned count) -> unsigned { return count >= 1 ? 1u : 0u; },
            0u,
            std::plus<unsigned>()).get();
        BOOST_REQUIRE_EQUAL(shards_that_applied, this_smp_shard_count());
    });
}

// A callback registered on a single shard (here shard 0, via local()) is invoked only on
// that shard, never on the others.
SEASTAR_TEST_CASE(test_callback_registered_on_shard0_fires_only_on_shard0) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE KEYSPACE ks_s0 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}").get();
        e.execute_cql("CREATE TABLE ks_s0.t1 (pk int PRIMARY KEY)").get();
        e.execute_cql("ALTER TABLE ks_s0.t1 WITH auto_repair_enabled = true").get();

        sharded<db::cluster_config_manager> mgr;
        mgr.start(std::ref(mgr), std::ref(e.db()), std::ref(e.qp())).get();
        auto stop_mgr = seastar::defer([&mgr] noexcept {
            mgr.stop().get();
        });

        sharded<unsigned> applies;
        applies.start(0u).get();
        auto stop_applies = seastar::defer([&applies] noexcept {
            applies.stop().get();
        });

        // Register on shard 0 only.
        auto registration = mgr.local().register_config_callback(
            "auto_repair_enabled",
            [&applies] (const db::cluster_config_manager::lookup_context& ctx, std::optional<sstring> value) {
                if (ctx.keyspace_name == "ks_s0" && ctx.table_name == "t1" && value && *value == "true") {
                    applies.local()++;
                }
                return make_ready_future<>();
            }).get();

        mgr.local().refresh().get();

        // Shard 0 fired at least once; no other shard fired, so the total equals shard 0's count.
        auto shard0_applies = applies.invoke_on(0, [] (unsigned& c) { return c; }).get();
        auto total_applies = applies.map_reduce0(
            [] (unsigned count) { return count; },
            0u,
            std::plus<unsigned>()).get();
        BOOST_REQUIRE_GE(shard0_applies, 1u);
        BOOST_REQUIRE_EQUAL(total_applies, shard0_applies);
    });
}

SEASTAR_TEST_CASE(test_callback_is_unregistered_when_registration_is_destroyed) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE KEYSPACE ks_rg WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}").get();
        e.execute_cql("CREATE TABLE ks_rg.t1 (pk int PRIMARY KEY)").get();
        e.execute_cql("ALTER TABLE ks_rg.t1 WITH auto_repair_enabled = true").get();

        sharded<db::cluster_config_manager> mgr;
        mgr.start(std::ref(mgr), std::ref(e.db()), std::ref(e.qp())).get();
        auto stop_mgr = seastar::defer([&mgr] noexcept {
            mgr.stop().get();
        });

        unsigned applies = 0;
        {
            auto registration = mgr.local().register_config_callback(
                "auto_repair_enabled",
                [&applies] (const db::cluster_config_manager::lookup_context& ctx, std::optional<sstring> value) {
                    if (ctx.keyspace_name == "ks_rg" && ctx.table_name == "t1" && value) {
                        applies++;
                    }
                    return make_ready_future<>();
                }).get();
            mgr.local().refresh().get();
            BOOST_REQUIRE_GE(applies, 1u);
        }

        // The registration was destroyed, so the callback must not fire again. Change the
        // value first: a refresh over an unchanged value is skipped by change detection and
        // would leave the counter alone whether or not the callback is still registered.
        const auto applies_after_unregistration = applies;
        e.execute_cql("ALTER TABLE ks_rg.t1 WITH auto_repair_enabled = false").get();
        mgr.local().refresh().get();
        BOOST_REQUIRE_EQUAL(applies, applies_after_unregistration);
    });
}

// The registration handle is movable: moving it (construction and assignment) transfers
// ownership of the unregistration, so destroying a moved-from husk must not unregister
// the callback, and destroying the final owner must.
SEASTAR_TEST_CASE(test_callback_registration_move_transfers_unregistration) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE KEYSPACE ks_mvreg WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}").get();
        e.execute_cql("CREATE TABLE ks_mvreg.t1 (pk int PRIMARY KEY)").get();
        e.execute_cql("ALTER TABLE ks_mvreg.t1 WITH auto_repair_enabled = true").get();

        sharded<db::cluster_config_manager> mgr;
        mgr.start(std::ref(mgr), std::ref(e.db()), std::ref(e.qp())).get();
        auto stop_mgr = seastar::defer([&mgr] noexcept {
            mgr.stop().get();
        });

        unsigned applies = 0;
        {
            db::cluster_config_manager::config_callback_registration owner;
            {
                auto registration = mgr.local().register_config_callback(
                    "auto_repair_enabled",
                    [&applies] (const db::cluster_config_manager::lookup_context& ctx, std::optional<sstring> value) {
                        if (ctx.keyspace_name == "ks_mvreg" && ctx.table_name == "t1" && value) {
                            ++applies;
                        }
                        return make_ready_future<>();
                    }).get();
                auto moved = std::move(registration); // move-construct
                owner = std::move(moved);             // move-assign
            }
            // The moved-from husks were destroyed above; the callback must still be
            // registered and fire.
            mgr.local().refresh().get();
            BOOST_REQUIRE_GE(applies, 1u);
        }
        // `owner` was the last holder, so its destruction unregistered the callback. Change
        // the value so the callback would fire again if it were still registered (an
        // unchanged value would be skipped by change detection and prove nothing).
        const auto applies_after_unregistration = applies;
        e.execute_cql("ALTER TABLE ks_mvreg.t1 WITH auto_repair_enabled = false").get();
        mgr.local().refresh().get();
        BOOST_REQUIRE_EQUAL(applies, applies_after_unregistration);
    });
}

// Unregistering a callback from inside another callback, while a refresh pass is still
// running, must not corrupt the manager's index-based iteration or fire the unregistered
// callback afterwards. The manager marks the callback unregistered immediately but defers
// erasing it (sweep_unregistered_callbacks) until the pass completes; an immediate erase
// would shrink the vector under the captured callback_count and read out of bounds.
SEASTAR_TEST_CASE(test_callback_unregistered_during_invocation_is_swept_safely) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE KEYSPACE ks_sweep WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}").get();
        // Several tables so the table-oriented callback A fires multiple times in one pass.
        e.execute_cql("CREATE TABLE ks_sweep.t1 (pk int PRIMARY KEY)").get();
        e.execute_cql("CREATE TABLE ks_sweep.t2 (pk int PRIMARY KEY)").get();
        e.execute_cql("CREATE TABLE ks_sweep.t3 (pk int PRIMARY KEY)").get();
        e.execute_cql("ALTER CLUSTER WITH auto_repair_enabled = true").get();

        sharded<db::cluster_config_manager> mgr;
        mgr.start(std::ref(mgr), std::ref(e.db()), std::ref(e.qp())).get();
        auto stop_mgr = seastar::defer([&mgr] noexcept {
            mgr.stop().get();
        });

        auto b_registration = std::make_shared<std::optional<db::cluster_config_manager::config_callback_registration>>();
        unsigned a_invocations = 0;
        unsigned b_invocations = 0;

        // A is registered first, so it runs before B within a pass. On its first invocation it
        // destroys B's registration while the pass is still in progress.
        auto a_registration = mgr.local().register_config_callback(
            "auto_repair_enabled",
            [&] (const db::cluster_config_manager::lookup_context&, std::optional<sstring>) {
                ++a_invocations;
                b_registration->reset();
                return make_ready_future<>();
            }).get();
        *b_registration = mgr.local().register_config_callback(
            "auto_repair_enabled",
            [&] (const db::cluster_config_manager::lookup_context&, std::optional<sstring>) {
                ++b_invocations;
                return make_ready_future<>();
            }).get();

        mgr.local().refresh().get();

        // A ran; B never fired because it was unregistered before the pass reached it, and the
        // deferred sweep avoided any out-of-bounds access.
        BOOST_REQUIRE_GE(a_invocations, 1u);
        BOOST_REQUIRE_EQUAL(b_invocations, 0u);

        // A subsequent pass still works and still never invokes the swept-away B. The value
        // is changed first so that A actually fires again: an unchanged value would be
        // skipped by the manager's per-target change detection.
        a_invocations = 0;
        e.execute_cql("ALTER CLUSTER WITH auto_repair_enabled = false").get();
        mgr.local().refresh().get();
        BOOST_REQUIRE_GE(a_invocations, 1u);
        BOOST_REQUIRE_EQUAL(b_invocations, 0u);
    });
}

BOOST_AUTO_TEST_SUITE_END()
