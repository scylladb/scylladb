/*
 * Copyright (C) 2015-present-2020 ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */


#include "locator/abstract_replication_strategy.hh"
#include "locator/tablets.hh"
#include "replica/tablets.hh"
#include <boost/test/unit_test.hpp>
#include <boost/multiprecision/cpp_int.hpp>

#include <fmt/ranges.h>
#include <fmt/std.h>

#include <seastar/net/inet_address.hh>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/eventually.hh"
#include "test/lib/log.hh"
#include "test/lib/test_utils.hh"
#include "test/lib/eventually.hh"

#include <seastar/core/future-util.hh>
#include <seastar/core/sleep.hh>
#include "transport/messages/result_message.hh"
#include "transport/messages/result_message_base.hh"
#include "types/types.hh"
#include "utils/assert.hh"
#include "utils/big_decimal.hh"
#include "types/map.hh"
#include "types/list.hh"
#include "types/set.hh"
#include "types/vector.hh"
#include "db/config.hh"
#include "db/extensions.hh"
#include "auth/authenticated_user.hh"
#include "service/client_state.hh"
#include "cql3/cql_config.hh"
#include "test/lib/exception_utils.hh"
#include "service/qos/qos_common.hh"
#include "utils/rjson.hh"
#include "schema/schema_builder.hh"
#include "service/migration_manager.hh"
#include <boost/regex.hpp>
#include "service/qos/qos_common.hh"
#include "utils/UUID_gen.hh"
#include "tombstone_gc_extension.hh"
#include "db/tags/extension.hh"
#include "cdc/cdc_extension.hh"
#include "db/paxos_grace_seconds_extension.hh"
#include "db/per_partition_rate_limit_extension.hh"
#include "replica/schema_describe_helper.hh"
#include "sstables/sstables.hh"
#include "replica/distributed_loader.hh"
#include "compaction/compaction_manager.hh"
#include "service/query_state.hh"
#include "service_permit.hh"
#include "service/strong_consistency/coordinator.hh"
#include "service/strong_consistency/groups_manager.hh"
#include "db/cluster_config_registry.hh"
#include "locator/token_metadata.hh"
#include "locator/topology.hh"
#include <seastar/core/smp.hh>


BOOST_AUTO_TEST_SUITE(cql_query_test)

using namespace std::literals::chrono_literals;
using namespace tests;

// All create_statement cells of a multi-row describe (e.g. DESC SCHEMA), in row order.
static std::vector<sstring> describe_create_statements(cql_test_env& e, std::string_view query) {
    auto msg = e.execute_cql(query).get();
    auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
    BOOST_REQUIRE(rows);

    std::vector<sstring> result;
    for (const auto& row : rows->rs().result_set().rows()) {
        BOOST_REQUIRE_EQUAL(row.size(), 4);
        if (row[3]) {
            result.push_back(value_cast<sstring>(utf8_type->deserialize(*row[3])));
        }
    }
    return result;
}

SEASTAR_TEST_CASE(test_alter_cluster_without_auth_enabled_is_allowed) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        BOOST_REQUIRE_NO_THROW(e.execute_cql("ALTER CLUSTER WITH auto_repair_enabled = true").get());
    });
}

SEASTAR_TEST_CASE(test_registry_backed_cluster_config_statements_reject_when_feature_is_disabled) {
    cql_test_config cfg;
    cfg.disabled_features.emplace("CLUSTER_CONFIG_REGISTRY_V0");

    return do_with_cql_env_thread([](cql_test_env& e) {
        auto feature_disabled = [] (const exceptions::invalid_request_exception& ex) {
            return std::string_view(ex.what()).find("Cluster config option 'auto_repair_enabled' is not yet supported by this cluster") != std::string_view::npos;
        };

        BOOST_REQUIRE_EXCEPTION(
            e.execute_cql("ALTER CLUSTER WITH auto_repair_enabled = true").get(),
            exceptions::invalid_request_exception,
            feature_disabled);

        e.execute_cql("CREATE KEYSPACE ks_cfg_disabled WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}").get();

        BOOST_REQUIRE_EXCEPTION(
            e.execute_cql("CREATE TABLE ks_cfg_disabled.tbl (pk int PRIMARY KEY) WITH auto_repair_enabled = true").get(),
            exceptions::invalid_request_exception,
            feature_disabled);

        e.execute_cql("CREATE TABLE ks_cfg_disabled.tbl (pk int PRIMARY KEY)").get();

        BOOST_REQUIRE_EXCEPTION(
            e.execute_cql("ALTER KEYSPACE ks_cfg_disabled WITH auto_repair_enabled = true").get(),
            exceptions::invalid_request_exception,
            feature_disabled);

        BOOST_REQUIRE_EXCEPTION(
            e.execute_cql("ALTER TABLE ks_cfg_disabled.tbl WITH auto_repair_enabled = true").get(),
            exceptions::invalid_request_exception,
            feature_disabled);

        // DESCRIBE mirrors the write side: with the feature disabled, no registry option
        // is described at any scope, not even as a commented-out default.
        for (const auto& query : {"DESCRIBE KEYSPACE ks_cfg_disabled", "DESCRIBE TABLE ks_cfg_disabled.tbl", "DESCRIBE SCHEMA"}) {
            for (const auto& stmt : describe_create_statements(e, query)) {
                BOOST_REQUIRE_MESSAGE(stmt.find("auto_repair_enabled") == sstring::npos,
                        seastar::format("{} described a registry option while the feature is disabled: {}", query, stmt));
            }
        }
    }, cfg);
}

// No shipping option supports the node-oriented scopes yet, so we inject a test-only option
// to exercise the ALTER DATACENTER/RACK/NODE paths. Verifies that each node-oriented write is
// schema-backed (bumps the global schema version), persists its value out-of-band in the
// matching scylla_datacenters/scylla_racks/scylla_nodes config table, and - because it never
// raises a per-table schema-change notification - leaves an unrelated table's prepared
// statement valid.
SEASTAR_TEST_CASE(test_alter_node_oriented_scopes_persist_and_bump_version_without_invalidation) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        static constexpr auto node_oriented_scopes = db::cluster_config_registry::scope_set::of<
                db::cluster_config_registry::scope::cluster,
                db::cluster_config_registry::scope::datacenter,
                db::cluster_config_registry::scope::rack,
                db::cluster_config_registry::scope::node>();
        db::cluster_config_registry::add_test_only_option_on_all_shards(db::cluster_config_registry::option{
            .name = "test_node_only_option",
            .scopes = node_oriented_scopes,
            .min_version = db::cluster_config_registry::version::v0,
            .default_value = int64_t(0),
        }).get();
        auto clear_test_options = defer([] noexcept {
            db::cluster_config_registry::clear_test_only_options_on_all_shards().get();
        });

        const auto& topo = e.shared_token_metadata().local().get()->get_topology();
        const sstring dc = topo.get_datacenter();
        const sstring rack = topo.get_rack();
        const auto node_uuid = topo.my_host_id().uuid();

        auto configs_type = map_type_impl::get_instance(utf8_type, utf8_type, false);
        auto global_version = [&] { return e.db().local().get_version(); };
        auto expect_configs = [&] (sstring query, sstring value) {
            assert_that(e.execute_cql(query).get())
                .is_rows().with_rows({{
                    {configs_type->decompose(make_map_value(configs_type, map_type_impl::native_type({
                        {sstring("test_node_only_option"), value},
                    })))}
                }});
        };

        e.execute_cql("CREATE KEYSPACE ks_node WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}").get();
        e.execute_cql("CREATE TABLE ks_node.tbl (pk int PRIMARY KEY, v int)").get();
        auto prepared = e.prepare("SELECT * FROM ks_node.tbl WHERE pk = 0").get();

        // Datacenter scope.
        auto before = global_version();
        e.execute_cql(seastar::format("ALTER DATACENTER \"{}\" WITH test_node_only_option = 7", dc)).get();
        BOOST_REQUIRE_NE(global_version(), before);
        expect_configs(seastar::format("SELECT configs FROM system_schema.scylla_datacenters WHERE dc_name = '{}'", dc), "7");

        // Rack scope.
        before = global_version();
        e.execute_cql(seastar::format("ALTER RACK \"{}\" \"{}\" WITH test_node_only_option = 8", dc, rack)).get();
        BOOST_REQUIRE_NE(global_version(), before);
        expect_configs(seastar::format("SELECT configs FROM system_schema.scylla_racks WHERE dc_name = '{}' AND rack_name = '{}'", dc, rack), "8");

        // Node scope.
        before = global_version();
        e.execute_cql(seastar::format("ALTER NODE {} WITH test_node_only_option = 9", node_uuid)).get();
        BOOST_REQUIRE_NE(global_version(), before);
        expect_configs(seastar::format("SELECT configs FROM system_schema.scylla_nodes WHERE host_id = {}", node_uuid), "9");

        // None of the node-oriented writes raise a per-table notification, so the prepared
        // statement on an unrelated table is still valid.
        BOOST_REQUIRE_NO_THROW(e.execute_prepared(prepared, {}).get());
    });
}

// Verifies that the node-oriented ALTER statements validate the scope target against the live
// topology and reject datacenters/racks/nodes that do not exist.
SEASTAR_TEST_CASE(test_alter_node_oriented_scopes_reject_unknown_targets) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        static constexpr auto node_oriented_scopes = db::cluster_config_registry::scope_set::of<
                db::cluster_config_registry::scope::cluster,
                db::cluster_config_registry::scope::datacenter,
                db::cluster_config_registry::scope::rack,
                db::cluster_config_registry::scope::node>();
        db::cluster_config_registry::add_test_only_option_on_all_shards(db::cluster_config_registry::option{
            .name = "test_node_only_option",
            .scopes = node_oriented_scopes,
            .min_version = db::cluster_config_registry::version::v0,
            .default_value = int64_t(0),
        }).get();
        auto clear_test_options = defer([] noexcept {
            db::cluster_config_registry::clear_test_only_options_on_all_shards().get();
        });

        const auto& topo = e.shared_token_metadata().local().get()->get_topology();
        const sstring dc = topo.get_datacenter();

        auto missing_target = [] (std::string_view fragment) {
            return [fragment] (const exceptions::invalid_request_exception& ex) {
                return std::string_view(ex.what()).find(fragment) != std::string_view::npos;
            };
        };

        BOOST_REQUIRE_EXCEPTION(
            e.execute_cql("ALTER DATACENTER no_such_dc WITH test_node_only_option = 1").get(),
            exceptions::invalid_request_exception,
            missing_target("does not exist"));

        BOOST_REQUIRE_EXCEPTION(
            e.execute_cql(seastar::format("ALTER RACK \"{}\" no_such_rack WITH test_node_only_option = 1", dc)).get(),
            exceptions::invalid_request_exception,
            missing_target("does not exist"));

        BOOST_REQUIRE_EXCEPTION(
            e.execute_cql(seastar::format("ALTER NODE {} WITH test_node_only_option = 1", utils::make_random_uuid())).get(),
            exceptions::invalid_request_exception,
            missing_target("does not exist"));
    });
}

SEASTAR_TEST_CASE(test_ttl) {
    return do_with_cql_env([] (cql_test_env& e) {
        auto make_my_list_type = [] { return list_type_impl::get_instance(utf8_type, true); };
        auto my_list_type = make_my_list_type();
        return e.create_table([make_my_list_type] (std::string_view ks_name) {
            return *schema_builder(this_smp_shard_count(), ks_name, "cf")
                    .with_column("p1", utf8_type, column_kind::partition_key)
                    .with_column("r1", utf8_type)
                    .with_column("r2", utf8_type)
                    .with_column("r3", make_my_list_type())
                    .build();
        }).then([&e] {
            return e.execute_cql(
                "update cf using ttl 100000 set r1 = 'value1_1', r3 = ['a', 'b', 'c'] where p1 = 'key1';").discard_result();
        }).then([&e] {
            return e.execute_cql(
                "update cf using ttl 100 set r1 = 'value1_3', r3 = ['a', 'b', 'c'] where p1 = 'key3';").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf using ttl 100 set r3[1] = 'b' where p1 = 'key1';").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf using ttl 100 set r1 = 'value1_2' where p1 = 'key2';").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into cf (p1, r2) values ('key2', 'value2_2');").discard_result();
        }).then([&e, my_list_type] {
            return e.execute_cql("select r1 from cf;").then([](shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_size(3)
                    .with_row({utf8_type->decompose(sstring("value1_1"))})
                    .with_row({utf8_type->decompose(sstring("value1_2"))})
                    .with_row({utf8_type->decompose(sstring("value1_3"))});
            });
        }).then([&e, my_list_type] {
            return e.execute_cql("select r3 from cf where p1 = 'key1';").then([my_list_type] (shared_ptr<cql_transport::messages::result_message> msg) {
                auto my_list_type = list_type_impl::get_instance(utf8_type, true);
                assert_that(msg).is_rows().with_rows({
                    {my_list_type->decompose(make_list_value(my_list_type, list_type_impl::native_type{{sstring("a"), sstring("b"), sstring("c")}}))}
                });
            });
        }).then([&e] {
            forward_jump_clocks(200s);
            return e.execute_cql("select r1, r2 from cf;").then([](shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_size(2)
                    .with_row({{}, utf8_type->decompose(sstring("value2_2"))})
                    .with_row({utf8_type->decompose(sstring("value1_1")), {}});
            });
        }).then([&e] {
            return e.execute_cql("select r2 from cf;").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_size(2)
                    .with_row({ utf8_type->decompose(sstring("value2_2")) })
                    .with_row({ {} });
            });
        }).then([&e] {
            return e.execute_cql("select r1 from cf;").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_size(2)
                    .with_row({ {} })
                    .with_row({ utf8_type->decompose(sstring("value1_1")) });
            });
        }).then([&e, my_list_type] {
            return e.execute_cql("select r3 from cf where p1 = 'key1';").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
                auto my_list_type = list_type_impl::get_instance(utf8_type, true);
                assert_that(msg).is_rows().with_rows({
                    {my_list_type->decompose(make_list_value(my_list_type, list_type_impl::native_type{{sstring("a"), sstring("c")}}))}
                });
            });
        }).then([&e] {
            return e.execute_cql("create table cf2 (p1 text PRIMARY KEY, r1 text, r2 text);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into cf2 (p1, r1) values ('foo', 'bar') using ttl 500;").discard_result();
        }).then([&e] {
            return e.execute_cql("select p1, r1 from cf2 where p1 = 'foo';").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({
                    {utf8_type->decompose(sstring("foo")), utf8_type->decompose(sstring("bar"))}
                });
            });
        }).then([&e] {
            forward_jump_clocks(600s);
            return e.execute_cql("select p1, r1 from cf2 where p1 = 'foo';").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({ });
            });
        }).then([&e] {
            return e.execute_cql("select p1, r1 from cf2;").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({ });
            });
        }).then([&e] {
            return e.execute_cql("select count(*) from cf2;").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({
                    {long_type->decompose(int64_t(0))}
                });
            });
        }).then([&e] {
            return e.execute_cql("insert into cf2 (p1, r1) values ('foo', 'bar') using ttl 500;").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf2 set r1 = null where p1 = 'foo';").discard_result();
        }).then([&e] {
            return e.execute_cql("select p1, r1 from cf2 where p1 = 'foo';").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({
                    {utf8_type->decompose(sstring("foo")), { }}
                });
            });
        }).then([&e] {
            forward_jump_clocks(600s);
            return e.execute_cql("select p1, r1 from cf2 where p1 = 'foo';").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({ });
            });
        }).then([&e] {
            return e.execute_cql("insert into cf2 (p1, r1) values ('foo', 'bar') using ttl 500;").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into cf2 (p1, r2) values ('foo', null);").discard_result();
        }).then([&e] {
            forward_jump_clocks(600s);
            return e.execute_cql("select p1, r1 from cf2 where p1 = 'foo';").then([] (shared_ptr<cql_transport::messages::result_message> msg) {
                assert_that(msg).is_rows().with_rows({
                    {utf8_type->decompose(sstring("foo")), { }}
                });
            });
        });
    });
}

uint64_t
run_and_examine_cache_read_stats_change(cql_test_env& e, std::string_view cf_name, std::function<void (cql_test_env& e)> func) {
    auto read_stat = [&] {
        return e.db().map_reduce0([&cf_name] (const replica::database& db) {
            auto& t = db.find_column_family("ks", cf_name);
            auto& stats = t.get_row_cache().stats();
            return stats.reads_with_misses.count() + stats.reads_with_no_misses.count();
        }, uint64_t(0), std::plus<uint64_t>()).get();
    };
    auto before = read_stat();
    func(e);
    auto after = read_stat();
    return after - before;
}

SEASTAR_TEST_CASE(test_cache_bypass) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE t (k int PRIMARY KEY)").get();
        auto with_cache = run_and_examine_cache_read_stats_change(e, "t", [] (cql_test_env& e) {
            e.execute_cql("SELECT * FROM t").get();
        });
        BOOST_REQUIRE(with_cache >= this_smp_shard_count());  // scan may make multiple passes per shard
        auto without_cache = run_and_examine_cache_read_stats_change(e, "t", [] (cql_test_env& e) {
            e.execute_cql("SELECT * FROM t BYPASS CACHE").get();
        });
        BOOST_REQUIRE_EQUAL(without_cache, 0);
    });
}

SEASTAR_TEST_CASE(test_view_with_two_regular_base_columns_in_key) {
    return do_with_cql_env_thread([] (auto& e) {
        cquery_nofail(e, "CREATE TABLE t (p int, c int, v1 int, v2 int, primary key(p,c))");
        auto schema = e.local_db().find_schema("ks", "t");

        // Create a CQL-illegal view with two regular base columns in the view key
        schema_builder view_builder(this_smp_shard_count(), "ks", "tv");
        view_builder.with_column(to_bytes("v1"), int32_type, column_kind::partition_key)
                .with_column(to_bytes("v2"), int32_type, column_kind::clustering_key)
                .with_column(to_bytes("p"), int32_type, column_kind::clustering_key)
                .with_column(to_bytes("c"), int32_type, column_kind::clustering_key)
                .with_view_info(schema, false, "v1 IS NOT NULL AND v2 IS NOT NULL AND p IS NOT NULL AND c IS NOT NULL");

        schema_ptr view_schema = view_builder.build();
        auto& mm = e.migration_manager().local();
        auto group0_guard = mm.start_group0_operation().get();
        auto ts = group0_guard.write_timestamp();
        mm.announce(service::prepare_new_view_announcement(mm.get_storage_proxy(), view_ptr(view_schema), ts).get(), std::move(group0_guard), "").get();

        // Verify that deleting and restoring columns behaves as expected - i.e. the row is deleted and regenerated
        cquery_nofail(e, "INSERT INTO t (p, c, v1, v2) VALUES (1, 2, 3, 4)");
        auto msg = cquery_nofail(e, "SELECT * FROM tv");
        assert_that(msg).is_rows().with_rows({
            {{int32_type->decompose(3), int32_type->decompose(4), int32_type->decompose(1), int32_type->decompose(2)}},
        });

        cquery_nofail(e, "UPDATE t SET v2 = NULL WHERE p = 1 AND c = 2");
        msg = cquery_nofail(e, "SELECT * FROM tv");
        assert_that(msg).is_rows().with_size(0);

        cquery_nofail(e, "UPDATE t SET v2 = 7 WHERE p = 1 AND c = 2");
        msg = cquery_nofail(e, "SELECT * FROM tv");
        assert_that(msg).is_rows().with_rows({
            {{int32_type->decompose(3), int32_type->decompose(7), int32_type->decompose(1), int32_type->decompose(2)}},
        });

        cquery_nofail(e, "UPDATE t SET v1 = NULL WHERE p = 1 AND c = 2");
        msg = cquery_nofail(e, "SELECT * FROM tv");
        assert_that(msg).is_rows().with_size(0);


        cquery_nofail(e, "UPDATE t SET v1 = 9 WHERE p = 1 AND c = 2");
        msg = cquery_nofail(e, "SELECT * FROM tv");
        assert_that(msg).is_rows().with_rows({
            {{int32_type->decompose(9), int32_type->decompose(7), int32_type->decompose(1), int32_type->decompose(2)}},
        });

        cquery_nofail(e, "UPDATE t SET v1 = NULL, v2 = NULL WHERE p = 1 AND c = 2");
        msg = cquery_nofail(e, "SELECT * FROM tv");
        assert_that(msg).is_rows().with_size(0);

        cquery_nofail(e, "UPDATE t SET v1 = 11, v2 = 13 WHERE p = 1 AND c = 2");
        msg = cquery_nofail(e, "SELECT * FROM tv");
        assert_that(msg).is_rows().with_rows({
            {{int32_type->decompose(11), int32_type->decompose(13), int32_type->decompose(1), int32_type->decompose(2)}},
        });

        // Reproduce issue #6008 - updates with not-previously-existing row,
        // not setting both v1 and v2 - should not create a view row, and
        // definitely not cause a crash as they did in #6008. Same for
        // deletes when no previous row exists.
        cquery_nofail(e, "DELETE FROM t WHERE p = 1 AND c = 2");
        msg = cquery_nofail(e, "SELECT * FROM tv");
        assert_that(msg).is_rows().with_size(0);
        cquery_nofail(e, "UPDATE t SET v1 = 17 WHERE p = 1 AND c = 2");
        msg = cquery_nofail(e, "SELECT * FROM tv");
        assert_that(msg).is_rows().with_size(0);

        cquery_nofail(e, "DELETE FROM t WHERE p = 1 AND c = 2");
        msg = cquery_nofail(e, "SELECT * FROM tv");
        assert_that(msg).is_rows().with_size(0);
        cquery_nofail(e, "UPDATE t SET v2 = 7 WHERE p = 1 AND c = 2");
        msg = cquery_nofail(e, "SELECT * FROM tv");
        assert_that(msg).is_rows().with_size(0);
        // Same tests as above, but with a row marker left behind, so there
        // is an existing base row - it's just empty.
        cquery_nofail(e, "INSERT INTO t (p, c, v1, v2) VALUES (1, 2, 3, 4)");
        msg = cquery_nofail(e, "SELECT * FROM tv");
        assert_that(msg).is_rows().with_rows({
            {{int32_type->decompose(3), int32_type->decompose(4), int32_type->decompose(1), int32_type->decompose(2)}},
        });
        cquery_nofail(e, "UPDATE t SET v1 = NULL, v2 = NULL WHERE p = 1 AND c = 2");
        msg = cquery_nofail(e, "SELECT * FROM tv");
        assert_that(msg).is_rows().with_size(0);
        cquery_nofail(e, "UPDATE t SET v1 = 17 WHERE p = 1 AND c = 2");
        msg = cquery_nofail(e, "SELECT * FROM tv");
        assert_that(msg).is_rows().with_size(0);

        cquery_nofail(e, "UPDATE t SET v1 = NULL, v2 = NULL WHERE p = 1 AND c = 2");
        msg = cquery_nofail(e, "SELECT * FROM tv");
        assert_that(msg).is_rows().with_size(0);
        cquery_nofail(e, "UPDATE t SET v2 = 7 WHERE p = 1 AND c = 2");
        msg = cquery_nofail(e, "SELECT * FROM tv");
        assert_that(msg).is_rows().with_size(0);
    });
}

SEASTAR_TEST_CASE(test_internal_schema_changes_on_a_distributed_table) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int primary key, v int)");
        const auto local_err = exception_predicate::message_contains("internal query");
        BOOST_REQUIRE_EXCEPTION(e.local_qp().execute_internal("alter table ks.t add col abcd", cql3::query_processor::cache_internal::yes).get(), std::logic_error, local_err);
        BOOST_REQUIRE_EXCEPTION(e.local_qp().execute_internal("create table ks.t2 (id int primary key)", cql3::query_processor::cache_internal::yes).get(), std::logic_error, local_err);
        BOOST_REQUIRE_EXCEPTION(e.local_qp().execute_internal("create index on ks.t(v)", cql3::query_processor::cache_internal::yes).get(), std::logic_error, local_err);
        BOOST_REQUIRE_EXCEPTION(e.local_qp().execute_internal("drop table ks.t", cql3::query_processor::cache_internal::yes).get(), std::logic_error, local_err);
        BOOST_REQUIRE_EXCEPTION(e.local_qp().execute_internal("drop keyspace ks", cql3::query_processor::cache_internal::yes).get(), std::logic_error, local_err);
    });
}

static future<> with_parallelized_aggregation_enabled_thread(std::function<void(cql_test_env&)>&& func) {
    auto db_cfg_ptr = make_shared<db::config>();
    auto& db_cfg = *db_cfg_ptr;
    db_cfg.enable_parallelized_aggregation({true}, db::config::config_source::CommandLine);
    return do_with_cql_env_thread(std::forward<std::function<void(cql_test_env&)>>(func), db_cfg_ptr);
}

SEASTAR_TEST_CASE(test_parallelized_select_min) {
    return with_parallelized_aggregation_enabled_thread([](cql_test_env& e) {
        auto& qp = e.local_qp();
        auto stat_parallelized = qp.get_cql_stats().select_parallelized;

        e.execute_cql("CREATE TABLE tbl (k int, PRIMARY KEY (k));").get();
        int value_count = 10;
        for (int i = 0; i < value_count; i++) {
            e.execute_cql(format("INSERT INTO tbl (k) VALUES ({:d});", i)).get();
        }
        auto msg = e.execute_cql("SELECT MIN(k) FROM tbl;").get();
        assert_that(msg).is_rows().with_rows({
            {int32_type->decompose(int32_t(0))}
        });
        BOOST_CHECK_EQUAL(stat_parallelized + 1, qp.get_cql_stats().select_parallelized);
    });
}

SEASTAR_TEST_CASE(test_parallelized_select_max) {
    return with_parallelized_aggregation_enabled_thread([](cql_test_env& e) {
        auto& qp = e.local_qp();
        auto stat_parallelized = qp.get_cql_stats().select_parallelized;

        e.execute_cql("CREATE TABLE tbl (k int, PRIMARY KEY (k));").get();
        int value_count = 10;
        for (int i = 0; i < value_count; i++) {
            e.execute_cql(format("INSERT INTO tbl (k) VALUES ({:d});", i)).get();
        }
        auto msg = e.execute_cql("SELECT MAX(k) FROM tbl;").get();
        assert_that(msg).is_rows().with_rows({
            {int32_type->decompose(int32_t(value_count - 1))}
        });

        BOOST_CHECK_EQUAL(stat_parallelized + 1, qp.get_cql_stats().select_parallelized);
    });
}

SEASTAR_TEST_CASE(test_parallelized_select_sum) {
    return with_parallelized_aggregation_enabled_thread([](cql_test_env& e) {
        auto& qp = e.local_qp();
        auto stat_parallelized = qp.get_cql_stats().select_parallelized;

        e.execute_cql("CREATE TABLE tbl (k int, PRIMARY KEY (k));").get();
        int value_count = 10;
        for (int i = 0; i < value_count; i++) {
            e.execute_cql(format("INSERT INTO tbl (k) VALUES ({:d});", i)).get();
        }
        auto msg = e.execute_cql("SELECT SUM(k) FROM tbl;").get();
        assert_that(msg).is_rows().with_rows({
            {int32_type->decompose(int32_t((value_count - 1) * value_count / 2))}
        });

        BOOST_CHECK_EQUAL(stat_parallelized + 1, qp.get_cql_stats().select_parallelized);
    });
}

SEASTAR_TEST_CASE(test_non_parallelized_multiple_select) {
    return with_parallelized_aggregation_enabled_thread([](cql_test_env& e) {
        auto& qp = e.local_qp();
        auto stat_parallelized = qp.get_cql_stats().select_parallelized;

        e.execute_cql("CREATE TABLE tbl (k int, PRIMARY KEY (k));").get();
        int value_count = 10;
        for (int i = 0; i < value_count; i++) {
            e.execute_cql(format("INSERT INTO tbl (k) VALUES ({:d});", i)).get();
        }
        auto msg = e.execute_cql("SELECT MIN(k), MAX(k) FROM tbl;").get();
        assert_that(msg).is_rows().with_rows({
            {int32_type->decompose(int32_t(0)), int32_type->decompose(int32_t(value_count - 1))}
        });

        BOOST_CHECK_EQUAL(stat_parallelized + 1, qp.get_cql_stats().select_parallelized);
    });
}

SEASTAR_TEST_CASE(test_parallelized_select_sum_group_by) {
    return with_parallelized_aggregation_enabled_thread([](cql_test_env& e) {
        auto& qp = e.local_qp();
        auto stat_parallelized = qp.get_cql_stats().select_parallelized;

        e.execute_cql("CREATE TABLE tbl (k int, c int, v int, PRIMARY KEY (k, c));").get();
        int value_count = 10;
        for (int k = 0; k < 2; k++) {
            for (int c = 0; c < value_count; c++) {
                e.execute_cql(format("INSERT INTO tbl (k, c, v) VALUES ({:d}, {:d}, {:d});", k, c, c)).get();
            }
        }
    
        auto msg = e.execute_cql("SELECT k, SUM(v) FROM tbl GROUP BY k;").get();
        assert_that(msg).is_rows().with_rows({
            {int32_type->decompose(int32_t(1)), int32_type->decompose(int32_t((value_count - 1) * value_count / 2))},
            {int32_type->decompose(int32_t(0)), int32_type->decompose(int32_t((value_count - 1) * value_count / 2))}
        });

        BOOST_CHECK_EQUAL(stat_parallelized, qp.get_cql_stats().select_parallelized);
    });
}

SEASTAR_TEST_CASE(test_parallelized_select_counter_type) {
    return with_parallelized_aggregation_enabled_thread([](cql_test_env& e) {
        auto& qp = e.local_qp();
        auto stat_parallelized = qp.get_cql_stats().select_parallelized;

        e.execute_cql("CREATE TABLE tbl (k int, c counter, PRIMARY KEY (k));").get();
        e.execute_cql("UPDATE tbl SET c = c + 4 WHERE k = 0;").get();
        e.execute_cql("UPDATE tbl SET c = c + 2 WHERE k = 1;").get();

        auto msg_sum = e.execute_cql("SELECT SUM(c) FROM tbl;").get();
        assert_that(msg_sum).is_rows().with_rows({
            {long_type->decompose(int64_t(6))}
        });
        auto msg_min = e.execute_cql("SELECT MIN(c) FROM tbl;").get();
        assert_that(msg_min).is_rows().with_rows({
            {long_type->decompose(int64_t(2))}
        });
        auto msg_max = e.execute_cql("SELECT MAX(c) FROM tbl;").get();
        assert_that(msg_max).is_rows().with_rows({
            {long_type->decompose(int64_t(4))}
        });
        auto msg_avg = e.execute_cql("SELECT AVG(c) FROM tbl;").get();
        assert_that(msg_avg).is_rows().with_rows({
            {long_type->decompose(int64_t(3))}
        });

        BOOST_CHECK_EQUAL(stat_parallelized + 4, qp.get_cql_stats().select_parallelized);
    });
}

SEASTAR_TEST_CASE(test_single_partition_aggregation_is_not_parallelized) {
    // It's pointless from performance pov to parallelize 
    // aggregation queries which reads only single partition.
    
    return with_parallelized_aggregation_enabled_thread([](cql_test_env& e) {
        auto& qp = e.local_qp();
        const auto stat_parallelized = qp.get_cql_stats().select_parallelized;

        e.execute_cql("CREATE TABLE tbl (pk int, ck int, col int, PRIMARY KEY (pk, ck));").get();
        const int value_count = 10;
        for (int pk = 0; pk < 2; pk++) {
            for (int c = 0; c < value_count; c++) {
                e.execute_cql(format("INSERT INTO tbl (pk, ck, col) VALUES ({:d}, {:d}, {:d});", pk, c, c)).get();
            }
        }
        
        const auto result1 = e.execute_cql("SELECT COUNT(*) FROM tbl WHERE pk = 1;").get();
        assert_that(result1).is_rows().with_rows({
            {long_type->decompose(int64_t(value_count))}
        });
        BOOST_CHECK_EQUAL(stat_parallelized, qp.get_cql_stats().select_parallelized);

        const auto result2 = e.execute_cql("SELECT COUNT(*) FROM tbl WHERE pk = 1 AND ck = 1;").get();
        assert_that(result2).is_rows().with_rows({
            {long_type->decompose(int64_t(1))}
        });
        BOOST_CHECK_EQUAL(stat_parallelized, qp.get_cql_stats().select_parallelized);

        const auto result3 = e.execute_cql("SELECT COUNT(*) FROM tbl WHERE token(pk) = 1;").get();
        // We don't check value of count(*) here but only if it wasn't parallelized
        BOOST_CHECK_EQUAL(stat_parallelized, qp.get_cql_stats().select_parallelized);
        
        const auto result4 = e.execute_cql("SELECT COUNT(*) FROM tbl WHERE pk = 1 AND pk = 2;").get();
        assert_that(result4).is_rows().with_rows({
            {long_type->decompose(int64_t(0))}
        });
        BOOST_CHECK_EQUAL(stat_parallelized, qp.get_cql_stats().select_parallelized);


        e.execute_cql("CREATE TABLE tbl2 (pk1 int, pk2 int, ck int, col int, PRIMARY KEY((pk1, pk2), ck));").get();
        for (int pk1 = 0; pk1 < 2; pk1++) {
            for (int pk2 = 0; pk2 < 2; pk2++) {
                for (int c = 0; c < value_count; c++) {
                    e.execute_cql(format("INSERT INTO tbl2 (pk1, pk2, ck, col) VALUES ({:d}, {:d}, {:d}, {:d});", pk1, pk2, c, c)).get();
                }
            }
        }
        
        const auto result_pk12 = e.execute_cql("SELECT COUNT(*) FROM tbl2 WHERE pk1 = 1 AND pk2 = 0;").get();
        assert_that(result_pk12).is_rows().with_rows({
            {long_type->decompose(int64_t(value_count))}
        });
        BOOST_CHECK_EQUAL(stat_parallelized, qp.get_cql_stats().select_parallelized);

        // Query with only partly restricted partition key requires `ALLOW FILTERING` clause
        // and we doesn't parallelize queries which need filtering.
        // See issue #19369.
        const auto result_pk1 = e.execute_cql("SELECT COUNT(*) FROM tbl2 WHERE pk1 = 1 ALLOW FILTERING;").get();
        // This query contains also column for pk1
        assert_that(result_pk1).is_rows().with_rows({
            {long_type->decompose(int64_t(value_count * 2)), int32_type->decompose(int32_t(1))}
        });
        BOOST_CHECK_EQUAL(stat_parallelized, qp.get_cql_stats().select_parallelized);
    });
}

static future<> with_udf_and_parallel_aggregation_enabled_thread(std::function<void(cql_test_env&)>&& func) {
    auto db_cfg_ptr = make_shared<db::config>();
    auto& db_cfg = *db_cfg_ptr;
    db_cfg.enable_user_defined_functions({true}, db::config::config_source::CommandLine);
    db_cfg.user_defined_function_time_limit_ms(1000);
    db_cfg.experimental_features({db::experimental_features_t::feature::UDF}, db::config::config_source::CommandLine);
    db_cfg.enable_parallelized_aggregation({true}, db::config::config_source::CommandLine);
    return do_with_cql_env_thread(std::forward<std::function<void(cql_test_env&)>>(func), db_cfg_ptr);
}

SEASTAR_TEST_CASE(test_parallelized_select_uda) {
    return with_udf_and_parallel_aggregation_enabled_thread([](cql_test_env& e) {
        auto& qp = e.local_qp();
        auto stat_parallelized = qp.get_cql_stats().select_parallelized;

        e.execute_cql("CREATE FUNCTION row_fct(acc bigint, val int) "
                        "RETURNS NULL ON NULL INPUT "
                        "RETURNS bigint "
                        "LANGUAGE lua "
                        "AS $$ "
                        "return acc+val "
                        "$$;").get();
        e.execute_cql("CREATE FUNCTION reduce_fct(acc1 bigint, acc2 bigint) "
                        "RETURNS NULL ON NULL INPUT "
                        "RETURNS bigint "
                        "LANGUAGE lua "
                        "AS $$ "
                        "return acc1+acc2 "
                        "$$;").get();
        e.execute_cql("CREATE FUNCTION final_fct(acc bigint) "
                        "RETURNS NULL ON NULL INPUT "
                        "RETURNS bigint "
                        "LANGUAGE lua "
                        "AS $$ "
                        "return -acc "
                        "$$;").get();
        e.execute_cql("CREATE AGGREGATE aggr(int) "
                        "SFUNC row_fct "
                        "STYPE bigint "
                        "REDUCEFUNC reduce_fct "
                        "FINALFUNC final_fct "
                        "INITCOND 0;").get();
        e.execute_cql("CREATE TABLE tbl (k int, PRIMARY KEY (k));").get();
        int value_count = 10;
        for (int i = 0; i < value_count; i++) {
            e.execute_cql(format("INSERT INTO tbl (k) VALUES ({:d});", i)).get();
        }
        auto msg = e.execute_cql("SELECT aggr(k) FROM tbl;").get();
        assert_that(msg).is_rows().with_rows({
            {long_type->decompose(-int64_t((value_count - 1) * value_count / 2))}
        });

        BOOST_CHECK_EQUAL(stat_parallelized + 1, qp.get_cql_stats().select_parallelized);
    });
}

SEASTAR_TEST_CASE(test_not_parallelized_select_uda) {
    return with_udf_and_parallel_aggregation_enabled_thread([](cql_test_env& e) {
        auto& qp = e.local_qp();
        auto stat_parallelized = qp.get_cql_stats().select_parallelized;

        e.execute_cql("CREATE FUNCTION row_fct(acc bigint, val int) "
                        "RETURNS NULL ON NULL INPUT "
                        "RETURNS bigint "
                        "LANGUAGE lua "
                        "AS $$ "
                        "return acc+val "
                        "$$;").get();
        e.execute_cql("CREATE FUNCTION final_fct(acc bigint) "
                        "RETURNS NULL ON NULL INPUT "
                        "RETURNS bigint "
                        "LANGUAGE lua "
                        "AS $$ "
                        "return -acc "
                        "$$;").get();
        e.execute_cql("CREATE AGGREGATE aggr(int) "
                        "SFUNC row_fct "
                        "STYPE bigint "
                        "FINALFUNC final_fct "
                        "INITCOND 0;").get();
        
        e.execute_cql("CREATE TABLE tbl (k int, PRIMARY KEY (k));").get();
        int value_count = 10;
        for (int i = 0; i < value_count; i++) {
            e.execute_cql(format("INSERT INTO tbl (k) VALUES ({:d});", i)).get();
        }
        auto msg = e.execute_cql("SELECT aggr(k) FROM tbl;").get();
        assert_that(msg).is_rows().with_rows({
            {long_type->decompose(-int64_t((value_count - 1) * value_count / 2))}
        });

        BOOST_CHECK_EQUAL(stat_parallelized, qp.get_cql_stats().select_parallelized);
    });
}

cql3::raw_value make_collection_raw_value(size_t size_to_write, const std::vector<cql3::raw_value>& elements_to_write) {
    size_t serialized_len = 0;
    serialized_len += collection_size_len();
    for (const cql3::raw_value& val : elements_to_write) {
        serialized_len += collection_value_len();
        if (val.is_value()) {
            serialized_len += val.view().with_value([](const FragmentedView auto& view) {
                return view.size_bytes();
            });
        }
    }

    bytes b(bytes::initialized_later(), serialized_len);
    bytes::iterator out = b.begin();

    write_collection_size(out, size_to_write);
    for (const cql3::raw_value& val : elements_to_write) {
        if (val.is_null()) {
                write_int32(out, -1);
        } else {
            val.view().with_value([&](const FragmentedView auto& val_view) {
                write_collection_value(out, linearized(val_view));
            });
        }
    }

    return cql3::raw_value::make_value(b);
}

SEASTAR_TEST_CASE(test_null_and_unset_in_collections) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("CREATE TABLE null_in_col (p int primary key, l list<int>, s set<int>, m map<int, int>);").get();

        // The predicate that checks the message has to be a lambda to preserve source_location
        auto check_null_msg = [](std::source_location loc = std::source_location::current()) {
            return exception_predicate::message_matches(".*(null|NULL).*", loc);
        };

        auto check_unset_msg = [](std::source_location loc = std::source_location::current()) {
            return exception_predicate::message_contains("unset", loc);
        };

        // Test null when specified inside a collection literal
        // It's impossible to specify unset value this way
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("INSERT INTO null_in_col (p, l) VALUES (0, [1, null, 3])").get(),
                                exceptions::invalid_request_exception, check_null_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("INSERT INTO null_in_col (p, s) VALUES (0, {1, null, 3})").get(),
                                exceptions::invalid_request_exception, check_null_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("INSERT INTO null_in_col (p, m) VALUES (0, {0:1, null:3, 4:5})").get(),
                                exceptions::invalid_request_exception, check_null_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("INSERT INTO null_in_col (p, m) VALUES (0, {0:1, 2:null, 4:5})").get(),
                                exceptions::invalid_request_exception, check_null_msg());


        // Test null and unset when sent as bind marker for collection value
        auto insert_list_with_marker = e.prepare("INSERT INTO null_in_col (p, l) VALUES (0, [1, ?, 3])").get();
        auto insert_set_with_marker = e.prepare("INSERT INTO null_in_col (p, s) VALUES (0, {1, ?, 3})").get();
        auto insert_map_with_key_marker = e.prepare("INSERT INTO null_in_col (p, m) VALUES (0, {0:1, ?:3, 4:5})").get();
        auto insert_map_with_value_marker = e.prepare("INSERT INTO null_in_col (p, m) VALUES (0, {0:1, 2:?, 4:5})").get();

        cql3::raw_value null_value = cql3::raw_value::make_null();

        BOOST_REQUIRE_EXCEPTION(e.execute_prepared(insert_list_with_marker, {null_value}).get(),
                                exceptions::invalid_request_exception, check_null_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_prepared(insert_set_with_marker, {null_value}).get(),
                                exceptions::invalid_request_exception, check_null_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_prepared(insert_map_with_key_marker, {null_value}).get(),
                                exceptions::invalid_request_exception, check_null_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_prepared(insert_map_with_value_marker, {null_value}).get(),
                                exceptions::invalid_request_exception, check_null_msg());

        auto bind_variable_list_with_unset = cql3::raw_value_vector_with_unset({null_value}, {true});

        BOOST_REQUIRE_EXCEPTION(e.execute_prepared(insert_list_with_marker, bind_variable_list_with_unset).get(),
                                exceptions::invalid_request_exception, check_unset_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_prepared(insert_set_with_marker, bind_variable_list_with_unset).get(),
                                exceptions::invalid_request_exception, check_unset_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_prepared(insert_map_with_key_marker, bind_variable_list_with_unset).get(),
                                exceptions::invalid_request_exception, check_unset_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_prepared(insert_map_with_value_marker, bind_variable_list_with_unset).get(),
                                exceptions::invalid_request_exception, check_unset_msg());


        // Test sending whole collections with null and unset inside as bound value
        auto insert_list = e.prepare("INSERT INTO null_in_col (p, l) VALUES (0, ?)").get();
        auto insert_set = e.prepare("INSERT INTO null_in_col (p, s) VALUES (0, ?)").get();
        auto insert_map = e.prepare("INSERT INTO null_in_col (p, m) VALUES (0, ?)").get();

        auto make_int = [](int val) -> cql3::raw_value {
            return cql3::raw_value::make_value(int32_type->decompose(val));
        };

        cql3::raw_value list_with_null = make_collection_raw_value(3, {make_int(1), null_value, make_int(2)});
        cql3::raw_value set_with_null = make_collection_raw_value(3, {make_int(1), null_value, make_int(2)});

        cql3::raw_value map_with_null_key = make_collection_raw_value(3, {make_int(0), make_int(1),
                                                                     null_value, make_int(3),
                                                                     make_int(4), make_int(5)});

        cql3::raw_value map_with_null_value = make_collection_raw_value(3, {make_int(0), make_int(1),
                                                                       make_int(2), null_value,
                                                                       make_int(4), make_int(5)});

        BOOST_REQUIRE_EXCEPTION(e.execute_prepared(insert_list, {list_with_null}).get(),
                                exceptions::invalid_request_exception, check_null_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_prepared(insert_set, {set_with_null}).get(),
                                exceptions::invalid_request_exception, check_null_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_prepared(insert_map, {map_with_null_key}).get(),
                                exceptions::invalid_request_exception, check_null_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_prepared(insert_map, {map_with_null_value}).get(),
                                exceptions::invalid_request_exception, check_null_msg());


        // Update setting to bad collection value
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("UPDATE null_in_col SET l = [1, null, 2] WHERE p = 0").get(),
                                exceptions::invalid_request_exception, check_null_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("UPDATE null_in_col SET s = {1, null, 2} WHERE p = 0").get(),
                                exceptions::invalid_request_exception, check_null_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("UPDATE null_in_col SET m = {0:1, null:3, 4:5} WHERE p = 0").get(),
                                exceptions::invalid_request_exception, check_null_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("UPDATE null_in_col SET m = {0:1, 2:null, 4:5} WHERE p = 0").get(),
                                exceptions::invalid_request_exception, check_null_msg());

        // Update adding a bad single-element collection value
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("UPDATE null_in_col SET l = l + [null] WHERE p = 0").get(),
                                exceptions::invalid_request_exception, check_null_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("UPDATE null_in_col SET s = s + {null} WHERE p = 0").get(),
                                exceptions::invalid_request_exception, check_null_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("UPDATE null_in_col SET m = m + {null:3} WHERE p = 0").get(),
                                exceptions::invalid_request_exception, check_null_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("UPDATE null_in_col SET m = m + {2:null} WHERE p = 0").get(),
                                exceptions::invalid_request_exception, check_null_msg());

        // Update adding a bad collection value
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("UPDATE null_in_col SET l = l + [1, null, 2] WHERE p = 0").get(),
                                exceptions::invalid_request_exception, check_null_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("UPDATE null_in_col SET s = s + {1, null, 2} WHERE p = 0").get(),
                                exceptions::invalid_request_exception, check_null_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("UPDATE null_in_col SET m = m + {0:1, null:3, 4:5} WHERE p = 0").get(),
                                exceptions::invalid_request_exception, check_null_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("UPDATE null_in_col SET m = m + {0:1, 2:null, 4:5} WHERE p = 0").get(),
                                exceptions::invalid_request_exception, check_null_msg());

        // Update adding a collection value with bad bind marker
        auto add_list_with_marker = e.prepare("UPDATE null_in_col SET l = l + [1, ?, 2] WHERE p = 0").get();
        auto add_set_with_marker = e.prepare("UPDATE null_in_col SET s = s + {1, ?, 2} WHERE p = 0").get();
        auto add_map_with_key_marker = e.prepare("UPDATE null_in_col SET m = m + {0:1, ?:3, 4:5} WHERE p = 0").get();
        auto add_map_with_value_marker = e.prepare("UPDATE null_in_col SET m = m + {0:1, 2:?, 4:5} WHERE p = 0").get();

        BOOST_REQUIRE_EXCEPTION(e.execute_prepared(add_list_with_marker, {null_value}).get(),
                                exceptions::invalid_request_exception, check_null_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_prepared(add_set_with_marker, {null_value}).get(),
                                exceptions::invalid_request_exception, check_null_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_prepared(add_map_with_key_marker, {null_value}).get(),
                                exceptions::invalid_request_exception, check_null_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_prepared(add_map_with_value_marker, {null_value}).get(),
                                exceptions::invalid_request_exception, check_null_msg());

        BOOST_REQUIRE_EXCEPTION(e.execute_prepared(add_list_with_marker, bind_variable_list_with_unset).get(),
                                exceptions::invalid_request_exception, check_unset_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_prepared(add_set_with_marker, bind_variable_list_with_unset).get(),
                                exceptions::invalid_request_exception, check_unset_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_prepared(add_map_with_key_marker, bind_variable_list_with_unset).get(),
                                exceptions::invalid_request_exception, check_unset_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_prepared(add_map_with_value_marker, bind_variable_list_with_unset).get(),
                                exceptions::invalid_request_exception, check_unset_msg());

        // Update adding a collection value with bad bind marker
        auto add_list = e.prepare("UPDATE null_in_col SET l = l + ? WHERE p = 0").get();
        auto add_set = e.prepare("UPDATE null_in_col SET s = s + ? WHERE p = 0").get();
        auto add_map = e.prepare("UPDATE null_in_col SET m = m + ? WHERE p = 0").get();

        BOOST_REQUIRE_EXCEPTION(e.execute_prepared(add_list, {list_with_null}).get(),
                                exceptions::invalid_request_exception, check_null_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_prepared(add_set, {set_with_null}).get(),
                                exceptions::invalid_request_exception, check_null_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_prepared(add_map, {map_with_null_key}).get(),
                                exceptions::invalid_request_exception, check_null_msg());
        BOOST_REQUIRE_EXCEPTION(e.execute_prepared(add_map, {map_with_null_value}).get(),
                                exceptions::invalid_request_exception, check_null_msg());

        // List of IN values can contain NULL (which doesn't match anything)
        auto msg1 = e.execute_cql("SELECT * FROM null_in_col WHERE p IN (1, null, 2)").get();
        assert_that(msg1).is_rows().with_rows({});

        auto where_in_list_with_marker = e.prepare("SELECT * FROM null_in_col WHERE p IN (1, ?, 2)").get();

        auto msg2 = e.execute_prepared(where_in_list_with_marker, {null_value}).get();
        assert_that(msg2).is_rows().with_rows({});

        BOOST_REQUIRE_EXCEPTION(e.execute_prepared(where_in_list_with_marker, bind_variable_list_with_unset).get(),
                                exceptions::invalid_request_exception, check_unset_msg());

        auto where_in_list_marker = e.prepare("SELECT * FROM null_in_col WHERE p IN ?").get();

        auto msg = e.execute_prepared(where_in_list_marker, {list_with_null}).get();
        assert_that(msg).is_rows().with_rows({});
    });
}

SEASTAR_TEST_CASE(test_bind_variable_type_checking) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("CREATE TABLE tab1 (p int primary key, a int, b text, c int)").get();

        // The predicate that checks the message has to be a lambda to preserve source_location
        auto check_type_conflict = [](std::source_location loc = std::source_location::current()) {
            return exception_predicate::message_contains("variable :var has type", loc);
        };

        // Test :var needing to have two conflicting types
        BOOST_REQUIRE_EXCEPTION(e.prepare("INSERT INTO tab1 (p, a, b) VALUES (0, :var, :var)").get(),
                                exceptions::invalid_request_exception, check_type_conflict());
        BOOST_REQUIRE_EXCEPTION(e.prepare("SELECT * FROM tab1 WHERE a = :var AND b = :var ALLOW FILTERING").get(),
                                exceptions::invalid_request_exception, check_type_conflict());

        // Test :var with a compatible type
        e.prepare("INSERT INTO tab1 (p, a, c) VALUES (0, :var, :var)").get();
        e.prepare("SELECT * FROM tab1 WHERE a = :var AND c = :var ALLOW FILTERING").get();
    });
}

SEASTAR_TEST_CASE(test_bind_variable_type_checking_disabled) {
    auto db_config = make_shared<db::config>();
    db_config->cql_duplicate_bind_variable_names_refer_to_same_variable(false);
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("CREATE TABLE tab1 (p int primary key, a int, b text, c int)").get();

        // Test :var needing to have two conflicting types; will fail without
        // cql_duplicate_bind_variable_names_refer_to_same_variable = false
        auto prepared = e.prepare("INSERT INTO tab1 (p, a, b) VALUES (0, :var, :var)").get();

        // Verify that the parameters passed positionally work (non-positional won't make sense)
        auto a = int32_type->decompose(1);
        auto b = utf8_type->decompose("abc");
        e.execute_prepared(prepared, {cql3::raw_value::make_value(a), cql3::raw_value::make_value(b)}).get();

        auto msg = e.execute_cql("SELECT a, b FROM tab1 WHERE p = 0").get();
        assert_that(msg).is_rows().with_rows({{a, b}});
    }, cql_test_config{db_config});
}

static sstring prepared_variable_names(cql_test_env& e, const sstring& query) {
    const auto prepared = e.local_qp().get_prepared(e.prepare(query).get());
    BOOST_REQUIRE(prepared);
    sstring names;
    for (const auto& spec : prepared->bound_names) {
        if (!names.empty()) {
            names += ", ";
        }
        names += spec->name->text();
    }
    return names;
}

SEASTAR_TEST_CASE(test_in_bind_variable_name) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("CREATE TABLE tab (p int, c int, v int, PRIMARY KEY (p, c))").get();

        BOOST_REQUIRE_EQUAL(prepared_variable_names(e, "SELECT * FROM tab WHERE p = ? AND c IN ?"), "p, IN(c)");
        // The IF condition of an LWT statement is prepared as an expression rather than
        // as a restriction, so the name is reached along a different path.
        BOOST_REQUIRE_EQUAL(prepared_variable_names(e, "UPDATE tab SET v = 1 WHERE p = 0 AND c = 0 IF v IN ?"), "IN(v)");
    });
}

SEASTAR_TEST_CASE(test_in_bind_variable_name_lowercase_operator) {
    auto db_config = make_shared<db::config>();
    db_config->cql_in_bind_variable_name_uses_uppercase_operator(false);
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("CREATE TABLE tab (p int, c int, v int, PRIMARY KEY (p, c))").get();

        BOOST_REQUIRE_EQUAL(prepared_variable_names(e, "SELECT * FROM tab WHERE p = ? AND c IN ?"), "p, in(c)");
        BOOST_REQUIRE_EQUAL(prepared_variable_names(e, "UPDATE tab SET v = 1 WHERE p = 0 AND c = 0 IF v IN ?"), "in(v)");
    }, cql_test_config{db_config});
}

SEASTAR_TEST_CASE(test_setting_synchronous_updates_property) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("create table base (k int, v int, primary key (k));").get();

        // Check if setting synchronous_updates property works with CREATE
        // MATERIALIZED VIEW and ALTER MATERIALIZED VIEW statements.
        e.execute_cql("create materialized view mv as select * from base "
                       "where k is not null and v is not null primary key (v, k)"
                       "with synchronous_updates = true").get();
        e.execute_cql("alter materialized view mv with synchronous_updates = true").get();
        e.execute_cql("alter materialized view mv with synchronous_updates = false").get();

        // Check if index can be altered
        e.execute_cql("create index on base (v)").get();
        e.execute_cql("alter materialized view base_v_idx_index with synchronous_updates = true").get();

        // Setting synchronous_updates in CREATE TABLE or ALTER TABLE is
        // invalid
        BOOST_REQUIRE_THROW(
            e.execute_cql(
                "create table t (k int, v int, primary key (k)) with synchronous_updates = true"
            ).get(),
            exceptions::invalid_request_exception
        );
        BOOST_REQUIRE_THROW(
            e.execute_cql("alter table base with synchronous_updates = true").get(),
            exceptions::invalid_request_exception
        );
    });
}

static
cql_test_config tablet_cql_test_config() {
    cql_test_config c;
    c.db_config->tablets_mode_for_new_keyspaces.set(db::tablets_mode_t::mode::enabled);
    return c;
}

static
bool has_tablet_routing(::shared_ptr<cql_transport::messages::result_message> result) {
    auto custom_payload = result->custom_payload();
    if (!custom_payload.has_value() || custom_payload->find("tablets-routing-v1") == custom_payload->end()) {
        return false;
    }
    return true;
}

SEASTAR_TEST_CASE(test_sending_tablet_info_unprepared_insert) {
    BOOST_ASSERT(this_smp_shard_count() == 2);
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("create keyspace ks_tablet with replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1 } and tablets = {'initial': 8};").get();
        e.execute_cql("create table ks_tablet.test_tablet (pk int, ck int, v int, PRIMARY KEY (pk, ck));").get();

        smp::submit_to(0, [&] {
            return seastar::async([&] {
                auto result = e.execute_cql("insert into ks_tablet.test_tablet (pk, ck, v) VALUES (1, 2, 3);").get();
                BOOST_ASSERT(!has_tablet_routing(result));
            });
        }).get();

        smp::submit_to(1, [&] {
            return seastar::async([&] {
                auto result = e.execute_cql("insert into ks_tablet.test_tablet (pk, ck, v) VALUES (1, 2, 3);").get();
                BOOST_ASSERT(!has_tablet_routing(result));
            });
        }).get();
    }, tablet_cql_test_config());
}

SEASTAR_TEST_CASE(test_sending_tablet_info_unprepared_select) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("create keyspace ks_tablet with replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1 } and tablets = {'initial': 8};").get();
        e.execute_cql("create table ks_tablet.test_tablet (pk int, ck int, v int, PRIMARY KEY (pk, ck));").get();
        e.execute_cql("insert into ks_tablet.test_tablet (pk, ck, v) VALUES (1, 2, 3);").get();

        smp::submit_to(0, [&] {
            return seastar::async([&] {
                auto result = e.execute_cql("select pk, ck, v FROM ks_tablet.test_tablet WHERE pk = 1;").get();
                BOOST_ASSERT(!has_tablet_routing(result));
            });
        }).get();

        smp::submit_to(1, [&] {
            return seastar::async([&] {
                auto result = e.execute_cql("select pk, ck, v FROM ks_tablet.test_tablet WHERE pk = 1;").get();
                BOOST_ASSERT(!has_tablet_routing(result));
            });
        }).get();
    }, tablet_cql_test_config());
}

// Reproduces #20768
SEASTAR_TEST_CASE(test_alter_keyspace_updates_in_memory_objects_with_data_from_system_schema_scylla_keyspaces) {
        return do_with_cql_env_thread([] (cql_test_env& e) {
            e.execute_cql("create keyspace ks_tablet with replication = { 'class': 'NetworkTopologyStrategy', "
                          "'replication_factor': 1 } and tablets = { 'initial': 1 }").get();
            e.execute_cql("alter keyspace ks_tablet with tablets = { 'initial': 2 }").get();
            e.execute_cql("alter keyspace ks_tablet with tablets = { 'initial': 3 }").get();
            auto& ks = e.local_db().find_keyspace("ks_tablet");
            BOOST_REQUIRE(ks.metadata()->initial_tablets() == 3);
        }, tablet_cql_test_config());
}

SEASTAR_TEST_CASE(test_sending_tablet_info_insert) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("create keyspace ks_tablet with replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1 } and tablets = {'initial': 8};").get();
        e.execute_cql("create table ks_tablet.test_tablet (pk int, ck int, v int, PRIMARY KEY (pk, ck));").get();
        auto insert = e.prepare("insert into ks_tablet.test_tablet (pk, ck, v) VALUES (?, ?, ?);").get();
        
        std::vector<cql3::raw_value> raw_values;
        raw_values.emplace_back(cql3::raw_value::make_value(int32_type->decompose(int32_t{1})));
        raw_values.emplace_back(cql3::raw_value::make_value(int32_type->decompose(int32_t{2})));
        raw_values.emplace_back(cql3::raw_value::make_value(int32_type->decompose(int32_t{3})));

        const auto sptr = e.local_db().find_schema("ks_tablet", "test_tablet");

        auto pk = partition_key::from_singular(*sptr, int32_t(1));

        unsigned local_shard = sptr->table().shard_for_reads(dht::get_token(*sptr, pk.view()));

        smp::submit_to(local_shard, [&] {
            return seastar::async([&] { 
                auto result = e.execute_prepared(insert, raw_values).get();
                BOOST_ASSERT(!has_tablet_routing(result));
            });
        }).get();

        std::vector<cql3::raw_value> raw_values2;
        raw_values2.emplace_back(cql3::raw_value::make_value(int32_type->decompose(int32_t{2})));
        raw_values2.emplace_back(cql3::raw_value::make_value(int32_type->decompose(int32_t{3})));
        raw_values2.emplace_back(cql3::raw_value::make_value(int32_type->decompose(int32_t{4})));

        auto pk2 = partition_key::from_singular(*sptr, int32_t(2));

        unsigned local_shard2 = sptr->table().shard_for_reads(dht::get_token(*sptr, pk2.view()));
        unsigned foreign_shard = (local_shard2 + 1) % this_smp_shard_count();

        smp::submit_to(foreign_shard, [&] { 
            return seastar::async([&] {
                auto result = e.execute_prepared(insert, raw_values2).get();
                BOOST_ASSERT(has_tablet_routing(result));
            });
        }).get();
    }, tablet_cql_test_config());
}

SEASTAR_TEST_CASE(test_sending_tablet_info_select) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("create keyspace ks_tablet with replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} and tablets = {'initial': 8};").get();
        e.execute_cql("create table ks_tablet.test_tablet (pk int, ck int, v int, PRIMARY KEY (pk, ck));").get();
        e.execute_cql("insert into ks_tablet.test_tablet (pk, ck, v) VALUES (1, 2, 3);").get();
        
        auto select = e.prepare("select pk, ck, v FROM ks_tablet.test_tablet WHERE pk = ?;").get();
        std::vector<cql3::raw_value> raw_values;
        raw_values.emplace_back(cql3::raw_value::make_value(int32_type->decompose(int32_t{1})));

        const auto sptr = e.local_db().find_schema("ks_tablet", "test_tablet");

        auto pk = partition_key::from_singular(*sptr, int32_t(1));

        unsigned local_shard = sptr->table().shard_for_reads(dht::get_token(*sptr, pk.view()));
        unsigned foreign_shard = (local_shard + 1) % this_smp_shard_count();

        smp::submit_to(local_shard, [&] { 
            return seastar::async([&] {
                auto result = e.execute_prepared(select, raw_values).get();
                BOOST_ASSERT(!has_tablet_routing(result));
            });
        }).get();

        smp::submit_to(foreign_shard, [&] { 
            return seastar::async([&] {
                auto result = e.execute_prepared(select, raw_values).get();
                BOOST_ASSERT(has_tablet_routing(result));
            });
        }).get();
    }, tablet_cql_test_config());
}

// Regression test for scylladb/scylladb#29874:
// After an internal CAS shard bounce, TABLETS_ROUTING_V1 payload must still
// be returned when the client originally routed to the wrong shard.
//
// The test simulates what the transport layer does during a CAS shard bounce:
// 1. The request arrives on shard X (the "foreign" shard, not owning the tablet).
// 2. The LWT statement detects the wrong shard and returns a bounce to shard Y
//    (the tablet shard).
// 3. The transport layer transfers client_state from X to Y via
//    client_state_for_another_shard, preserving _original_shard = X.
// 4. The statement re-executes on shard Y. check_locality() compares against
//    _original_shard (X, wrong) rather than this_shard_id() (Y, correct),
//    so it returns tablet routing info.
//
// We arrange the test so that the test thread's shard is the "foreign" shard
// (not the tablet shard). This way local_client_state() belongs to the current
// shard, and move_to_other_shard() is called on the owning shard.
SEASTAR_TEST_CASE(test_tablet_routing_info_after_cas_shard_bounce) {
    BOOST_REQUIRE_GT(this_smp_shard_count(), 1u);
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("create keyspace ks_tablet with replication = "
            "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
            "and tablets = {'initial': 1};").get();

        // Create dummy tables until the next table's single tablet lands on
        // a shard other than ours. Each dummy table occupies one shard in the
        // load balancer, so after at most this_smp_shard_count() dummies shard 0 (or
        // whichever shard we're on) is no longer the least loaded.
        schema_ptr schema;
        unsigned tablet_shard;
        for (unsigned i = 0; ; ++i) {
            BOOST_REQUIRE_MESSAGE(i <= this_smp_shard_count(), "Could not place tablet on a foreign shard");
            auto tbl = format("tbl_{}", i);
            e.execute_cql(format("create table ks_tablet.{} (pk int PRIMARY KEY, v int);", tbl)).get();
            schema = e.local_db().find_schema("ks_tablet", tbl);
            auto pk = partition_key::from_singular(*schema, int32_t(1));
            tablet_shard = schema->table().shard_for_reads(dht::get_token(*schema, pk.view()));
            if (tablet_shard != this_shard_id()) {
                break;
            }
        }

        e.execute_cql(format("insert into ks_tablet.{} (pk, v) VALUES (1, 1);", schema->cf_name())).get();
        const auto lwt_id = e.prepare(format("update ks_tablet.{} set v = ? where pk = ? if v = ?;", schema->cf_name())).get();

        // Execute LWT on this shard (the foreign shard). Expect a bounce.
        {
            auto raw_val = [] (int32_t v) { return cql3::raw_value::make_value(int32_type->decompose(v)); };
            const auto result = e.execute_prepared(lwt_id, {raw_val(2), raw_val(1), raw_val(1)}).get();
            BOOST_REQUIRE(result->as_bounce());
            BOOST_REQUIRE_EQUAL(result->as_bounce()->target_shard(), tablet_shard);
        }

        // Simulate the transport-layer bounce: transfer client_state from
        // this shard (foreign, owning the client_state) to the tablet shard.
        // client_state_for_another_shard preserves _original_shard = this shard.
        const auto gcs = e.local_client_state().move_to_other_shard();

        smp::submit_to(tablet_shard, [&] {
            return seastar::async([&] {
                auto cs = gcs.get();
                auto qs = ::make_shared<service::query_state>(cs, empty_service_permit());
                const auto prepared = e.local_qp().get_prepared(lwt_id);
                BOOST_REQUIRE(prepared);

                const auto options = e.local_qp().make_internal_options(prepared, 
                    {data_value(2), data_value(1), data_value(1)},
                    db::consistency_level::ONE);

                auto res = e.local_qp().execute_prepared_without_checking_exception_message(
                    *qs, prepared->statement, options,
                    std::move(prepared), lwt_id, false).get();
                res = cql_transport::messages::propagate_exception_as_future(
                    std::move(res)).get();

                BOOST_REQUIRE(has_tablet_routing(res));
            });
        }).get();
    }, [] {
        auto cfg = tablet_cql_test_config();
        cfg.need_remote_proxy = true;
        return cfg;
    }());
}

// check if create statements emit schema change event properly
// we emit it even if resource wasn't created due to github.com/scylladb/scylladb/issues/16909
SEASTAR_TEST_CASE(test_schema_change_events) {
     return do_with_cql_env_thread([] (cql_test_env& e) {
        using event_t = cql_transport::messages::result_message::schema_change;
        // keyspace
        auto res = e.execute_cql("create keyspace ks2 with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };").get();
        BOOST_REQUIRE(dynamic_pointer_cast<event_t>(res));
        res = e.execute_cql("create keyspace if not exists ks2 with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };").get();
        BOOST_REQUIRE(dynamic_pointer_cast<event_t>(res));

        // table
        res = e.execute_cql("create table users (user_name varchar PRIMARY KEY);").get();
        BOOST_REQUIRE(dynamic_pointer_cast<event_t>(res));
        res = e.execute_cql("create table if not exists users (user_name varchar PRIMARY KEY);").get();
        BOOST_REQUIRE(dynamic_pointer_cast<event_t>(res));

        // view
        res = e.execute_cql("create materialized view users_view as select user_name from users where user_name is not null primary key (user_name)").get();
        BOOST_REQUIRE(dynamic_pointer_cast<event_t>(res));
        res = e.execute_cql("create materialized view if not exists users_view as select user_name from users where user_name is not null primary key (user_name)").get();
        BOOST_REQUIRE(dynamic_pointer_cast<event_t>(res));

        // type
        res = e.execute_cql("create type my_type (first text);").get();
        BOOST_REQUIRE(dynamic_pointer_cast<event_t>(res));
        res = e.execute_cql("create type if not exists my_type (first text);").get();
        BOOST_REQUIRE(dynamic_pointer_cast<event_t>(res));
     });
}

// check that we can load sstable with mixed numerical and uuid generation types
SEASTAR_TEST_CASE(test_sstable_load_mixed_generation_type) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        // Create table
        e.execute_cql("create table ks.test (k int PRIMARY KEY, v int);").get();

        auto& tbl = e.local_db().find_column_family("ks", "test");
        auto upload_dir = table_dir(tbl) / sstables::upload_dir;

        // Load sstables with mixed generation types
        copy_directory("test/resource/sstables/mixed_generation_type", upload_dir);
        replica::distributed_loader::process_upload_dir(e.db(), e.view_builder(), e.view_building_worker(), "ks", "test", false, false).get();

        // Verify the expected data is present
        assert_that(e.execute_cql("SELECT * FROM ks.test").get()).is_rows()
            .with_size(3)
            .with_rows_ignore_order({
                {int32_type->decompose(0), int32_type->decompose(0)},
                {int32_type->decompose(1), int32_type->decompose(1)},
                {int32_type->decompose(2), int32_type->decompose(2)}
            });

        // Run major compaction to ensure that the mixed generation types are handled correctly
        auto& compaction_module = e.local_db().get_compaction_manager().get_task_manager_module();
        std::vector<table_info> table_infos({{"test", tbl.schema()->id()}});
        auto task = compaction_module.make_and_start_task<compaction::major_keyspace_compaction_task_impl>(tasks::make_empty_task_info(), "ks", tasks::task_id::create_null_id(), e.db(), table_infos, compaction::flush_mode::skip, false).get();
        task->done().get();

        // Verify the expected data again
        assert_that(e.execute_cql("SELECT * FROM ks.test").get()).is_rows()
            .with_size(3)
            .with_rows_ignore_order({
                {int32_type->decompose(0), int32_type->decompose(0)},
                {int32_type->decompose(1), int32_type->decompose(1)},
                {int32_type->decompose(2), int32_type->decompose(2)}
            });
    });
}

static
cql_test_config tablet_v2_cql_test_config() {
    auto c = tablet_cql_test_config();
    c.db_config->experimental_features(
        {db::experimental_features_t::feature::STRONGLY_CONSISTENT_TABLES},
        db::config::config_source::CommandLine
    );
    return c;
}

static
bool has_tablets_routing_v2(::shared_ptr<cql_transport::messages::result_message> result) {
    auto custom_payload = result->custom_payload();
    return custom_payload.has_value() && custom_payload->contains("tablets-routing-v2");
}

static
locator::tablet_version_block extract_tablet_version_block(locator::tablet_version hash, uint8_t block_idx) {
    uint64_t hash_value = hash.value();
    hash_value >>= (block_idx * 4);
    uint8_t block_index = block_idx << 4;
    uint8_t block_value = static_cast<uint8_t>(hash_value & 0x0F);
    return locator::tablet_version_block{block_index | block_value};
}

static
locator::tablet_version get_eventually_consistent_tablet_version(
        const locator::effective_replication_map& erm,
        const dht::token& token)
{
    const locator::tablet_version_block blocks[2] = {
        locator::tablet_version_block{0x00}, locator::tablet_version_block{0x01}
    };
    for (auto block : blocks) {
        auto result = erm.check_tablet_version(token, block);
        if (result) {
            return result->hash;
        }
    }
    throw std::runtime_error("Couldn't obtain tablet version");
}

// Verify that when a TABLETS_ROUTING_V2 connection sends:
// (A) a matching tablet version block, the response doesn't contain
//     "tablets-routing-v2" payload;
// (B) a mismatching tablet version block, the response does contain
//     "tablets-routing-v2" payload with the correct format and contents.
SEASTAR_TEST_CASE(test_tablets_routing_v2_match_and_mismatch) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE KEYSPACE ks_tablet WITH replication = "
            "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
            "AND tablets = {'initial': 1}").get();
        e.execute_cql("CREATE TABLE ks_tablet.tbl (pk int PRIMARY KEY, v int)").get();

        const auto schema = e.local_db().find_schema("ks_tablet", "tbl");
        const auto erm = schema->table().get_effective_replication_map();

        // Get the actual tablet version for the partition token.
        const auto pk = partition_key::from_singular(*schema, int32_t{1});
        const auto token = dht::get_token(*schema, pk.view());
        const auto tv = get_eventually_consistent_tablet_version(*erm, token);

        const auto correct_tvb = extract_tablet_version_block(tv, 0);
        // Keep the same index of the block, but change its value.
        const auto wrong_tvb = locator::tablet_version_block{correct_tvb.value() ^ 0x0F};

        const auto local_shard = schema->table().shard_for_reads(token);

        smp::submit_to(local_shard, [&e, correct_tvb, wrong_tvb] {
            return seastar::async([=, &e] {
                // Set up V2 protocol extension on client state.
                cql_transport::cql_protocol_extension_enum_set exts = e.local_client_state().get_protocol_extensions();
                exts.remove(cql_transport::cql_protocol_extension::TABLETS_ROUTING_V1);
                exts.set(cql_transport::cql_protocol_extension::TABLETS_ROUTING_V2_EXPERIMENTAL);
                e.local_client_state().set_protocol_extensions(std::move(exts));

                e.execute_cql("INSERT INTO ks_tablet.tbl (pk, v) VALUES (1, 100)").get();

                const auto insert_id = e.prepare("INSERT INTO ks_tablet.tbl (pk, v) VALUES (?, ?)").get();
                const auto select_id = e.prepare("SELECT v FROM ks_tablet.tbl WHERE pk = ?").get();

                /* INSERT */ {
                    const auto make_insert_options = [] {
                        return std::make_unique<cql3::query_options>(
                            db::consistency_level::ONE,
                            cql3::raw_value_vector_with_unset({
                                cql3::raw_value::make_value(int32_type->decompose(int32_t{1})),
                                cql3::raw_value::make_value(int32_type->decompose(int32_t{42})),
                            }),
                            cql3::query_options::specific_options::DEFAULT);
                    };

                    // Case 1. Pass a correct tablet version block. Tablets-routing-v2 information
                    //         should NOT be returned.
                    auto correct_insert_options = make_insert_options();
                    correct_insert_options->set_tablet_version_block(correct_tvb);

                    const auto ok_result = e.execute_prepared_with_qo(insert_id, std::move(correct_insert_options)).get();
                    BOOST_REQUIRE_MESSAGE(!has_tablets_routing_v2(ok_result),
                        "Tablets-routing-v2 payload was not expected to be returned (INSERT)");

                    // Case 2. Pass the wrong tablet version block. Tablets-routing-v2 information
                    //         SHOULD be returned.
                    auto wrong_insert_options = make_insert_options();
                    wrong_insert_options->set_tablet_version_block(wrong_tvb);

                    const auto bad_result = e.execute_prepared_with_qo(insert_id, std::move(wrong_insert_options)).get();
                    BOOST_REQUIRE_MESSAGE(has_tablets_routing_v2(bad_result),
                        "Expected tablets-routing-v2 payload on version mismatch (INSERT)");

                    // Verify the payload can be deserialized.
                    const auto& payload = bad_result->custom_payload().value().at("tablets-routing-v2");
                    const auto type = replica::get_tablet_info_v2_type();
                    // Deserialize the tuple: (u64, u64, List<Tuple<UUID, u32>>, u64).
                    const auto val = type->deserialize(payload);
                    BOOST_REQUIRE(!val.is_null());
                }

                /* SELECT */ {
                    // Case 1. Pass a correct tablet version block. Tablets-routing-v2 information
                    //         should NOT be returned.
                    const auto make_select_options = [] {
                        return std::make_unique<cql3::query_options>(
                            db::consistency_level::ONE,
                            cql3::raw_value_vector_with_unset({
                                cql3::raw_value::make_value(int32_type->decompose(int32_t{1})),
                            }),
                            cql3::query_options::specific_options::DEFAULT);
                    };

                    auto correct_select_options = make_select_options();
                    correct_select_options->set_tablet_version_block(correct_tvb);

                    const auto ok_result = e.execute_prepared_with_qo(select_id, std::move(correct_select_options)).get();
                    BOOST_REQUIRE_MESSAGE(!has_tablets_routing_v2(ok_result),
                        "Tablets-routing-v2 payload was not expected to be returned (SELECT)");

                    // Case 2. Pass the wrong tablet version block. Tablets-routing-v2 information
                    //         SHOULD be returned.
                    auto wrong_select_options = make_select_options();
                    wrong_select_options->set_tablet_version_block(wrong_tvb);

                    const auto bad_result = e.execute_prepared_with_qo(select_id, std::move(wrong_select_options)).get();
                    BOOST_REQUIRE_MESSAGE(has_tablets_routing_v2(bad_result),
                        "Expected tablets-routing-v2 payload on version mismatch (SELECT)");

                    // Verify the payload can be deserialized.
                    const auto& payload = bad_result->custom_payload().value().at("tablets-routing-v2");
                    const auto type = replica::get_tablet_info_v2_type();
                    // Deserialize the tuple: (u64, u64, List<Tuple<UUID, u32>>, u64).
                    const auto val = type->deserialize(payload);
                    BOOST_REQUIRE(!val.is_null());
                }
            });
        }).get();
    }, tablet_v2_cql_test_config());
}

// Test the interaction between tablets-routing versions.
//
// (A) (V1, V2) = (enabled, disabled):
//     We should fall back to the old protocol for tablet awareness.
// (B) (V1, V2) = (disabled, enabled):
//     We should ONLY use the new protocol. Even when hitting
//     the wrong shard, tablets-routing-v1 payload should NOT be returned.
//
// We also test the pathological situation when both V1 and V2 are enabled.
// In that case, the server should behave as if V1 were disabled.
SEASTAR_TEST_CASE(test_tablets_routing_v1_v2_interaction) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE KEYSPACE ks_tablet WITH replication = "
            "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
            "AND tablets = {'initial': 1}").get();
        e.execute_cql("CREATE TABLE ks_tablet.tbl (pk int PRIMARY KEY, v int)").get();

        const auto schema = e.local_db().find_schema("ks_tablet", "tbl");
        const auto erm = schema->table().get_effective_replication_map();

        // Get the actual tablet version for the partition token.
        const auto pk = partition_key::from_singular(*schema, int32_t{1});
        const auto token = dht::get_token(*schema, pk.view());
        const auto tv = get_eventually_consistent_tablet_version(*erm, token);

        const auto correct_tvb = extract_tablet_version_block(tv, 0);
        // Keep the same index of the block, but change its value.
        const auto wrong_tvb = locator::tablet_version_block{correct_tvb.value() ^ 0x0F};

        const auto local_shard = schema->table().shard_for_reads(token);
        const auto foreign_shard = (local_shard + 1) % this_smp_shard_count();
        BOOST_REQUIRE(local_shard != foreign_shard);

        smp::submit_to(foreign_shard, [&e, correct_tvb, wrong_tvb] {
            return seastar::async([=, &e] {
                const auto insert_id = e.prepare("INSERT INTO ks_tablet.tbl (pk, v) VALUES (?, ?)").get();

                /* (V1, V2) = (enabled, disabled) */ {
                    cql_transport::cql_protocol_extension_enum_set exts = e.local_client_state().get_protocol_extensions();
                    exts.set(cql_transport::cql_protocol_extension::TABLETS_ROUTING_V1);
                    exts.remove(cql_transport::cql_protocol_extension::TABLETS_ROUTING_V2_EXPERIMENTAL);
                    e.local_client_state().set_protocol_extensions(std::move(exts));

                    std::vector<cql3::raw_value> raw_values;
                    raw_values.emplace_back(cql3::raw_value::make_value(int32_type->decompose(int32_t{2})));
                    raw_values.emplace_back(cql3::raw_value::make_value(int32_type->decompose(int32_t{99})));


                    const auto result = e.execute_prepared(insert_id, std::move(raw_values)).get();
                    // V1 should fire (cross-shard).
                    BOOST_REQUIRE_MESSAGE(has_tablet_routing(result),
                        "Expected tablets-routing-v1 payload on cross-shard request");
                    // V2 should NOT be present.
                    BOOST_REQUIRE_MESSAGE(!has_tablets_routing_v2(result),
                        "V1-only connection must not receive tablets-routing-v2 payload");
                }

                /* (V1, V2) = (enabled, enabled) */ {
                    // Both tablets-routing versions should NOT be enabled
                    // simultaneously, but let's test it! V1 should be ignored.
                    cql_transport::cql_protocol_extension_enum_set exts = e.local_client_state().get_protocol_extensions();
                    exts.set(cql_transport::cql_protocol_extension::TABLETS_ROUTING_V1);
                    exts.set(cql_transport::cql_protocol_extension::TABLETS_ROUTING_V2_EXPERIMENTAL);
                    e.local_client_state().set_protocol_extensions(std::move(exts));

                    const auto options = std::make_unique<cql3::query_options>(
                        db::consistency_level::ONE,
                        cql3::raw_value_vector_with_unset({
                            cql3::raw_value::make_value(int32_type->decompose(int32_t{1})),
                            cql3::raw_value::make_value(int32_type->decompose(int32_t{42})),
                        }),
                        cql3::query_options::specific_options::DEFAULT);

                    auto bad_options = std::make_unique<cql3::query_options>(*options);
                    bad_options->set_tablet_version_block(wrong_tvb);

                    auto bad_result = e.execute_prepared_with_qo(insert_id, std::move(bad_options)).get();
                    BOOST_REQUIRE_MESSAGE(!has_tablet_routing(bad_result),
                        "Expected tablets-routing-v2 payload only");
                    BOOST_REQUIRE_MESSAGE(has_tablets_routing_v2(bad_result),
                        "Expected tablets-routing-v2 payload");

                    // With a match, no payload should be returned.
                    auto ok_options = std::make_unique<cql3::query_options>(*options);
                    ok_options->set_tablet_version_block(correct_tvb);

                    auto ok_result = e.execute_prepared_with_qo(insert_id, std::move(ok_options)).get();
                    BOOST_REQUIRE_MESSAGE(!has_tablet_routing(ok_result),
                        "Didn't expect tablets-routing-v1 payload");
                    BOOST_REQUIRE_MESSAGE(!has_tablets_routing_v2(ok_result),
                        "Didn't expect tablets-routing-v2 payload");
                }

                /* (V1, V2) = (disabled, enabled) */ {
                    // Even though we target the wrong shard (foreign shard),
                    // tablets-routing-v1 information should NOT be returned.
                    cql_transport::cql_protocol_extension_enum_set exts = e.local_client_state().get_protocol_extensions();
                    exts.remove(cql_transport::cql_protocol_extension::TABLETS_ROUTING_V1);
                    exts.set(cql_transport::cql_protocol_extension::TABLETS_ROUTING_V2_EXPERIMENTAL);
                    e.local_client_state().set_protocol_extensions(std::move(exts));

                    const auto make_options = [] {
                        return std::make_unique<cql3::query_options>(
                            db::consistency_level::ONE,
                            cql3::raw_value_vector_with_unset({
                                cql3::raw_value::make_value(int32_type->decompose(int32_t{1})),
                                cql3::raw_value::make_value(int32_type->decompose(int32_t{42})),
                            }),
                            cql3::query_options::specific_options::DEFAULT);
                    };

                    auto bad_options = make_options();
                    bad_options->set_tablet_version_block(wrong_tvb);

                    auto bad_result = e.execute_prepared_with_qo(insert_id, std::move(bad_options)).get();
                    BOOST_REQUIRE_MESSAGE(!has_tablet_routing(bad_result),
                        "Expected tablets-routing-v2 payload only");
                    BOOST_REQUIRE_MESSAGE(has_tablets_routing_v2(bad_result),
                        "Expected tablets-routing-v2 payload");

                    // With a match, no payload should be returned.
                    auto ok_options = make_options();
                    ok_options->set_tablet_version_block(correct_tvb);

                    auto ok_result = e.execute_prepared_with_qo(insert_id, std::move(ok_options)).get();
                    BOOST_REQUIRE_MESSAGE(!has_tablet_routing(ok_result),
                        "Didn't expect tablets-routing-v1 payload");
                    BOOST_REQUIRE_MESSAGE(!has_tablets_routing_v2(ok_result),
                        "Didn't expect tablets-routing-v2 payload");
                }
            });
        }).get();
    }, tablet_v2_cql_test_config());
}

// Verifies the format of the "tablets-routing-v2" payload:
// TupleType(u64, u64, List<Tuple<UUID, u32>>, u64)
// containing (first_token, last_token, replicas, tablet_version).
SEASTAR_TEST_CASE(test_tablets_routing_v2_payload_format) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE KEYSPACE ks_tablet WITH replication = "
            "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
            "AND tablets = {'initial': 1}").get();
        e.execute_cql("CREATE TABLE ks_tablet.tbl (pk int PRIMARY KEY, v int)").get();

        const auto schema = e.local_db().find_schema("ks_tablet", "tbl");
        const auto erm = schema->table().get_effective_replication_map();
        const auto& tablet_map = erm->get_token_metadata().tablets().get_tablet_map(schema->id());

        // Get the actual tablet version for the partition token.
        const auto pk = partition_key::from_singular(*schema, int32_t{1});
        const auto token = dht::get_token(*schema, pk.view());
        const auto tid = tablet_map.get_tablet_id(token);
        const auto tv = get_eventually_consistent_tablet_version(*erm, token);

        const auto correct_tvb = extract_tablet_version_block(tv, 0);
        // Keep the same index of the block, but change its value.
        const auto wrong_tvb = locator::tablet_version_block{correct_tvb.value() ^ 0x0F};

        // Get expected values from the tablet_map.
        const auto& info = tablet_map.get_tablet_info(tid);
        const auto last_token = tablet_map.get_last_token(tid);
        const auto first_token = (tid == tablet_map.first_tablet())
            ? dht::minimum_token()
            : tablet_map.get_last_token(locator::tablet_id(size_t(tid) - 1));

        const auto local_shard = schema->table().shard_for_reads(token);

        smp::submit_to(local_shard, [&] {
            return seastar::async([&] {
                // Set up V2 protocol extension on client state.
                cql_transport::cql_protocol_extension_enum_set exts = e.local_client_state().get_protocol_extensions();
                exts.remove(cql_transport::cql_protocol_extension::TABLETS_ROUTING_V1);
                exts.set(cql_transport::cql_protocol_extension::TABLETS_ROUTING_V2_EXPERIMENTAL);
                e.local_client_state().set_protocol_extensions(std::move(exts));

                const auto insert_id = e.prepare("INSERT INTO ks_tablet.tbl (pk, v) VALUES (?, ?)").get();

                // Send a mismatching block to trigger V2 payload.
                auto options = std::make_unique<cql3::query_options>(
                    db::consistency_level::ONE,
                    cql3::raw_value_vector_with_unset({
                        cql3::raw_value::make_value(int32_type->decompose(int32_t{1})),
                        cql3::raw_value::make_value(int32_type->decompose(int32_t{1})),
                    }),
                    cql3::query_options::specific_options::DEFAULT);
                options->set_tablet_version_block(wrong_tvb);

                auto result = e.execute_prepared_with_qo(insert_id, std::move(options)).get();
                BOOST_REQUIRE(has_tablets_routing_v2(result));

                // Deserialize the payload.
                auto& payload_bytes = result->custom_payload().value().at("tablets-routing-v2");
                auto type = replica::get_tablet_info_v2_type();
                auto val = type->deserialize(payload_bytes);
                BOOST_REQUIRE(!val.is_null());

                // Extract tuple elements: (first_token, last_token, replicas, version).
                auto tuple_val = value_cast<tuple_type_impl::native_type>(val);
                BOOST_REQUIRE_EQUAL(tuple_val.size(), 4u);

                // Element 0: first_token.
                auto returned_first = value_cast<int64_t>(tuple_val[0]);
                BOOST_REQUIRE_EQUAL(returned_first, dht::token::to_int64(first_token));

                // Element 1: last_token.
                auto returned_last = value_cast<int64_t>(tuple_val[1]);
                BOOST_REQUIRE_EQUAL(returned_last, dht::token::to_int64(last_token));

                // Element 2: replicas list.
                auto replicas_list = value_cast<list_type_impl::native_type>(tuple_val[2]);
                BOOST_REQUIRE_EQUAL(replicas_list.size(), info.replicas.size());

                // Element 3: tablet_version (int64 serialized).
                auto returned_version = static_cast<locator::tablet_version>(
                    value_cast<int64_t>(tuple_val[3]));
                BOOST_REQUIRE_EQUAL(returned_version, tv);
            });
        }).get();
    }, tablet_v2_cql_test_config());
}

// Test the basic interaction between a strongly consistent table
// and tablets-routing protocols.
// We verify that the table is "immune" to tablets-routing-v1, i.e.
// we will never get the corresponding payload, even if a request targets
// the wrong shard.
// We also verify tablets-routing-v2: a request carrying a mismatching tablet
// version block must receive the "tablets-routing-v2" payload, and a matching
// one must not.
SEASTAR_TEST_CASE(test_tablets_routing_strong_consistency) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE KEYSPACE ks_tablet WITH replication = "
            "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
            "AND tablets = {'initial': 1} "
            "AND consistency = 'global'").get();
        e.execute_cql("CREATE TABLE ks_tablet.tbl (pk int PRIMARY KEY, v int)").get();

        const auto schema = e.local_db().find_schema("ks_tablet", "tbl");
        const auto pk = partition_key::from_singular(*schema, int32_t{1});
        const auto token = dht::get_token(*schema, pk.view());

        const auto local_shard = schema->table().shard_for_reads(token);
        const auto foreign_shard = (local_shard + 1) % this_smp_shard_count();
        BOOST_REQUIRE(local_shard != foreign_shard);

        // Part 1 (cross-shard): a strongly consistent table must be immune to
        // tablets-routing-v1 even when the request hits the wrong shard.
        smp::submit_to(foreign_shard, [&e] {
            return seastar::async([&e] {
                cql_transport::cql_protocol_extension_enum_set exts = e.local_client_state().get_protocol_extensions();
                exts.set(cql_transport::cql_protocol_extension::TABLETS_ROUTING_V1);
                exts.remove(cql_transport::cql_protocol_extension::TABLETS_ROUTING_V2_EXPERIMENTAL);
                e.local_client_state().set_protocol_extensions(std::move(exts));

                const auto insert_id = e.prepare("INSERT INTO ks_tablet.tbl (pk, v) VALUES (?, ?)").get();
                auto options = std::make_unique<cql3::query_options>(
                    db::consistency_level::QUORUM,
                    cql3::raw_value_vector_with_unset({
                        cql3::raw_value::make_value(int32_type->decompose(int32_t{1})),
                        cql3::raw_value::make_value(int32_type->decompose(int32_t{42})),
                    }),
                    cql3::query_options::specific_options::DEFAULT);

                const auto result = e.execute_prepared_with_qo(insert_id, std::move(options)).get();
                // Even though we target the wrong shard (foreign shard),
                // tablets-routing-v1 information should NOT be returned.
                BOOST_REQUIRE_MESSAGE(!has_tablet_routing(result),
                    "Did not expect tablets-routing-v1 payload on cross-shard request for strongly consistent table");
                BOOST_REQUIRE_MESSAGE(!has_tablets_routing_v2(result),
                    "V1-only connection must not receive tablets-routing-v2 payload");
            });
        }).get();

        // Part 2 (replica shard): with V2 enabled, a mismatching tablet version
        // block must yield a tablets-routing-v2 payload, and a matching one must
        // not. The check only runs on the shard that hosts the tablet replica;
        // a request to any other shard is redirected before the version check.
        smp::submit_to(local_shard, [&e, token] {
            return seastar::async([&e, token] {
                cql_transport::cql_protocol_extension_enum_set exts = e.local_client_state().get_protocol_extensions();
                exts.remove(cql_transport::cql_protocol_extension::TABLETS_ROUTING_V1);
                exts.set(cql_transport::cql_protocol_extension::TABLETS_ROUTING_V2_EXPERIMENTAL);
                e.local_client_state().set_protocol_extensions(std::move(exts));

                const auto local_schema = e.local_db().find_schema("ks_tablet", "tbl");
                const auto& [coordinator_ref, _] = e.local_qp().acquire_strongly_consistent_coordinator();
                auto& groups_manager = coordinator_ref.get().get_groups_manager();

                const auto get_tablet_version = [&] (const replica::table& table, const dht::token& token) -> std::optional<locator::tablet_version> {
                    const locator::tablet_version_block blocks[] = {
                        locator::tablet_version_block{0x00}, locator::tablet_version_block{0x01}
                    };
                    for (auto block : blocks) {
                        auto result = groups_manager.check_tablet_version(table, token, block);
                        if (result) {
                            return std::make_optional(result->hash);
                        }
                    }
                    return std::nullopt;
                };

                // Leader might not be available instantly, so poll until we can compute the tablet version.
                locator::tablet_version version{0};
                const bool version_ready = eventually_true([&] {
                    if (const auto v = get_tablet_version(local_schema->table(), token)) {
                        version = *v;
                        return true;
                    }
                    return false;
                });
                BOOST_REQUIRE_MESSAGE(version_ready,
                    "Strongly consistent tablet version was not computed in time");

                const auto correct_tvb = extract_tablet_version_block(version, 0);
                // Keep the same block index, but change its value to force a mismatch.
                const auto wrong_tvb = locator::tablet_version_block{correct_tvb.value() ^ 0x0F};

                const auto insert_id = e.prepare("INSERT INTO ks_tablet.tbl (pk, v) VALUES (?, ?)").get();
                const auto make_options = [] {
                    return std::make_unique<cql3::query_options>(
                        db::consistency_level::QUORUM,
                        cql3::raw_value_vector_with_unset({
                            cql3::raw_value::make_value(int32_type->decompose(int32_t{1})),
                            cql3::raw_value::make_value(int32_type->decompose(int32_t{42})),
                        }),
                        cql3::query_options::specific_options::DEFAULT);
                };

                // Mismatching block: tablets-routing-v2 payload SHOULD be returned.
                auto wrong_options = make_options();
                wrong_options->set_tablet_version_block(wrong_tvb);

                const auto bad_result = e.execute_prepared_with_qo(insert_id, std::move(wrong_options)).get();
                BOOST_REQUIRE_MESSAGE(!has_tablet_routing(bad_result),
                    "Did not expect tablets-routing-v1 payload");
                BOOST_REQUIRE_MESSAGE(has_tablets_routing_v2(bad_result),
                    "Expected tablets-routing-v2 payload on version mismatch");

                // Verify the payload can be deserialized.
                const auto& payload = bad_result->custom_payload().value().at("tablets-routing-v2");
                const auto type = replica::get_tablet_info_v2_type();
                // Deserialize the tuple: (u64, u64, List<Tuple<UUID, u32>>, u64).
                const auto val = type->deserialize(payload);
                BOOST_REQUIRE(!val.is_null());

                // Matching block: no payload should be returned.
                auto ok_options = make_options();
                ok_options->set_tablet_version_block(correct_tvb);

                const auto ok_result = e.execute_prepared_with_qo(insert_id, std::move(ok_options)).get();
                BOOST_REQUIRE_MESSAGE(!has_tablet_routing(ok_result),
                    "Did not expect tablets-routing-v1 payload");
                BOOST_REQUIRE_MESSAGE(!has_tablets_routing_v2(ok_result),
                    "Did not expect tablets-routing-v2 payload on version match");
            });
        }).get();
    }, tablet_v2_cql_test_config());
}

SEASTAR_TEST_CASE(test_select_constant_type_inference) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        // Integer constant — small value fits int32
        auto msg = e.execute_cql("SELECT 1 FROM system.local").get();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_column_types({int32_type})
            .with_row({{int32_type->decompose(1)}});

        // Integer constant — large value requires bigint
        msg = e.execute_cql("SELECT 10000000000 FROM system.local").get();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_column_types({long_type})
            .with_row({{long_type->decompose(int64_t(10000000000))}});

        // String constant
        msg = e.execute_cql("SELECT 'hello' FROM system.local").get();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_column_types({utf8_type})
            .with_row({{utf8_type->decompose(sstring("hello"))}});

        // Boolean constant
        msg = e.execute_cql("SELECT true FROM system.local").get();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_column_types({boolean_type})
            .with_row({{boolean_type->decompose(true)}});

        // Floating-point constant
        msg = e.execute_cql("SELECT 3.14 FROM system.local").get();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_column_types({double_type});

        // Negative integer constant
        msg = e.execute_cql("SELECT -1 FROM system.local").get();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_column_types({int32_type});

        // Scientific notation is inferred as double
        msg = e.execute_cql("SELECT 1e6 FROM system.local").get();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_column_types({double_type});

        // Multiple constants
        msg = e.execute_cql("SELECT 1, 'hello', true FROM system.local").get();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_column_types({int32_type, utf8_type, boolean_type});

        // Function call as a top-level selector — type inferred from return type
        msg = e.execute_cql("SELECT now() FROM system.local").get();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_column_types({timeuuid_type});

        // count(1) works
        msg = e.execute_cql("SELECT count(1) FROM system.local").get();
        assert_that(msg).is_rows()
            .with_size(1);
    });
}

SEASTAR_TEST_CASE(test_select_collection_literal_type_inference) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        // List literal
        auto msg = e.execute_cql("SELECT [1, 2, 3] FROM system.local").get();
        auto expected_list_type = list_type_impl::get_instance(int32_type, false);
        assert_that(msg).is_rows()
            .with_size(1)
            .with_column_types({expected_list_type});

        // Set literal
        msg = e.execute_cql("SELECT {1, 2, 3} FROM system.local").get();
        auto expected_set_type = set_type_impl::get_instance(int32_type, false);
        assert_that(msg).is_rows()
            .with_size(1)
            .with_column_types({expected_set_type});

        // Map literal
        msg = e.execute_cql("SELECT {'a': 1, 'b': 2} FROM system.local").get();
        auto expected_map_type = map_type_impl::get_instance(utf8_type, int32_type, false);
        assert_that(msg).is_rows()
            .with_size(1)
            .with_column_types({expected_map_type});

        // Nested collection literal
        msg = e.execute_cql("SELECT [[1, 2], [3, 4]] FROM system.local").get();
        auto expected_nested_list = list_type_impl::get_instance(
                list_type_impl::get_instance(int32_type, false), false);
        assert_that(msg).is_rows()
            .with_size(1)
            .with_column_types({expected_nested_list});

        // Type widening within integer chain. The int literal 1 must be converted to
        // the 8-byte bigint representation, not just relabelled.
        msg = e.execute_cql("SELECT [1, 10000000000] FROM system.local").get();
        auto expected_bigint_list = list_type_impl::get_instance(long_type, false);
        assert_that(msg).is_rows()
            .with_size(1)
            .with_column_types({expected_bigint_list})
            .with_rows({{
                expected_bigint_list->decompose(
                    make_list_value(expected_bigint_list, list_type_impl::native_type({int64_t(1), int64_t(10000000000)})))
            }});

        msg = e.execute_cql("SELECT {1, 10000000000} FROM system.local").get();
        auto expected_bigint_set = set_type_impl::get_instance(long_type, false);
        assert_that(msg).is_rows()
            .with_size(1)
            .with_column_types({expected_bigint_set});

        msg = e.execute_cql("SELECT {'a': 1, 'b': 10000000000} FROM system.local").get();
        auto expected_text_bigint_map = map_type_impl::get_instance(utf8_type, long_type, false);
        assert_that(msg).is_rows()
            .with_size(1)
            .with_column_types({expected_text_bigint_map});

        // Integer widening: int32 + int64 + varint (from literal values)
        msg = e.execute_cql("SELECT [1, 10000000000, 99999999999999999999] FROM system.local").get();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_column_types({list_type_impl::get_instance(varint_type, false)});

        // Full integer widening chain with C-style type hints
        msg = e.execute_cql("SELECT [(tinyint)1, (smallint)2, 3, 10000000000, 99999999999999999999] FROM system.local").get();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_column_types({list_type_impl::get_instance(varint_type, false)});

        // Float chain: float + double → double. The (float)1.0 element must be
        // converted to a double value, not reinterpreted as 4 bytes.
        msg = e.execute_cql("SELECT [(float)1.0, 3.14] FROM system.local").get();
        auto expected_double_list = list_type_impl::get_instance(double_type, false);
        assert_that(msg).is_rows()
            .with_size(1)
            .with_column_types({expected_double_list})
            .with_rows({{
                expected_double_list->decompose(
                    make_list_value(expected_double_list, list_type_impl::native_type({double(1.0), double(3.14)})))
            }});

        // Cross-chain widening (int + double) should fail
        BOOST_REQUIRE_THROW(
            e.execute_cql("SELECT [1, 3.14] FROM system.local").get(),
            exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(
            e.execute_cql("SELECT {1, 3.14} FROM system.local").get(),
            exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(
            e.execute_cql("SELECT {'a': 1, 'b': 3.14} FROM system.local").get(),
            exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(
            e.execute_cql("SELECT {1: 'a', 3.14: 'b'} FROM system.local").get(),
            exceptions::invalid_request_exception);
        // Cross-chain: float + int
        BOOST_REQUIRE_THROW(
            e.execute_cql("SELECT [(float)1.0, 1] FROM system.local").get(),
            exceptions::invalid_request_exception);

        // Mixed types in collection should fail
        BOOST_REQUIRE_THROW(
            e.execute_cql("SELECT [1, 'hello'] FROM system.local").get(),
            exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(
            e.execute_cql("SELECT {1, 'hello'} FROM system.local").get(),
            exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(
            e.execute_cql("SELECT {'a': 1, 'b': 'hello'} FROM system.local").get(),
            exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(
            e.execute_cql("SELECT {'a': 1, 2: 3} FROM system.local").get(),
            exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(
            e.execute_cql("SELECT [1, true] FROM system.local").get(),
            exceptions::invalid_request_exception);

        // Nested collection type mismatch
        BOOST_REQUIRE_THROW(
            e.execute_cql("SELECT [[1, 2], [3, 'a']] FROM system.local").get(),
            exceptions::invalid_request_exception);

        // Empty collection should fail (can't infer type)
        BOOST_REQUIRE_THROW(
            e.execute_cql("SELECT {} FROM system.local").get(),
            exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(
            e.execute_cql("SELECT [] FROM system.local").get(),
            exceptions::invalid_request_exception);

        // Null has no inherent type. As a top-level selector there is no
        // column or function parameter to provide type context, so type
        // inference fails.
        BOOST_REQUIRE_THROW(
            e.execute_cql("SELECT null FROM system.local").get(),
            exceptions::invalid_request_exception);

        // Null cannot self-type, so collections containing null fail
        // at type inference time ("Could not infer type of ...").
        BOOST_REQUIRE_THROW(
            e.execute_cql("SELECT [1, null] FROM system.local").get(),
            exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(
            e.execute_cql("SELECT {1, null} FROM system.local").get(),
            exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(
            e.execute_cql("SELECT {'a': null} FROM system.local").get(),
            exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(
            e.execute_cql("SELECT {null: 1} FROM system.local").get(),
            exceptions::invalid_request_exception);

        // All nulls in collection — no consensus possible
        BOOST_REQUIRE_THROW(
            e.execute_cql("SELECT [null, null] FROM system.local").get(),
            exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(
            e.execute_cql("SELECT {null: null} FROM system.local").get(),
            exceptions::invalid_request_exception);

        // Function calls inside collections — infer_type resolves return types
        // via functions::get() without producing prepared expressions
        msg = e.execute_cql("SELECT [now(), now()] FROM system.local").get();
        auto expected_timeuuid_list = list_type_impl::get_instance(timeuuid_type, false);
        assert_that(msg).is_rows()
            .with_size(1)
            .with_column_types({expected_timeuuid_list});

        // Tuple literal
        msg = e.execute_cql("SELECT (1, 'hello', true) FROM system.local").get();
        auto expected_tuple_type = tuple_type_impl::get_instance({int32_type, utf8_type, boolean_type});
        assert_that(msg).is_rows()
            .with_size(1)
            .with_column_types({expected_tuple_type});

        // SQL CAST in SELECT clause
        msg = e.execute_cql("SELECT CAST(1 AS bigint) FROM system.local").get();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_column_types({long_type})
            .with_row({{long_type->decompose(int64_t(1))}});
    });
}

// A narrower (T) cast widening into a wider sink (clustering key, column, WHERE RHS) must be
// converted to the sink's representation, not just relabelled.
SEASTAR_TEST_CASE(test_widening_value_into_wider_sink) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE wide (pk int, ck bigint, d double, PRIMARY KEY (pk, ck))").get();

        // INSERT sink: (int)5 widens to the bigint clustering key, (float)2.5 widens to
        // the double column. A relabel would store 4 bytes; we require real conversion.
        e.execute_cql("INSERT INTO wide (pk, ck, d) VALUES (1, (int)5, (float)2.5)").get();

        auto msg = e.execute_cql("SELECT ck, d FROM wide WHERE pk = 1").get();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_row({long_type->decompose(int64_t(5)), double_type->decompose(double(2.5))});

        // WHERE RHS sink: (int)5 must be converted to bigint to match the clustering key.
        // An unconverted 4-byte value would never compare equal to the stored bigint.
        msg = e.execute_cql("SELECT ck, d FROM wide WHERE pk = 1 AND ck = (int)5").get();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_row({long_type->decompose(int64_t(5)), double_type->decompose(double(2.5))});

        // UPDATE sink + WHERE-key widening together.
        e.execute_cql("UPDATE wide SET d = (float)3.5 WHERE pk = 1 AND ck = (int)5").get();
        msg = e.execute_cql("SELECT d FROM wide WHERE pk = 1").get();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_row({double_type->decompose(double(3.5))});
    });
}

// The parser counts markers for one statement at a time, but used to hand
// every statement of a multi-statement parse all the markers it had seen so
// far, so a later statement inherited the markers of the ones before it.
SEASTAR_TEST_CASE(test_a_parsed_statement_gets_only_the_markers_of_its_text) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.tbl (pk int PRIMARY KEY)").get();

        auto& qp = e.local_qp();
        auto stmts = cql3::query_processor::parse_statements(
                "INSERT INTO ks.tbl (pk) VALUES (?); SELECT * FROM ks.tbl;", cql3::internal_dialect());
        BOOST_REQUIRE_EQUAL(stmts.size(), 2);
        auto insert = stmts[0]->prepare(qp.db(), qp.get_cql_stats(), qp.get_cql_config());
        BOOST_REQUIRE_EQUAL(insert->bound_names.size(), 1);
        auto select = stmts[1]->prepare(qp.db(), qp.get_cql_stats(), qp.get_cql_config());
        BOOST_REQUIRE_EQUAL(select->bound_names.size(), 0);

        // A marker name is only a name within its own statement: reused in a
        // later one, it is that statement's own marker, not a reference to
        // the marker the name stood for before.
        auto named = cql3::query_processor::parse_statements(
                "INSERT INTO ks.tbl (pk) VALUES (:a); INSERT INTO ks.tbl (pk) VALUES (:a);", cql3::internal_dialect());
        BOOST_REQUIRE_EQUAL(named.size(), 2);
        for (auto& stmt : named) {
            BOOST_REQUIRE_EQUAL(stmt->prepare(qp.db(), qp.get_cql_stats(), qp.get_cql_config())->bound_names.size(), 1);
        }
    });
}

// Reproduces a bug in which TWCS sstable sets filtered sstables by clustering key
// using the query-schema (reversed) ranges, which `sstable::may_contain_rows()`
// interprets as table-schema ranges. Both the optimized TWCS read path
// (`time_series_sstable_set::create_single_key_sstable_reader()`) and the regular
// path (`filter_sstable_for_reader_by_ck()`) had the bug, so we exercise both,
// switching between them with the `enable_optimized_twcs_queries` option.
static void test_twcs_reversed_restricted_query(bool enable_optimized_twcs_queries) {
    do_with_cql_env_thread([enable_optimized_twcs_queries] (cql_test_env& e) {
        e.execute_cql(
                format("CREATE TABLE tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))"
                " WITH compaction = {{"
                "   'compaction_window_size': '1',"
                "   'compaction_window_unit': 'MINUTES',"
                "   'enable_optimized_twcs_queries': '{}',"
                "   'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy'"
                "}}", enable_optimized_twcs_queries ? "true" : "false")).get();

        // Compactions would merge the sstables together, defeating the purpose
        // of the test. Note that we can't use the `enabled: false` compaction
        // option for this, because it replaces the strategy (and hence the
        // sstable set) with the null strategy, which wouldn't exercise the
        // TWCS read paths at all.
        e.db().invoke_on_all([] (replica::database& db) {
            return db.find_column_family("ks", "tbl").disable_auto_compaction();
        }).get();

        // One sstable per clustering key, so that each sstable has a narrow
        // min/max clustering position range.
        constexpr int n = 10;
        for (int i = 0; i < n; ++i) {
            e.execute_cql(format("INSERT INTO tbl (pk, ck, v) VALUES (0, {}, {})", i, i)).get();
            e.db().invoke_on_all([] (replica::database& db) {
                return db.flush_all_memtables();
            }).get();
        }

        auto count = [&e] (const sstring& q) {
            auto msg = e.execute_cql(q).get();
            auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
            BOOST_REQUIRE(rows);
            return rows->rs().result_set().size();
        };

        BOOST_CHECK_EQUAL(count("SELECT * FROM tbl WHERE pk = 0 BYPASS CACHE"), n);
        BOOST_CHECK_EQUAL(count("SELECT * FROM tbl WHERE pk = 0 ORDER BY ck DESC BYPASS CACHE"), n);
        BOOST_CHECK_EQUAL(count("SELECT * FROM tbl WHERE pk = 0 AND ck >= 5 BYPASS CACHE"), n - 5);
        BOOST_CHECK_EQUAL(count("SELECT * FROM tbl WHERE pk = 0 AND ck >= 5 ORDER BY ck DESC BYPASS CACHE"), n - 5);
        BOOST_CHECK_EQUAL(count("SELECT * FROM tbl WHERE pk = 0 AND ck < 5 BYPASS CACHE"), 5);
        BOOST_CHECK_EQUAL(count("SELECT * FROM tbl WHERE pk = 0 AND ck < 5 ORDER BY ck DESC BYPASS CACHE"), 5);
        BOOST_CHECK_EQUAL(count("SELECT * FROM tbl WHERE pk = 0 AND ck >= 3 AND ck <= 6 BYPASS CACHE"), 4);
        BOOST_CHECK_EQUAL(count("SELECT * FROM tbl WHERE pk = 0 AND ck >= 3 AND ck <= 6 ORDER BY ck DESC BYPASS CACHE"), 4);
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_twcs_reversed_restricted_query_optimized) {
    test_twcs_reversed_restricted_query(true);
}

SEASTAR_THREAD_TEST_CASE(test_twcs_reversed_restricted_query_regular) {
    test_twcs_reversed_restricted_query(false);
}

BOOST_AUTO_TEST_SUITE_END()
