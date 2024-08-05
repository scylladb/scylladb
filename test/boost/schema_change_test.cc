/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <iostream>
#include <fmt/ranges.h>
#include <seastar/core/thread.hh>
#include "test/lib/scylla_test_case.hh"
#include <seastar/util/defer.hh>

#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "schema/schema_builder.hh"
#include "schema/schema_registry.hh"
#include "db/extensions.hh"
#include "db/schema_tables.hh"
#include "types/list.hh"
#include "types/user.hh"
#include "db/config.hh"
#include "test/lib/tmpdir.hh"
#include "test/lib/exception_utils.hh"
#include "test/lib/log.hh"
#include "cdc/cdc_extension.hh"

static cql_test_config run_with_raft_recovery_config() {
    cql_test_config c;
    c.run_with_raft_recovery = true;
    return c;
}

SEASTAR_TEST_CASE(test_new_schema_with_no_structural_change_is_propagated) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            auto partial = schema_builder("tests", "table")
                    .with_column("pk", bytes_type, column_kind::partition_key)
                    .with_column("v1", bytes_type);

            e.execute_cql("create keyspace tests with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };").get();

            auto old_schema = partial.build();

            auto& mm = e.migration_manager().local();

            {
                auto group0_guard = mm.start_group0_operation().get();
                auto ts = group0_guard.write_timestamp();
                mm.announce(service::prepare_new_column_family_announcement(mm.get_storage_proxy(), old_schema, ts).get(), std::move(group0_guard), "").get();
            }

            auto old_table_version = e.db().local().find_schema(old_schema->id())->version();
            auto old_node_version = e.db().local().get_version();

            auto new_schema = partial.build();
            BOOST_REQUIRE_NE(new_schema->version(), old_schema->version());

            auto group0_guard = mm.start_group0_operation().get();
            auto ts = group0_guard.write_timestamp();
            mm.announce(service::prepare_column_family_update_announcement(mm.get_storage_proxy(),
                    new_schema, std::vector<view_ptr>(), ts).get(), std::move(group0_guard), "").get();

            BOOST_REQUIRE_NE(e.db().local().find_schema(old_schema->id())->version(), old_table_version);
            BOOST_REQUIRE_NE(e.db().local().get_version(), old_node_version);
        });
    });
}

SEASTAR_TEST_CASE(test_schema_is_updated_in_keyspace) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            auto builder = schema_builder("tests", "table")
                    .with_column("pk", bytes_type, column_kind::partition_key)
                    .with_column("v1", bytes_type);

            e.execute_cql("create keyspace tests with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };").get();

            auto old_schema = builder.build();

            auto& mm = e.migration_manager().local();
            {
                auto group0_guard = mm.start_group0_operation().get();
                auto ts = group0_guard.write_timestamp();
                mm.announce(service::prepare_new_column_family_announcement(mm.get_storage_proxy(), old_schema, ts).get(), std::move(group0_guard), "").get();
            }

            auto s = e.local_db().find_schema(old_schema->id());
            BOOST_REQUIRE_EQUAL(*old_schema, *s);
            BOOST_REQUIRE_EQUAL(864000, s->gc_grace_seconds().count());
            BOOST_REQUIRE_EQUAL(*s, *e.local_db().find_keyspace(s->ks_name()).metadata()->cf_meta_data().at(s->cf_name()));

            builder.set_gc_grace_seconds(1);
            auto new_schema = builder.build();

            auto group0_guard = mm.start_group0_operation().get();
            auto ts = group0_guard.write_timestamp();
            mm.announce(service::prepare_column_family_update_announcement(mm.get_storage_proxy(),
                    new_schema, std::vector<view_ptr>(), ts).get(), std::move(group0_guard), "").get();

            s = e.local_db().find_schema(old_schema->id());
            BOOST_REQUIRE_NE(*old_schema, *s);
            BOOST_REQUIRE_EQUAL(*new_schema, *s);
            BOOST_REQUIRE_EQUAL(1, s->gc_grace_seconds().count());
            BOOST_REQUIRE_EQUAL(*s, *e.local_db().find_keyspace(s->ks_name()).metadata()->cf_meta_data().at(s->cf_name()));
        });
    });
}

SEASTAR_TEST_CASE(test_tombstones_are_ignored_in_version_calculation) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            e.execute_cql("create keyspace tests with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };").get();

            auto table_schema = schema_builder("ks", "table")
                    .with_column("pk", bytes_type, column_kind::partition_key)
                    .with_column("v1", bytes_type)
                    .build();

            auto& mm = e.migration_manager().local();
            auto group0_guard = mm.start_group0_operation().get();
            auto ts = group0_guard.write_timestamp();
            mm.announce(service::prepare_new_column_family_announcement(mm.get_storage_proxy(), table_schema, ts).get(), std::move(group0_guard), "").get();

            auto old_table_version = e.db().local().find_schema(table_schema->id())->version();
            auto old_node_version = e.db().local().get_version();

            {
                testlog.info("Applying a no-op tombstone to v1 column definition");
                auto s = db::schema_tables::columns();
                auto pkey = partition_key::from_singular(*s, table_schema->ks_name());
                mutation m(s, pkey);
                auto ckey = clustering_key::from_exploded(*s, {utf8_type->decompose(table_schema->cf_name()), "v1"});
                m.partition().apply_delete(*s, ckey, tombstone(api::min_timestamp, gc_clock::now()));
                mm.announce(std::vector<mutation>({m}), mm.start_group0_operation().get(), "").get();
            }

            auto new_table_version = e.db().local().find_schema(table_schema->id())->version();
            auto new_node_version = e.db().local().get_version();

            BOOST_REQUIRE_EQUAL(new_table_version, old_table_version);

            // With group 0 schema changes and GROUP0_SCHEMA_VERSIONING, this check wouldn't pass,
            // because the version after the first schema change is not a digest, but taken
            // to be the version sent in the schema change mutations; in this case,
            // `prepare_new_column_family_announcement` took `table_schema->version()`
            //
            // On the other hand, the second schema change mutations do not contain
            // the ususal `system_schema.scylla_tables` mutation (which would contain the version);
            // they are 'incomplete' schema mutations created by the above piece of code,
            // not by the usual `prepare_..._announcement` functions. This causes
            // a digest to be calculated when applying the schema change, and the digest
            // will be different than the first version sent.
            //
            // Hence we use `run_with_raft_recovery_config()` in this test.
            BOOST_REQUIRE_EQUAL(new_node_version, old_node_version);
        });
    }, run_with_raft_recovery_config());
}

SEASTAR_TEST_CASE(test_concurrent_column_addition) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            e.execute_cql("create keyspace tests with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };").get();

            service::migration_manager& mm = e.migration_manager().local();

            auto s0 = schema_builder("ks", "table")
                    .with_column("pk", bytes_type, column_kind::partition_key)
                    .with_column("v1", bytes_type)
                    .build();

            auto s1 = schema_builder("ks", "table")
                    .with_column("pk", bytes_type, column_kind::partition_key)
                    .with_column("v1", bytes_type)
                    .with_column("v3", bytes_type)
                    .build();

            auto s2 = schema_builder("ks", "table", std::make_optional(s1->id()))
                    .with_column("pk", bytes_type, column_kind::partition_key)
                    .with_column("v1", bytes_type)
                    .with_column("v2", bytes_type)
                    .build();

            {
                auto group0_guard = mm.start_group0_operation().get();
                auto ts = group0_guard.write_timestamp();
                mm.announce(service::prepare_new_column_family_announcement(mm.get_storage_proxy(), s1, ts).get(), std::move(group0_guard), "").get();
            }
            auto old_version = e.db().local().find_schema(s1->id())->version();

            // Apply s0 -> s2 change.
            {
                auto group0_guard = mm.start_group0_operation().get();
                auto&& keyspace = e.db().local().find_keyspace(s0->ks_name()).metadata();
                auto muts = db::schema_tables::make_update_table_mutations(e.db().local(), keyspace, s0, s2,
                        group0_guard.write_timestamp());
                mm.announce(std::move(muts), std::move(group0_guard), "").get();
            }

            auto new_schema = e.db().local().find_schema(s1->id());

            BOOST_REQUIRE(new_schema->get_column_definition(to_bytes("v1")) != nullptr);
            BOOST_REQUIRE(new_schema->get_column_definition(to_bytes("v2")) != nullptr);
            BOOST_REQUIRE(new_schema->get_column_definition(to_bytes("v3")) != nullptr);

            BOOST_REQUIRE(new_schema->version() != old_version);

            // With group 0 schema changes and GROUP0_SCHEMA_VERSIONING, this check wouldn't pass,
            // because the version resulting after schema change is not a digest, but taken to be
            // the version sent in the schema change mutations; in this case, `make_update_table_mutations`
            // takes `s2->version()`.
            //
            // This is fine with group 0 where all schema changes are linearized, so this scenario
            // of merging concurrent schema changes doesn't happen.
            //
            // Hence we use `run_with_raft_recovery_config()` in this test.
            BOOST_REQUIRE(new_schema->version() != s2->version());
        });
    }, run_with_raft_recovery_config());
}

SEASTAR_TEST_CASE(test_sort_type_in_update) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        service::migration_manager& mm = e.migration_manager().local();
        auto group0_guard = mm.start_group0_operation().get();
        auto ts = group0_guard.write_timestamp();

        auto&& keyspace = e.db().local().find_keyspace("ks").metadata();

        auto type1 = user_type_impl::get_instance("ks", to_bytes("type1"), {}, {}, true);
        auto muts1 = db::schema_tables::make_create_type_mutations(keyspace, type1, ts);

        auto type3 = user_type_impl::get_instance("ks", to_bytes("type3"), {}, {}, true);
        auto muts3 = db::schema_tables::make_create_type_mutations(keyspace, type3, ts);

        // type2 must be created after type1 and type3. This tests that announce sorts them.
        auto type2 = user_type_impl::get_instance("ks", to_bytes("type2"), {"field1", "field3"}, {type1, type3}, true);
        auto muts2 = db::schema_tables::make_create_type_mutations(keyspace, type2, ts);

        auto muts = muts2;
        muts.insert(muts.end(), muts1.begin(), muts1.end());
        muts.insert(muts.end(), muts3.begin(), muts3.end());
        mm.announce(std::move(muts), std::move(group0_guard), "").get();
    });
}

SEASTAR_TEST_CASE(test_column_is_dropped) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            e.execute_cql("create keyspace tests with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };").get();
            e.execute_cql("create table tests.table1 (pk int primary key, c1 int, c2 int);").get();
            e.execute_cql("alter table tests.table1 drop c2;").get();
            e.execute_cql("alter table tests.table1 add s1 int;").get();

            schema_ptr s = e.db().local().find_schema("tests", "table1");
            BOOST_REQUIRE(s->columns_by_name().contains(to_bytes("c1")));
            BOOST_REQUIRE(!s->columns_by_name().contains(to_bytes("c2")));
            BOOST_REQUIRE(s->columns_by_name().contains(to_bytes("s1")));
        });
    });
}

SEASTAR_TEST_CASE(test_static_column_is_dropped) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("create keyspace tests with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };").get();
        e.execute_cql("create table tests.table1 (pk int, c1 int, c2 int static, primary key (pk, c1));").get();

        e.execute_cql("alter table tests.table1 drop c2;").get();
        e.execute_cql("alter table tests.table1 add s1 int static;").get();
        schema_ptr s = e.db().local().find_schema("tests", "table1");
        BOOST_REQUIRE(s->columns_by_name().contains(to_bytes("c1")));
        BOOST_REQUIRE(!s->columns_by_name().contains(to_bytes("c2")));
        BOOST_REQUIRE(s->columns_by_name().contains(to_bytes("s1")));

        e.execute_cql("alter table tests.table1 drop s1;").get();
        s = e.db().local().find_schema("tests", "table1");
        BOOST_REQUIRE(s->columns_by_name().contains(to_bytes("c1")));
        BOOST_REQUIRE(!s->columns_by_name().contains(to_bytes("c2")));
        BOOST_REQUIRE(!s->columns_by_name().contains(to_bytes("s1")));
    });
}

SEASTAR_TEST_CASE(test_multiple_columns_add_and_drop) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("create keyspace tests with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };").get();
        e.execute_cql("create table tests.table1 (pk int primary key, c1 int, c2 int, c3 int);").get();

        e.execute_cql("alter table tests.table1 drop (c2);").get();
        e.execute_cql("alter table tests.table1 add (s1 int);").get();
        schema_ptr s = e.db().local().find_schema("tests", "table1");
        BOOST_REQUIRE(s->columns_by_name().contains(to_bytes("c1")));
        BOOST_REQUIRE(!s->columns_by_name().contains(to_bytes("c2")));
        BOOST_REQUIRE(s->columns_by_name().contains(to_bytes("c3")));
        BOOST_REQUIRE(s->columns_by_name().contains(to_bytes("s1")));

        e.execute_cql("alter table tests.table1 drop (c1, c3);").get();
        e.execute_cql("alter table tests.table1 add (s2 int, s3 int);").get();
        s = e.db().local().find_schema("tests", "table1");
        BOOST_REQUIRE(!s->columns_by_name().contains(to_bytes("c1")));
        BOOST_REQUIRE(!s->columns_by_name().contains(to_bytes("c2")));
        BOOST_REQUIRE(!s->columns_by_name().contains(to_bytes("c3")));
        BOOST_REQUIRE(s->columns_by_name().contains(to_bytes("s1")));
        BOOST_REQUIRE(s->columns_by_name().contains(to_bytes("s2")));
        BOOST_REQUIRE(s->columns_by_name().contains(to_bytes("s3")));
    });
}

SEASTAR_TEST_CASE(test_multiple_static_columns_add_and_drop) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("create keyspace tests with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };").get();
        e.execute_cql("create table tests.table1 (pk int, c1 int, c2 int static, c3 int, primary key(pk, c1));").get();

        e.execute_cql("alter table tests.table1 drop (c2);").get();
        e.execute_cql("alter table tests.table1 add (s1 int static);").get();
        schema_ptr s = e.db().local().find_schema("tests", "table1");
        BOOST_REQUIRE(s->columns_by_name().contains(to_bytes("c1")));
        BOOST_REQUIRE(!s->columns_by_name().contains(to_bytes("c2")));
        BOOST_REQUIRE(s->columns_by_name().contains(to_bytes("c3")));
        BOOST_REQUIRE(s->columns_by_name().contains(to_bytes("s1")));

        e.execute_cql("alter table tests.table1 drop (c3, s1);").get();
        e.execute_cql("alter table tests.table1 add (s2 int, s3 int static);").get();
        s = e.db().local().find_schema("tests", "table1");
        BOOST_REQUIRE(s->columns_by_name().contains(to_bytes("c1")));
        BOOST_REQUIRE(!s->columns_by_name().contains(to_bytes("c2")));
        BOOST_REQUIRE(!s->columns_by_name().contains(to_bytes("c3")));
        BOOST_REQUIRE(!s->columns_by_name().contains(to_bytes("s1")));
        BOOST_REQUIRE(s->columns_by_name().contains(to_bytes("s2")));
        BOOST_REQUIRE(s->columns_by_name().contains(to_bytes("s3")));
    });
}

SEASTAR_TEST_CASE(test_combined_column_add_and_drop) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            service::migration_manager& mm = e.migration_manager().local();

            e.execute_cql("create keyspace tests with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };").get();

            auto s1 = schema_builder("ks", "table1")
                    .with_column("pk", bytes_type, column_kind::partition_key)
                    .with_column("v1", bytes_type)
                    .build();

            {
                auto group0_guard = mm.start_group0_operation().get();
                auto ts = group0_guard.write_timestamp();
                mm.announce(service::prepare_new_column_family_announcement(mm.get_storage_proxy(), s1, ts).get(), std::move(group0_guard), "").get();
            }

            auto&& keyspace = e.db().local().find_keyspace(s1->ks_name()).metadata();

            auto s2 = schema_builder("ks", "table1", std::make_optional(s1->id()))
                    .with_column("pk", bytes_type, column_kind::partition_key)
                    .without_column("v1", bytes_type, api::new_timestamp())
                    .build();

            // Drop v1
            {
                auto group0_guard = mm.start_group0_operation().get();
                auto muts = db::schema_tables::make_update_table_mutations(e.db().local(), keyspace, s1, s2,
                    group0_guard.write_timestamp());
                mm.announce(std::move(muts), std::move(group0_guard), "").get();
            }

            // Add a new v1 and drop it
            {
                auto s3 = schema_builder("ks", "table1", std::make_optional(s1->id()))
                        .with_column("pk", bytes_type, column_kind::partition_key)
                        .with_column("v1", list_type_impl::get_instance(int32_type, true))
                        .build();

                auto s4 = schema_builder("ks", "table1", std::make_optional(s1->id()))
                        .with_column("pk", bytes_type, column_kind::partition_key)
                        .without_column("v1", list_type_impl::get_instance(int32_type, true), api::new_timestamp())
                        .build();

                auto group0_guard = mm.start_group0_operation().get();
                auto muts = db::schema_tables::make_update_table_mutations(e.db().local(), keyspace, s3, s4,
                    group0_guard.write_timestamp());
                mm.announce(std::move(muts), std::move(group0_guard), "").get();
            }

            auto new_schema = e.db().local().find_schema(s1->id());
            BOOST_REQUIRE(new_schema->get_column_definition(to_bytes("v1")) == nullptr);

            assert_that_failed(e.execute_cql("alter table ks.table1 add v1 list<text>;"));
        });
    });
}

// Tests behavior when incompatible CREATE TABLE statements are
// issued concurrently in the cluster.
// Relevant only for the pre-raft schema management, with raft the scenario should not happen.
SEASTAR_TEST_CASE(test_concurrent_table_creation_with_different_schema) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            service::migration_manager& mm = e.migration_manager().local();

            e.execute_cql("create keyspace tests with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };").get();

            auto s1 = schema_builder("ks", "table1")
                    .with_column("pk1", bytes_type, column_kind::partition_key)
                    .with_column("pk2", bytes_type, column_kind::partition_key)
                    .with_column("v1", bytes_type)
                    .build();

            auto s2 = schema_builder("ks", "table1")
                    .with_column("pk1", bytes_type, column_kind::partition_key)
                    .with_column("pk3", bytes_type, column_kind::partition_key)
                    .with_column("v1", bytes_type)
                    .build();

            auto ann1 = service::prepare_new_column_family_announcement(mm.get_storage_proxy(), s1, api::new_timestamp()).get();
            auto ann2 = service::prepare_new_column_family_announcement(mm.get_storage_proxy(), s2, api::new_timestamp()).get();

            {
                auto group0_guard = mm.start_group0_operation().get();
                mm.announce(std::move(ann1), std::move(group0_guard), "").get();
            }

            {
                auto group0_guard = mm.start_group0_operation().get();
                mm.announce(std::move(ann2), std::move(group0_guard), "").get();
            }

            auto&& keyspace = e.db().local().find_keyspace(s1->ks_name()).metadata();

            // s2 should win because it has higher timestamp
            auto new_schema = e.db().local().find_schema(s2->id());
            BOOST_REQUIRE(*new_schema == *s2);
        });
    });
}

SEASTAR_TEST_CASE(test_merging_does_not_alter_tables_which_didnt_change) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            service::migration_manager& mm = e.migration_manager().local();

            auto&& keyspace = e.db().local().find_keyspace("ks").metadata();

            auto s0 = schema_builder("ks", "table1")
                .with_column("pk", bytes_type, column_kind::partition_key)
                .with_column("v1", bytes_type)
                .build();

            auto find_table = [&] () -> replica::column_family& {
                return e.db().local().find_column_family("ks", "table1");
            };

            std::vector<mutation> muts1;
            {
                auto group0_guard = mm.start_group0_operation().get();
                muts1 = db::schema_tables::make_create_table_mutations(s0, group0_guard.write_timestamp());
                mm.announce(muts1, std::move(group0_guard), "").get();
            }

            auto s1 = find_table().schema();

            auto legacy_version = s1->version();

            {
                auto group0_guard = mm.start_group0_operation().get();
                mm.announce(muts1, std::move(group0_guard), "").get();
            }

            BOOST_REQUIRE(s1 == find_table().schema());
            BOOST_REQUIRE_EQUAL(legacy_version, find_table().schema()->version());

            {
                auto group0_guard = mm.start_group0_operation().get();
                auto muts2 = muts1;
                muts2.push_back(db::schema_tables::make_scylla_tables_mutation(s0, group0_guard.write_timestamp()));
                mm.announce(muts2, std::move(group0_guard), "").get();
            }

            BOOST_REQUIRE(s1 == find_table().schema());
            BOOST_REQUIRE_EQUAL(legacy_version, find_table().schema()->version());
        });
    });
}

SEASTAR_TEST_CASE(test_merging_creates_a_table_even_if_keyspace_was_recreated) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            service::migration_manager& mm = e.migration_manager().local();

            auto&& keyspace = e.db().local().find_keyspace("ks").metadata();

            auto s0 = schema_builder("ks", "table1")
                .with_column("pk", bytes_type, column_kind::partition_key)
                .with_column("v1", bytes_type)
                .build();

            auto find_table = [&] () -> replica::column_family& {
                return e.db().local().find_column_family("ks", "table1");
            };

            std::vector<mutation> all_muts;

            {
                auto group0_guard = mm.start_group0_operation().get();
                const auto ts = group0_guard.write_timestamp();
                auto muts = service::prepare_keyspace_drop_announcement(e.local_db(), "ks", ts).get();
                boost::copy(muts, std::back_inserter(all_muts));
                mm.announce(muts, std::move(group0_guard), "").get();
            }

            {
                auto group0_guard = mm.start_group0_operation().get();
                const auto ts = group0_guard.write_timestamp();

                // all_muts contains keyspace drop.
                auto muts = service::prepare_new_keyspace_announcement(e.db().local(), keyspace, ts);
                boost::copy(muts, std::back_inserter(all_muts));
                mm.announce(muts, std::move(group0_guard), "").get();
            }

            {
                auto group0_guard = mm.start_group0_operation().get();
                const auto ts = group0_guard.write_timestamp();

                auto muts = service::prepare_new_column_family_announcement(mm.get_storage_proxy(), s0, ts).get();
                boost::copy(muts, std::back_inserter(all_muts));

                mm.announce(all_muts, std::move(group0_guard), "").get();
            }

            auto s1 = find_table().schema();
            BOOST_REQUIRE(s1 == find_table().schema());
        });
    });
}

class counting_migration_listener : public service::migration_listener {
public:
    int create_keyspace_count = 0;
    int create_column_family_count = 0;
    int create_user_type_count = 0;
    int create_function_count = 0;
    int create_aggregate_count = 0;
    int create_view_count = 0;
    int update_keyspace_count = 0;
    int update_column_family_count = 0;
    int columns_changed_count = 0;
    int update_user_type_count = 0;
    int update_function_count = 0;
    int update_aggregate_count = 0;
    int update_view_count = 0;
    int update_tablets = 0;
    int drop_keyspace_count = 0;
    int drop_column_family_count = 0;
    int drop_user_type_count = 0;
    int drop_function_count = 0;
    int drop_aggregate_count = 0;
    int drop_view_count = 0;
public:
    virtual void on_create_keyspace(const sstring&) override { ++create_keyspace_count; }
    virtual void on_create_column_family(const sstring&, const sstring&) override { ++create_column_family_count; }
    virtual void on_create_user_type(const sstring&, const sstring&) override { ++create_user_type_count; }
    virtual void on_create_function(const sstring&, const sstring&) override { ++create_function_count; }
    virtual void on_create_aggregate(const sstring&, const sstring&) override { ++create_aggregate_count; }
    virtual void on_create_view(const sstring&, const sstring&) override { ++create_view_count; }
    virtual void on_update_keyspace(const sstring&) override { ++update_keyspace_count; }
    virtual void on_update_column_family(const sstring&, const sstring&, bool columns_changed) override {
        ++update_column_family_count;
        columns_changed_count += int(columns_changed);
    }
    virtual void on_update_user_type(const sstring&, const sstring&) override { ++update_user_type_count; }
    virtual void on_update_function(const sstring&, const sstring&) override { ++update_function_count; }
    virtual void on_update_aggregate(const sstring&, const sstring&) override { ++update_aggregate_count; }
    virtual void on_update_view(const sstring&, const sstring&, bool) override { ++update_view_count; }
    virtual void on_update_tablet_metadata(const locator::tablet_metadata_change_hint&) override { ++update_tablets; }
    virtual void on_drop_keyspace(const sstring&) override { ++drop_keyspace_count; }
    virtual void on_drop_column_family(const sstring&, const sstring&) override { ++drop_column_family_count; }
    virtual void on_drop_user_type(const sstring&, const sstring&) override { ++drop_user_type_count; }
    virtual void on_drop_function(const sstring&, const sstring&) override { ++drop_function_count; }
    virtual void on_drop_aggregate(const sstring&, const sstring&) override { ++drop_aggregate_count; }
    virtual void on_drop_view(const sstring&, const sstring&) override { ++drop_view_count; }
};

SEASTAR_TEST_CASE(test_alter_nested_type) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("CREATE TYPE foo (foo_k int);").get();
        e.execute_cql("CREATE TYPE bar (bar_k frozen<foo>);").get();
        e.execute_cql("alter type foo add zed_v int;").get();
        e.execute_cql("CREATE TABLE tbl (key int PRIMARY KEY, val frozen<bar>);").get();
        e.execute_cql("insert into tbl (key, val) values (1, {bar_k: {foo_k: 2, zed_v: 3} });").get();
    });
}

SEASTAR_TEST_CASE(test_nested_type_mutation_in_update) {
    // ALTER TYPE always creates a mutation with a single type. This
    // creates a mutation with 2 types, one nested in the other, to
    // show that we can handle that.
    return do_with_cql_env_thread([](cql_test_env& e) {
        counting_migration_listener listener;
        e.local_mnotifier().register_listener(&listener);

        e.execute_cql("CREATE TYPE foo (foo_k int);").get();
        e.execute_cql("CREATE TYPE bar (bar_k frozen<foo>);").get();

        BOOST_REQUIRE_EQUAL(listener.create_user_type_count, 2);

        service::migration_manager& mm = e.migration_manager().local();
        auto group0_guard = mm.start_group0_operation().get();
        auto ts = group0_guard.write_timestamp();
        auto&& keyspace = e.db().local().find_keyspace("ks").metadata();

        auto type1 = user_type_impl::get_instance("ks", to_bytes("foo"), {"foo_k", "extra"}, {int32_type, int32_type}, true);
        auto muts1 = db::schema_tables::make_create_type_mutations(keyspace, type1, ts);

        auto type2 = user_type_impl::get_instance("ks", to_bytes("bar"), {"bar_k", "extra"}, {type1, int32_type}, true);
        auto muts2 = db::schema_tables::make_create_type_mutations(keyspace, type2, ts);

        auto muts = muts1;
        muts.insert(muts.end(), muts2.begin(), muts2.end());
        mm.announce(std::move(muts), std::move(group0_guard), "").get();

        BOOST_REQUIRE_EQUAL(listener.create_user_type_count, 2);
        BOOST_REQUIRE_EQUAL(listener.update_user_type_count, 2);
    });
}

SEASTAR_TEST_CASE(test_notifications) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            counting_migration_listener listener;
            e.local_mnotifier().register_listener(&listener);
            auto listener_lease = defer([&e, &listener] { e.local_mnotifier().register_listener(&listener); });

            e.execute_cql("create keyspace tests with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };").get();

            BOOST_REQUIRE_EQUAL(listener.create_keyspace_count, 1);

            e.execute_cql("create table tests.table1 (pk int primary key, c1 int, c2 int);").get();

            BOOST_REQUIRE_EQUAL(listener.create_column_family_count, 1);
            BOOST_REQUIRE_EQUAL(listener.columns_changed_count, 0);

            e.execute_cql("alter table tests.table1 drop c2;").get();

            BOOST_REQUIRE_EQUAL(listener.update_column_family_count, 1);
            BOOST_REQUIRE_EQUAL(listener.columns_changed_count, 1);

            e.execute_cql("alter table tests.table1 add s1 int;").get();

            BOOST_REQUIRE_EQUAL(listener.update_column_family_count, 2);
            BOOST_REQUIRE_EQUAL(listener.columns_changed_count, 2);

            e.execute_cql("alter table tests.table1 alter s1 type blob;").get();

            BOOST_REQUIRE_EQUAL(listener.update_column_family_count, 3);
            BOOST_REQUIRE_EQUAL(listener.columns_changed_count, 3);

            e.execute_cql("drop table tests.table1;").get();

            BOOST_REQUIRE_EQUAL(listener.drop_column_family_count, 1);

            e.execute_cql("create type tests.type1 (field1 text, field2 text);").get();

            BOOST_REQUIRE_EQUAL(listener.create_user_type_count, 1);

            e.execute_cql("drop type tests.type1;").get();

            BOOST_REQUIRE_EQUAL(listener.drop_user_type_count, 1);

            e.execute_cql("create type tests.type1 (field1 text, field2 text);").get();
            e.execute_cql("create type tests.type2 (field1 text, field2 text);").get();

            BOOST_REQUIRE_EQUAL(listener.create_user_type_count, 3);

            e.execute_cql("drop type tests.type1;").get();

            BOOST_REQUIRE_EQUAL(listener.drop_user_type_count, 2);

            e.execute_cql("alter type tests.type2 add field3 text;").get();

            BOOST_REQUIRE_EQUAL(listener.update_user_type_count, 1);

            e.execute_cql("alter type tests.type2 alter field3 type blob;").get();

            BOOST_REQUIRE_EQUAL(listener.update_user_type_count, 2);

            e.execute_cql("alter type tests.type2 rename field2 to field4 and field3 to field5;").get();

            BOOST_REQUIRE_EQUAL(listener.update_user_type_count, 3);
        });
    });
}

SEASTAR_TEST_CASE(test_drop_user_type_in_use) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("create type simple_type (user_number int);").get();
        e.execute_cql("create table simple_table (key int primary key, val frozen<simple_type>);").get();
        e.execute_cql("insert into simple_table (key, val) values (42, {user_number: 1});").get();
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("drop type simple_type;").get(), exceptions::invalid_request_exception,
                exception_predicate::message_equals("Cannot drop user type ks.simple_type as it is still used by table ks.simple_table"));
    });
}

SEASTAR_TEST_CASE(test_drop_nested_user_type_in_use) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("create type simple_type (user_number int);").get();
        e.execute_cql("create table nested_table (key int primary key, val tuple<int, frozen<simple_type>>);").get();
        e.execute_cql("insert into nested_table (key, val) values (42, (41, {user_number: 1}));").get();
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("drop type simple_type;").get(), exceptions::invalid_request_exception,
                exception_predicate::message_equals(
                        "Cannot drop user type ks.simple_type as it is still used by table ks.nested_table"));
    });
}

SEASTAR_TEST_CASE(test_prepared_statement_is_invalidated_by_schema_change) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            logging::logger_registry().set_logger_level("query_processor", logging::log_level::debug);
            e.execute_cql("create keyspace tests with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };").get();
            e.execute_cql("create table tests.table1 (pk int primary key, c1 int, c2 int);").get();
            auto id = e.prepare("select * from tests.table1;").get();

            e.execute_cql("alter table tests.table1 add s1 int;").get();

            try {
                e.execute_prepared(id, {}).get();
                BOOST_FAIL("Should have failed");
            } catch (const not_prepared_exception&) {
                // expected
            }
        });
    });
}

// We don't want schema digest to change between Scylla versions because that results in a schema disagreement
// during rolling upgrade.
// This test is *not* supposed to check that the schema does not change.
// It only checks that the digest itself does not change *given* that the schema does not change.
// If the schema changes, the digest will change too (as expected), which will cause the test
// to fail unless you regenerate the test data. That's by design of the test.
// Note that changing the schema may introduce rolling upgrade problems, e.g. if changing the schema
// on an upgraded node doesn't force other nodes to follow-up. This test is not intended to catch such bugs.
// To test that rolling upgrade works in case of schema changes, we need to actually run two different
// versions of Scylla, and that cannot be done in a unit test.
// See also #6582.
future<> test_schema_digest_does_not_change_with_disabled_features(sstring data_dir,
        std::set<sstring> disabled_features, std::vector<utils::UUID> expected_digests,
        std::function<void(cql_test_env& e)> extra_schema_changes,
        std::shared_ptr<db::extensions> extensions = std::make_shared<db::extensions>()) {
    using namespace db;
    using namespace db::schema_tables;

    auto tmp = tmpdir();
    // NOTICE: Regenerating data for this test may be necessary when a system table is added.
    // This test uses pre-generated sstables and relies on the fact that they are up to date
    // with the current system schema. If it is not, the schema will be updated, which will cause
    // new timestamps to appear and schema digests will not match anymore.
    // Warning: if you regenerate the data (and digests), please make sure that you don't accidentally
    // hide a digest calculation bug. Separate commits that touch the schema from commits which
    // could potentially modify the digest calculation algorithm (for example). And DO test whether
    // rolling upgrade works.
    const bool regenerate = false;

    auto db_cfg_ptr = ::make_shared<db::config>(std::move(extensions));
    auto& db_cfg = *db_cfg_ptr;
    db_cfg.enable_user_defined_functions({true}, db::config::config_source::CommandLine);
    db_cfg.experimental_features({experimental_features_t::feature::UDF, experimental_features_t::feature::KEYSPACE_STORAGE_OPTIONS}, db::config::config_source::CommandLine);
    std::unordered_map<sstring, s3::endpoint_config> so_config;
    so_config["localhost"] = s3::endpoint_config{};
    db_cfg.object_storage_config.set(std::move(so_config));
    if (regenerate) {
        db_cfg.data_file_directories({data_dir}, db::config::config_source::CommandLine);
    } else {
        fs::copy(std::string(data_dir), std::string(tmp.path().string()), fs::copy_options::recursive);
        db_cfg.data_file_directories({tmp.path().string()}, db::config::config_source::CommandLine);
    }
    cql_test_config cfg_in(db_cfg_ptr);
    cfg_in.disabled_features = std::move(disabled_features);
    // Copying the data directory makes the node incorrectly think it restarts. Then,
    // after noticing it is not a part of group 0, the node would start the raft upgrade
    // procedure if we didn't run it in the raft RECOVERY mode. This procedure would get
    // stuck because it depends on messaging being enabled even if the node communicates
    // only with itself and messaging is disabled in boost tests.
    cfg_in.run_with_raft_recovery = true;

    return do_with_cql_env_thread([expected_digests = std::move(expected_digests), extra_schema_changes = std::move(extra_schema_changes)] (cql_test_env& e) {
        if (regenerate) {
            // Exercise many different kinds of schema changes.
            e.execute_cql(
                "create keyspace tests with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };").get();
            e.execute_cql("create table tests.table1 (pk int primary key, c1 int, c2 int);").get();
            e.execute_cql("create type tests.basic_info (c1 timestamp, v2 text);").get();
            e.execute_cql("create index on tests.table1 (c1);").get();
            e.execute_cql("create table ks.tbl (a int, b int, c float, PRIMARY KEY (a))").get();
            e.execute_cql(
                "create materialized view ks.tbl_view AS SELECT c FROM ks.tbl WHERE c IS NOT NULL PRIMARY KEY (c, a)").get();
            e.execute_cql(
                "create materialized view ks.tbl_view_2 AS SELECT a FROM ks.tbl WHERE a IS NOT NULL PRIMARY KEY (a)").get();
            e.execute_cql(
                "create keyspace tests2 with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };").get();
            e.execute_cql("drop keyspace tests2;").get();
            extra_schema_changes(e);
        }

        auto expect_digest = [&] (schema_features sf, utils::UUID expected) {
            // Changes in the system_distributed keyspace was a common source of regenerating this test case.
            // It was not the original idea of this test to regenerate after each change in the distributed
            // system keyspace, since these changes are propagated naturally on their own anyway - the schema
            // with highest timestamp will win and be sent to other nodes.
            // Thus, system_distributed.* tables are officially not taken into account,
            // which makes it less likely that this test case would need to be needlessly regenerated.
            auto actual = calculate_schema_digest(e.get_storage_proxy(), sf, std::not_fn(&is_internal_keyspace)).get();
            if (regenerate) {
                std::cout << format("        utils::UUID(\"{}\"),", actual) << "\n";
            } else {
                BOOST_REQUIRE_EQUAL(actual.uuid(), expected);
            }
        };

        auto expect_version = [&] (sstring ks_name, sstring cf_name, utils::UUID expected) {
            auto actual = e.local_db().find_column_family(ks_name, cf_name).schema()->version();
            if (regenerate) {
                std::cout << format("        utils::UUID(\"{}\"),", actual) << "\n";
            } else {
                BOOST_REQUIRE_EQUAL(actual.uuid(), expected);
            }
        };

        schema_features sf = schema_features::of<schema_feature::DIGEST_INSENSITIVE_TO_EXPIRY>();

        expect_digest(sf, expected_digests[0]);

        sf = schema_features::full();
        sf.remove<schema_feature::SCYLLA_KEYSPACES>();
        expect_digest(sf, expected_digests[1]);

        // Causes tombstones to become expired
        // This is in order to test that schema disagreement doesn't form due to expired tombstones being collected
        // Refs https://github.com/scylladb/scylla/issues/4485
        forward_jump_clocks(std::chrono::seconds(60*60*24*31));

        expect_digest(sf, expected_digests[2]);

        expect_version("tests", "table1", expected_digests[3]);
        expect_version("ks", "tbl", expected_digests[4]);
        expect_version("ks", "tbl_view", expected_digests[5]);
        expect_version("ks", "tbl_view_2", expected_digests[6]);

        // Check that system_schema.scylla_keyspaces info is taken into account
        sf = schema_features::full();
        expect_digest(sf, expected_digests[7]);

    }, cfg_in).then([tmp = std::move(tmp)] {});
}

SEASTAR_TEST_CASE(test_schema_digest_does_not_change_without_digest_feature) {
    std::vector<utils::UUID> expected_digests{
        utils::UUID("d2035515-b299-3265-b920-7dbe5306e72a"),
        utils::UUID("de49e92f-a00d-3f24-8779-d07de26708cb"),
        utils::UUID("de49e92f-a00d-3f24-8779-d07de26708cb"),
        utils::UUID("bbf064ed-0a98-3c19-8c4a-5c929cabd936"),
        utils::UUID("0db2a3f8-6779-388f-951d-de6a537789a7"),
        utils::UUID("21a89984-ffc6-325d-b818-66fc29da51e7"),
        utils::UUID("e69a05e8-80a6-3e8f-bd34-1b5837374c79"),
        utils::UUID("de49e92f-a00d-3f24-8779-d07de26708cb"),
    };
    return test_schema_digest_does_not_change_with_disabled_features("./test/resource/sstables/schema_digest_test",
            std::set<sstring>{"COMPUTED_COLUMNS", "CDC", "KEYSPACE_STORAGE_OPTIONS", "TABLE_DIGEST_INSENSITIVE_TO_EXPIRY"},
            std::move(expected_digests), [] (cql_test_env& e) {});
}

SEASTAR_TEST_CASE(test_schema_digest_does_not_change_after_computed_columns_without_digest_feature) {
    std::vector<utils::UUID> expected_digests{
        utils::UUID("fa2e7735-7604-3202-8ce9-399996305aca"),
        utils::UUID("94606636-ae43-3e0a-b238-e7f0e33ef600"),
        utils::UUID("94606636-ae43-3e0a-b238-e7f0e33ef600"),
        utils::UUID("13d418e8-968e-3bd8-801a-6df5e5327d44"),
        utils::UUID("75808956-93e2-331a-96cc-8dc9cdea79ca"),
        utils::UUID("7eccb793-c6f4-3e84-ae35-788d03188baf"),
        utils::UUID("467deb84-d7de-36cb-a1fa-f3672cb66340"),
        utils::UUID("94606636-ae43-3e0a-b238-e7f0e33ef600"),
    };
    return test_schema_digest_does_not_change_with_disabled_features("./test/resource/sstables/schema_digest_test_computed_columns",
            std::set<sstring>{"CDC", "KEYSPACE_STORAGE_OPTIONS", "TABLE_DIGEST_INSENSITIVE_TO_EXPIRY"}, std::move(expected_digests), [] (cql_test_env& e) {});
}

SEASTAR_TEST_CASE(test_schema_digest_does_not_change_with_functions_without_digest_feature) {
    std::vector<utils::UUID> expected_digests{
        utils::UUID("649bf7ec-fd64-3ccb-adde-3887fc1432be"),
        utils::UUID("48fd0c1b-9777-34be-8c16-187c6ab55cfc"),
        utils::UUID("48fd0c1b-9777-34be-8c16-187c6ab55cfc"),
        utils::UUID("6c7bef38-05b7-3bf3-a52c-352333da7214"),
        utils::UUID("cb23910d-ced8-35e5-99f3-57f362860f3c"),
        utils::UUID("b0ecf791-0637-34a5-a940-bc217517f1aa"),
        utils::UUID("47b87dfc-3c18-324b-b280-58300ac5d3ca"),
        utils::UUID("48fd0c1b-9777-34be-8c16-187c6ab55cfc"),
    };
    return test_schema_digest_does_not_change_with_disabled_features(
        "./test/resource/sstables/schema_digest_with_functions_test",
        std::set<sstring>{"CDC", "KEYSPACE_STORAGE_OPTIONS", "TABLE_DIGEST_INSENSITIVE_TO_EXPIRY"},
        std::move(expected_digests),
        [] (cql_test_env& e) {
            e.execute_cql("create function twice(val int) called on null input returns int language lua as 'return 2 * val';").get();
            e.execute_cql("create function my_add(a int, b int) called on null input returns int language lua as 'return a + b';").get();
        });
}

SEASTAR_TEST_CASE(test_schema_digest_does_not_change_with_cdc_options_without_digest_feature) {
    auto ext = std::make_shared<db::extensions>();
    ext->add_schema_extension<cdc::cdc_extension>(cdc::cdc_extension::NAME);
    std::vector<utils::UUID> expected_digests{
        utils::UUID("ae9f0511-1c1d-3566-a36f-8e1c8abc66fc"),
        utils::UUID("09899769-4e7f-3119-9769-e3db3d99455b"),
        utils::UUID("09899769-4e7f-3119-9769-e3db3d99455b"),
        utils::UUID("265be25f-b268-3f43-a54d-9c6e379a901d"),
        utils::UUID("c604f5c9-988e-393f-b9d8-2ed55b9a540c"),
        utils::UUID("45d9f25d-58a1-3f1e-85a1-f09d82c52588"),
        utils::UUID("7ef45dd2-aab9-38f1-bcc6-ba9c94666e36"),
        utils::UUID("09899769-4e7f-3119-9769-e3db3d99455b"),
    };
    return test_schema_digest_does_not_change_with_disabled_features(
        "./test/resource/sstables/schema_digest_test_cdc_options",
        std::set<sstring>{"KEYSPACE_STORAGE_OPTIONS", "TABLE_DIGEST_INSENSITIVE_TO_EXPIRY"},
        std::move(expected_digests),
        [] (cql_test_env& e) {
            e.execute_cql("create table tests.table_cdc (pk int primary key, c1 int, c2 int) with cdc = {'enabled':'true'};").get();
        },
        std::move(ext));
}

SEASTAR_TEST_CASE(test_schema_digest_does_not_change_with_keyspace_storage_options_without_digest_feature) {
    std::vector<utils::UUID> expected_digests{
        utils::UUID("30e2cf99-389d-381f-82b9-3fcdcf66a1fb"),
        utils::UUID("98d63879-6633-3708-880e-8716fcbadda0"),
        utils::UUID("98d63879-6633-3708-880e-8716fcbadda0"),
        utils::UUID("f4ca70f9-170c-3a69-a274-76e711d2841e"),
        utils::UUID("cbf9aa1e-2488-3485-8c28-e42bf42a2dcf"),
        utils::UUID("67c0db2d-8fd6-30f4-beee-f15a80a889fd"),
        utils::UUID("9c5c996a-6b27-346e-96d4-26d545d4601a"),
        utils::UUID("3fc03c97-8010-3746-8cea-e8b9ac27fe4e"),
    };
    return test_schema_digest_does_not_change_with_disabled_features(
        "./test/resource/sstables/schema_digest_test_keyspace_storage_options",
        std::set<sstring>{"TABLE_DIGEST_INSENSITIVE_TO_EXPIRY"},
        std::move(expected_digests),
        [] (cql_test_env& e) {
            e.execute_cql("create keyspace tests_s3 with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }"
                    " and storage = { 'type': 'S3', 'bucket': 'b1', 'endpoint': 'localhost' };").get();
            e.execute_cql("create table tests_s3.table1 (pk int primary key, c1 int, c2 int)").get();
        });
}
SEASTAR_TEST_CASE(test_schema_digest_does_not_change) {
    std::vector<utils::UUID> expected_digests{
        utils::UUID("d2035515-b299-3265-b920-7dbe5306e72a"),
        utils::UUID("de49e92f-a00d-3f24-8779-d07de26708cb"),
        utils::UUID("de49e92f-a00d-3f24-8779-d07de26708cb"),
        utils::UUID("75550bef-3a95-3901-be80-4540f7d8a311"),
        utils::UUID("aff80a2a-4a72-35bb-9ac3-f851013610d0"),
        utils::UUID("030100b2-27aa-32f2-8964-0090a1af75f8"),
        utils::UUID("16ba4b2d-7c61-393b-ba51-0890e25f4e22"),
        utils::UUID("de49e92f-a00d-3f24-8779-d07de26708cb"),
    };
    return test_schema_digest_does_not_change_with_disabled_features("./test/resource/sstables/schema_digest_test",
            std::set<sstring>{"COMPUTED_COLUMNS", "CDC", "KEYSPACE_STORAGE_OPTIONS"},
            std::move(expected_digests), [] (cql_test_env& e) {});
}

SEASTAR_TEST_CASE(test_schema_digest_does_not_change_after_computed_columns) {
    std::vector<utils::UUID> expected_digests{
        utils::UUID("fa2e7735-7604-3202-8ce9-399996305aca"),
        utils::UUID("94606636-ae43-3e0a-b238-e7f0e33ef600"),
        utils::UUID("94606636-ae43-3e0a-b238-e7f0e33ef600"),
        utils::UUID("5a89ff92-9b5c-32eb-ad5a-5def856e3024"),
        utils::UUID("26808d79-e22a-3d20-88a7-d812301ff342"),
        utils::UUID("371527f3-2f26-32a6-8b29-bb0ce0735b61"),
        utils::UUID("02ed06b1-c384-3f83-b116-fe94f5bf647a"),
        utils::UUID("94606636-ae43-3e0a-b238-e7f0e33ef600"),
    };
    return test_schema_digest_does_not_change_with_disabled_features("./test/resource/sstables/schema_digest_test_computed_columns",
            std::set<sstring>{"CDC", "KEYSPACE_STORAGE_OPTIONS"}, std::move(expected_digests), [] (cql_test_env& e) {});
}

SEASTAR_TEST_CASE(test_schema_digest_does_not_change_with_functions) {
    std::vector<utils::UUID> expected_digests{
        utils::UUID("649bf7ec-fd64-3ccb-adde-3887fc1432be"),
        utils::UUID("48fd0c1b-9777-34be-8c16-187c6ab55cfc"),
        utils::UUID("48fd0c1b-9777-34be-8c16-187c6ab55cfc"),
        utils::UUID("9b842fb8-2b89-3f9f-a344-f648cb27a226"),
        utils::UUID("e596cbc4-60f8-3788-96e6-fdfb105ba39f"),
        utils::UUID("0f214b9c-81a5-3771-8722-4763ab8fd0ee"),
        utils::UUID("08624ebc-c0d2-3e7a-bcd7-4fcb442626e4"),
        utils::UUID("48fd0c1b-9777-34be-8c16-187c6ab55cfc"),
    };
    return test_schema_digest_does_not_change_with_disabled_features(
        "./test/resource/sstables/schema_digest_with_functions_test",
        std::set<sstring>{"CDC", "KEYSPACE_STORAGE_OPTIONS"},
        std::move(expected_digests),
        [] (cql_test_env& e) {
            e.execute_cql("create function twice(val int) called on null input returns int language lua as 'return 2 * val';").get();
            e.execute_cql("create function my_add(a int, b int) called on null input returns int language lua as 'return a + b';").get();
        });
}

SEASTAR_TEST_CASE(test_schema_digest_does_not_change_with_cdc_options) {
    auto ext = std::make_shared<db::extensions>();
    ext->add_schema_extension<cdc::cdc_extension>(cdc::cdc_extension::NAME);
    std::vector<utils::UUID> expected_digests{
        utils::UUID("ae9f0511-1c1d-3566-a36f-8e1c8abc66fc"),
        utils::UUID("09899769-4e7f-3119-9769-e3db3d99455b"),
        utils::UUID("09899769-4e7f-3119-9769-e3db3d99455b"),
        utils::UUID("fdfdea09-fee9-3fd4-945f-b91a7a2e0e39"),
        utils::UUID("44e79540-5cc5-3617-88c0-267fe7cc2232"),
        utils::UUID("e2b673e7-04c0-37cb-b076-77951f2f5452"),
        utils::UUID("089d5e42-065a-3e19-a608-58a960816c51"),
        utils::UUID("09899769-4e7f-3119-9769-e3db3d99455b"),
    };
    return test_schema_digest_does_not_change_with_disabled_features(
        "./test/resource/sstables/schema_digest_test_cdc_options",
        std::set<sstring>{"KEYSPACE_STORAGE_OPTIONS"},
        std::move(expected_digests),
        [] (cql_test_env& e) {
            e.execute_cql("create table tests.table_cdc (pk int primary key, c1 int, c2 int) with cdc = {'enabled':'true'};").get();
        },
        std::move(ext));
}

SEASTAR_TEST_CASE(test_schema_digest_does_not_change_with_keyspace_storage_options) {
    std::vector<utils::UUID> expected_digests{
        utils::UUID("30e2cf99-389d-381f-82b9-3fcdcf66a1fb"),
        utils::UUID("98d63879-6633-3708-880e-8716fcbadda0"),
        utils::UUID("98d63879-6633-3708-880e-8716fcbadda0"),
        utils::UUID("1f971ee2-42d1-3564-ae89-0090803d6d58"),
        utils::UUID("60444aca-708a-387f-b571-e4c0806ab78d"),
        utils::UUID("11c00de3-d47f-38bd-84f1-0f5e1179a168"),
        utils::UUID("c495feac-b2a4-3c50-91a5-363630f878d6"),
        utils::UUID("3fc03c97-8010-3746-8cea-e8b9ac27fe4e"),
    };
    return test_schema_digest_does_not_change_with_disabled_features(
        "./test/resource/sstables/schema_digest_test_keyspace_storage_options",
        std::set<sstring>{},
        std::move(expected_digests),
        [] (cql_test_env& e) {
            e.execute_cql("create keyspace tests_s3 with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }"
                    " and storage = { 'type': 'S3', 'bucket': 'b1', 'endpoint': 'localhost' };").get();
            e.execute_cql("create table tests_s3.table1 (pk int primary key, c1 int, c2 int)").get();
        });
}

// Regression test, ensuring people don't forget to set the null sharder
// for newly added schema tables.
SEASTAR_TEST_CASE(test_schema_tables_use_null_sharder) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.db().invoke_on_all([] (replica::database& db) {
            {
                auto ks_metadata = db.find_keyspace("system_schema").metadata();
                auto& cf_metadata = ks_metadata->cf_meta_data();
                for (auto [_, s]: cf_metadata) {
                    std::cout << "checking " << s->cf_name() << std::endl;
                    BOOST_REQUIRE_EQUAL(s->get_sharder().shard_count(), 1);
                }
            }

            // There are some other tables which reside in the "system" keyspace
            // but need to use shard 0, too.
            auto ks_metadata = db.find_keyspace("system").metadata();
            auto& cf_metadata = ks_metadata->cf_meta_data();

            auto it = cf_metadata.find("scylla_table_schema_history");
            BOOST_REQUIRE(it != cf_metadata.end());
            BOOST_REQUIRE_EQUAL(it->second->get_sharder().shard_count(), 1);

            it = cf_metadata.find("raft");
            BOOST_REQUIRE(it != cf_metadata.end());
            BOOST_REQUIRE_EQUAL(it->second->get_sharder().shard_count(), 1);

            it = cf_metadata.find("raft_snapshots");
            BOOST_REQUIRE(it != cf_metadata.end());
            BOOST_REQUIRE_EQUAL(it->second->get_sharder().shard_count(), 1);

            it = cf_metadata.find("raft_snapshot_config");
            BOOST_REQUIRE(it != cf_metadata.end());
            BOOST_REQUIRE_EQUAL(it->second->get_sharder().shard_count(), 1);

            // The schemas returned by all_tables() may be different than those stored in the `db` object:
            // the schemas stored inside `db` come from deserializing mutations. The schemas in all_tables()
            // are hardcoded. If there is some information in the schema object that is not serialized into
            // mutations (say... the sharder - for now, at least), the two schema objects may differ, if
            // one is not careful.
            for (auto s: db::schema_tables::all_tables(db::schema_features::full())) {
                BOOST_REQUIRE_EQUAL(s->get_sharder().shard_count(), 1);
            }
        }).get();
    });
}

SEASTAR_TEST_CASE(test_schema_make_reversed) {
    auto schema = schema_builder("ks", get_name())
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("ck", bytes_type, column_kind::clustering_key)
            .with_column("v1", bytes_type)
            .build();
    testlog.info("            schema->version(): {}", schema->version());

    auto reversed_schema = schema->make_reversed();
    testlog.info("   reversed_schema->version(): {}", reversed_schema->version());

    BOOST_REQUIRE(schema->version() != reversed_schema->version());
    BOOST_REQUIRE(reversed(schema->version()) == reversed_schema->version());

    auto re_reversed_schema = reversed_schema->make_reversed();
    testlog.info("re_reversed_schema->version(): {}", re_reversed_schema->version());

    BOOST_REQUIRE(schema->version() == re_reversed_schema->version());
    BOOST_REQUIRE(reversed_schema->version() != re_reversed_schema->version());

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_schema_get_reversed) {
    return do_with_cql_env([] (cql_test_env& e) {
        auto schema = schema_builder("ks", get_name())
                .with_column("pk", bytes_type, column_kind::partition_key)
                .with_column("ck", bytes_type, column_kind::clustering_key)
                .with_column("v1", bytes_type)
                .build();
        schema = local_schema_registry().learn(schema);
        auto reversed_schema = schema->get_reversed();

        testlog.info("        &schema: {}", fmt::ptr(schema.get()));
        testlog.info("&reverse_schema: {}", fmt::ptr(reversed_schema.get()));

        BOOST_REQUIRE_EQUAL(reversed_schema->get_reversed().get(), schema.get());
        BOOST_REQUIRE_EQUAL(schema->get_reversed().get(), reversed_schema.get());

        return make_ready_future<>();
    });
}
