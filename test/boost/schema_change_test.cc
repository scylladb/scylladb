/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */


#include "bytes.hh"
#include <iostream>
#include <fmt/ranges.h>
#include <seastar/core/thread.hh>
#include <utility>
#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/util/defer.hh>

#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "schema/schema_builder.hh"
#include "schema/schema_registry.hh"
#include "db/extensions.hh"
#include "db/schema_tables.hh"
#include "db/object_storage_endpoint_param.hh"
#include "types/list.hh"
#include "types/user.hh"
#include "db/config.hh"
#include "db/system_keyspace.hh"
#include "test/lib/tmpdir.hh"
#include "test/lib/exception_utils.hh"
#include "test/lib/log.hh"
#include "cdc/cdc_extension.hh"
#include "test/lib/test_utils.hh"

BOOST_AUTO_TEST_SUITE(schema_change_test)

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
                auto muts = db::schema_tables::make_update_table_mutations(e.get_storage_proxy().local(), keyspace, s1, s2,
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
                auto muts = db::schema_tables::make_update_table_mutations(e.get_storage_proxy().local(), keyspace, s3, s4,
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

            utils::chunked_vector<mutation> muts1;
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

            utils::chunked_vector<mutation> all_muts;

            {
                auto group0_guard = mm.start_group0_operation().get();
                const auto ts = group0_guard.write_timestamp();
                auto muts = service::prepare_keyspace_drop_announcement(e.get_storage_proxy().local(), "ks", ts).get();
                std::ranges::copy(muts, std::back_inserter(all_muts));
                mm.announce(muts, std::move(group0_guard), "").get();
            }

            {
                auto group0_guard = mm.start_group0_operation().get();
                const auto ts = group0_guard.write_timestamp();

                // all_muts contains keyspace drop.
                auto muts = service::prepare_new_keyspace_announcement(e.db().local(), keyspace, ts);
                std::ranges::copy(muts, std::back_inserter(all_muts));
                mm.announce(muts, std::move(group0_guard), "").get();
            }

            {
                auto group0_guard = mm.start_group0_operation().get();
                const auto ts = group0_guard.write_timestamp();

                auto muts = service::prepare_new_column_family_announcement(mm.get_storage_proxy(), s0, ts).get();
                std::ranges::copy(muts, std::back_inserter(all_muts));

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
    db_cfg.object_storage_endpoints({db::object_storage_endpoint_param{"localhost", s3::endpoint_config{}}}, db::config::config_source::CommandLine);
    if (regenerate) {
        db_cfg.data_file_directories({data_dir}, db::config::config_source::CommandLine);
    } else {
        fs::copy(std::string(data_dir), std::string(tmp.path().string()), fs::copy_options::recursive);
        db_cfg.data_file_directories({tmp.path().string()}, db::config::config_source::CommandLine);
    }
    cql_test_config cfg_in(db_cfg_ptr);
    cfg_in.disabled_features = std::move(disabled_features);

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
        utils::UUID("c9e7f00a-0d19-366a-ba6c-ebdfe94f1dd1"),
        utils::UUID("f2385053-24c2-3ca9-a772-981be0316a7f"),
        utils::UUID("f2385053-24c2-3ca9-a772-981be0316a7f"),
        utils::UUID("8dfb7c90-01c5-11f1-8db2-0f688d7fda1b"),
        utils::UUID("8dfc66f1-01c5-11f1-8db2-0f688d7fda1b"),
        utils::UUID("8dfd5151-01c5-11f1-8db2-0f688d7fda1b"),
        utils::UUID("8dfe89d1-01c5-11f1-8db2-0f688d7fda1b"),
        utils::UUID("f2385053-24c2-3ca9-a772-981be0316a7f"),
    };
    return test_schema_digest_does_not_change_with_disabled_features("./test/resource/sstables/schema_digest_test",
            std::set<sstring>{"COMPUTED_COLUMNS", "CDC", "KEYSPACE_STORAGE_OPTIONS", "TABLE_DIGEST_INSENSITIVE_TO_EXPIRY"},
            std::move(expected_digests), [] (cql_test_env& e) {});
}

SEASTAR_TEST_CASE(test_schema_digest_does_not_change_after_computed_columns_without_digest_feature) {
    std::vector<utils::UUID> expected_digests{
        utils::UUID("a9c88159-aa6a-372e-8aca-59401fdcf38b"),
        utils::UUID("134bbc8e-bed6-39e0-88a7-7519ccb6bfc8"),
        utils::UUID("134bbc8e-bed6-39e0-88a7-7519ccb6bfc8"),
        utils::UUID("53cb5430-01c7-11f1-9bc3-72881aed3f75"),
        utils::UUID("53cc3e91-01c7-11f1-9bc3-72881aed3f75"),
        utils::UUID("53cd28f1-01c7-11f1-9bc3-72881aed3f75"),
        utils::UUID("53ceaf91-01c7-11f1-9bc3-72881aed3f75"),
        utils::UUID("134bbc8e-bed6-39e0-88a7-7519ccb6bfc8"),
    };
    return test_schema_digest_does_not_change_with_disabled_features("./test/resource/sstables/schema_digest_test_computed_columns",
            std::set<sstring>{"CDC", "KEYSPACE_STORAGE_OPTIONS", "TABLE_DIGEST_INSENSITIVE_TO_EXPIRY"}, std::move(expected_digests), [] (cql_test_env& e) {});
}

SEASTAR_TEST_CASE(test_schema_digest_does_not_change_with_functions_without_digest_feature) {
    std::vector<utils::UUID> expected_digests{
        utils::UUID("8c90f476-3872-35f1-9652-ebf895d25499"),
        utils::UUID("d6201692-67ee-321c-a038-24bc56141b54"),
        utils::UUID("d6201692-67ee-321c-a038-24bc56141b54"),
        utils::UUID("d87be7a0-01c5-11f1-bc23-679f0beca436"),
        utils::UUID("d87caaf2-01c5-11f1-bc23-679f0beca436"),
        utils::UUID("d87d9552-01c5-11f1-bc23-679f0beca436"),
        utils::UUID("d87ef4e2-01c5-11f1-bc23-679f0beca436"),
        utils::UUID("d6201692-67ee-321c-a038-24bc56141b54"),
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
        utils::UUID("95ea2227-255d-3dad-9e5b-825d3b8fae56"),
        utils::UUID("ea6ef7b6-7bc1-3c21-b638-54861462ef4c"),
        utils::UUID("ea6ef7b6-7bc1-3c21-b638-54861462ef4c"),
        utils::UUID("109b6d90-01c6-11f1-a0c5-3beaef6855ba"),
        utils::UUID("109c30e2-01c6-11f1-a0c5-3beaef6855ba"),
        utils::UUID("109d4251-01c6-11f1-a0c5-3beaef6855ba"),
        utils::UUID("109ea1e1-01c6-11f1-a0c5-3beaef6855ba"),
        utils::UUID("ea6ef7b6-7bc1-3c21-b638-54861462ef4c"),
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
        utils::UUID("0fa8fa0b-c272-3e7d-965f-e339d118c4de"),
        utils::UUID("43d02ec5-e69e-39d2-a1a3-9c385e807d17"),
        utils::UUID("43d02ec5-e69e-39d2-a1a3-9c385e807d17"),
        utils::UUID("46bd27b0-01c6-11f1-9091-5ea6a2cb6845"),
        utils::UUID("46bdeb02-01c6-11f1-9091-5ea6a2cb6845"),
        utils::UUID("46befc71-01c6-11f1-9091-5ea6a2cb6845"),
        utils::UUID("46c08311-01c6-11f1-9091-5ea6a2cb6845"),
        utils::UUID("efce863c-19d7-3e3a-bf95-fdf76f5775f5"),
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
        utils::UUID("c9e7f00a-0d19-366a-ba6c-ebdfe94f1dd1"),
        utils::UUID("f2385053-24c2-3ca9-a772-981be0316a7f"),
        utils::UUID("f2385053-24c2-3ca9-a772-981be0316a7f"),
        utils::UUID("8dfb7c90-01c5-11f1-8db2-0f688d7fda1b"),
        utils::UUID("8dfc66f1-01c5-11f1-8db2-0f688d7fda1b"),
        utils::UUID("8dfd5151-01c5-11f1-8db2-0f688d7fda1b"),
        utils::UUID("8dfe89d1-01c5-11f1-8db2-0f688d7fda1b"),
        utils::UUID("f2385053-24c2-3ca9-a772-981be0316a7f"),
    };
    return test_schema_digest_does_not_change_with_disabled_features("./test/resource/sstables/schema_digest_test",
            std::set<sstring>{"COMPUTED_COLUMNS", "CDC", "KEYSPACE_STORAGE_OPTIONS"},
            std::move(expected_digests), [] (cql_test_env& e) {});
}

SEASTAR_TEST_CASE(test_schema_digest_does_not_change_after_computed_columns) {
    std::vector<utils::UUID> expected_digests{
        utils::UUID("a9c88159-aa6a-372e-8aca-59401fdcf38b"),
        utils::UUID("134bbc8e-bed6-39e0-88a7-7519ccb6bfc8"),
        utils::UUID("134bbc8e-bed6-39e0-88a7-7519ccb6bfc8"),
        utils::UUID("53cb5430-01c7-11f1-9bc3-72881aed3f75"),
        utils::UUID("53cc3e91-01c7-11f1-9bc3-72881aed3f75"),
        utils::UUID("53cd28f1-01c7-11f1-9bc3-72881aed3f75"),
        utils::UUID("53ceaf91-01c7-11f1-9bc3-72881aed3f75"),
        utils::UUID("134bbc8e-bed6-39e0-88a7-7519ccb6bfc8"),
    };
    return test_schema_digest_does_not_change_with_disabled_features("./test/resource/sstables/schema_digest_test_computed_columns",
            std::set<sstring>{"CDC", "KEYSPACE_STORAGE_OPTIONS"}, std::move(expected_digests), [] (cql_test_env& e) {});
}

SEASTAR_TEST_CASE(test_schema_digest_does_not_change_with_functions) {
    std::vector<utils::UUID> expected_digests{
        utils::UUID("8c90f476-3872-35f1-9652-ebf895d25499"),
        utils::UUID("d6201692-67ee-321c-a038-24bc56141b54"),
        utils::UUID("d6201692-67ee-321c-a038-24bc56141b54"),
        utils::UUID("d87be7a0-01c5-11f1-bc23-679f0beca436"),
        utils::UUID("d87caaf2-01c5-11f1-bc23-679f0beca436"),
        utils::UUID("d87d9552-01c5-11f1-bc23-679f0beca436"),
        utils::UUID("d87ef4e2-01c5-11f1-bc23-679f0beca436"),
        utils::UUID("d6201692-67ee-321c-a038-24bc56141b54"),
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
        utils::UUID("95ea2227-255d-3dad-9e5b-825d3b8fae56"),
        utils::UUID("ea6ef7b6-7bc1-3c21-b638-54861462ef4c"),
        utils::UUID("ea6ef7b6-7bc1-3c21-b638-54861462ef4c"),
        utils::UUID("109b6d90-01c6-11f1-a0c5-3beaef6855ba"),
        utils::UUID("109c30e2-01c6-11f1-a0c5-3beaef6855ba"),
        utils::UUID("109d4251-01c6-11f1-a0c5-3beaef6855ba"),
        utils::UUID("109ea1e1-01c6-11f1-a0c5-3beaef6855ba"),
        utils::UUID("ea6ef7b6-7bc1-3c21-b638-54861462ef4c"),
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
        utils::UUID("0fa8fa0b-c272-3e7d-965f-e339d118c4de"),
        utils::UUID("43d02ec5-e69e-39d2-a1a3-9c385e807d17"),
        utils::UUID("43d02ec5-e69e-39d2-a1a3-9c385e807d17"),
        utils::UUID("46bd27b0-01c6-11f1-9091-5ea6a2cb6845"),
        utils::UUID("46bdeb02-01c6-11f1-9091-5ea6a2cb6845"),
        utils::UUID("46befc71-01c6-11f1-9091-5ea6a2cb6845"),
        utils::UUID("46c08311-01c6-11f1-9091-5ea6a2cb6845"),
        utils::UUID("efce863c-19d7-3e3a-bf95-fdf76f5775f5"),
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

// The purpose of the test is to avoid unintended changes of schema version
// of system tables due to changes in generic code in schema_builder.
//
// It's enough to check only one system table as all tables share the version
// calculation code. The test chooses to check system.batchlog, whose schema
// shouldn't change often and cause failures due to intended version changes.
SEASTAR_TEST_CASE(test_system_schema_version_is_stable) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto s = db::system_keyspace::batchlog();

        // If you changed the schema of system.batchlog then this is expected to fail.
        // Just replace expected version with the new version.
        BOOST_REQUIRE_EQUAL(s->version(), table_schema_version(utils::UUID("1f504ac7-350f-37aa-8a9e-105b1325d8e3")));
    });
}

// The purpose of this check is to make sure that we don't accidentally change the metadata_id.
// The metadata_id should be stable to avoid ping-pong when driver connects to a mixed cluster.
void verify_metadata_id_is_stable(cql3::cql_metadata_id_type metadata_id, sstring known_hash) {
    BOOST_REQUIRE_EQUAL(metadata_id._metadata_id, from_hex(known_hash));
}

BOOST_AUTO_TEST_CASE(metadata_id_from_empty_metadata) {
    auto m = cql3::metadata{std::vector<lw_shared_ptr<cql3::column_specification>>{}};
    auto metadata_id = m.calculate_metadata_id();
    BOOST_REQUIRE_EQUAL(metadata_id._metadata_id.size(), 16);
    verify_metadata_id_is_stable(metadata_id, "e3b0c44298fc1c149afbf4c8996fb924");
}

cql3::cql_metadata_id_type compute_metadata_id(std::vector<std::pair<sstring, shared_ptr<const abstract_type>>> columns, sstring ks = "ks", sstring cf = "cf") {
    std::vector<lw_shared_ptr<cql3::column_specification>> columns_specification;
    for (const auto& column : columns) {
        columns_specification.push_back(make_lw_shared(cql3::column_specification(ks, cf, make_shared<cql3::column_identifier>(column.first, false), column.second)));
    }
    return cql3::metadata{columns_specification}.calculate_metadata_id();
}

BOOST_AUTO_TEST_CASE(metadata_id_with_different_keyspace_and_table) {
    const auto c = std::make_pair("id", uuid_type);
    auto h1 = compute_metadata_id({c}, "ks1", "cf1");
    auto h2 = compute_metadata_id({c}, "ks2", "cf2");

    BOOST_REQUIRE_EQUAL(h1, h2);
    verify_metadata_id_is_stable(h1, "d0c38eb409a57bb14497c35b80dfaaf1");
}

BOOST_AUTO_TEST_CASE(metadata_id_with_different_column_name) {
    auto h1 = compute_metadata_id({{"id", uuid_type}});
    auto h2 = compute_metadata_id({{"id2", uuid_type}});

    BOOST_REQUIRE_NE(h1, h2);
    verify_metadata_id_is_stable(h1, "d0c38eb409a57bb14497c35b80dfaaf1");
    verify_metadata_id_is_stable(h2, "ae0bc2741d0480f0ebf4ee18a9bca7c7");
}

BOOST_AUTO_TEST_CASE(metadata_id_with_different_column_type) {
    const auto column_name = "id";
    auto h1 = compute_metadata_id({{column_name, uuid_type}});
    auto h2 = compute_metadata_id({{column_name, int32_type}});

    BOOST_REQUIRE_NE(h1, h2);
    verify_metadata_id_is_stable(h1, "d0c38eb409a57bb14497c35b80dfaaf1");
    verify_metadata_id_is_stable(h2, "b62d95c978e2e2498100ad8d20979868");
}

BOOST_AUTO_TEST_CASE(metadata_id_with_different_column_number) {
    const auto c1 = std::make_pair("val1", int32_type);
    const auto c2 = std::make_pair("val2", int32_type);
    auto h1 = compute_metadata_id({c1});
    auto h2 = compute_metadata_id({c1, c2});

    BOOST_REQUIRE_NE(h1, h2);
    verify_metadata_id_is_stable(h1, "f38171ab2b2e4d98e3f76a4640de5b32");
    verify_metadata_id_is_stable(h2, "31c5cb5d0d41fbc426266248cc37941a");
}

BOOST_AUTO_TEST_CASE(metadata_id_with_different_column_order) {
    const auto c1 = std::make_pair("val1", int32_type);
    const auto c2 = std::make_pair("val2", int32_type);
    auto h1 = compute_metadata_id({c1, c2});
    auto h2 = compute_metadata_id({c2, c1});

    BOOST_REQUIRE_NE(h1, h2);
    verify_metadata_id_is_stable(h1, "31c5cb5d0d41fbc426266248cc37941a");
    verify_metadata_id_is_stable(h2, "b52512f2b76d3e0695dcaf7b0a71efac");
}

BOOST_AUTO_TEST_CASE(metadata_id_with_udt) {

    auto compute_metadata_id_for_type = [&](
        const std::vector<bytes>& names,
        const std::vector<data_type>& types,
        const char* udt_name = "udt_name",
        const bool multi_cell = true) {
        BOOST_REQUIRE_EQUAL(names.size(), types.size());
        return compute_metadata_id({{
            "val1",
            user_type_impl::get_instance("ks", udt_name, names, types, multi_cell)}}
        );
    };

    auto h1 = compute_metadata_id_for_type({"f1"}, {int32_type});

    // Different field number
    auto h2 = compute_metadata_id_for_type({"f1", "f2"}, {int32_type, int32_type});
    BOOST_REQUIRE_NE(h1, h2);

    // Different field name
    auto h3 = compute_metadata_id_for_type({"f2"}, {int32_type});
    BOOST_REQUIRE_NE(h1, h3);

    // Different field type
    auto h4 = compute_metadata_id_for_type({"f1"}, {float_type});
    BOOST_REQUIRE_NE(h1, h4);

    // Different UDT name
    auto h5 = compute_metadata_id_for_type({"f1"}, {int32_type}, "different_udt_name");
    BOOST_REQUIRE_NE(h1, h5);

    // False multi_cell mark
    auto h6 = compute_metadata_id_for_type({"f1"}, {int32_type}, "udt_name", false);
    BOOST_REQUIRE_NE(h1, h6);

    verify_metadata_id_is_stable(h1, "9e556a9632191ac829c961c94719073a");
    verify_metadata_id_is_stable(h2, "f0a58cd95fed3009b67ff6b4bda1fae1");
    verify_metadata_id_is_stable(h3, "6a99234baebad33d9b9081cbdef9cd8b");
    verify_metadata_id_is_stable(h4, "72780d64c71ec0265bb48194ec5b0f75");
    verify_metadata_id_is_stable(h5, "767b01cdb5a61f90af9d824338de40e9");
    verify_metadata_id_is_stable(h6, "02f16bdc4b235791a44983fe56618006");
}

cql3::cql_metadata_id_type get_metadata_id(cql_test_env& e, sstring const& table) {
    auto msg = e.execute_cql(format("SELECT * FROM {};", table)).get();
    auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
    return rows->rs().get_metadata().calculate_metadata_id();
}

SEASTAR_TEST_CASE(metadata_id_unchanged) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "CREATE TABLE t(p int PRIMARY KEY, c1 int)");
        cquery_nofail(e, "INSERT INTO  t(p, c1) VALUES (0, 0)");
        const auto initial_metadata_id = get_metadata_id(e, "t");

        cquery_nofail(e, "ALTER TABLE t ADD (c2 int)");
        BOOST_REQUIRE_NE(initial_metadata_id, get_metadata_id(e, "t"));

        cquery_nofail(e, "ALTER TABLE t DROP c2");
        BOOST_REQUIRE_EQUAL(initial_metadata_id, get_metadata_id(e, "t"));
    });
}

BOOST_AUTO_TEST_SUITE_END()
