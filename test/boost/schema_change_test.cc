/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
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
#include "db/schema_tables.hh"
#include "types/list.hh"
#include "types/user.hh"
#include "db/system_keyspace.hh"
#include "test/lib/exception_utils.hh"
#include "test/lib/log.hh"
#include "test/lib/test_utils.hh"

BOOST_AUTO_TEST_SUITE(schema_change_test)

SEASTAR_TEST_CASE(test_new_schema_with_no_structural_change_is_propagated) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            auto partial = schema_builder(this_smp_shard_count(), "tests", "table")
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
            auto builder = schema_builder(this_smp_shard_count(), "tests", "table")
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

            auto s1 = schema_builder(this_smp_shard_count(), "ks", "table1")
                    .with_column("pk", bytes_type, column_kind::partition_key)
                    .with_column("v1", bytes_type)
                    .build();

            {
                auto group0_guard = mm.start_group0_operation().get();
                auto ts = group0_guard.write_timestamp();
                mm.announce(service::prepare_new_column_family_announcement(mm.get_storage_proxy(), s1, ts).get(), std::move(group0_guard), "").get();
            }

            auto&& keyspace = e.db().local().find_keyspace(s1->ks_name()).metadata();

            auto s2 = schema_builder(this_smp_shard_count(), "ks", "table1", std::make_optional(s1->id()))
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
                auto s3 = schema_builder(this_smp_shard_count(), "ks", "table1", std::make_optional(s1->id()))
                        .with_column("pk", bytes_type, column_kind::partition_key)
                        .with_column("v1", list_type_impl::get_instance(int32_type, true))
                        .build();

                auto s4 = schema_builder(this_smp_shard_count(), "ks", "table1", std::make_optional(s1->id()))
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

SEASTAR_TEST_CASE(test_merging_does_not_alter_tables_which_didnt_change) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            service::migration_manager& mm = e.migration_manager().local();

            auto&& keyspace = e.db().local().find_keyspace("ks").metadata();

            auto s0 = schema_builder(this_smp_shard_count(), "ks", "table1")
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

            auto s0 = schema_builder(this_smp_shard_count(), "ks", "table1")
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
            // May throw, but zero probability in a test
            auto listener_lease = defer([&e, &listener] noexcept { e.local_mnotifier().register_listener(&listener); });

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

SEASTAR_TEST_CASE(test_prepared_statement_is_invalidated_when_referenced_column_is_dropped) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            e.execute_cql("create keyspace tests with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };").get();
            e.execute_cql("create table tests.table1 (pk int primary key, c1 int, c2 int);").get();
            auto id = e.prepare("select c1 from tests.table1;").get();

            e.execute_cql("alter table tests.table1 drop c1;").get();

            try {
                e.execute_prepared(id, {}).get();
                BOOST_FAIL("prepared statement should have been invalidated after dropping a referenced column");
            } catch (const not_prepared_exception&) {
                // expected
            }
        });
    });
}

SEASTAR_TEST_CASE(test_prepared_statement_is_invalidated_by_existing_table_option_change) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            e.execute_cql("create keyspace tests with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };").get();
            e.execute_cql("create table tests.table1 (pk int primary key, c1 int);").get();
            auto id = e.prepare("select * from tests.table1;").get();

            e.execute_cql("alter table tests.table1 with gc_grace_seconds = 42;").get();

            try {
                e.execute_prepared(id, {}).get();
                BOOST_FAIL("prepared statement should have been invalidated after changing an existing table option");
            } catch (const not_prepared_exception&) {
                // expected
            }
        });
    });
}

SEASTAR_TEST_CASE(test_prepared_statement_is_not_invalidated_by_existing_keyspace_option_change) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            e.execute_cql("create keyspace tests with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };").get();
            e.execute_cql("create table tests.table1 (pk int primary key, c1 int);").get();
            auto id = e.prepare("select * from tests.table1;").get();

            e.execute_cql("alter keyspace tests with durable_writes = false;").get();

            BOOST_REQUIRE_NO_THROW(e.execute_prepared(id, {}).get());
        });
    });
}

SEASTAR_TEST_CASE(test_inherited_auto_repair_scope_change_bumps_schema_without_invalidating_prepared_statement) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            e.execute_cql("create keyspace tests with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };").get();
            e.execute_cql("create table tests.table1 (pk int primary key, c1 int);").get();
            auto initial_version = e.db().local().get_version();
            auto id = e.prepare("select * from tests.table1;").get();

            e.execute_cql("alter keyspace tests with auto_repair_enabled = true;").get();
            BOOST_REQUIRE_NE(e.db().local().get_version(), initial_version);
            BOOST_REQUIRE_NO_THROW(e.execute_prepared(id, {}).get());
        });
    });
}

SEASTAR_TEST_CASE(test_cluster_auto_repair_scope_change_bumps_schema_without_invalidating_prepared_statement) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            e.execute_cql("create keyspace tests with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };").get();
            e.execute_cql("create table tests.table1 (pk int primary key, c1 int);").get();
            auto initial_version = e.db().local().get_version();
            auto id = e.prepare("select * from tests.table1;").get();

            e.execute_cql("alter cluster with auto_repair_enabled = true;").get();
            BOOST_REQUIRE_NE(e.db().local().get_version(), initial_version);
            BOOST_REQUIRE_NO_THROW(e.execute_prepared(id, {}).get());
        });
    });
}

SEASTAR_TEST_CASE(test_table_auto_repair_scope_change_invalidates_prepared_statement) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            e.execute_cql("create keyspace tests with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };").get();
            e.execute_cql("create table tests.table1 (pk int primary key, c1 int);").get();
            auto initial_version = e.db().local().get_version();
            auto id = e.prepare("select * from tests.table1;").get();

            e.execute_cql("alter table tests.table1 with auto_repair_enabled = true;").get();
            BOOST_REQUIRE_NE(e.db().local().get_version(), initial_version);
            try {
                e.execute_prepared(id, {}).get();
                BOOST_FAIL("Should have failed");
            } catch (const not_prepared_exception&) {
                // expected
            }
        });
    });
}

// Verifies the mechanism documented in the cluster-config design (Part 0 "Schema-Backed
// Ownership"): every config write is schema-backed and therefore bumps the global schema
// version, at every scope. The value lives in the scylla_clusters/scylla_keyspaces/...
// config tables, not in the altered table's own schema row, so a config write never
// changes that table's per-table (cf) schema version - regardless of scope. Prepared-
// statement invalidation is therefore decided by scope (an explicit per-table notification
// fan-out for TABLE scope), not by a change in the table's cf schema version. This is why
// CLUSTER and KEYSPACE writes leave prepared statements valid while TABLE writes do not.
SEASTAR_TEST_CASE(test_cluster_config_writes_bump_global_version_but_not_table_schema_version) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            e.execute_cql("create keyspace tests with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };").get();
            e.execute_cql("create table tests.table1 (pk int primary key, c1 int);").get();

            auto global_version = [&] { return e.db().local().get_version(); };
            auto table_version = [&] { return e.local_db().find_schema("tests", "table1")->version(); };

            // The config value is stored out-of-band, so the table's own schema row never
            // changes and its cf schema version stays constant across all of the writes below.
            const auto table_version_baseline = table_version();

            // Cluster scope: global version bumps, table cf schema version is untouched.
            auto global_before = global_version();
            e.execute_cql("alter cluster with auto_repair_enabled = true;").get();
            BOOST_REQUIRE_NE(global_version(), global_before);
            BOOST_REQUIRE_EQUAL(table_version(), table_version_baseline);

            // Keyspace scope: global version bumps, table cf schema version is untouched.
            global_before = global_version();
            e.execute_cql("alter keyspace tests with auto_repair_enabled = true;").get();
            BOOST_REQUIRE_NE(global_version(), global_before);
            BOOST_REQUIRE_EQUAL(table_version(), table_version_baseline);

            // Table scope: global version still bumps and the table cf schema version is
            // *still* untouched - prepared-statement invalidation is driven by an explicit
            // notification for the affected table, not by a change in its cf schema version.
            global_before = global_version();
            e.execute_cql("alter table tests.table1 with auto_repair_enabled = true;").get();
            BOOST_REQUIRE_NE(global_version(), global_before);
            BOOST_REQUIRE_EQUAL(table_version(), table_version_baseline);
        });
    });
}

// Verifies that removing an override (`= null`) is also a config write that bumps the
// global schema version, matching the Part 0 claim that *every* config write bumps the
// version (the schema-backed path is also the agreement/versioning mechanism).
SEASTAR_TEST_CASE(test_cluster_config_unset_bumps_schema_version) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            e.execute_cql("alter cluster with auto_repair_enabled = true;").get();

            auto version_before_unset = e.db().local().get_version();
            e.execute_cql("alter cluster with auto_repair_enabled = null;").get();
            BOOST_REQUIRE_NE(e.db().local().get_version(), version_before_unset);
        });
    });
}

// Verifies scope precision: a table-scope config change on one table invalidates only that
// table's prepared statements, leaving an unrelated table's prepared statement valid. This
// confirms invalidation is keyed on the modified table's own schema row, not the global
// schema version (which bumps for both).
SEASTAR_TEST_CASE(test_table_scope_change_does_not_invalidate_other_table_prepared_statement) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            e.execute_cql("create keyspace tests with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };").get();
            e.execute_cql("create table tests.table1 (pk int primary key, c1 int);").get();
            e.execute_cql("create table tests.table2 (pk int primary key, c1 int);").get();
            auto id1 = e.prepare("select * from tests.table1;").get();
            auto id2 = e.prepare("select * from tests.table2;").get();

            e.execute_cql("alter table tests.table1 with auto_repair_enabled = true;").get();

            // table1's prepared statement is invalidated by the change to its own schema.
            try {
                e.execute_prepared(id1, {}).get();
                BOOST_FAIL("table1 prepared statement should have been invalidated");
            } catch (const not_prepared_exception&) {
                // expected
            }

            // table2 was not touched, so its prepared statement remains valid.
            BOOST_REQUIRE_NO_THROW(e.execute_prepared(id2, {}).get());
        });
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
    auto schema = schema_builder(this_smp_shard_count(), "ks", get_name())
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
        auto schema = schema_builder(this_smp_shard_count(), "ks", get_name())
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
        BOOST_REQUIRE_EQUAL(s->version(), table_schema_version(utils::UUID("95ec5e20-12f2-361a-90b3-662f8999400b")));
    });
}

SEASTAR_TEST_CASE(test_cluster_config_schema_tables_are_registered) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto all_tables = db::schema_tables::all_tables(db::schema_features::full());
        auto find_table = [&] (std::string_view table_name) -> schema_ptr {
            auto it = std::ranges::find_if(all_tables, [table_name] (const schema_ptr& schema) {
                return schema->cf_name() == table_name;
            });
            BOOST_REQUIRE(it != all_tables.end());
            return *it;
        };

        auto configs_type = map_type_impl::get_instance(utf8_type, utf8_type, true);
        auto scylla_keyspaces = find_table("scylla_keyspaces");
        auto scylla_tables = find_table("scylla_tables");
        auto keyspace_configs = scylla_keyspaces->get_column_definition("configs");
        auto table_configs = scylla_tables->get_column_definition("configs");

        BOOST_REQUIRE(keyspace_configs);
        BOOST_REQUIRE(table_configs);
        BOOST_REQUIRE(keyspace_configs->type == configs_type);
        BOOST_REQUIRE(table_configs->type == configs_type);

        (void)find_table("scylla_clusters");
        (void)find_table("scylla_datacenters");
        (void)find_table("scylla_racks");
        (void)find_table("scylla_nodes");

        (void)e;
    });
}

// The node-oriented cluster-config tables must only enter the schema
// (and therefore schema digest/version computation) when the CLUSTER_CONFIG_TABLES
// schema feature is set. Otherwise an upgraded node would perturb the schema digest
// of a mixed-version cluster in which the cluster-config cluster feature
// (CLUSTER_CONFIG_REGISTRY_V0) is not yet enabled, breaking schema agreement
// with nodes that predate cluster config.
SEASTAR_TEST_CASE(test_cluster_config_tables_gated_by_schema_feature) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto contains = [] (const std::vector<schema_ptr>& tables, std::string_view name) {
            return std::ranges::any_of(tables, [name] (const schema_ptr& s) { return s->cf_name() == name; });
        };
        constexpr std::array config_tables = {"scylla_clusters", "scylla_datacenters", "scylla_racks", "scylla_nodes"};

        // Without the feature, none of the config tables are part of the schema.
        auto without = db::schema_tables::all_tables(db::schema_features{});
        for (auto name : config_tables) {
            BOOST_REQUIRE_MESSAGE(!contains(without, name),
                seastar::format("table {} must not be present without CLUSTER_CONFIG_TABLES", name));
        }

        // With the feature, all of them are.
        auto with = db::schema_tables::all_tables(db::schema_features::of<db::schema_feature::CLUSTER_CONFIG_TABLES>());
        for (auto name : config_tables) {
            BOOST_REQUIRE_MESSAGE(contains(with, name),
                seastar::format("table {} must be present with CLUSTER_CONFIG_TABLES", name));
        }

        // full() includes the feature, so the tables remain registered for local
        // creation / auth / tooling.
        auto full = db::schema_tables::all_tables(db::schema_features::full());
        for (auto name : config_tables) {
            BOOST_REQUIRE(contains(full, name));
        }

        (void)e;
    });
}

// As long as the cluster-config tables are empty (no overrides have been
// written, which is the case throughout the mixed-version window before the cluster
// feature is enabled), enabling CLUSTER_CONFIG_TABLES must not perturb the schema
// state used for digest/version computation. This is what lets an upgraded node
// agree on schema with a not-yet-upgraded (cluster-config-unaware) node.
//
// Schema digests are computed from the mutations of every table returned by
// all_table_infos()/all_tables() (see db::schema_tables). We verify the neutrality
// property directly at that level: the extra config tables introduced by
// CLUSTER_CONFIG_TABLES contain no partitions, so they cannot contribute anything
// to whatever digest/version computation consumes those mutations.
SEASTAR_TEST_CASE(test_cluster_config_tables_digest_neutral_when_empty) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        constexpr std::array config_tables = {"scylla_clusters", "scylla_datacenters", "scylla_racks", "scylla_nodes"};

        auto with = db::schema_tables::all_tables(db::schema_features::full());
        auto without = with;
        std::erase_if(without, [] (const schema_ptr& s) {
            return s->cf_name() == "scylla_clusters" || s->cf_name() == "scylla_datacenters"
                || s->cf_name() == "scylla_racks" || s->cf_name() == "scylla_nodes";
        });
        // Sanity: the only difference between the two table sets is the 4 config tables.
        BOOST_REQUIRE_EQUAL(with.size(), without.size() + 4);

        for (auto name : config_tables) {
            auto it = std::ranges::find_if(with, [name] (const schema_ptr& s) { return s->cf_name() == name; });
            BOOST_REQUIRE(it != with.end());
            auto rs = db::system_keyspace::query_mutations(e.db(), *it).get();
            BOOST_REQUIRE_MESSAGE(rs->partitions().empty(),
                seastar::format("table {} must be empty so it cannot affect the schema digest", name));
        }
    });
}

SEASTAR_TEST_CASE(test_save_system_schema_skips_topology_config_tables) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        BOOST_REQUIRE_NO_THROW(db::schema_tables::save_system_schema(e.local_qp()).get());
    });
}

// The node-oriented config tables are not partition-keyed by keyspace_name
// (scylla_nodes is even keyed by a uuid). A keyspace DROP must therefore not fabricate
// a keyspace_name-keyed mutation for any of them; doing so writes a spurious (and, for
// scylla_nodes, type-invalid) partition into a config table. Verify no drop-keyspace
// mutation targets these tables.
SEASTAR_TEST_CASE(test_drop_keyspace_does_not_mutate_config_tables) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto keyspace = e.db().local().find_keyspace("ks").metadata();
        auto features = e.db().local().features().cluster_schema_features();
        auto mutations = db::schema_tables::make_drop_keyspace_mutations(
            features, keyspace, api::new_timestamp());

        for (const auto& m : mutations) {
            auto name = m.schema()->cf_name();
            BOOST_REQUIRE_MESSAGE(
                name != "scylla_clusters" && name != "scylla_datacenters"
                    && name != "scylla_racks" && name != "scylla_nodes",
                seastar::format("drop-keyspace must not produce a mutation for config table {}", name));
        }
    });
}

// The purpose of this check is to make sure that we don't accidentally change the metadata_id.
// The metadata_id should be stable to avoid ping-pong when driver connects to a mixed cluster.
void verify_metadata_id_is_stable(cql3::cql_metadata_id_type metadata_id, sstring known_hash) {
    BOOST_REQUIRE_EQUAL(metadata_id.to_bytes_view(), from_hex(known_hash));
}

BOOST_AUTO_TEST_CASE(metadata_id_from_empty_metadata) {
    auto m = cql3::metadata{std::vector<lw_shared_ptr<cql3::column_specification>>{}};
    auto metadata_id = m.calculate_metadata_id();
    BOOST_REQUIRE_EQUAL(metadata_id.to_bytes_view().size(), 16);
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

BOOST_AUTO_TEST_CASE(metadata_id_accepts_any_input_size) {
    // Empty input: zero-initialized 16 bytes
    cql3::cql_metadata_id_type from_empty{bytes_view{}};
    bytes zero_16(bytes::initialized_later(), 16);
    std::fill(zero_16.begin(), zero_16.end(), int8_t(0));
    BOOST_REQUIRE_EQUAL(from_empty.to_bytes_view(), bytes_view(zero_16));

    // Smaller than 16 bytes: zero-padded to 16
    bytes small = {int8_t(1), int8_t(2), int8_t(3), int8_t(4), int8_t(5)};
    cql3::cql_metadata_id_type from_small{bytes_view(small)};
    bytes small_expected(bytes::initialized_later(), 16);
    auto it = std::copy(small.begin(), small.end(), small_expected.begin());
    std::fill(it, small_expected.end(), int8_t(0));
    BOOST_REQUIRE_EQUAL(from_small.to_bytes_view(), bytes_view(small_expected));

    // Larger than 16 bytes: first 16 bytes stored, rest discarded
    bytes large(bytes::initialized_later(), 32);
    std::fill(large.begin(), large.end(), int8_t(0x42));
    cql3::cql_metadata_id_type from_large{bytes_view(large)};
    bytes expected_16(bytes::initialized_later(), 16);
    std::fill(expected_16.begin(), expected_16.end(), int8_t(0x42));
    BOOST_REQUIRE_EQUAL(from_large.to_bytes_view(), bytes_view(expected_16));
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
