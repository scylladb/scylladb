/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */


#include <seastar/core/thread.hh>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/lowres_clock.hh>
#include "data_dictionary/user_types_metadata.hh"
#include "schema/schema_registry.hh"
#include "schema/schema_builder.hh"
#include "test/lib/mutation_source_test.hh"
#include "db/config.hh"
#include "db/schema_applier.hh"
#include "db/schema_tables.hh"
#include "types/list.hh"
#include "utils/throttle.hh"
#include "test/lib/cql_test_env.hh"
#include "gms/feature_service.hh"
#include "view_info.hh"

BOOST_AUTO_TEST_SUITE(schema_registry_test)

static bytes random_column_name() {
    return to_bytes(to_hex(make_blob(32)));
}

static schema_ptr random_schema() {
    return schema_builder("ks", "cf")
           .with_column("pk", bytes_type, column_kind::partition_key)
           .with_column(random_column_name(), bytes_type)
           .build();
}

struct dummy_init {
    std::unique_ptr<db::config> config;
    gms::feature_service fs;
    seastar::lowres_clock::duration grace_period;
    dummy_init()
            : config(std::make_unique<db::config>())
            , fs(gms::feature_config_from_db_config(*config))
            , grace_period(std::chrono::seconds(config->schema_registry_grace_period())) {
        local_schema_registry().init(db::schema_ctxt(*config, std::make_shared<data_dictionary::dummy_user_types_storage>(), fs));
    }
};

SEASTAR_THREAD_TEST_CASE(test_load_with_non_nantive_type) {
    dummy_init dummy;
    auto my_list_type = list_type_impl::get_instance(utf8_type, true);

    auto s = schema_builder("ks", "cf")
           .with_column("pk", bytes_type, column_kind::partition_key)
           .with_column("val", my_list_type)
           .build();

    local_schema_registry().get_or_load(s->version(), [s] (table_schema_version) {
        return make_ready_future<base_and_view_schemas>(frozen_schema(s));
    }).get();
}

SEASTAR_TEST_CASE(test_async_loading) {
    return seastar::async([] {
        dummy_init dummy;
        auto s1 = random_schema();
        auto s2 = random_schema();

        auto s1_loaded = local_schema_registry().get_or_load(s1->version(), [s1] (table_schema_version) {
            return make_ready_future<base_and_view_schemas>(frozen_schema(s1));
        }).get();

        BOOST_REQUIRE(s1_loaded);
        BOOST_REQUIRE(s1_loaded->version() == s1->version());
        auto s1_later = local_schema_registry().get_or_null(s1->version());
        BOOST_REQUIRE(s1_later);

        auto s2_loaded = local_schema_registry().get_or_load(s2->version(), [s2] (table_schema_version) {
            return yield().then([s2] -> base_and_view_schemas { return {frozen_schema(s2)}; });
        }).get();

        BOOST_REQUIRE(s2_loaded);
        BOOST_REQUIRE(s2_loaded->version() == s2->version());
        auto s2_later = local_schema_registry().get_or_null(s2_loaded->version());
        BOOST_REQUIRE(s2_later);
    });
}

SEASTAR_TEST_CASE(test_schema_is_synced_when_syncer_doesnt_defer) {
    return seastar::async([] {
        dummy_init dummy;
        auto s = random_schema();
        s = local_schema_registry().get_or_load(s->version(), [s] (table_schema_version) -> base_and_view_schemas { return {frozen_schema(s)}; });
        BOOST_REQUIRE(!s->is_synced());
        s->registry_entry()->maybe_sync([] { return make_ready_future<>(); }).get();
        BOOST_REQUIRE(s->is_synced());
    });
}

SEASTAR_TEST_CASE(test_schema_is_synced_when_syncer_defers) {
    return seastar::async([] {
        dummy_init dummy;
        auto s = random_schema();
        s = local_schema_registry().get_or_load(s->version(), [s] (table_schema_version) -> base_and_view_schemas { return {frozen_schema(s)}; });
        BOOST_REQUIRE(!s->is_synced());
        s->registry_entry()->maybe_sync([] { return yield(); }).get();
        BOOST_REQUIRE(s->is_synced());
    });
}

SEASTAR_TEST_CASE(test_failed_sync_can_be_retried) {
    return seastar::async([] {
        dummy_init dummy;
        auto s = random_schema();
        s = local_schema_registry().get_or_load(s->version(), [s] (table_schema_version) -> base_and_view_schemas { return {frozen_schema(s)}; });
        BOOST_REQUIRE(!s->is_synced());

        promise<> fail_sync;

        auto f1 = s->registry_entry()->maybe_sync([&fail_sync] () mutable {
            return fail_sync.get_future().then([] {
                throw std::runtime_error("sync failed");
            });
        });

        // concurrent maybe_sync should attach the the current one
        auto f2 = s->registry_entry()->maybe_sync([] { return make_ready_future<>(); });

        fail_sync.set_value();

        try {
            f1.get();
            BOOST_FAIL("Should have failed");
        } catch (...) {
            // expected
        }

        try {
            f2.get();
            BOOST_FAIL("Should have failed");
        } catch (...) {
            // expected
        }

        BOOST_REQUIRE(!s->is_synced());

        s->registry_entry()->maybe_sync([] { return make_ready_future<>(); }).get();
        BOOST_REQUIRE(s->is_synced());
    });
}

SEASTAR_THREAD_TEST_CASE(test_table_is_attached) {
    do_with_cql_env_thread([] (cql_test_env& e) {
        auto s0 = schema_builder("ks", "cf")
                .with_column("pk", bytes_type, column_kind::partition_key)
                .with_column("v1", bytes_type)
                .build();

        auto s0_wild = schema_builder(s0)
                .with_column(random_column_name(), bytes_type)
                .build();

        // Simulate fetching schema version before the table is created.
        local_schema_registry().learn(s0);

        // Use schema mutations so that table id is the same as in s0.
        {
            auto sm0 = db::schema_tables::make_schema_mutations(s0, api::new_timestamp(), true);
            std::vector<mutation> muts;
            sm0.copy_to(muts);
            db::schema_tables::merge_schema(e.get_system_keyspace(), e.get_storage_proxy(),
                                            e.get_feature_service().local(), muts).get();
        }

        // This should attach the table
        s0->registry_entry()->maybe_sync([] { return make_ready_future<>(); }).get();
        BOOST_REQUIRE(s0->maybe_table());

        // mark_synced() should attach the table
        local_schema_registry().learn(s0_wild);
        s0_wild->registry_entry()->mark_synced();
        BOOST_REQUIRE(s0_wild->maybe_table());

        auto s1 = schema_builder(s0)
                .with_column(random_column_name(), bytes_type)
                .build();
        BOOST_REQUIRE(!s1->maybe_table());

        e.execute_cql("ALTER TABLE ks.cf ADD dummy int").get();

        s1 = local_schema_registry().learn(s1);
        s1->registry_entry()->maybe_sync([] { return make_ready_future<>(); }).get();
        BOOST_REQUIRE(&s1->table() == s0->maybe_table());

        auto learned_s1 = local_schema_registry().learn(s1);
        BOOST_REQUIRE(learned_s1->maybe_table() == s0->maybe_table());
        BOOST_REQUIRE(&learned_s1->table() == s0->maybe_table());

        auto s2 = schema_builder(s0)
                .with_column(random_column_name(), bytes_type)
                .build();

        auto learned_s2 = local_schema_registry().get_or_load(s2->version(), [&] (table_schema_version) -> base_and_view_schemas {
            return {frozen_schema(s2)};
        });
        BOOST_REQUIRE(learned_s2->maybe_table() == s0->maybe_table());

        if (smp::count > 1) {
            smp::submit_to(1, [&e, gs = global_schema_ptr(learned_s2)] {
                schema_ptr s0 = e.local_db().find_column_family("ks", "cf").schema();
                BOOST_REQUIRE(gs.get()->maybe_table());
                BOOST_REQUIRE(gs.get()->maybe_table() == s0->maybe_table());
            }).get();
        }

        // Simulate concurrent schema version fetch and learn() from schema merge which cuts the race.
        auto s3 = schema_builder(s2)
                .with_column(random_column_name(), bytes_type)
                .build();
        utils::throttle s3_thr;
        auto s3_entered = s3_thr.block();
        auto learned_s3 = local_schema_registry().get_or_load(s3->version(), [&, fs = frozen_schema(s3)] (table_schema_version) -> future<base_and_view_schemas> {
            co_await s3_thr.enter();
            co_return base_and_view_schemas{fs};
        });
        s3_entered.get();
        local_schema_registry().learn(s3);
        s3_thr.unblock();
        auto s3_s = learned_s3.get();
        BOOST_REQUIRE(s3_s->maybe_table() == s0->maybe_table());
        BOOST_REQUIRE(s3->maybe_table() == s0->maybe_table());

        // Simulate concurrent schema version fetch and get_or_load() from global_schema_ptr which cuts the race.
        auto s4 = schema_builder(s3)
                .with_column(random_column_name(), bytes_type)
                .build();
        utils::throttle s4_thr;
        auto s4_entered = s4_thr.block();
        auto learned_s4 = local_schema_registry().get_or_load(s4->version(), [&, fs = frozen_schema(s4)] (table_schema_version) -> future<base_and_view_schemas> {
            co_await s4_thr.enter();
            co_return base_and_view_schemas(fs);
        });
        s4_entered.get();
        s4 = local_schema_registry().get_or_load(s4->version(), [&, fs = frozen_schema(s4)] (table_schema_version) -> base_and_view_schemas { return {fs}; });
        s4_thr.unblock();
        auto s4_s = learned_s4.get();
        BOOST_REQUIRE(s4_s->maybe_table() == s0->maybe_table());
        BOOST_REQUIRE(s4->maybe_table() == s0->maybe_table());

        e.execute_cql("DROP TABLE ks.cf;").get();

        BOOST_REQUIRE(!s0->maybe_table());
        BOOST_REQUIRE(!learned_s1->maybe_table());
        BOOST_REQUIRE(!learned_s2->maybe_table());
        BOOST_REQUIRE_THROW(learned_s1->table(), replica::no_such_column_family);
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_schema_is_recovered_after_dying) {
    dummy_init dummy;
    auto base_schema = schema_builder("ks", "cf")
        .with_column("pk", int32_type, column_kind::partition_key)
        .with_column("v", int32_type)
        .build();
    auto base_registry_schema = local_schema_registry().get_or_load(base_schema->version(),
        [base_schema] (table_schema_version) -> base_and_view_schemas { return {frozen_schema(base_schema)}; });
    base_registry_schema = nullptr;
    auto recovered_registry_schema = local_schema_registry().get_or_null(base_schema->version());
    BOOST_REQUIRE(recovered_registry_schema);
    recovered_registry_schema = nullptr;
    seastar::sleep(dummy.grace_period).get();
    BOOST_REQUIRE(!local_schema_registry().get_or_null(base_schema->version()));
}

SEASTAR_THREAD_TEST_CASE(test_view_info_is_recovered_after_dying) {
    dummy_init dummy;
    auto base_schema = schema_builder("ks", "cf")
        .with_column("pk", int32_type, column_kind::partition_key)
        .with_column("v", int32_type)
        .build();
    schema_builder view_builder("ks", "cf_view");
    auto view_schema = schema_builder("ks", "cf_view")
            .with_column("v", int32_type, column_kind::partition_key)
            .with_column("pk", int32_type)
            .with_view_info(*base_schema, false, "pk IS NOT NULL AND v IS NOT NULL")
            .build();
    view_schema->view_info()->set_base_info(view_schema->view_info()->make_base_dependent_view_info(*base_schema));
    local_schema_registry().get_or_load(view_schema->version(),
        [view_schema, base_schema] (table_schema_version) -> base_and_view_schemas { return {frozen_schema(view_schema), base_schema}; });
    auto view_registry_schema = local_schema_registry().get_or_null(view_schema->version());
    BOOST_REQUIRE(view_registry_schema);
    BOOST_REQUIRE(view_registry_schema->view_info()->base_info());
}

BOOST_AUTO_TEST_SUITE_END()
