/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <seastar/core/thread.hh>

#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include "data_dictionary/user_types_metadata.hh"
#include "schema/schema_registry.hh"
#include "schema/schema_builder.hh"
#include "test/lib/mutation_source_test.hh"
#include "db/config.hh"
#include "db/schema_tables.hh"
#include "types/list.hh"
#include "utils/throttle.hh"
#include "test/lib/cql_test_env.hh"
#include "gms/feature_service.hh"

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

    dummy_init()
            : config(std::make_unique<db::config>())
            , fs(gms::feature_config_from_db_config(*config)) {
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
        return make_ready_future<frozen_schema>(frozen_schema(s));
    }).get();
}

SEASTAR_TEST_CASE(test_async_loading) {
    return seastar::async([] {
        dummy_init dummy;
        auto s1 = random_schema();
        auto s2 = random_schema();

        auto s1_loaded = local_schema_registry().get_or_load(s1->version(), [s1] (table_schema_version) {
            return make_ready_future<frozen_schema>(frozen_schema(s1));
        }).get();

        BOOST_REQUIRE(s1_loaded);
        BOOST_REQUIRE(s1_loaded->version() == s1->version());
        auto s1_later = local_schema_registry().get_or_null(s1->version());
        BOOST_REQUIRE(s1_later);

        auto s2_loaded = local_schema_registry().get_or_load(s2->version(), [s2] (table_schema_version) {
            return yield().then([s2] { return frozen_schema(s2); });
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
        s = local_schema_registry().get_or_load(s->version(), [s] (table_schema_version) { return frozen_schema(s); });
        BOOST_REQUIRE(!s->is_synced());
        s->registry_entry()->maybe_sync([] { return make_ready_future<>(); }).get();
        BOOST_REQUIRE(s->is_synced());
    });
}

SEASTAR_TEST_CASE(test_schema_is_synced_when_syncer_defers) {
    return seastar::async([] {
        dummy_init dummy;
        auto s = random_schema();
        s = local_schema_registry().get_or_load(s->version(), [s] (table_schema_version) { return frozen_schema(s); });
        BOOST_REQUIRE(!s->is_synced());
        s->registry_entry()->maybe_sync([] { return yield(); }).get();
        BOOST_REQUIRE(s->is_synced());
    });
}

SEASTAR_TEST_CASE(test_failed_sync_can_be_retried) {
    return seastar::async([] {
        dummy_init dummy;
        auto s = random_schema();
        s = local_schema_registry().get_or_load(s->version(), [s] (table_schema_version) { return frozen_schema(s); });
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

        auto learned_s2 = local_schema_registry().get_or_load(s2->version(), [&] (table_schema_version) {
            return frozen_schema(s2);
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
        auto learned_s3 = local_schema_registry().get_or_load(s3->version(), [&, fs = frozen_schema(s3)] (table_schema_version) -> future<frozen_schema> {
            co_await s3_thr.enter();
            co_return fs;
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
        auto learned_s4 = local_schema_registry().get_or_load(s4->version(), [&, fs = frozen_schema(s4)] (table_schema_version) -> future<frozen_schema> {
            co_await s4_thr.enter();
            co_return fs;
        });
        s4_entered.get();
        s4 = local_schema_registry().get_or_load(s4->version(), [&, fs = frozen_schema(s4)] (table_schema_version) { return fs; });
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
