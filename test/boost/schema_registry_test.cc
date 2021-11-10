/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */


#include <seastar/core/thread.hh>

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "schema_registry.hh"
#include "schema_builder.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/schema_registry.hh"
#include "db/config.hh"
#include "db/schema_tables.hh"
#include "types/list.hh"
#include "utils/UUID_gen.hh"

static table_schema_version random_schema_version() {
    return utils::UUID_gen::get_time_UUID();
}

static bytes random_column_name() {
    return to_bytes(to_hex(make_blob(32)));
}

static schema_ptr random_schema(schema_registry& registry, table_schema_version v) {
    return schema_builder(registry, "ks", "cf")
           .with_column("pk", bytes_type, column_kind::partition_key)
           .with_column(random_column_name(), bytes_type)
           .with_version(v)
           .build();
}

SEASTAR_THREAD_TEST_CASE(test_load_with_non_nantive_type) {
    tests::schema_registry_wrapper registry;
    auto my_list_type = list_type_impl::get_instance(utf8_type, true);

    auto version = random_schema_version();

    registry->get_or_load(version, [&] (table_schema_version v) {
        return schema_builder(registry, "ks", "cf")
               .with_column("pk", bytes_type, column_kind::partition_key)
               .with_column("val", my_list_type)
               .with_version(v)
               .build();
    }).get();
}

SEASTAR_TEST_CASE(test_async_loading) {
    return seastar::async([] {
        tests::schema_registry_wrapper registry;
        auto v1 = random_schema_version();
        auto v2 = random_schema_version();

        auto s1_loaded = registry->get_or_load(v1, [&registry] (table_schema_version v) {
            return make_ready_future<frozen_schema>(frozen_schema(random_schema(registry, v)));
        }).get0();

        BOOST_REQUIRE(s1_loaded);
        BOOST_REQUIRE(s1_loaded->version() == v1);
        auto s1_later = registry->get_or_null(v1);
        BOOST_REQUIRE(s1_later);

        auto s2_loaded = registry->get_or_load(v2, [&registry] (table_schema_version v) {
            return later().then([&registry, v] { return frozen_schema(random_schema(registry, v)); });
        }).get0();

        BOOST_REQUIRE(s2_loaded);
        BOOST_REQUIRE(s2_loaded->version() == v2);
        auto s2_later = registry->get_or_null(s2_loaded->version());
        BOOST_REQUIRE(s2_later);
    });
}

SEASTAR_TEST_CASE(test_schema_is_synced_when_syncer_doesnt_defer) {
    return seastar::async([] {
        tests::schema_registry_wrapper registry;
        auto s = registry->get_or_load(random_schema_version(), [&registry] (table_schema_version v) { return random_schema(registry, v); });
        BOOST_REQUIRE(!s->is_synced());
        s->registry_entry().maybe_sync([] { return make_ready_future<>(); }).get();
        BOOST_REQUIRE(s->is_synced());
    });
}

SEASTAR_TEST_CASE(test_schema_is_synced_when_syncer_defers) {
    return seastar::async([] {
        tests::schema_registry_wrapper registry;
        auto s = registry->get_or_load(random_schema_version(), [&registry] (table_schema_version v) { return random_schema(registry, v); });
        BOOST_REQUIRE(!s->is_synced());
        s->registry_entry().maybe_sync([] { return later(); }).get();
        BOOST_REQUIRE(s->is_synced());
    });
}

SEASTAR_TEST_CASE(test_failed_sync_can_be_retried) {
    return seastar::async([] {
        tests::schema_registry_wrapper registry;
        auto s = registry->get_or_load(random_schema_version(), [&registry] (table_schema_version v) { return random_schema(registry, v); });
        BOOST_REQUIRE(!s->is_synced());

        promise<> fail_sync;

        auto f1 = s->registry_entry().maybe_sync([&fail_sync] () mutable {
            return fail_sync.get_future().then([] {
                throw std::runtime_error("sync failed");
            });
        });

        // concurrent maybe_sync should attach the the current one
        auto f2 = s->registry_entry().maybe_sync([] { return make_ready_future<>(); });

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

        s->registry_entry().maybe_sync([] { return make_ready_future<>(); }).get();
        BOOST_REQUIRE(s->is_synced());
    });
}
