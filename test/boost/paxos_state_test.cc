/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>

#include <string_view>
#include <utility>

#include "test/lib/cql_test_env.hh"
#include "test/lib/mutation_assertions.hh"

#include "db/system_keyspace.hh"
#include "schema/schema_builder.hh"

#include "service/paxos/paxos_state.hh"

using namespace std::chrono_literals;

BOOST_AUTO_TEST_SUITE(paxos_state_test)

SEASTAR_TEST_CASE(test_prepared_statement_cache_prunes_after_table_drop) {
    return seastar::async([] {
        auto check = [] (std::string_view name, std::string_view drop_statement) {
            BOOST_TEST_CONTEXT(name) {
                cql_test_config cfg;
                // With vnodes, system.paxos is never dropped, so the weak pointers aren't invalidated immediately after
                // dropping the base table. Hence, we run the test only with tablets.
                cfg.initial_tablets = 1;
                cfg.need_remote_proxy = true;

                do_with_cql_env_thread([drop_statement] (cql_test_env& env) {
                    auto paxos_prepared_statements_cache_size = [&env] {
                        return env.get_paxos_store().local().get_prepared_statements_cache().size();
                    };

                    env.get_paxos_store().local().set_prepared_statements_prune_period(1ms);
                    BOOST_REQUIRE_EQUAL(paxos_prepared_statements_cache_size(), 0);

                    env.execute_cql("CREATE TABLE t (pk int PRIMARY KEY, v int)").get();
                    env.execute_cql("INSERT INTO t (pk, v) VALUES (0, 0) IF NOT EXISTS").get();

                    BOOST_REQUIRE_GT(paxos_prepared_statements_cache_size(), 0);

                    env.execute_cql(drop_statement).get();

                    const auto deadline = seastar::lowres_clock::now() + 10s;
                    while (paxos_prepared_statements_cache_size() != 0 && seastar::lowres_clock::now() < deadline) {
                        seastar::sleep(2ms).get();
                    }

                    BOOST_REQUIRE_EQUAL(paxos_prepared_statements_cache_size(), 0);
                }, std::move(cfg)).get();
            }
        };

        check("drop table", "DROP TABLE t");
        check("drop keyspace", "DROP KEYSPACE ks");
    });
}

namespace {

struct paxos_state_conversion_fixture {
    schema_ptr base = schema_builder(this_smp_shard_count(), "ks", "t").with_column("pk", int32_type, column_kind::partition_key).build();
    schema_ptr other = schema_builder(this_smp_shard_count(), "ks", "u").with_column("pk", int32_type, column_kind::partition_key).build();
    schema_ptr state = service::paxos::paxos_store::create_paxos_state_schema(*base);
    schema_ptr system_paxos = db::system_keyspace::paxos();
    partition_key row_key = partition_key::from_single_value(*system_paxos, bytes("key"));
    gc_clock::time_point now = gc_clock::now();
    utils::UUID ballot = utils::UUID_gen::get_time_UUID();

    clustering_key system_paxos_ck(const schema& s) const {
        return clustering_key::from_single_value(*system_paxos, uuid_type->decompose(s.id().uuid()));
    }

    // Sets the live cells of a Paxos state row: a promise and a commit ballot, expiring.
    void set_live_state(mutation& m, const clustering_key& ck) const {
        const auto& s = *m.schema();
        for (const auto& [name, ts] : {std::pair{"promise", 10}, std::pair{"most_recent_commit_at", 30}}) {
            const auto& def = *s.get_column_definition(name);
            m.set_clustered_cell(ck, def, atomic_cell::make_live(*def.type, ts,
                    def.type->decompose(ballot), now + std::chrono::hours(1), std::chrono::hours(1)));
        }
    }

    // Sets live and dead cells: the live state, a proposal nulled by a decision and a
    // pruned commit value.
    void set_state(mutation& m, const clustering_key& ck) const {
        set_live_state(m, ck);
        const auto& s = *m.schema();
        m.set_clustered_cell(ck, *s.get_column_definition("proposal"), atomic_cell::make_dead(20, now));
        m.set_clustered_cell(ck, *s.get_column_definition("most_recent_commit"), atomic_cell::make_dead(30, now));
    }
};

} // anonymous namespace

// The copy of the Paxos state between system.paxos and <table>$paxos during a
// vnodes-to-tablets migration carries live cells with their timestamps and expiry,
// and only the rows of the tables it copies.
SEASTAR_THREAD_TEST_CASE(test_paxos_state_conversion_round_trip) {
    paxos_state_conversion_fixture f;

    mutation system_paxos_mutation(f.system_paxos, f.row_key);
    f.set_state(system_paxos_mutation, f.system_paxos_ck(*f.base));
    // A row of another table in the same partition must not be copied.
    mutation other_row(f.system_paxos, f.row_key);
    f.set_state(other_row, f.system_paxos_ck(*f.other));
    mutation both = system_paxos_mutation;
    both.apply(other_row);

    const auto converted = service::paxos::internal::system_paxos_to_paxos_state_tables(both, {{f.base->id(), f.state}}, f.now);
    BOOST_REQUIRE_EQUAL(converted.size(), 1);

    mutation expected(f.state, f.row_key);
    f.set_live_state(expected, clustering_key::make_empty());
    assert_that(converted.front()).is_equal_to(expected);

    const auto back = service::paxos::internal::paxos_state_table_to_system_paxos(converted.front(), f.base->id(), f.now);
    BOOST_REQUIRE(back);
    mutation expected_back(f.system_paxos, f.row_key);
    f.set_live_state(expected_back, f.system_paxos_ck(*f.base));
    assert_that(*back).is_equal_to(expected_back);
}

// Dead Paxos state is not copied: deleted, shadowed or expired cells.
SEASTAR_THREAD_TEST_CASE(test_paxos_state_conversion_skips_dead_state) {
    paxos_state_conversion_fixture f;
    const auto ck = f.system_paxos_ck(*f.base);
    const std::unordered_map<table_id, schema_ptr> state_schemas{{f.base->id(), f.state}};

    // A row tombstone newer than every cell.
    {
        mutation m(f.system_paxos, f.row_key);
        f.set_state(m, ck);
        m.partition().apply_delete(*f.system_paxos, ck, tombstone(40, f.now));
        BOOST_REQUIRE(service::paxos::internal::system_paxos_to_paxos_state_tables(m, state_schemas, f.now).empty());
    }

    // A partition tombstone newer than the promise only.
    {
        mutation m(f.system_paxos, f.row_key);
        f.set_state(m, ck);
        m.partition().apply(tombstone(15, f.now));
        const auto converted = service::paxos::internal::system_paxos_to_paxos_state_tables(m, state_schemas, f.now);
        BOOST_REQUIRE_EQUAL(converted.size(), 1);
        mutation expected(f.state, f.row_key);
        const auto& def = *f.state->get_column_definition("most_recent_commit_at");
        expected.set_clustered_cell(clustering_key::make_empty(), def, atomic_cell::make_live(*def.type, 30,
                def.type->decompose(f.ballot), f.now + std::chrono::hours(1), std::chrono::hours(1)));
        assert_that(converted.front()).is_equal_to(expected);
    }

    // Expired cells.
    {
        mutation m(f.system_paxos, f.row_key);
        f.set_state(m, ck);
        BOOST_REQUIRE(service::paxos::internal::system_paxos_to_paxos_state_tables(m, state_schemas, f.now + std::chrono::hours(2)).empty());
    }

    // Back into system.paxos: a partition tombstone of the Paxos state table.
    {
        mutation m(f.state, f.row_key);
        f.set_state(m, clustering_key::make_empty());
        m.partition().apply(tombstone(40, f.now));
        BOOST_REQUIRE(!service::paxos::internal::paxos_state_table_to_system_paxos(m, f.base->id(), f.now));
    }
}

BOOST_AUTO_TEST_SUITE_END()
