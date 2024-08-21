/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <fmt/ranges.h>

#include "test/lib/scylla_test_case.hh"
#include "test/lib/cql_test_env.hh"

#include "db/config.hh"
#include "service/migration_manager.hh"
#include "service/raft/raft_group_registry.hh"
#include "service/raft/group0_state_machine.hh"
#include "service/raft/group0_state_machine_merger.hh"
#include "idl/experimental/broadcast_tables_lang.dist.hh"
#include "idl/experimental/broadcast_tables_lang.dist.impl.hh"
#include "idl/group0_state_machine.dist.hh"
#include "idl/group0_state_machine.dist.impl.hh"
#include "utils/error_injection.hh"
#include "test/lib/expr_test_utils.hh"

const auto OLD_TIMEUUID = utils::UUID_gen::get_time_UUID(std::chrono::system_clock::time_point::min());

static service::group0_command create_command(utils::UUID id) {
    auto mut = canonical_mutation{mutation{db::system_keyspace::group0_history(), partition_key::make_empty()}};

    return service::group0_command {
        .change{service::write_mutations{std::vector<canonical_mutation>{mut}}},
        .history_append{db::system_keyspace::make_group0_history_state_id_mutation(
                        id, std::nullopt, "test")},
        .new_state_id = id,
        .creator_addr{},
        .creator_id{raft::server_id::create_random_id()},
    };
}

SEASTAR_TEST_CASE(test_group0_state_machine_merger_timeuuid_order) {
    // Regression test for #14568 and #14600.
    auto [db, db_impl] = cql3::expr::test_utils::make_data_dictionary_database(db::system_keyspace::group0_history());

    semaphore s{1};
    auto mutex_holder = co_await get_units(s, 1);

    // From #7729
    // UUID's `<=>` comparator does not preserve timeuuid monotonicity.
    // Here, t1 and t2 are ordered differently as UUIDs than as timeuuids.
    auto t1 = utils::UUID("13814000-1dd2-11ff-8080-808080808080"); // 2038-09-06
    auto t2 = utils::UUID("6b1b3620-33fd-11eb-8080-808080808080"); // 2020-12-01

    // check if UUID comparison works as expected.
    BOOST_REQUIRE(t2 > t1);
    // Check if timeuuid comparison works as expected.
    BOOST_REQUIRE(utils::timeuuid_tri_compare(t2, t1) == std::strong_ordering::less);

    auto cmd1 = create_command(t1);
    auto cmd2 = create_command(t2);

    service::group0_state_machine_merger merger{OLD_TIMEUUID, std::move(mutex_holder), 1024 * 1024, db};
    size_t size = merger.cmd_size(cmd1);

    merger.add(std::move(cmd1), size);
    merger.add(std::move(cmd2), size);

    // The current state_id stored in the `system.group0_history` table is always the greatest one in the timeuuid order.
    // This is why we expect t1 to be stored in the merger as the current state_id.
    BOOST_REQUIRE_EQUAL(merger.last_id(), t1);
}

SEASTAR_TEST_CASE(test_group0_cmd_merge) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
    fmt::print("Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n");
    return make_ready_future<>();
#else
    cql_test_config cfg;
    cfg.db_config->commitlog_segment_size_in_mb(1);
    return do_with_cql_env_thread([] (cql_test_env& env) {
        auto& group0 = env.get_raft_group_registry().local().group0();
        auto& mm = env.migration_manager().local();
        auto id = utils::UUID_gen::get_time_UUID();
        service::group0_command group0_cmd {
            .history_append{db::system_keyspace::make_group0_history_state_id_mutation(
                            id, gc_clock::duration{0}, "test")},
            .new_state_id = id,
            .creator_addr{env.db().local().get_token_metadata().get_topology().my_address()},
            .creator_id{group0.id()}
        };
        std::vector<canonical_mutation> cms;
        size_t size = 0;
        auto muts = service::prepare_keyspace_drop_announcement(env.local_db(), "ks", api::new_timestamp()).get();
        // Maximum mutation size is 1/3 of commitlog segment size which we set
        // to 1M. Make one command a little bit larger than third of the max size.
        while (size < 150*1024) {
            for (auto&& m : muts) {
                cms.emplace_back(m);
                size += cms.back().representation().size();
            }
        }
        group0_cmd.change = service::schema_change{std::move(cms)};
        raft::command cmd;
        ser::serialize(cmd, group0_cmd);
        auto merges = mm.canonical_mutation_merge_count;
        utils::get_local_injector().enable("poll_fsm_output/pause");
        auto f = when_all(
                   group0.add_entry(cmd, raft::wait_type::applied, nullptr),
                   group0.add_entry(cmd, raft::wait_type::applied, nullptr),
                   group0.add_entry(cmd, raft::wait_type::applied, nullptr));
        // Sleep is needed for all the entries added above to hit the log
        seastar::sleep(std::chrono::milliseconds(100)).get();
        // After unpause all entries added above will be committed and applied together
        utils::get_local_injector().disable("poll_fsm_output/pause");
        f.get(); // Wait for apply to complete
        // Thete should be two calls to migration manager since two out of
        // three command should be merged.
        BOOST_REQUIRE_EQUAL(mm.canonical_mutation_merge_count - merges, 2);
    }, cfg);
#endif
}
