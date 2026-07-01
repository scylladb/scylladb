/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <fmt/ranges.h>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include "test/lib/cql_test_env.hh"

#include "db/config.hh"
#include "db/system_keyspace.hh"
#include "service/migration_manager.hh"
#include "service/raft/group0_state_machine.hh"
#include "service/raft/group0_state_machine_merger.hh"
#include "idl/experimental/broadcast_tables_lang.dist.hh"
#include "idl/experimental/broadcast_tables_lang.dist.impl.hh"
#include "idl/group0_state_machine.dist.hh"
#include "idl/group0_state_machine.dist.impl.hh"
#include "test/lib/expr_test_utils.hh"

BOOST_AUTO_TEST_SUITE(group0_cmd_merge_test)

const auto OLD_TIMEUUID = utils::UUID_gen::get_time_UUID(std::chrono::system_clock::time_point::min());

static service::group0_command create_command(utils::UUID id) {
    auto mut = canonical_mutation{mutation{db::system_keyspace::group0_history(), partition_key::make_empty()}};

    return service::group0_command {
        .change{service::write_mutations{utils::chunked_vector<canonical_mutation>{mut}}},
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
    cql_test_config cfg;
    cfg.db_config->commitlog_segment_size_in_mb(1);
    return do_with_cql_env_thread([] (cql_test_env& env) {
        auto make_large_schema_change = [&env] {
            auto id = utils::UUID_gen::get_time_UUID();
            service::group0_command cmd {
                .history_append{db::system_keyspace::make_group0_history_state_id_mutation(
                                id, gc_clock::duration{0}, "test")},
                .new_state_id = id,
                .creator_addr{env.db().local().get_token_metadata().get_topology().my_address()},
                .creator_id{raft::server_id::create_random_id()}
            };
            utils::chunked_vector<canonical_mutation> cms;
            size_t size = 0;
            auto muts = service::prepare_keyspace_drop_announcement(env.get_storage_proxy().local(), "ks", api::new_timestamp()).get();
            // Maximum mutation size is 1/3 of commitlog segment size which we set
            // to 1M. Make one command a little bit larger than third of the max size.
            while (size < 150*1024) {
                for (auto&& m : muts) {
                    cms.emplace_back(m);
                    size += cms.back().representation().size();
                }
            }
            cmd.change = service::schema_change{std::move(cms)};
            return cmd;
        };

        semaphore s{1};
        auto mutex_holder = get_units(s, 1).get();
        service::group0_state_machine_merger merger{OLD_TIMEUUID, std::move(mutex_holder), 1024 * 1024 / 3, env.data_dictionary()};

        size_t merges = 0;
        for (auto i = 0; i < 3; ++i) {
            auto cmd = make_large_schema_change();
            size_t size = merger.cmd_size(cmd);
            if (!merger.can_merge(cmd, size)) {
                merger.merge();
                ++merges;
            }
            merger.add(std::move(cmd), size);
        }
        if (!merger.empty()) {
            merger.merge();
            ++merges;
        }

        // Two commands fit into the first batch, while the third one starts
        // a new batch because of the command-size limit.
        BOOST_REQUIRE_EQUAL(merges, 2);
    }, cfg);
}

BOOST_AUTO_TEST_SUITE_END()
