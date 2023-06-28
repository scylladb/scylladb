/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "test/lib/scylla_test_case.hh"
#include "test/lib/cql_test_env.hh"

#include "service/migration_manager.hh"
#include "service/raft/raft_group_registry.hh"
#include "service/raft/group0_state_machine.hh"
#include "idl/experimental/broadcast_tables_lang.dist.hh"
#include "idl/experimental/broadcast_tables_lang.dist.impl.hh"
#include "idl/group0_state_machine.dist.hh"
#include "idl/group0_state_machine.dist.impl.hh"
#include "utils/error_injection.hh"

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
            .creator_addr{utils::fb_utilities::get_broadcast_address()},
            .creator_id{group0.id()}
        };
        std::vector<canonical_mutation> cms;
        size_t size = 0;
        auto muts = mm.prepare_keyspace_drop_announcement("ks", api::new_timestamp()).get0();
        // Maximum mutation size is half of commitlog segment size wich we set
        // to 1M. Make one command a little bit larger than third of the max size.
        while (size < 200*1024) {
            for (auto&& m : muts) {
                cms.emplace_back(m);
                size += cms.back().representation().size();
            }
        }
        group0_cmd.change = service::schema_change{std::move(cms)};
        raft::command cmd;
        ser::serialize(cmd, group0_cmd);
        auto merges = mm.canonical_mutation_merge_count;
        utils::get_local_injector().enable("fsm::poll_output/pause");
        auto f = when_all(
                   group0.add_entry(cmd, raft::wait_type::applied, nullptr),
                   group0.add_entry(cmd, raft::wait_type::applied, nullptr),
                   group0.add_entry(cmd, raft::wait_type::applied, nullptr));
        // Sleep is needed for all the entreis added above to hit the log
        seastar::sleep(std::chrono::milliseconds(100)).get();
        // After unpause all entreis added above will be committed and applied together
        utils::get_local_injector().disable("fsm::poll_output/pause");
        f.get(); // Wait for apply to complete
        // Thete should be two calls to migration manager since two out of
        // three command should be merged.
        BOOST_REQUIRE_EQUAL(mm.canonical_mutation_merge_count - merges, 2);
    }, cfg);
#endif
}
