#include "replication.hh"
#include "utils/error_injection.hh"
#include <seastar/util/defer.hh>

#ifdef SEASTAR_DEBUG
// Increase tick time to allow debug to process messages
 const auto tick_delay = 200ms;
#else
const auto tick_delay = 100ms;
#endif

SEASTAR_THREAD_TEST_CASE(test_check_abort_on_client_api) {
    raft_cluster<std::chrono::steady_clock> cluster(
            test_case { .nodes = 1 },
            [](raft::server_id id, const std::vector<raft::command_cref>& commands, lw_shared_ptr<hasher_int> hasher) {
                return 0;
            },
            0,
            0,
            0, false, tick_delay, rpc_config{});
    cluster.start_all().get();

    cluster.stop_server(0, "test crash").get();

    auto check_error = [](const raft::stopped_error& e) {
        return sstring(e.what()) == sstring("Raft instance is stopped, reason: \"test crash\"");
    };
    BOOST_CHECK_EXCEPTION(cluster.add_entries(1, 0).get(), raft::stopped_error, check_error);
    BOOST_CHECK_EXCEPTION(cluster.get_server(0).modify_config({}, {to_raft_id(0)}, nullptr).get(), raft::stopped_error, check_error);
    BOOST_CHECK_EXCEPTION(cluster.get_server(0).read_barrier(nullptr).get(), raft::stopped_error, check_error);
    BOOST_CHECK_EXCEPTION(cluster.get_server(0).set_configuration({}, nullptr).get(), raft::stopped_error, check_error);
}

SEASTAR_THREAD_TEST_CASE(test_release_memory_if_add_entry_throws) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
    std::cerr << "Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n";
#else
    const size_t command_size = sizeof(size_t);
    raft_cluster<std::chrono::steady_clock> cluster(
            test_case {
                .nodes = 1,
                .config = std::vector<raft::server::configuration>({
                    raft::server::configuration {
                        .snapshot_threshold_log_size = 0,
                        .snapshot_trailing_size = 0,
                        .max_log_size = command_size,
                        .max_command_size = command_size
                    }
                })
            },
            ::apply_changes,
            0,
            0,
            0, false, tick_delay, rpc_config{});
    cluster.start_all().get();
    auto stop = defer([&cluster] { cluster.stop_all().get(); });

    utils::get_local_injector().enable("fsm::add_entry/test-failure", true);
    auto check_error = [](const std::runtime_error& e) {
        return sstring(e.what()) == sstring("fsm::add_entry/test-failure");
    };
    BOOST_CHECK_EXCEPTION(cluster.add_entries(1, 0).get(), std::runtime_error, check_error);

    // we would block forever if the memory wasn't released
    // when the exception was thrown from the first add_entry
    cluster.add_entries(1, 0).get();
    cluster.read(read_value{0, 1}).get();
#endif
}
