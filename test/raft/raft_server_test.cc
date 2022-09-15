#include "replication.hh"

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
    cluster.start_all().get0();

    cluster.stop_server(0, "test crash").get0();

    auto check_error = [](const raft::stopped_error& e) {
        return e.what() == sstring("Raft instance is stopped, reason: \"test crash\"");
    };
    BOOST_CHECK_EXCEPTION(cluster.add_entries(1, 0).get0(), raft::stopped_error, check_error);
    BOOST_CHECK_EXCEPTION(cluster.get_server(0).modify_config({}, {to_raft_id(0)}).get0(), raft::stopped_error, check_error);
    BOOST_CHECK_EXCEPTION(cluster.get_server(0).read_barrier().get0(), raft::stopped_error, check_error);
    BOOST_CHECK_EXCEPTION(cluster.get_server(0).set_configuration({}).get0(), raft::stopped_error, check_error);
}
