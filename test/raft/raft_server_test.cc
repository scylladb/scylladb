#include <fmt/std.h>
#include "raft/raft.hh"
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

// A simple test verifying the most basic properties of `wait_for_state_change`:
// * Triggering the passed abort_source will abort the operation.
//   The future will be resolved.
// * The future will contain an exception, and its type will be `raft::request_aborted`.
// Reproduces SCYLLADB-665.
SEASTAR_THREAD_TEST_CASE(test_aborting_wait_for_state_change) {
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

    auto& server = cluster.get_server(0);
    server.wait_for_leader(nullptr).get();

    abort_source as;
    // Note that this future cannot resolve immediately.
    // In particular, the leader election we awaited above cannot
    // influence it since the promises corresponding to
    // waiting for a leader and state change are resolved
    // within the same call, one after the other
    // (cf. server_impl::process_fsm_output).
    future<> fut_default_ex = server.wait_for_state_change(&as);
    as.request_abort();
    BOOST_CHECK_THROW((void) fut_default_ex.get(), raft::request_aborted);
}

// Reproducer of SCYLLA-841.
SEASTAR_THREAD_TEST_CASE(test_add_entry_on_aborted_server) {
    bool forwarding_values[] = {false, true};

    for (const bool enable_forwarding : forwarding_values) {
        const size_t command_size = sizeof(size_t);
        raft_cluster<std::chrono::steady_clock> cluster(
                test_case {
                    .nodes = 2,
                    .config = std::vector<raft::server::configuration>({
                        raft::server::configuration {
                            .snapshot_threshold_log_size = 0,
                            .snapshot_trailing_size = 0,
                            .max_log_size = command_size,
                            .enable_forwarding = enable_forwarding,
                            .max_command_size = command_size
                        }
                    })
                },
                ::apply_changes,
                0,
                0,
                0, false, tick_delay, rpc_config{});

        constexpr std::string_view error_message = "some unfunny error message";
        int val = 0;

        auto add_entry = [&val] (raft::server& server, abort_source* as) {
            return server.add_entry(create_command(val++), raft::wait_type::committed, as);
        };
        auto check_default_message = [] (const raft::stopped_error& e) {
            return std::string_view(e.what()) == "Raft instance is stopped";
        };
        auto check_error_message = [&error_message] (const raft::stopped_error& e) {
            return std::string_view(e.what()) == std::format("Raft instance is stopped, reason: \"{}\"", error_message);
        };

        /* Case 1. Default error message*/ {
            const size_t server_id = 0;
            auto& s1 = cluster.get_server(server_id);
            s1.start().get();
            s1.abort().get();

            abort_source as;

            // Regardless of the state of the passed abort_source, we should get raft::stopped_error.
            BOOST_CHECK_EXCEPTION(add_entry(s1, nullptr).get(), raft::stopped_error,
                    check_default_message);
            BOOST_CHECK_EXCEPTION(add_entry(s1, &as).get(), raft::stopped_error,
                    check_default_message);
            as.request_abort();
            BOOST_CHECK_EXCEPTION(add_entry(s1, &as).get(), raft::stopped_error,
                    check_default_message);
        }

        /* Case 2. Custom error message */ {
            auto& s2 = cluster.get_server(1);
            s2.start().get();
            s2.abort(sstring(error_message)).get();

            abort_source as;

            // The same checks as above: we just verify that the error message is what we want.
            BOOST_CHECK_EXCEPTION(add_entry(s2, nullptr).get(), raft::stopped_error,
                    check_error_message);
            BOOST_CHECK_EXCEPTION(add_entry(s2, &as).get(), raft::stopped_error,
                    check_error_message);
            as.request_abort();
            BOOST_CHECK_EXCEPTION(add_entry(s2, &as).get(), raft::stopped_error,
                    check_error_message);
        }
    }
}
