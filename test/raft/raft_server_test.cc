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

// The word "default" means "usually used by the tests here".
template <typename clock_type = std::chrono::steady_clock>
static raft_cluster<clock_type> get_default_cluster(test_case test_config) {
    return raft_cluster<clock_type>{
        std::move(test_config),
        ::apply_changes,
        0,
        0,
        0, false, tick_delay, rpc_config{}
    };
}

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
    test_case test_config {
        .nodes = 1,
        .config = std::vector<raft::server::configuration>({
            raft::server::configuration {
                .snapshot_threshold_log_size = 0,
                .snapshot_trailing_size = 0,
                .max_log_size = command_size,
                .max_command_size = command_size
            }
        })
    };
    auto cluster = get_default_cluster(std::move(test_config));
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
    auto cluster = get_default_cluster(test_case{ .nodes = 1 });
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

static void test_func_on_aborted_server_aux(
    std::function<future<>(raft::server&, abort_source*)> func,
    const raft::server::configuration& config = raft::server::configuration{})
{
    const size_t node_count = 2;
    auto test_config = test_case {
        .nodes = node_count,
        .config = std::vector<raft::server::configuration>(node_count, config)
    };
    auto cluster = get_default_cluster(std::move(test_config));

    constexpr std::string_view error_message = "some unfunny error message";
    auto check_default_message = [] (const raft::stopped_error& e) {
        return std::string_view(e.what()) == "Raft instance is stopped";
    };
    auto check_error_message = [&error_message] (const raft::stopped_error& e) {
        return std::string_view(e.what()) == fmt::format("Raft instance is stopped, reason: \"{}\"", error_message);
    };

    /* Case 1. Default error message */ {
        auto& s1 = cluster.get_server(0);
        s1.start().get();
        s1.abort().get();

        abort_source as;

        // Regardless of the state of the passed abort_source, we should get raft::stopped_error.
        BOOST_CHECK_EXCEPTION((void) func(s1, nullptr).get(), raft::stopped_error, check_default_message);
        BOOST_CHECK_EXCEPTION((void) func(s1, &as).get(), raft::stopped_error, check_default_message);
        as.request_abort();
        BOOST_CHECK_EXCEPTION((void) func(s1, &as).get(), raft::stopped_error, check_default_message);
    }

    /* Case 2. Custom error message */ {
        auto& s2 = cluster.get_server(1);
        s2.start().get();
        s2.abort(sstring(error_message)).get();

        abort_source as;

        // The same checks as above: we just verify that the error message is what we want.
        BOOST_CHECK_EXCEPTION((void) func(s2, nullptr).get(), raft::stopped_error, check_error_message);
        BOOST_CHECK_EXCEPTION((void) func(s2, &as).get(), raft::stopped_error, check_error_message);
        as.request_abort();
        BOOST_CHECK_EXCEPTION((void) func(s2, &as).get(), raft::stopped_error, check_error_message);
    }
}

static void test_add_entry_on_aborted_server_aux(const bool enable_forwarding) {
    raft::server::configuration config { .enable_forwarding = enable_forwarding };
    int val = 0;
    auto add_entry = [&val] (raft::server& server, abort_source* as) {
        return server.add_entry(create_command(val++), raft::wait_type::committed, as);
    };
    test_func_on_aborted_server_aux(add_entry, config);
}

static void test_modify_config_on_aborted_server_aux(const bool enable_forwarding) {
    raft::server::configuration config { .enable_forwarding = enable_forwarding };
    auto modify_config = [] (raft::server& server, abort_source* as) {
        return server.modify_config({}, {}, as);
    };
    test_func_on_aborted_server_aux(modify_config, config);
}

// Reproducers of SCYLLADB-841: After raft::server had been aborted, both
// add_entry and modify_config used to return raft::not_a_leader with
// a null ID when forwarding was disabled.
//
// We verify that that's not the case. Furthermore, we check that
// raft::stopped_error is preferred over raft::request_aborted
// if both exceptions apply. That's a more natural choice.
SEASTAR_THREAD_TEST_CASE(test_add_entry_on_aborted_server_disabled_forwarding) {
    test_add_entry_on_aborted_server_aux(false);
}
SEASTAR_THREAD_TEST_CASE(test_add_entry_on_aborted_server_enabled_forwarding) {
    test_add_entry_on_aborted_server_aux(true);
}
SEASTAR_THREAD_TEST_CASE(test_modify_config_on_aborted_server_disabled_forwarding) {
    test_modify_config_on_aborted_server_aux(false);
}
SEASTAR_THREAD_TEST_CASE(test_modify_config_on_aborted_server_enabled_forwarding) {
    test_modify_config_on_aborted_server_aux(true);
}

// A call to raft::server::wait_for_leader should complete with
// raft::stopped_error if the server has been aborted, regardless
// of the state of the passed abort_source.
// Reproducer of SCYLLADB-841.
SEASTAR_THREAD_TEST_CASE(test_wait_for_leader_on_aborted_server) {
    test_func_on_aborted_server_aux(&raft::server::wait_for_leader);
}

// A call to raft::server::wait_for_state_change should complete with
// raft::stopped_error if the server has been aborted, regardless
// of the state of the passed abort_source.
// Reproducer of SCYLLADB-841.
SEASTAR_THREAD_TEST_CASE(test_wait_for_state_change_on_aborted_server) {
    test_func_on_aborted_server_aux(&raft::server::wait_for_state_change);
}
