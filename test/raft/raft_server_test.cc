#include <seastar/core/with_timeout.hh>
#include <fmt/std.h>
#include "raft/raft.hh"
#include "replication.hh"
#include "utils/error_injection.hh"
#include "test/lib/error_injection.hh"
#include <seastar/util/defer.hh>
#include <seastar/core/when_all.hh>

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

// Builds a state machine apply function which resolves `applied` as soon as
// node 0 (the leader in the tests using this) applies the entry carrying
// `value`. Entries added on a follower are forwarded to the leader
// asynchronously, so a test that must act only after the leader applied such
// an entry needs an explicit signal like this one.
//
// The value is expected to be added exactly once, so seeing it applied more
// than once means the test does something it didn't intend to and the check
// below fails it.
static auto signal_when_leader_applies(int value, seastar::promise<>& applied) {
    return [value, &applied, signalled = false] (raft::server_id id, const raft::log_entry_ptr_list& commands,
            lw_shared_ptr<hasher_int> hasher) mutable {
        if (id == to_raft_id(0)) {
            for (auto& entry : commands) {
                auto is = ser::as_input_stream(std::get<raft::command>(entry->data));
                if (ser::deserialize(is, std::type_identity<int>()) == value) {
                    BOOST_REQUIRE(!std::exchange(signalled, true));
                    applied.set_value();
                }
            }
        }
        return apply_changes(id, commands, hasher);
    };
}

SEASTAR_THREAD_TEST_CASE(test_check_abort_on_client_api) {
    raft_cluster<std::chrono::steady_clock> cluster(
            test_case { .nodes = 1 },
            [](raft::server_id id, const raft::log_entry_ptr_list& commands, lw_shared_ptr<hasher_int> hasher) {
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
    auto stop = defer([&cluster] noexcept { cluster.stop_all().get(); });

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
    auto stop = defer([&cluster] noexcept { cluster.stop_all().get(); });

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

// Auxiliary function for testing add_entry behavior when a snapshot that
// includes the entry being added is taken before wait_for_entry runs.
//
// Uses a 1-node cluster with aggressive snapshotting and an error injection
// point that pauses add_entry after the entry is added to the log but before
// wait_for_entry checks its status. During the pause, the entry is committed,
// applied, and a snapshot is taken.
//
// If `advance_snapshot_past_entry` is true, a second entry is added so the
// snapshot moves past the first entry's index, fully truncating it from the
// log (term_for returns nullopt). Otherwise the snapshot is taken at the
// entry's index (term_for returns the snapshot's term).
//
// In both cases, wait_for_entry should succeed for both wait types, since
// the snapshot's term matching the entry's term proves the entry was committed
// and included in the snapshot.
static void test_add_entry_load_snapshot_before_wait_aux(raft::wait_type type, bool advance_snapshot_past_entry) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
    std::cerr << "Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n";
    return;
#endif
    const size_t command_size = sizeof(size_t);
    test_case test_config {
        .nodes = 1,
        .config = std::vector<raft::server::configuration>({
            raft::server::configuration {
                // Snapshot after every entry; truncate aggressively.
                .snapshot_threshold = 1,
                .snapshot_threshold_log_size = 1,
                .snapshot_trailing = 0,
                .snapshot_trailing_size = 0,
                .max_log_size = 10 * (command_size + sizeof(raft::log_entry)),
                .enable_forwarding = false,
                .max_command_size = command_size
            }
         })
    };
    // apply_entries must be greater than the number of entries added
    // during the test, otherwise the state machine's done promise fires
    // prematurely.
    auto cluster = raft_cluster<std::chrono::steady_clock>{
        std::move(test_config),
        ::apply_changes,
        100,  // apply_entries
        0,
        0, false, tick_delay, rpc_config{}
    };
    cluster.start_all().get();
    auto stop = defer([&cluster] noexcept { cluster.stop_all().get(); });

    cluster.add_entries(5, 0).get();

    // one_shot: only the first add_entry is paused; the second one
    // (if used) bypasses the injection.
    utils::get_local_injector().enable("block_raft_add_entry_before_wait_for_entry", true);

    auto& server = cluster.get_server(0);
    auto fut = server.add_entry(create_command(42), type, nullptr);

    // Wait for add_entry(42) to reach the injection point.
    wait_for_injection_enter("block_raft_add_entry_before_wait_for_entry").get();

    // Wait for the entry to be applied.
    server.read_barrier(nullptr).get();

    if (advance_snapshot_past_entry) {
        // Add another entry so the snapshot moves past the first entry,
        // fully truncating it from the log (term_for returns nullopt).
        // The injection is one-shot and already consumed, so this goes through.
        server.add_entry(create_command(43), raft::wait_type::applied, nullptr).get();
    }

    // Take a snapshot, truncating the entry from the log.
    server.trigger_snapshot(nullptr).get();

    // Unblock wait_for_entry.
    utils::get_local_injector().receive_message("block_raft_add_entry_before_wait_for_entry");

    // Both wait types should succeed: the snapshot's term matches the entry's
    // term, proving the entry was committed and included in the snapshot.
    BOOST_CHECK_NO_THROW(fut.get());
}

// Snapshot at the entry's index: term_for(eid.idx) returns the snapshot's term.
// Tests wait_for_entry site where the removed `applied` check used to throw
// commit_status_unknown.
SEASTAR_THREAD_TEST_CASE(test_add_entry_applied_load_snapshot_at_entry) {
    test_add_entry_load_snapshot_before_wait_aux(raft::wait_type::applied, false);
}

SEASTAR_THREAD_TEST_CASE(test_add_entry_committed_load_snapshot_at_entry) {
    test_add_entry_load_snapshot_before_wait_aux(raft::wait_type::committed, false);
}

// Snapshot past the entry's index: term_for(eid.idx) returns nullopt.
// Tests the `!term` branch in wait_for_entry where `snap_term == eid.term`
// now succeeds for both wait types.
SEASTAR_THREAD_TEST_CASE(test_add_entry_applied_load_snapshot_past_entry) {
    test_add_entry_load_snapshot_before_wait_aux(raft::wait_type::applied, true);
}

SEASTAR_THREAD_TEST_CASE(test_add_entry_committed_load_snapshot_past_entry) {
    test_add_entry_load_snapshot_before_wait_aux(raft::wait_type::committed, true);
}

// Auxiliary function for testing add_entry behavior when a follower receives
// the entry via a snapshot (load_snapshot) instead of applying it locally.
//
// Setup: 3-node cluster. Node 1 (follower) is blocked from receiving
// messages from the leader (node 0), but can still send to it. Node 1
// forwards add_entry to the leader, which commits the entry (with node 2),
// applies it, and takes a snapshot. When node 1 is reconnected, the leader
// sends a snapshot (since the log entries are truncated). Node 1 loads the
// snapshot via load_snapshot(), which calls drop_waiters(). The pending
// waiter for the forwarded entry is resolved successfully because the
// snapshot's term matches the entry's term.
static void test_add_entry_wait_resolved_via_drop_waiters_aux(raft::wait_type type) {
    const size_t command_size = sizeof(size_t);
    raft::server::configuration srv_config {
        .snapshot_threshold = 1,
        .snapshot_threshold_log_size = 1,
        .snapshot_trailing = 0,
        .snapshot_trailing_size = 0,
        .max_log_size = 10 * (command_size + sizeof(raft::log_entry)),
        .max_command_size = command_size
    };
    test_case test_config {
        .nodes = 3,
        .config = std::vector<raft::server::configuration>({srv_config, srv_config, srv_config})
    };
    // Resolved when node 0 (the leader) applies the entry with command 42
    // added below.
    seastar::promise<> leader_applied_42;
    // apply_entries must be greater than the number of entries added
    // during the test, otherwise the state machine's done promise fires
    // prematurely.
    auto cluster = raft_cluster<std::chrono::steady_clock>{
        std::move(test_config),
        signal_when_leader_applies(42, leader_applied_42),
        100,  // apply_entries
        0,
        0, false, tick_delay, rpc_config{}
    };
    cluster.start_all().get();
    auto stop = defer([&cluster] noexcept { cluster.stop_all().get(); });

    // Add a few entries so all nodes are caught up.
    cluster.add_entries(5, 0).get();

    // Block node 1 from receiving messages from node 0 (leader).
    // Node 1 can still send to node 0 (forwarding works).
    cluster.block_receive(1, 0);

    // Node 1 forwards add_entry to node 0. Node 0 commits (with node 2),
    // applies, and takes a snapshot. Node 1 registers a waiter but never
    // receives the entry via append entries.
    auto& follower = cluster.get_server(1);
    auto fut = follower.add_entry(create_command(42), type, nullptr);

    // The snapshot must cover the entry, otherwise node 1, once reconnected,
    // catches up by append entries and drop_waiters() is never exercised.
    //
    // The signal fires inside apply(), before the applied index the snapshot is
    // taken at is assigned, so the barrier is what puts the entry under that
    // index. The barrier alone wouldn't do: it only waits for entries committed
    // when it registers, and the forwarded entry may not have reached the
    // leader yet.
    //
    // trigger_snapshot() waits for a persisted snapshot at or above the applied
    // index; false means automatic snapshotting (snapshot_threshold = 1 above)
    // already took it.
    auto& leader = cluster.get_server(0);
    leader_applied_42.get_future().get();
    leader.read_barrier(nullptr).get();
    leader.trigger_snapshot(nullptr).get();

    // Reconnect node 1. The leader will send a snapshot since the log
    // entries are truncated (snapshot_trailing = 0).
    cluster.connect_all();

    // drop_waiters resolves the waiter successfully since the snapshot's
    // term matches the entry's term, proving it was committed.
    BOOST_CHECK_NO_THROW(fut.get());
}

SEASTAR_THREAD_TEST_CASE(test_add_entry_applied_wait_resolved_via_drop_waiters) {
    test_add_entry_wait_resolved_via_drop_waiters_aux(raft::wait_type::applied);
}

SEASTAR_THREAD_TEST_CASE(test_add_entry_committed_wait_resolved_via_drop_waiters) {
    test_add_entry_wait_resolved_via_drop_waiters_aux(raft::wait_type::committed);
}

// Reproducer of the race originally fixed by 88a6e2446d: commit waiters must be
// resolved in index order, otherwise the ordering assert in notify_waiters()
// (entry_idx >= first_idx) trips. A waiter covered by a snapshot received from
// the leader must therefore be dropped before the entries committed above that
// snapshot's index are notified.
//
// Setup: 3-node cluster. Node 1 (follower) is blocked from receiving messages
// from the leader (node 0), but can still send to it, so its add_entry is
// forwarded to the leader and a commit waiter is registered locally, while the
// entry itself never arrives via append entries. The leader commits the entry
// (with node 2), applies it and takes a snapshot; trigger_snapshot leaves no
// trailing entries, so once node 1 is reconnected the only way for it to catch
// up is a snapshot transfer.
//
// Node 1's applier fiber is then held before load_snapshot() and another entry,
// whose index is above the snapshot index, is committed. Node 1's io_fiber
// notifies the commit waiters for that entry while the snapshot is still not
// applied. If the waiter for the first entry were dropped only by the applier
// fiber (as it used to be, when the snapshot was processed), it would still sit
// in _awaited_commits below the first index of the committed batch and trip the
// assert. io_fiber drops it when it processes the snapshot, so the order holds.
SEASTAR_THREAD_TEST_CASE(test_commit_waiter_dropped_before_notifying_above_snapshot) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
    std::cerr << "Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n";
    return;
#else
    // The default snapshot thresholds are high enough that no snapshot is taken
    // automatically during the test: the only snapshot is the one triggered
    // explicitly below.
    // Resolved when node 0 (the leader) applies the entry with command 42
    // added below.
    seastar::promise<> leader_applied_42;
    // apply_entries must be greater than the number of entries added during the
    // test, otherwise the state machine's done promise fires prematurely.
    auto cluster = raft_cluster<std::chrono::steady_clock>{
        test_case { .nodes = 3 },
        signal_when_leader_applies(42, leader_applied_42),
        100,  // apply_entries
        0,
        0, false, tick_delay, rpc_config{}
    };
    cluster.start_all().get();
    auto stop = defer([&cluster] noexcept { cluster.stop_all().get(); });

    auto& leader = cluster.get_server(0);
    auto& follower = cluster.get_server(1);

    // A server learns who the leader is from the messages the leader sends it,
    // so node 1 must observe the leader before it stops receiving from node 0.
    // Otherwise the forwarding below has nowhere to forward the entry to and
    // blocks until node 1 is reconnected.
    follower.wait_for_leader(nullptr).get();

    // Block node 1 from receiving messages from node 0 (leader).
    // Node 1 can still send to node 0 (forwarding works).
    cluster.block_receive(1, 0);

    // Node 1 forwards the entry to node 0, which commits it with node 2.
    // Node 1 registers a commit waiter but never receives the entry via
    // append entries.
    auto fut = follower.add_entry(create_command(42), raft::wait_type::committed, nullptr);

    // The snapshot must cover the entry, otherwise node 1, once reconnected,
    // catches up by append entries instead of a snapshot transfer. As in
    // test_add_entry_wait_resolved_via_drop_waiters_aux, the signal fires inside
    // apply(), before the applied index the snapshot is taken at is assigned, so
    // the barrier is what puts the entry under that index. The thresholds here
    // are the default ones, so nothing snapshots automatically and
    // trigger_snapshot() must take the snapshot itself.
    leader_applied_42.get_future().get();
    leader.read_barrier(nullptr).get();
    BOOST_REQUIRE(leader.trigger_snapshot(nullptr).get());

    // Hold the applier fiber before it loads the snapshot node 1 is about to
    // receive. Node 1 is the only server that can reach this injection point:
    // it only fires for snapshots received from the leader, node 0 is the
    // leader and node 2 is up to date. The injection is not one-shot, so it is
    // not consumed by a single server: whoever reaches it waits for the message
    // sent below. It is disabled when this scope ends, which both releases
    // anything still waiting and keeps a later snapshot transfer, if any,
    // from stalling the cluster shutdown.
    constexpr auto block_applier = "block_raft_applier_fiber_before_load_snapshot";
    scoped_error_injection blocked_applier{block_applier};

    // Reconnect node 1. The leader sends a snapshot since the entry is no
    // longer in its log.
    cluster.connect_all();
    wait_for_injection_enter(block_applier).get();

    // Commit an entry above the snapshot index while node 1 still hasn't
    // applied the snapshot. Waiting for it on node 1 guarantees that node 1's
    // io_fiber notified the commit waiters for a batch starting above the
    // snapshot index, which is the notification that used to trip the assert.
    follower.add_entry(create_command(43), raft::wait_type::committed, nullptr).get();

    // Let the applier fiber load the snapshot.
    utils::get_local_injector().receive_message(block_applier);

    // The waiter for the first entry is resolved successfully: the snapshot's
    // term matches the entry's term, which proves the entry was committed and
    // included in the snapshot.
    BOOST_CHECK_NO_THROW(fut.get());
#endif
}

// Concurrent add_entry() calls on a leader must be appended to the log - and
// therefore applied - in the order in which the calls entered add_entry().
//
// This discriminates only where the reactor shuffles its task queue: Debug, Sanitize and Fuzz
// builds. That shuffling is what lets the submissions below reach the memory permit out of order.
// Elsewhere they reach it in order anyway, a seastar semaphore handing its units out strictly
// first-come-first-served, so the test passes with or without the admission.
SEASTAR_THREAD_TEST_CASE(test_add_entry_preserves_submission_order) {
    constexpr int n = 100;
    const size_t command_size = sizeof(size_t);
    auto applied = make_lw_shared<std::vector<int>>();
    test_case test_config {
        .nodes = 1,
        .config = std::vector<raft::server::configuration>({
            raft::server::configuration {
                // Room for a couple of entries at a time - the commands are serialized ints, so
                // two of them fit in max_log_size - with a snapshot after every entry so that the
                // room comes back. Without this the limiter has 4MB and every one of the hundred
                // submissions gets its memory permit synchronously, which orders the appends by
                // construction and leaves the admission below untested.
                .snapshot_threshold = 1,
                .snapshot_threshold_log_size = 1,
                .snapshot_trailing = 0,
                .snapshot_trailing_size = 0,
                .max_log_size = command_size,
                // The path strongly consistent tables take, and the only one the ordering is
                // kept on.
                .enable_forwarding = false,
                .max_command_size = command_size
            }
        })
    };
    raft_cluster<std::chrono::steady_clock> cluster(
            std::move(test_config),
            [applied](raft::server_id id, const raft::log_entry_ptr_list& commands, lw_shared_ptr<hasher_int> hasher) {
                for (auto&& entry : commands) {
                    auto&& d = std::get<raft::command>(entry->data);
                    auto is = ser::as_input_stream(d);
                    applied->push_back(ser::deserialize(is, std::type_identity<int>()));
                }
                return commands.size();
            },
            n, 0, 0, false, tick_delay, rpc_config{});
    cluster.start_all().get();
    auto stop = defer([&cluster] noexcept { cluster.stop_all().get(); });

    // Submitted without awaiting in between, so that the submission order is the order
    // of the loop. Waited for as applied, since the check below reads what was applied.
    auto& server = cluster.get_server(0);
    std::vector<future<>> futures;
    futures.reserve(n);
    for (int v = 0; v < n; ++v) {
        futures.push_back(server.add_entry(create_command(v), raft::wait_type::applied, nullptr));
    }
    seastar::when_all_succeed(futures.begin(), futures.end()).get();

    BOOST_REQUIRE_EQUAL(applied->size(), size_t(n));
    for (int v = 0; v < n; ++v) {
        BOOST_REQUIRE_EQUAL((*applied)[v], v);
    }
}

// A configuration change completes even when the entry that completes it is
// gone from the log before this server reports it committed.
//
// set_configuration() appends the joint configuration, waits for it to commit,
// and then waits for the non-joint C_new entry the fsm appends once the joint
// one is committed. That second wait is resolved from a committed batch and
// from nowhere else, so if the C_new entry is replaced by a snapshot before
// this server ever reports it -- which is what happens when the server loses
// leadership before C_new commits and rejoins behind a snapshot that covers it
// -- the caller waits for a batch that will never come. The snapshot's own
// configuration is the sign that the change went through.
// A snapshot that does not reach the joint configuration entry says nothing
// about the configuration change waiting for it.
//
// set_configuration() engages the promise before it knows the joint entry is
// committed, so a snapshot can arrive while that first wait is still pending.
// Such a snapshot is accepted as long as its index is above the commit index,
// which is still below the joint entry, and the configuration it carries is
// the one committed at its own index -- the configuration the change is
// replacing, not its result. Taking that for the change having gone through
// would complete a change that never committed.
SEASTAR_THREAD_TEST_CASE(test_conf_change_not_completed_by_an_earlier_snapshot) {
    test_case test_config {
        .nodes = 3,
        // No forwarding, so that the change fails with the old leader instead
        // of being retried against the new one, which would complete it for
        // real and hide what is under test here.
        .config = std::vector<raft::server::configuration>(3,
                raft::server::configuration { .enable_forwarding = false })
    };
    auto cluster = raft_cluster<std::chrono::steady_clock>{
        std::move(test_config),
        ::apply_changes,
        100,  // apply_entries
        0,
        0, false, tick_delay, rpc_config{}
    };
    cluster.start_all().get();
    auto stop = defer([&cluster] noexcept { cluster.stop_all().get(); });

    auto& leader = cluster.get_server(0);
    cluster.get_server(1).wait_for_leader(nullptr).get();

    // Cut the leader off first, so that everything it appends from here on
    // stays uncommitted and the configuration change never gets past its
    // first wait.
    cluster.disconnect(0);

    // Entries below the joint one, so that the snapshot the others take lands
    // strictly between this server's commit index and the joint entry. They
    // stay uncommitted: committing them would carry the commit index up to
    // just below the joint entry and leave no index for such a snapshot.
    constexpr int below = 4;
    const auto base = leader.log_last_idx_term().first;
    std::vector<future<>> orphaned;
    for (int i = 0; i < below; ++i) {
        orphaned.push_back(leader.add_entry(create_command(i), raft::wait_type::committed, nullptr));
    }
    auto observe = defer([&orphaned] noexcept {
        for (auto& f : orphaned) {
            (void)std::move(f).handle_exception([] (std::exception_ptr) {});
        }
    });
    // add_entry() suspends before it appends, so wait for the entries to be in
    // the log rather than assume the joint entry lands above them.
    while (leader.log_last_idx_term().first < base + raft::index_t{below}) {
        seastar::yield().get();
    }

    auto change = leader.modify_config({}, {to_raft_id(2)}, nullptr);

    // The other two elect a leader between themselves, node 0 being isolated;
    // which of them wins does not matter. Waited for rather than driven with
    // elect_new_leader(), which reconnects the old leader for a moment so it
    // can vote -- long enough for the entries above to escape.
    const auto elect_deadline = std::chrono::steady_clock::now() + tick_delay * 400;
    size_t new_leader_id = 0;
    while (new_leader_id == 0) {
        for (size_t n = 1; n < 3; ++n) {
            if (cluster.get_server(n).is_leader()) {
                new_leader_id = n;
            }
        }
        BOOST_REQUIRE(std::chrono::steady_clock::now() < elect_deadline);
        seastar::sleep(tick_delay).get();
    }
    auto& new_leader = cluster.get_server(new_leader_id);

    // It snapshots at the dummy entry it appended on election, four indexes
    // below the joint entry. Keeping no trailing entries puts the start of its
    // log above the index where the old leader's log diverges, so the old
    // leader has to be sent the snapshot rather than caught up with
    // append_entries. Retried because the dummy has to be applied before there
    // is anything to snapshot.
    const auto snap_deadline = std::chrono::steady_clock::now() + tick_delay * 200;
    while (!new_leader.trigger_snapshot(nullptr).get()) {
        BOOST_REQUIRE(std::chrono::steady_clock::now() < snap_deadline);
        seastar::sleep(tick_delay).get();
    }
    BOOST_REQUIRE(!new_leader.get_configuration().is_joint());

    cluster.connect_all();

    // Wait for the old leader to take the snapshot. Its commit index can only
    // reach the snapshot index this way: the entry it holds there is its own,
    // at a stale term, and the new leader cannot append over it because it is
    // inside the snapshot it took. So the waiter for the entry at the snapshot
    // index is resolved exactly when the snapshot is processed -- and by
    // drop_commit_waiters(), which runs just ahead of the branch under test.
    const auto deadline = std::chrono::steady_clock::now() + tick_delay * 200;
    while (!orphaned[0].available()) {
        BOOST_REQUIRE(std::chrono::steady_clock::now() < deadline);
        seastar::yield().get();
    }

    // The change is still pending: the snapshot the old leader just took
    // carries the configuration its own joint entry was meant to replace, and
    // must not be read as the change having gone through.
    BOOST_REQUIRE(!change.available());

    // Now commit past the joint entry's index, so the change learns what
    // became of it instead of waiting for a commit that never comes.
    for (int i = 0; i < 6; ++i) {
        new_leader.add_entry(create_command(100 + i), raft::wait_type::applied, nullptr).get();
    }

    // And it fails, its joint entry having been replaced. With forwarding
    // disabled modify_config() reports whatever went wrong under the entry as
    // not_a_leader, which by then it is.
    try {
        seastar::with_timeout(std::chrono::steady_clock::now() + tick_delay * 200, std::move(change)).get();
        BOOST_ERROR("configuration change reported success");
    } catch (const raft::not_a_leader&) {
    } catch (...) {
        BOOST_ERROR(fmt::format("unexpected exception: {}", std::current_exception()));
    }
}

SEASTAR_THREAD_TEST_CASE(test_conf_change_completed_by_a_snapshot) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
    std::cerr << "Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n";
    return;
#else
    auto cluster = raft_cluster<std::chrono::steady_clock>{
        // Five nodes, and the change drops one of them, so that a majority of
        // C_new is three of {0,1,2,3} and the rest can carry the change through
        // without node 0. Removing a node from a three-node cluster would leave
        // C_new = {0,1}, whose majority needs node 0 itself, and cutting it off
        // would stall the change rather than orphan it.
        test_case { .nodes = 5 },
        ::apply_changes,
        100,  // apply_entries
        0,
        0, false, tick_delay, rpc_config{}
    };
    cluster.start_all().get();
    auto stop = defer([&cluster] noexcept { cluster.stop_all().get(); });

    auto& leader = cluster.get_server(0);
    cluster.get_server(1).wait_for_leader(nullptr).get();
    BOOST_REQUIRE(!leader.get_configuration().is_joint());

    // Two hooks on node 0, which drive it into the window under test. A
    // partition alone cannot: the joint entry commits under a majority of
    // C_new as well, so whoever can commit the joint entry can commit its
    // non-joint successor a round trip later, and node 0 would learn the
    // change went through the ordinary way.
    constexpr auto block_io_fiber = "block_raft_io_fiber_at_non_joint_conf";
    constexpr auto block_before_non_joint = "block_raft_set_configuration_before_non_joint";
    scoped_error_injection blocked_io{block_io_fiber};
    scoped_error_injection blocked_caller{block_before_non_joint};

    auto change = leader.modify_config({}, {to_raft_id(4)}, nullptr);

    // The first hook stops node 0's io fiber on the batch that appends the
    // non-joint entry, so that entry is never replicated and can never commit
    // here. The batch also carries the joint entry's commit, so the caller is
    // still on its first wait.
    wait_for_injection_enter(block_io_fiber).get();

    // Cut node 0 off and let the others finish the change and snapshot past
    // it. trigger_snapshot() keeps no trailing entries, so node 0's log tail
    // ends up below the new leader's first log index and it has to be sent the
    // snapshot rather than caught up with append_entries.
    cluster.disconnect(0);
    cluster.elect_new_leader(1).get();
    auto& new_leader = cluster.get_server(1);
    new_leader.add_entry(create_command(42), raft::wait_type::applied, nullptr).get();
    BOOST_REQUIRE(!new_leader.get_configuration().is_joint());
    BOOST_REQUIRE(new_leader.trigger_snapshot(nullptr).get());

    // Release the io fiber while node 0 is still isolated: it notifies the
    // joint entry's commit, so the caller reaches the second hook, and sends
    // the non-joint entry into the void. Waiting for that hook to be entered
    // is what makes the ordering exact -- the commit waiter for the joint
    // entry is resolved before the snapshot arrives, so the snapshot cannot
    // drop it as commit_status_unknown instead.
    utils::get_local_injector().receive_message(block_io_fiber);
    wait_for_injection_enter(block_before_non_joint).get();

    cluster.connect_all();
    utils::get_local_injector().receive_message(block_before_non_joint);

    // The snapshot node 0 is now sent carries a non-joint configuration, and
    // that is the only thing left that can complete the change: the entry the
    // caller would otherwise have waited for went with the log the snapshot
    // replaced.
    try {
        seastar::with_timeout(std::chrono::steady_clock::now() + tick_delay * 200, std::move(change)).get();
    } catch (...) {
        BOOST_FAIL(fmt::format("configuration change did not complete: {}", std::current_exception()));
    }
#endif
}

// A committed entry that a snapshot replaces in the raft log before the
// applier fiber gets to apply it still has its effect in the state machine
// once that snapshot is loaded, so a waiter for it must be resolved
// successfully rather than dropped.
//
// The applier fiber of node 0 is held inside apply(), so its io_fiber goes on
// committing entries it cannot apply. A new leader is then elected, which
// makes everything committed afterwards carry a higher term than the held
// entries, and node 0 is disconnected while the new leader commits, applies
// and snapshots past them. The snapshot's term therefore differs from the
// held entries' term, so the snapshot term rule of drop_waiters() cannot
// resolve their waiters; what does is op_status::committed, which io_fiber
// recorded when it reported the entry committed and its term was still known.
SEASTAR_THREAD_TEST_CASE(test_apply_waiter_resolved_when_snapshot_subsumes_committed_entry) {
    // The default snapshot thresholds are high enough that the only snapshot
    // in this test is the one triggered explicitly below, which is taken with
    // no trailing entries and so drops the whole log.
    // apply_entries must be greater than the number of entries added during
    // the test, otherwise the state machine's done promise fires prematurely.
    auto cluster = raft_cluster<std::chrono::steady_clock>{
        test_case { .nodes = 3 },
        ::apply_changes,
        100,  // apply_entries
        0,
        0, false, tick_delay, rpc_config{}
    };
    cluster.start_all().get();
    auto stop = defer([&cluster] noexcept { cluster.stop_all().get(); });

    auto& server = cluster.get_server(0);

    // Entries are added on node 0, the initial leader, so that they get their
    // indexes here and in this order: a forwarded add_entry() would not tell
    // us which entry ended up below which.
    delay_apply = to_raft_id(0);
    auto release_apply = defer([] noexcept {
        // A failed check below may leave the applier fiber waiting here, and
        // stop_all() would then wait for it forever.
        delay_apply.reset();
        if (apply_release.waiters()) {
            apply_release.signal();
        }
    });
    cluster.add_entries(1, 0).get();
    apply_entered.wait().get();

    // The waiter under test: committed while the applier fiber is held, so it
    // is marked committed and left for that fiber to resolve.
    auto fut = server.add_entry(create_command(42), raft::wait_type::applied, nullptr);
    // Commit waiters are notified in index order, so this one returning means
    // node 0 saw the entry above committed as well.
    server.add_entry(create_command(43), raft::wait_type::committed, nullptr).get();

    // From here on entries are committed with a higher term, so the snapshot
    // taken below has a different term than the two entries above.
    cluster.elect_new_leader(1).get();
    auto& leader = cluster.get_server(1);

    // Cut node 0 off, so the entries the new leader commits, applies and then
    // snapshots away never reach it as log entries.
    cluster.disconnect(0);
    leader.add_entry(create_command(44), raft::wait_type::applied, nullptr).get();
    BOOST_REQUIRE(leader.trigger_snapshot(nullptr).get());

    // Reconnect node 0. Its log tail is below the leader's first log index by
    // now, so the leader transfers the snapshot instead of appending entries.
    notify_snapshot_received = to_raft_id(0);
    auto clear_notify = defer([] noexcept { notify_snapshot_received.reset(); });
    cluster.connect_all();

    // Once node 0 has replied, its fsm has dropped the entries the waiter is
    // for and its io_fiber has handed the snapshot to its applier fiber -- all
    // while that fiber is still held inside apply(). This is what makes the
    // test deterministic: released any earlier, the applier fiber would apply
    // the entries normally and never exercise the path under test.
    snapshot_received.wait().get();

    apply_release.signal();

    // The waiter is resolved successfully by loading the snapshot.
    BOOST_CHECK_NO_THROW(fut.get());
}
