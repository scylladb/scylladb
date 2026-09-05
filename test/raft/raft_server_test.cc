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
