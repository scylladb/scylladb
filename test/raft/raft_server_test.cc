#include <fmt/std.h>
#include "raft/raft.hh"
#include "raft/server.hh"
#include "replication.hh"
#include "utils/error_injection.hh"
#include <seastar/util/defer.hh>
#include <seastar/core/sleep.hh>

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

namespace {

// State machine that blocks in take_snapshot() and/or load_snapshot() until
// abort() is called. Each blocking point signals a promise so the test can
// detect when the applier fiber has entered the operation.
class blocking_snapshot_state_machine : public raft::state_machine {
    seastar::abort_source _as;

    seastar::promise<> _entered_take_snapshot;
    bool _take_snapshot_signaled = false;

    seastar::promise<> _entered_load_snapshot;
    bool _load_snapshot_signaled = false;

    bool _block_take_snapshot;
    bool _block_load_snapshot;
public:
    explicit blocking_snapshot_state_machine(bool block_take = true, bool block_load = false)
        : _block_take_snapshot(block_take)
        , _block_load_snapshot(block_load)
    {}

    future<> apply(std::vector<raft::command_cref>) override {
        return make_ready_future<>();
    }

    future<raft::snapshot_id> take_snapshot() override {
        if (_block_take_snapshot) {
            if (!_take_snapshot_signaled) {
                _take_snapshot_signaled = true;
                _entered_take_snapshot.set_value();
            }
            co_await sleep_abortable(std::chrono::hours(24), _as);
        }
        co_return raft::snapshot_id::create_random_id();
    }

    void drop_snapshot(raft::snapshot_id) override {}

    future<> load_snapshot(raft::snapshot_id) override {
        if (_block_load_snapshot) {
            if (!_load_snapshot_signaled) {
                _load_snapshot_signaled = true;
                _entered_load_snapshot.set_value();
            }
            co_await sleep_abortable(std::chrono::hours(24), _as);
        }
    }

    future<> abort() override {
        _as.request_abort();
        return make_ready_future<>();
    }

    future<> wait_for_take_snapshot() {
        return _entered_take_snapshot.get_future();
    }

    future<> wait_for_load_snapshot() {
        return _entered_load_snapshot.get_future();
    }
};

// Minimal RPC for a single-node cluster (never sends anything).
// Exposes inject_apply_snapshot() to feed an install_snapshot into the
// server via the rpc_server interface, simulating a snapshot from a leader.
class noop_rpc : public raft::rpc {
public:
    future<raft::snapshot_reply> inject_apply_snapshot(raft::server_id from, raft::install_snapshot snp) {
        return _client->apply_snapshot(from, std::move(snp));
    }

    future<raft::snapshot_reply> send_snapshot(raft::server_id, const raft::install_snapshot&, seastar::abort_source&) override {
        return make_exception_future<raft::snapshot_reply>(std::logic_error("not implemented"));
    }
    future<> send_append_entries(raft::server_id, const raft::append_request&) override {
        return make_ready_future<>();
    }
    void send_append_entries_reply(raft::server_id, const raft::append_reply&) override {}
    void send_vote_request(raft::server_id, const raft::vote_request&) override {}
    void send_vote_reply(raft::server_id, const raft::vote_reply&) override {}
    void send_timeout_now(raft::server_id, const raft::timeout_now&) override {}
    void send_read_quorum(raft::server_id, const raft::read_quorum&) override {}
    void send_read_quorum_reply(raft::server_id, const raft::read_quorum_reply&) override {}
    future<raft::read_barrier_reply> execute_read_barrier_on_leader(raft::server_id) override {
        return make_exception_future<raft::read_barrier_reply>(std::logic_error("not implemented"));
    }
    future<raft::add_entry_reply> send_add_entry(raft::server_id, const raft::command&) override {
        return make_exception_future<raft::add_entry_reply>(std::logic_error("not implemented"));
    }
    future<raft::add_entry_reply> send_modify_config(raft::server_id, const std::vector<raft::config_member>&, const std::vector<raft::server_id>&) override {
        return make_exception_future<raft::add_entry_reply>(std::logic_error("not implemented"));
    }
    void on_configuration_change(raft::server_address_set, raft::server_address_set) override {}
    future<> abort() override { return make_ready_future<>(); }
};

class always_alive_failure_detector : public raft::failure_detector {
public:
    bool is_alive(raft::server_id) override { return true; }
};

// Minimal persistence that keeps state in memory.
class memory_persistence : public raft::persistence {
    raft::term_t _term;
    raft::server_id _vote;
    raft::snapshot_descriptor _snapshot;
    raft::index_t _commit_idx;
    raft::log_entries _log;
public:
    memory_persistence(raft::snapshot_descriptor snapshot)
        : _snapshot(std::move(snapshot))
    {}

    future<> store_term_and_vote(raft::term_t term, raft::server_id vote) override {
        _term = term;
        _vote = vote;
        return make_ready_future<>();
    }
    future<std::pair<raft::term_t, raft::server_id>> load_term_and_vote() override {
        return make_ready_future<std::pair<raft::term_t, raft::server_id>>(_term, _vote);
    }
    future<> store_commit_idx(raft::index_t idx) override {
        _commit_idx = idx;
        return make_ready_future<>();
    }
    future<raft::index_t> load_commit_idx() override {
        return make_ready_future<raft::index_t>(_commit_idx);
    }
    future<> store_snapshot_descriptor(const raft::snapshot_descriptor& snap, size_t) override {
        _snapshot = snap;
        return make_ready_future<>();
    }
    future<raft::snapshot_descriptor> load_snapshot_descriptor() override {
        return make_ready_future<raft::snapshot_descriptor>(_snapshot);
    }
    future<> store_log_entries(const std::vector<raft::log_entry_ptr>& entries) override {
        for (auto& e : entries) {
            _log.push_back(e);
        }
        return make_ready_future<>();
    }
    future<raft::log_entries> load_log() override {
        return make_ready_future<raft::log_entries>(raft::log_entries{});
    }
    future<> truncate_log(raft::index_t idx) override {
        while (!_log.empty() && _log.back()->idx >= idx) {
            _log.pop_back();
        }
        return make_ready_future<>();
    }
    future<> abort() override { return make_ready_future<>(); }
};

} // anonymous namespace

// Verify that server::abort() completes fast when the applier fiber is
// blocked inside state_machine::take_snapshot().
//
// Context:
// Before fixing SCYLLADB-1056, server_impl::abort() would deadlock: it waited
// for fibers to finish, but the fiber was stuck in take_snapshot(), and
// state_machine::abort() was only called after fibers completed.
//
// After fixing SCYLLADB-1056, state_machine::abort() is initiated
// before waiting for fibers, so it unblocks take_snapshot() via
// abort_requested_exception, which the applier fiber catch-handler converts
// to stop_apply_fiber.
//
// The test uses a custom state machine that blocks in take_snapshot() until
// abort() is called, a single-node cluster with a low snapshot threshold to
// trigger snapshotting quickly, and a timeout on server::abort() to detect
// a deadlock.
SEASTAR_THREAD_TEST_CASE(test_abort_unblocks_blocked_take_snapshot) {
    auto id = to_raft_id(0);

    auto sm = std::make_unique<blocking_snapshot_state_machine>(/*block_take=*/true, /*block_load=*/false);
    auto& sm_ref = *sm;

    auto rpc = std::make_unique<noop_rpc>();

    raft::configuration config;
    config.current.emplace(config_member_from_id(id));

    auto snapshot = raft::snapshot_descriptor{
        .idx = raft::index_t{0},
        .term = raft::term_t{1},
        .config = config,
    };
    auto persistence = std::make_unique<memory_persistence>(snapshot);

    auto fd = seastar::make_shared<always_alive_failure_detector>();

    // Use a snapshot_threshold of 2 so a snapshot is triggered after the
    // second applied entry (the first is a dummy entry committed when the
    // leader is elected). snapshot_threshold_log_size must be set high
    // enough to not trigger on its own for tiny entries.
    raft::server::configuration server_config{
        .snapshot_threshold = 2,
        .snapshot_threshold_log_size = 2 * 1024 * 1024,
        .snapshot_trailing = 0,
        .snapshot_trailing_size = 0,
        .max_log_size = 4 * 1024 * 1024,
        .enable_prevoting = false,
    };

    auto server = raft::create_server(id, std::move(rpc), std::move(sm),
            std::move(persistence), std::move(fd), std::move(server_config));

    server->start().get();

    // Become leader: tick past election timeout.
    server->wait_until_candidate();
    server->wait_election_done().get();

    // Add an entry and wait for it to be applied. This is the second entry
    // (after the leader's dummy entry), so the snapshot threshold of 2 is
    // reached and the applier fiber will call take_snapshot(), which blocks.
    server->add_entry(create_command(42), raft::wait_type::applied, nullptr).get();

    // Wait for the applier fiber to actually enter take_snapshot().
    sm_ref.wait_for_take_snapshot().get();

    // Now the applier fiber is blocked inside take_snapshot().
    // Call abort() -- it should complete because state machine abort
    // triggers abort_requested_exception inside take_snapshot(),
    // and the catch block converts it to stop_apply_fiber.
    server->abort("test abort during take_snapshot").get();
}

// Same as above, but for load_snapshot() -- the path taken when a follower
// receives a snapshot from a leader.
//
// The test injects an install_snapshot with a higher term through the RPC
// interface. The FSM steps down, accepts the snapshot, and io_fiber pushes
// the snapshot_descriptor to the applier queue. The applier fiber calls
// load_snapshot(), which blocks. abort() must unblock it.
SEASTAR_THREAD_TEST_CASE(test_abort_unblocks_blocked_load_snapshot) {
    auto id = to_raft_id(0);
    auto fake_leader_id = to_raft_id(1);

    auto sm = std::make_unique<blocking_snapshot_state_machine>(/*block_take=*/false, /*block_load=*/true);
    auto& sm_ref = *sm;

    auto rpc = std::make_unique<noop_rpc>();
    auto& rpc_ref = *rpc;

    // Single-node configuration so the server can become leader.
    // The FSM does not validate that the install_snapshot sender is in
    // the configuration -- it only checks the term and snapshot index.
    raft::configuration config;
    config.current.emplace(config_member_from_id(id));

    auto snapshot = raft::snapshot_descriptor{
        .idx = raft::index_t{0},
        .term = raft::term_t{1},
        .config = config,
    };
    auto persistence = std::make_unique<memory_persistence>(snapshot);

    auto fd = seastar::make_shared<always_alive_failure_detector>();

    // High thresholds so that take_snapshot is never triggered automatically.
    raft::server::configuration server_config{
        .snapshot_threshold = 999999,
        .snapshot_threshold_log_size = 100 * 1024 * 1024,
        .max_log_size = 200 * 1024 * 1024,
        .enable_prevoting = false,
    };

    auto server = raft::create_server(id, std::move(rpc), std::move(sm),
            std::move(persistence), std::move(fd), std::move(server_config));

    server->start().get();

    // Become leader so the server has some state with term 1.
    server->wait_until_candidate();
    server->wait_election_done().get();

    // Inject an install_snapshot from fake_leader_id at a higher term.
    // The FSM will step down (term 2 > term 1), accept the snapshot,
    // and io_fiber will push it to the applier queue.
    auto remote_snp_id = raft::snapshot_id::create_random_id();
    raft::install_snapshot snp{
        .current_term = raft::term_t{2},
        .snp = {
            .idx = raft::index_t{100},
            .term = raft::term_t{2},
            .config = config,
            .id = remote_snp_id,
        },
    };

    // Start apply_snapshot in the background -- it will block waiting for
    // the snapshot to be fully applied (which won't happen because
    // load_snapshot blocks).
    auto apply_future = rpc_ref.inject_apply_snapshot(fake_leader_id, std::move(snp));

    // Wait for the applier fiber to enter load_snapshot().
    sm_ref.wait_for_load_snapshot().get();

    // abort() should complete: the state machine abort fires abort_requested_exception
    // inside load_snapshot(), the catch handler converts it to stop_apply_fiber,
    // and the fiber exits.
    server->abort("test abort during load_snapshot").get();

    // The inject_apply_snapshot future was already resolved by io_fiber
    // before the applier fiber entered load_snapshot(): the FSM generates
    // a snapshot_reply message which send_message() intercepts to resolve
    // _snapshot_application_done. Consume it to avoid dangling futures.
    apply_future.get();
}
