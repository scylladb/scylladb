/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include "server.hh"

#include "utils/error_injection.hh"
#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/join.hpp>
#include <map>
#include <seastar/core/sleep.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/pipe.hh>
#include <seastar/core/metrics.hh>
#include <seastar/rpc/rpc_types.hh>
#include <absl/container/flat_hash_map.h>
#include <seastar/core/gate.hh>

#include "fsm.hh"
#include "log.hh"
#include "raft.hh"

#include "utils/exceptions.hh"

using namespace std::chrono_literals;

namespace raft {

struct active_read {
    read_id id;
    index_t idx;
    seastar::promise<read_barrier_reply> promise;
    optimized_optional<abort_source::subscription> abort;
};

struct awaited_index {
    seastar::promise<> promise;
    optimized_optional<abort_source::subscription> abort;
};

struct awaited_conf_change {
    seastar::promise<> promise;
    optimized_optional<abort_source::subscription> abort;
};

static const seastar::metrics::label server_id_label("id");
static const seastar::metrics::label log_entry_type("log_entry_type");
static const seastar::metrics::label message_type("message_type");

class server_impl : public rpc_server, public server {
public:
    explicit server_impl(server_id uuid, std::unique_ptr<rpc> rpc,
        std::unique_ptr<state_machine> state_machine, std::unique_ptr<persistence> persistence,
        seastar::shared_ptr<failure_detector> failure_detector, server::configuration config);

    server_impl(server_impl&&) = delete;

    ~server_impl() {}

    // rpc_server interface
    void append_entries(server_id from, append_request append_request) override;
    void append_entries_reply(server_id from, append_reply reply) override;
    void request_vote(server_id from, vote_request vote_request) override;
    void request_vote_reply(server_id from, vote_reply vote_reply) override;
    void timeout_now_request(server_id from, timeout_now timeout_now) override;
    void read_quorum_request(server_id from, struct read_quorum read_quorum) override;
    void read_quorum_reply(server_id from, struct read_quorum_reply read_quorum_reply) override;
    future<read_barrier_reply> execute_read_barrier(server_id, seastar::abort_source* as) override;
    future<add_entry_reply> execute_add_entry(server_id from, command cmd, seastar::abort_source* as) override;
    future<add_entry_reply> execute_modify_config(server_id from,
        std::vector<config_member> add, std::vector<server_id> del, seastar::abort_source* as) override;
    future<snapshot_reply> apply_snapshot(server_id from, install_snapshot snp) override;


    // server interface
    future<> add_entry(command command, wait_type type, seastar::abort_source* as) override;
    future<> set_configuration(config_member_set c_new, seastar::abort_source* as) override;
    raft::configuration get_configuration() const override;
    future<> start() override;
    future<> abort(sstring reason) override;
    bool is_alive() const override;
    term_t get_current_term() const override;
    future<> read_barrier(seastar::abort_source* as) override;
    void wait_until_candidate() override;
    future<> wait_election_done() override;
    future<> wait_log_idx_term(std::pair<index_t, term_t> idx_log) override;
    std::pair<index_t, term_t> log_last_idx_term() override;
    void elapse_election() override;
    bool is_leader() override;
    raft::server_id current_leader() const override;
    void tick() override;
    raft::server_id id() const override;
    void set_applier_queue_max_size(size_t queue_max_size) override;
    future<> stepdown(logical_clock::duration timeout) override;
    future<> modify_config(std::vector<config_member> add, std::vector<server_id> del, seastar::abort_source* as) override;
    future<entry_id> add_entry_on_leader(command command, seastar::abort_source* as);
    void register_metrics() override;
    size_t max_command_size() const override;
private:
    seastar::condition_variable _sm_events;

    std::unique_ptr<rpc> _rpc;
    std::unique_ptr<state_machine> _state_machine;
    std::unique_ptr<persistence> _persistence;
    seastar::shared_ptr<failure_detector> _failure_detector;
    // Protocol deterministic finite-state machine
    std::unique_ptr<fsm> _fsm;
    // id of this server
    server_id _id;
    server::configuration _config;
    std::optional<promise<>> _stepdown_promise;
    std::optional<shared_promise<>> _leader_promise;
    std::optional<awaited_conf_change> _non_joint_conf_commit_promise;
    std::optional<shared_promise<>> _state_change_promise;
    // Index of the last entry applied to `_state_machine`.
    index_t _applied_idx;
    std::list<active_read> _reads;
    std::multimap<index_t, awaited_index> _awaited_indexes;

    // Set to abort reason when abort() is called
    std::optional<sstring> _aborted;

    // Becomes true during start(), becomes false on abort() or a background error
    bool _is_alive = false;

    // Signaled when apply index is changed
    condition_variable _applied_index_changed;

    struct stop_apply_fiber{}; // exception to send when apply fiber is needs to be stopepd

    struct removed_from_config{}; // sent to applier_fiber when we're not a leader and we're outside the current configuration
    using applier_fiber_message = std::variant<
        std::vector<log_entry_ptr>,
        snapshot_descriptor,
        removed_from_config>;
    queue<applier_fiber_message> _apply_entries = queue<applier_fiber_message>(10);

    struct stats {
        uint64_t add_command = 0;
        uint64_t add_dummy = 0;
        uint64_t add_config = 0;
        uint64_t append_entries_received = 0;
        uint64_t append_entries_reply_received = 0;
        uint64_t request_vote_received = 0;
        uint64_t request_vote_reply_received = 0;
        uint64_t waiters_awoken = 0;
        uint64_t waiters_dropped = 0;
        uint64_t append_entries_reply_sent = 0;
        uint64_t append_entries_sent = 0;
        uint64_t vote_request_sent = 0;
        uint64_t vote_request_reply_sent = 0;
        uint64_t install_snapshot_sent = 0;
        uint64_t snapshot_reply_sent = 0;
        uint64_t polls = 0;
        uint64_t store_term_and_vote = 0;
        uint64_t store_snapshot = 0;
        uint64_t sm_load_snapshot = 0;
        uint64_t truncate_persisted_log = 0;
        uint64_t persisted_log_entries = 0;
        uint64_t queue_entries_for_apply = 0;
        uint64_t applied_entries = 0;
        uint64_t snapshots_taken = 0;
        uint64_t timeout_now_sent = 0;
        uint64_t timeout_now_received = 0;
        uint64_t read_quorum_sent = 0;
        uint64_t read_quorum_received = 0;
        uint64_t read_quorum_reply_sent = 0;
        uint64_t read_quorum_reply_received = 0;
    } _stats;

    struct op_status {
        term_t term; // term the entry was added with
        promise<> done; // notify when done here
        optimized_optional<seastar::abort_source::subscription> abort; // abort subscription
    };

    // Entries that have a waiter that needs to be notified when the
    // respective entry is known to be committed.
    std::map<index_t, op_status> _awaited_commits;

    // Entries that have a waiter that needs to be notified after
    // the respective entry is applied.
    std::map<index_t, op_status> _awaited_applies;

    uint64_t _next_snapshot_transfer_id = 0;

    struct snapshot_transfer {
        future<> f;
        seastar::abort_source as;
        uint64_t id;
    };

    // Contains active snapshot transfers, to be waited on exit.
    std::unordered_map<server_id, snapshot_transfer> _snapshot_transfers;

    // Contains aborted snapshot transfers with still unresolved futures
    std::unordered_map<uint64_t, future<>> _aborted_snapshot_transfers;

    // The optional is engaged when incoming snapshot is received
    // And the promise signalled when it is successfully applied or there was an error
    std::unordered_map<server_id, promise<snapshot_reply>> _snapshot_application_done;

    struct append_request_queue {
        size_t count = 0;
        future<> f = make_ready_future<>();
    };
    absl::flat_hash_map<server_id, append_request_queue> _append_request_status;

    // Called to commit entries (on a leader or otherwise).
    void notify_waiters(std::map<index_t, op_status>& waiters, const std::vector<log_entry_ptr>& entries);

    // Drop waiter that we lost track of, can happen due to a snapshot transfer,
    // or a leader removed from cluster while some entries added on it are uncommitted.
    void drop_waiters(std::optional<index_t> idx = {});

    // Wake up all waiter that wait for entries with idx smaller of equal to the one provided
    // to be applied.
    void signal_applied();

    // This fiber processes FSM output by doing the following steps in order:
    //  - persist the current term and vote
    //  - persist unstable log entries on disk.
    //  - send out messages
    future<> process_fsm_output(index_t& stable_idx, fsm_output&&);
    future<> io_fiber(index_t stable_idx);

    // This fiber runs in the background and applies committed entries.
    future<> applier_fiber();

    template <typename Message> void send_message(server_id id, Message m);

    // Abort all snapshot transfer.
    // Called when a server id is out of the configuration
    void abort_snapshot_transfer(server_id id);

    // Abort all snapshot transfers.
    // Called when no longer a leader or on shutdown
    void abort_snapshot_transfers();

    // Send snapshot in the background and notify FSM about the result.
    void send_snapshot(server_id id, install_snapshot&& snp);

    future<> _applier_status = make_ready_future<>();
    future<> _io_status = make_ready_future<>();

    seastar::metrics::metric_groups _metrics;

    // Server address set to be used by RPC module to maintain its address
    // mappings.
    // Doesn't really correspond to any configuration, neither
    // committed, nor applied. This is just an artificial address set
    // meant entirely for RPC purposes and is constructed from the last
    // configuration entry in the log (prior to sending out the messages in the
    // `io_fiber`) as follows:
    // * If the config is non-joint, it's the current configuration.
    // * If the config is joint, it's defined as a union of current and
    //   previous configurations.
    //   The motivation behind this is that server should have a collective
    //   set of addresses from both leaving and joining nodes before
    //   sending the messages, because it may send to both types of nodes.
    // After the new address set is built the diff between the last rpc config
    // observed by the `server_impl` instance and the one obtained from the last
    // conf entry is calculated. The diff is used to maintain rpc state for
    // joining and leaving servers.
    server_address_set _current_rpc_config;
    const server_address_set& get_rpc_config() const;
    // Per-item updates to rpc config.
    void add_to_rpc_config(server_address srv);
    void remove_from_rpc_config(const server_address& srv);

    // A helper to wait for a leader to get elected
    future<> wait_for_leader(seastar::abort_source* as);

    future<> wait_for_state_change(seastar::abort_source* as) override;

    // Get "safe to read" index from a leader
    future<read_barrier_reply> get_read_idx(server_id leader, seastar::abort_source* as);
    // Wait for an entry with a specific term to get committed or
    // applied locally.
    future<> wait_for_entry(entry_id eid, wait_type type, seastar::abort_source* as);
    // Wait for a read barrier index to be applied. The index
    // is typically already committed, so we don't worry about the
    // term.
    future<> wait_for_apply(index_t idx, abort_source*);

    void check_not_aborted();
    void handle_background_error(const char* fiber_name);

    // Triggered on the next tick, used to delay retries in add_entry, modify_config, read_barrier.
    std::optional<shared_promise<>> _tick_promise;
    future<> wait_for_next_tick(seastar::abort_source* as);


    seastar::gate _do_on_leader_gate;
    // Call a function on a current leader until it returns stop_iteration::yes.
    // Handles aborts and leader changes, adds a delay between
    // iterations to protect against tight loops.
    template <typename AsyncAction>
    requires requires(server_id& leader, AsyncAction aa) {
        { aa(leader) } -> std::same_as<future<stop_iteration>>;
    }
    future<> do_on_leader_with_retries(seastar::abort_source* as, AsyncAction&& action);

    friend std::ostream& operator<<(std::ostream& os, const server_impl& s);
};

server_impl::server_impl(server_id uuid, std::unique_ptr<rpc> rpc,
        std::unique_ptr<state_machine> state_machine, std::unique_ptr<persistence> persistence,
        seastar::shared_ptr<failure_detector> failure_detector, server::configuration config) :
                    _rpc(std::move(rpc)), _state_machine(std::move(state_machine)),
                    _persistence(std::move(persistence)), _failure_detector(failure_detector),
                    _id(uuid), _config(config) {
    set_rpc_server(_rpc.get());
    if (_config.snapshot_threshold_log_size > _config.max_log_size) {
        throw config_error(fmt::format("[{}] snapshot_threshold_log_size ({}) must not be greater than max_log_size ({})",
            _id, _config.snapshot_threshold_log_size, _config.max_log_size));
    }
    if (_config.snapshot_trailing_size > _config.snapshot_threshold_log_size) {
        throw config_error(fmt::format("[{}] snapshot_trailing_size ({}) must not be greater than snapshot_threshold_log_size ({})",
                                       _id, _config.snapshot_trailing_size, _config.snapshot_threshold_log_size));
    }
    if (_config.max_command_size > _config.max_log_size - _config.snapshot_trailing_size) {
        throw config_error(fmt::format(
            "[{}] max_command_size ({}) must not be greater than "
            "max_log_size - snapshot_trailing_size ({} - {} = {})",
            _id,
            _config.max_command_size,
            _config.max_log_size, _config.snapshot_trailing_size,
            _config.max_log_size - _config.snapshot_trailing_size));
    }
}

future<> server_impl::start() {
    auto [term, vote] = co_await _persistence->load_term_and_vote();
    auto snapshot  = co_await _persistence->load_snapshot_descriptor();
    auto log_entries = co_await _persistence->load_log();
    auto log = raft::log(snapshot, std::move(log_entries), _config.max_command_size);
    auto commit_idx = co_await _persistence->load_commit_idx();
    raft::configuration rpc_config = log.get_configuration();
    index_t stable_idx = log.stable_idx();
    logger.trace("[{}] start raft instance: snapshot id={} commit index={} last stable index={}", id(), snapshot.id, commit_idx, stable_idx);
    if (commit_idx > stable_idx) {
        on_internal_error(logger, "Raft init failed: committed index cannot be larger then persisted one");
    }
    _fsm = std::make_unique<fsm>(_id, term, vote, std::move(log), commit_idx, *_failure_detector,
                                 fsm_config {
                                     .append_request_threshold = _config.append_request_threshold,
                                     .max_log_size = _config.max_log_size,
                                     .enable_prevoting = _config.enable_prevoting
                                 },
                                 _sm_events);

    _applied_idx = index_t{0};
    if (snapshot.id) {
        co_await _state_machine->load_snapshot(snapshot.id);
        _applied_idx = snapshot.idx;
    }

    if (!rpc_config.current.empty()) {
        // Update RPC address map from the latest configuration (either from
        // the log or the snapshot)
        //
        // Account both for current and previous configurations since
        // the last configuration idx can point to the joint configuration entry.
        rpc_config.current.merge(rpc_config.previous);
        for (const auto& s: rpc_config.current) {
            add_to_rpc_config(s.addr);
        }
        _rpc->on_configuration_change(get_rpc_config(), {});
    }

    _is_alive = true;

    // start fiber to persist entries added to in-memory log
    _io_status = io_fiber(stable_idx);
    // start fiber to apply committed entries
    _applier_status = applier_fiber();

    // Wait for all committed entries to be applied before returning
    // to make sure that the user's state machine is up-to-date.
    while (_applied_idx < commit_idx) {
        co_await _applied_index_changed.wait();
    }

    co_return;
}

future<> server_impl::wait_for_next_tick(seastar::abort_source* as) {
    check_not_aborted();

    if (!_tick_promise) {
        _tick_promise.emplace();
    }
    try {
        co_await (as ? _tick_promise->get_shared_future(*as) : _tick_promise->get_shared_future());
    } catch (abort_requested_exception&) {
        throw request_aborted();
    }
}

future<> server_impl::wait_for_leader(seastar::abort_source* as) {
    if (_fsm->current_leader()) {
        co_return;
    }

    logger.trace("[{}] the leader is unknown, waiting through uncertainty", id());
    _fsm->ping_leader();
    if (!_leader_promise) {
        _leader_promise.emplace();
    }

    try {
        co_await (as ? _leader_promise->get_shared_future(*as) : _leader_promise->get_shared_future());
    } catch (abort_requested_exception&) {
        throw request_aborted();
    }
}

future<> server_impl::wait_for_state_change(seastar::abort_source* as) {
    if (!_state_change_promise) {
        _state_change_promise.emplace();
    }

    try {
        return as ? _state_change_promise->get_shared_future(*as) : _state_change_promise->get_shared_future();
    } catch (abort_requested_exception&) {
        throw request_aborted();
    }
}

future<> server_impl::wait_for_entry(entry_id eid, wait_type type, seastar::abort_source* as) {
    // The entry may have been already committed and even applied
    // in case it was forwarded to the leader. In this case
    // waiting for it is futile.
    if (eid.idx <= _fsm->commit_idx()) {
        if ((type == wait_type::committed) ||
            (type == wait_type::applied && eid.idx <= _applied_idx)) {

            auto term = _fsm->log_term_for(eid.idx);

            _stats.waiters_awoken++;

            if (!term) {
                // The entry at index `eid.idx` got truncated away.
                // Still, if the last snapshot's term is the same as `eid.term`, we can deduce
                // that our entry `eid` got committed at index `eid.idx` and not some different entry.
                // Indeed, let `snp_idx` be the last snapshot index (`snp_idx >= eid.idx`). Consider
                // the entry that was committed at `snp_idx`; it had the same term as the snapshot's term,
                // `snp_term`. If `eid.term == snp_term`, then we know that the entry at `snp_idx` was
                // created by the same leader as the entry `eid`. A leader doesn't replace an entry
                // that it previously appended, so when it appended the `snp_idx` entry, the entry at
                // `eid.idx` was still `eid`. By the Log Matching Property, every log that had the entry
                // `(snp_idx, snp_term)` also had the entry `eid`. Thus when the snapshot at `snp_idx`
                // was created, it included the entry `eid`.
                auto snap_idx = _fsm->log_last_snapshot_idx();
                auto snap_term = _fsm->log_term_for(snap_idx);
                assert(snap_term);
                assert(snap_idx >= eid.idx);
                if (type == wait_type::committed && snap_term == eid.term) {
                    logger.trace("[{}] wait_for_entry {}.{}: entry got truncated away, but has the snapshot's term"
                                 " (snapshot index: {})", id(), eid.term, eid.idx, snap_idx);
                    co_return;

                    // We don't do this for `wait_type::applied` - see below why.
                }

                logger.trace("[{}] wait_for_entry {}.{}: entry got truncated away", id(), eid.term, eid.idx);
                throw commit_status_unknown();
            }

            if (*term != eid.term) {
                throw dropped_entry();
            }

            if (type == wait_type::applied && _fsm->log_last_snapshot_idx() >= eid.idx) {
                // We know the entry was committed but the wait type is `applied`
                // and we don't know if the entry was applied with `state_machine::apply`
                // (we may've loaded a snapshot before we managed to apply the entry).
                // As specified by `add_entry`, throw `commit_status_unknown` in this case.
                //
                // FIXME: replace this with a different exception type - `commit_status_unknown`
                // gives too much uncertainty while we know that the entry was committed
                // and had to be applied on at least one server. Some callers of `add_entry`
                // need to know only that the current state includes that entry, whether it was done
                // through `apply` on this server or through receiving a snapshot.
                throw commit_status_unknown();
            }

            co_return;
        }
    }

    check_not_aborted();

    if (as && as->abort_requested()) {
        throw request_aborted();
    }

    auto& container = type == wait_type::committed ? _awaited_commits : _awaited_applies;
    logger.trace("[{}] waiting for entry {}.{}", id(), eid.term, eid.idx);

    // This will track the commit/apply status of the entry
    auto [it, inserted] = container.emplace(eid.idx, op_status{eid.term, promise<>()});
    if (!inserted) {
        // No two leaders can exist with the same term.
        assert(it->second.term != eid.term);

        auto term_of_commit_idx = *_fsm->log_term_for(_fsm->commit_idx());
        if (it->second.term > eid.term) {
            if (term_of_commit_idx > eid.term) {
                // There are some entries committed with a term
                // bigger than ours, our entry must have been
                // already dropped (see 3.6.2 "Committing entries
                // from previous terms").
                _stats.waiters_awoken++;
                throw dropped_entry();
            } else {
                // Our entry might still get committed if another
                // leader is elected with an older log tail, but oh
                // well, we can't wait for two entries with the same
                // index and see which one wins, keep waiting for
                // an entry with a bigger term and hope that the
                // newly elected leader will have a newer log tail.
                _stats.waiters_dropped++;
                throw commit_status_unknown();
            }
        }
        // Let's replace an older-term entry with a newer-term one.
        auto prev_wait = std::move(it->second);
        container.erase(it);
        std::tie(it, inserted) = container.emplace(eid.idx, op_status{eid.term, promise<>()});
        // Set the status of the replaced entry. Same reasoning
        // applies for choosing the right exception status as earlier.
        if (term_of_commit_idx > prev_wait.term) {
            prev_wait.done.set_exception(dropped_entry{});
            _stats.waiters_awoken++;
        } else {
            prev_wait.done.set_exception(commit_status_unknown{});
            _stats.waiters_dropped++;
        }
    }
    assert(inserted);
    if (as) {
        it->second.abort = as->subscribe([it = it, &container] () noexcept {
            it->second.done.set_exception(request_aborted());
            container.erase(it);
        });
        assert(it->second.abort);
    }
    co_await it->second.done.get_future();
    logger.trace("[{}] done waiting for {}.{}", id(), eid.term, eid.idx);
    co_return;
}

future<entry_id> server_impl::add_entry_on_leader(command cmd, seastar::abort_source* as) {
    // Wait for sufficient memory to become available
    semaphore_units<> memory_permit;
    while (true) {
        term_t t = _fsm->get_current_term();
        try {
            memory_permit = co_await _fsm->wait_for_memory_permit(as, log::memory_usage_of(cmd, _config.max_command_size));
        } catch (semaphore_aborted&) {
            throw request_aborted();
        }
        if (t == _fsm->get_current_term()) {
            break;
        }
        memory_permit.release();
    }
    logger.trace("[{}] adding entry after waiting for memory permit", id());

    try {
        const log_entry& e = _fsm->add_entry(std::move(cmd));
        memory_permit.release();
        co_return entry_id{.term = e.term, .idx = e.idx};
    } catch (const not_a_leader&) {
        // the semaphore is already destroyed, prevent memory_permit from accessing it
        memory_permit.release();
        throw;
    }
}

future<add_entry_reply> server_impl::execute_add_entry(server_id from, command cmd, seastar::abort_source* as) {
    if (from != _id && !_fsm->get_configuration().contains(from)) {
        // Do not accept entries from servers removed from the
        // configuration.
        co_return add_entry_reply{not_a_member{format("Add entry from {} was discarded since "
                                                         "it is not part of the configuration", from)}};
    }
    logger.trace("[{}] adding a forwarded entry from {}", id(), from);
    try {
        co_return add_entry_reply{co_await add_entry_on_leader(std::move(cmd), as)};
    } catch (raft::not_a_leader& e) {
        co_return add_entry_reply{transient_error{std::current_exception(), e.leader}};
    }
}

template <typename AsyncAction>
requires requires (server_id& leader, AsyncAction aa) {
    { aa(leader) } -> std::same_as<future<stop_iteration>>;
}
future<> server_impl::do_on_leader_with_retries(seastar::abort_source* as, AsyncAction&& action) {
    server_id leader = _fsm->current_leader(), prev_leader{};

    check_not_aborted();
    auto gh = _do_on_leader_gate.hold();

    while (true) {
        if (as && as->abort_requested()) {
            throw request_aborted();
        }
        check_not_aborted();
        if (leader == server_id{}) {
            co_await wait_for_leader(as);
            leader = _fsm->current_leader();
            continue;
        }
        if (prev_leader && leader == prev_leader) {
            // This is to protect against tight loop in case we didn't get
            // any new information about the current leader.
            // This can happen if the server responds with a transient_error with
            // an empty leader and the current node has not yet learned the new leader.
            // We neglect an excessive delay if the newly elected leader is the same as
            // the previous one, this supposed to be a rare.
            co_await wait_for_next_tick(as);
            prev_leader = leader = server_id{};
            continue;
        }
        prev_leader = leader;
        if (co_await action(leader) == stop_iteration::yes) {
            break;
        }
    }
}

future<> server_impl::add_entry(command command, wait_type type, seastar::abort_source* as) {
    if (command.size() > _config.max_command_size) {
        logger.trace("[{}] add_entry command size exceeds the limit: {} > {}",
                     id(), command.size(), _config.max_command_size);
        throw command_is_too_big_error(command.size(), _config.max_command_size);
    }
    _stats.add_command++;

    logger.trace("[{}] an entry is submitted", id());
    if (!_config.enable_forwarding) {
        if (const auto leader = _fsm->current_leader(); leader != _id) {
            throw not_a_leader{leader};
        }
        auto eid = co_await add_entry_on_leader(std::move(command), as);
        co_return co_await wait_for_entry(eid, type, as);
    }

    co_await do_on_leader_with_retries(as, [&](server_id& leader) -> future<stop_iteration> {
        auto reply = co_await [&]() -> future<add_entry_reply> {
            if (leader == _id) {
                logger.trace("[{}] an entry proceeds on a leader", id());
                // Make a copy of the command since we may still
                // retry and forward it.
                co_return co_await execute_add_entry(leader, command, as);
            } else {
                logger.trace("[{}] forwarding the entry to {}", id(), leader);
                try {
                    co_return co_await _rpc->send_add_entry(leader, command);
                } catch (const transport_error& e) {
                    logger.trace("[{}] send_add_entry on {} resulted in {}; "
                                 "rethrow as commit_status_unknown", _id, leader, e);
                    throw raft::commit_status_unknown();
                }
            }
        }();
        if (std::holds_alternative<raft::entry_id>(reply)) {
            co_await wait_for_entry(std::get<raft::entry_id>(reply), type, as);
            co_return stop_iteration::yes;
        }
        if (std::holds_alternative<raft::commit_status_unknown>(reply)) {
            // It should be impossible to obtain `commit_status_unknown` here
            // because neither `execute_add_entry` nor `send_add_entry` wait for the entry
            // to be committed/applied.
            on_internal_error(logger, "add_entry: `execute_add_entry` or `send_add_entry`"
                                      " returned `commit_status_unknown`");
        }
        if (std::holds_alternative<not_a_member>(reply)) {
            co_await coroutine::return_exception(std::get<not_a_member>(reply));
        }
        const auto& e = std::get<transient_error>(reply);
        logger.trace("[{}] got {}", _id, e);
        leader = e.leader;
        co_return stop_iteration::no;
    });
}

future<add_entry_reply> server_impl::execute_modify_config(server_id from,
    std::vector<config_member> add, std::vector<server_id> del, seastar::abort_source* as) {

    if (from != _id && !_fsm->get_configuration().contains(from)) {
        // Do not accept entries from servers removed from the
        // configuration.
        co_return add_entry_reply{not_a_member{format("Modify config from {} was discarded since "
                                                         "it is not part of the configuration", from)}};
    }
    try {
        // Wait for a new slot to become available
        auto cfg = get_configuration().current;
        for (auto& s : add) {
            logger.trace("[{}] adding server {} as {}", id(), s.addr.id,
                    s.can_vote? "voter" : "non-voter");
            auto it = cfg.find(s);
            if (it == cfg.end()) {
                cfg.insert(s);
            } else if (it->can_vote != s.can_vote) {
                cfg.erase(s);
                cfg.insert(s);
                logger.trace("[{}] server {} already in configuration now {}",
                        id(), s.addr.id, s.can_vote? "voter" : "non-voter");
            } else {
                logger.warn("[{}] the server {} already exists in configuration as {}",
                        id(), s.addr.id, s.can_vote? "voter" : "non-voter");
            }
        }
        for (auto& to_remove: del) {
            logger.trace("[{}] removing server {}", id(), to_remove);
            // erase(to_remove) only available from C++23
            auto it = cfg.find(to_remove);
            if (it != cfg.end()) {
                cfg.erase(it);
            }
        }
        co_await set_configuration(cfg, as);

        // `modify_config` doesn't actually need the entry id for anything
        // but we reuse the `add_entry` RPC verb which requires it.
        co_return add_entry_reply{entry_id{}};
    } catch (raft::error& e) {
        if (is_uncertainty(e)) {
            // Although modify_config() is safe to retry, preserve
            // information that the entry may already have been
            // committed in the return value.
            co_return add_entry_reply{commit_status_unknown()};
        }
        if (const auto* ex = dynamic_cast<const not_a_leader*>(&e)) {
            co_return add_entry_reply{transient_error{std::current_exception(), ex->leader}};
        }
        if (dynamic_cast<const dropped_entry*>(&e)) {
            co_return add_entry_reply{transient_error{std::current_exception(), {}}};
        }
        if (dynamic_cast<const conf_change_in_progress*>(&e)) {
            co_return add_entry_reply{transient_error{std::current_exception(), {}}};
        }
        throw;
    }
}

future<> server_impl::modify_config(std::vector<config_member> add, std::vector<server_id> del, seastar::abort_source* as) {
    if (!_config.enable_forwarding) {
        const auto leader = _fsm->current_leader();
        if (leader != _id) {
            throw not_a_leader{leader};
        }
        auto reply = co_await execute_modify_config(leader, std::move(add), std::move(del), as);
        if (std::holds_alternative<raft::entry_id>(reply)) {
            co_return;
        }
        throw raft::not_a_leader{_fsm->current_leader()};
    }

    co_await do_on_leader_with_retries(as, [&](server_id& leader) -> future<stop_iteration> {
        auto reply = co_await [&]() -> future<add_entry_reply> {
            if (leader == _id) {
                // Make a copy since of the params since we may
                // still retry and forward them.
                co_return co_await execute_modify_config(leader, add, del, as);
            } else {
                logger.trace("[{}] forwarding the entry to {}", id(), leader);
                try {
                    co_return co_await _rpc->send_modify_config(leader, add, del);
                } catch (const transport_error& e) {
                    logger.trace("[{}] send_modify_config on {} resulted in {}; "
                                 "rethrow as commit_status_unknown", _id, leader, e);
                    throw raft::commit_status_unknown();
                }
            }
        }();
        if (std::holds_alternative<raft::entry_id>(reply)) {
            // Do not wait for the entry locally. The reply means that the leader committed it,
            // and there is no reason to wait for our local commit index to match.
            // See also #9981.
            co_return stop_iteration::yes;
        }
        if (const auto e = std::get_if<raft::transient_error>(&reply)) {
            logger.trace("[{}] got {}", _id, *e);
            leader = e->leader;
            co_return stop_iteration::no;
        }
        if (std::holds_alternative<not_a_member>(reply)) {
            co_await coroutine::return_exception(std::get<not_a_member>(reply));
        }
        throw std::get<raft::commit_status_unknown>(reply);
    });
}

void server_impl::append_entries(server_id from, append_request append_request) {
    _stats.append_entries_received++;
    _fsm->step(from, std::move(append_request));
}

void server_impl::append_entries_reply(server_id from, append_reply reply) {
    _stats.append_entries_reply_received++;
    _fsm->step(from, std::move(reply));
}

void server_impl::request_vote(server_id from, vote_request vote_request) {
    _stats.request_vote_received++;
    _fsm->step(from, std::move(vote_request));
}

void server_impl::request_vote_reply(server_id from, vote_reply vote_reply) {
    _stats.request_vote_reply_received++;
    _fsm->step(from, std::move(vote_reply));
}

void server_impl::timeout_now_request(server_id from, timeout_now timeout_now) {
    _stats.timeout_now_received++;
    _fsm->step(from, std::move(timeout_now));
}

void server_impl::read_quorum_request(server_id from, struct read_quorum read_quorum) {
    _stats.read_quorum_received++;
    _fsm->step(from, std::move(read_quorum));
}

void server_impl::read_quorum_reply(server_id from, struct read_quorum_reply read_quorum_reply) {
    _stats.read_quorum_reply_received++;
    _fsm->step(from, std::move(read_quorum_reply));
}

void server_impl::notify_waiters(std::map<index_t, op_status>& waiters,
        const std::vector<log_entry_ptr>& entries) {
    index_t commit_idx = entries.back()->idx;
    index_t first_idx = entries.front()->idx;

    while (waiters.size() != 0) {
        auto it = waiters.begin();
        if (it->first > commit_idx) {
            break;
        }
        auto [entry_idx, status] = std::move(*it);

        // if there is a waiter entry with an index smaller than first entry
        // it means that notification is out of order which is prohibited
        assert(entry_idx >= first_idx);

        waiters.erase(it);
        if (status.term == entries[entry_idx - first_idx]->term) {
            status.done.set_value();
        } else {
            // The terms do not match which means that between the
            // times the entry was submitted and committed there
            // was a leadership change and the entry was replaced.
            status.done.set_exception(dropped_entry());
        }
        _stats.waiters_awoken++;
    }
    // Drop all waiters with smaller term that last one been committed
    // since there is no way they will be committed any longer (terms in
    // the log only grow).
    term_t last_committed_term = entries.back()->term;
    while (waiters.size() != 0) {
        auto it = waiters.begin();
        if (it->second.term < last_committed_term) {
            it->second.done.set_exception(dropped_entry());
            waiters.erase(it);
            _stats.waiters_awoken++;
        } else {
            break;
        }
    }
}

void server_impl::drop_waiters(std::optional<index_t> idx) {
    auto drop = [&] (std::map<index_t, op_status>& waiters) {
        while (waiters.size() != 0) {
            auto it = waiters.begin();
            if (idx && it->first > *idx) {
                break;
            }
            auto [entry_idx, status] = std::move(*it);
            waiters.erase(it);
            status.done.set_exception(commit_status_unknown());
            _stats.waiters_dropped++;
        }
    };
    drop(_awaited_commits);
    drop(_awaited_applies);
}

void server_impl::signal_applied() {
    auto it = _awaited_indexes.begin();

    while (it != _awaited_indexes.end()) {
        if (it->first > _applied_idx) {
            break;
        }
        it->second.promise.set_value();
        it = _awaited_indexes.erase(it);
    }
}

template <typename Message>
void server_impl::send_message(server_id id, Message m) {
    std::visit([this, id] (auto&& m) {
        using T = std::decay_t<decltype(m)>;
        if constexpr (std::is_same_v<T, append_reply>) {
            _stats.append_entries_reply_sent++;
            _rpc->send_append_entries_reply(id, m);
        } else if constexpr (std::is_same_v<T, append_request>) {
            _stats.append_entries_sent++;
             _append_request_status[id].count++;
             _append_request_status[id].f = _append_request_status[id].f.then([this, cm = std::move(m), cid = id] () noexcept -> future<> {
                // We need to copy everything from the capture because it cannot be accessed after co-routine yields.
                server_impl* server = this;
                auto m = std::move(cm);
                auto id = cid;
                try {
                    co_await server->_rpc->send_append_entries(id, m);
                } catch(...) {
                    logger.debug("[{}] io_fiber failed to send a message to {}: {}", server->_id, id, std::current_exception());
                }
                server->_append_request_status[id].count--;
                if (server->_append_request_status[id].count == 0) {
                   server->_append_request_status.erase(id);
                }
            });
        } else if constexpr (std::is_same_v<T, vote_request>) {
            _stats.vote_request_sent++;
            _rpc->send_vote_request(id, m);
        } else if constexpr (std::is_same_v<T, vote_reply>) {
            _stats.vote_request_reply_sent++;
            _rpc->send_vote_reply(id, m);
        } else if constexpr (std::is_same_v<T, timeout_now>) {
            _stats.timeout_now_sent++;
            _rpc->send_timeout_now(id, m);
        } else if constexpr (std::is_same_v<T, struct read_quorum>) {
            _stats.read_quorum_sent++;
            _rpc->send_read_quorum(id, std::move(m));
        } else if constexpr (std::is_same_v<T, struct read_quorum_reply>) {
            _stats.read_quorum_reply_sent++;
            _rpc->send_read_quorum_reply(id, std::move(m));
        } else if constexpr (std::is_same_v<T, install_snapshot>) {
            _stats.install_snapshot_sent++;
            // Send in the background.
            send_snapshot(id, std::move(m));
        } else if constexpr (std::is_same_v<T, snapshot_reply>) {
            _stats.snapshot_reply_sent++;
            assert(_snapshot_application_done.contains(id));
            // Send a reply to install_snapshot after
            // snapshot application is done.
            _snapshot_application_done[id].set_value(std::move(m));
            _snapshot_application_done.erase(id);
        } else {
            static_assert(!sizeof(T*), "not all message types are handled");
        }
    }, std::move(m));
}

// Like `configuration_diff` but with `can_vote` information forgotten.
struct rpc_config_diff {
    server_address_set joining, leaving;
};

static rpc_config_diff diff_address_sets(const server_address_set& prev, const config_member_set& current) {
    rpc_config_diff result;
    for (const auto& s : current) {
        if (!prev.contains(s.addr)) {
            result.joining.insert(s.addr);
        }
    }
    for (const auto& s : prev) {
        if (!current.contains(s.id)) {
            result.leaving.insert(s);
        }
    }
    return result;
}

future<> server_impl::process_fsm_output(index_t& last_stable, fsm_output&& batch) {
    if (batch.term_and_vote) {
        // Current term and vote are always persisted
        // together. A vote may change independently of
        // term, but it's safe to update both in this
        // case.
        co_await _persistence->store_term_and_vote(batch.term_and_vote->first, batch.term_and_vote->second);
        _stats.store_term_and_vote++;
    }

    if (batch.snp) {
        auto& [snp, is_local, max_trailing_entries] = *batch.snp;
        logger.trace("[{}] io_fiber storing snapshot {}", _id, snp.id);
        // Persist the snapshot
        co_await _persistence->store_snapshot_descriptor(snp, max_trailing_entries);
        _stats.store_snapshot++;
        // If this is locally generated snapshot there is no need to
        // load it.
        if (!is_local) {
            co_await _apply_entries.push_eventually(std::move(snp));
        }
    }

    for (const auto& snp_id: batch.snps_to_drop) {
        _state_machine->drop_snapshot(snp_id);
    }

    if (batch.log_entries.size()) {
        auto& entries = batch.log_entries;

        if (last_stable >= entries[0]->idx) {
            co_await _persistence->truncate_log(entries[0]->idx);
            _stats.truncate_persisted_log++;
        }

        utils::get_local_injector().inject("store_log_entries/test-failure",
            [] { throw std::runtime_error("store_log_entries/test-failure"); });

        // Combine saving and truncating into one call?
        // will require persistence to keep track of last idx
        co_await _persistence->store_log_entries(entries);

        last_stable = (*entries.crbegin())->idx;
        _stats.persisted_log_entries += entries.size();
    }

    // Update RPC server address mappings. Add servers which are joining
    // the cluster according to the new configuration (obtained from the
    // last_conf_idx).
    //
    // It should be done prior to sending the messages since the RPC
    // module needs to know who should it send the messages to (actual
    // network addresses of the joining servers).
    rpc_config_diff rpc_diff;
    if (batch.configuration) {
        rpc_diff = diff_address_sets(get_rpc_config(), *batch.configuration);
        for (const auto& addr: rpc_diff.joining) {
            add_to_rpc_config(addr);
        }
        _rpc->on_configuration_change(rpc_diff.joining, {});
    }

     // After entries are persisted we can send messages.
    for (auto&& m : batch.messages) {
        try {
            send_message(m.first, std::move(m.second));
        } catch(...) {
            // Not being able to send a message is not a critical error
            logger.debug("[{}] io_fiber failed to send a message to {}: {}", _id, m.first, std::current_exception());
        }
    }

    if (batch.configuration) {
        for (const auto& addr: rpc_diff.leaving) {
            abort_snapshot_transfer(addr.id);
            remove_from_rpc_config(addr);
        }
        _rpc->on_configuration_change({}, rpc_diff.leaving);
    }

    // Process committed entries.
    if (batch.committed.size()) {
        if (_non_joint_conf_commit_promise) {
            for (const auto& e: batch.committed) {
                const auto* cfg = get_if<raft::configuration>(&e->data);
                if (cfg != nullptr && !cfg->is_joint()) {
                    std::exchange(_non_joint_conf_commit_promise, std::nullopt)->promise.set_value();
                    break;
                }
            }
        }
        co_await _persistence->store_commit_idx(batch.committed.back()->idx);
        _stats.queue_entries_for_apply += batch.committed.size();
        co_await _apply_entries.push_eventually(std::move(batch.committed));
    }

    if (batch.max_read_id_with_quorum) {
        while (!_reads.empty() && _reads.front().id <= batch.max_read_id_with_quorum) {
            _reads.front().promise.set_value(_reads.front().idx);
            _reads.pop_front();
        }
    }
    if (!_fsm->is_leader()) {
        if (_stepdown_promise) {
            std::exchange(_stepdown_promise, std::nullopt)->set_value();
        }
        if (!_current_rpc_config.contains(_id)) {
            // - It's important we push this after we pushed committed entries above. It
            // will cause `applier_fiber` to drop waiters, which should be done after we
            // notify all waiters for entries committed in this batch.
            // - This may happen multiple times if `io_fiber` gets multiple batches when
            // we're outside the configuration, but it should eventually (and generally
            // quickly) stop happening (we're outside the config after all).
            co_await _apply_entries.push_eventually(removed_from_config{});
        }
        // request aborts of snapshot transfers
        abort_snapshot_transfers();
        // abort all read barriers
        for (auto& r : _reads) {
            r.promise.set_value(not_a_leader{_fsm->current_leader()});
        }
        _reads.clear();
    } else if (batch.abort_leadership_transfer) {
        if (_stepdown_promise) {
            std::exchange(_stepdown_promise, std::nullopt)->set_exception(timeout_error("Stepdown process timed out"));
        }
    }
    if (_leader_promise && _fsm->current_leader()) {
        std::exchange(_leader_promise, std::nullopt)->set_value();
    }
    if (_state_change_promise && batch.state_changed) {
        std::exchange(_state_change_promise, std::nullopt)->set_value();
    }
}

future<> server_impl::io_fiber(index_t last_stable) {
    logger.trace("[{}] io_fiber start", _id);
    try {
        while (true) {
            co_await _sm_events.when(std::bind_front(&fsm::has_output, _fsm.get()));

            while (utils::get_local_injector().enter("poll_fsm_output/pause")) {
                co_await seastar::sleep(std::chrono::milliseconds(100));
            }

            auto batch = _fsm->get_output();
            _stats.polls++;

            co_await process_fsm_output(last_stable, std::move(batch));
        }
    } catch (seastar::broken_condition_variable&) {
        // Log fiber is stopped explicitly.
    } catch (stop_apply_fiber&) {
        // Log fiber is stopped explicitly
    } catch (...) {
        handle_background_error("io");
    }
    co_return;
}

void server_impl::send_snapshot(server_id dst, install_snapshot&& snp) {
    seastar::abort_source as;
    uint64_t id = _next_snapshot_transfer_id++;
    // Use `yield()` to ensure that `_rpc->send_snapshot` is called after we emplace `f` in `_snapshot_transfers`.
    // This also catches any exceptions from `_rpc->send_snapshot` into `f`.
    future<> f = yield().then([this, &as, dst, id, snp = std::move(snp)] () mutable {
        return _rpc->send_snapshot(dst, std::move(snp), as).then_wrapped([this, dst, id] (future<snapshot_reply> f) {
            if (_aborted_snapshot_transfers.erase(id)) {
                // The transfer was aborted
                f.ignore_ready_future();
                return;
            }
            _snapshot_transfers.erase(dst);
            auto reply = raft::snapshot_reply{.current_term = _fsm->get_current_term(), .success = false};
            if (f.failed()) {
                auto eptr = f.get_exception();
                const log_level lvl = try_catch<raft::destination_not_alive_error>(eptr) != nullptr
                        ? log_level::debug
                        : log_level::error;
                logger.log(lvl, "[{}] Transferring snapshot to {} failed with: {}", _id, dst, eptr);
            } else {
                logger.trace("[{}] Transferred snapshot to {}", _id, dst);
                reply = f.get();
            }
            _fsm->step(dst, std::move(reply));
        });
    });
    auto res = _snapshot_transfers.emplace(dst, snapshot_transfer{std::move(f), std::move(as), id});
    assert(res.second);
}

future<snapshot_reply> server_impl::apply_snapshot(server_id from, install_snapshot snp) {
    snapshot_reply reply{_fsm->get_current_term(), false};
    // Previous snapshot processing may still be running if a connection from the leader was broken
    // after it sent install_snapshot but before it got a reply. It may case the snapshot to be resent
    // and it may arrive before the previous one is processed. In this rare case we return error and the leader
    // will try again later (or may be not if the snapshot that is been applied is recent enough)
    if (!_snapshot_application_done.contains(from)) {
        _fsm->step(from, std::move(snp));

        try {
            reply = co_await _snapshot_application_done[from].get_future();
        } catch (...) {
            logger.error("apply_snapshot[{}] failed with {}", _id, std::current_exception());
        }
    }
    co_return reply;
}

future<> server_impl::applier_fiber() {
    logger.trace("applier_fiber start");

    try {
        while (true) {
            auto v = co_await _apply_entries.pop_eventually();

            co_await std::visit(make_visitor(
            [this] (std::vector<log_entry_ptr>& batch) -> future<> {
                if (batch.empty()) {
                    logger.trace("[{}] applier fiber: received empty batch", _id);
                    co_return;
                }

                // Completion notification code assumes that previous snapshot is applied
                // before new entries are committed, otherwise it asserts that some
                // notifications were missing. To prevent a committed entry to
                // be notified before an earlier snapshot is applied do both
                // notification and snapshot application in the same fiber
                notify_waiters(_awaited_commits, batch);

                std::vector<command_cref> commands;
                commands.reserve(batch.size());

                index_t last_idx = batch.back()->idx;
                term_t last_term = batch.back()->term;
                assert(last_idx == _applied_idx + batch.size());

                boost::range::copy(
                       batch |
                       boost::adaptors::filtered([] (log_entry_ptr& entry) { return std::holds_alternative<command>(entry->data); }) |
                       boost::adaptors::transformed([] (log_entry_ptr& entry) { return std::cref(std::get<command>(entry->data)); }),
                       std::back_inserter(commands));

                auto size = commands.size();
                if (size) {
                    try {
                        co_await _state_machine->apply(std::move(commands));
                    } catch (abort_requested_exception& e) {
                        logger.info("[{}] applier fiber stopped because state machine was aborted: {}", _id, e);
                        throw stop_apply_fiber{};
                    } catch (...) {
                        std::throw_with_nested(raft::state_machine_error{});
                    }
                    _stats.applied_entries += size;
                }

               _applied_idx = last_idx;
               _applied_index_changed.broadcast();
               notify_waiters(_awaited_applies, batch);

               // It may happen that _fsm has already applied a later snapshot (from remote) that we didn't yet 'observe'
               // (i.e. didn't yet receive from _apply_entries queue) but will soon. We avoid unnecessary work
               // of taking snapshots ourselves but comparing our last index directly with what's currently in _fsm.
               auto last_snap_idx = _fsm->log_last_snapshot_idx();

               // Error injection to be set with one_shot
               utils::get_local_injector().inject("raft_server_snapshot_reduce_threshold",
                   [this] { _config.snapshot_threshold = 3; _config.snapshot_trailing = 1; });

               bool force_snapshot = utils::get_local_injector().enter("raft_server_force_snapshot");

               if (force_snapshot || (_applied_idx > last_snap_idx &&
                   (_applied_idx - last_snap_idx >= _config.snapshot_threshold ||
                   _fsm->log_memory_usage() >= _config.snapshot_threshold_log_size)))
               {
                   snapshot_descriptor snp;
                   snp.term = last_term;
                   snp.idx = _applied_idx;
                   snp.config = _fsm->log_last_conf_for(_applied_idx);
                   logger.trace("[{}] applier fiber: taking snapshot term={}, idx={}", _id, snp.term, snp.idx);
                   snp.id = co_await _state_machine->take_snapshot();
                   // Note that at this point (after the `co_await`), _fsm may already have applied a later snapshot.
                   // That's fine, `_fsm->apply_snapshot` will simply ignore our current attempt; we will soon receive
                   // a later snapshot from the queue.
                   if (!_fsm->apply_snapshot(snp,
                                             force_snapshot ? 0 : _config.snapshot_trailing,
                                             force_snapshot ? 0 : _config.snapshot_trailing_size, true)) {
                       logger.trace("[{}] applier fiber: while taking snapshot term={} idx={} id={},"
                              " fsm received a later snapshot at idx={}", _id, snp.term, snp.idx, snp.id, _fsm->log_last_snapshot_idx());
                   }
                   _stats.snapshots_taken++;
               }
            },
            [this] (snapshot_descriptor& snp) -> future<> {
                assert(snp.idx >= _applied_idx);
                // Apply snapshot it to the state machine
                logger.trace("[{}] apply_fiber applying snapshot {}", _id, snp.id);
                co_await _state_machine->load_snapshot(snp.id);
                drop_waiters(snp.idx);
                _applied_idx = snp.idx;
                _applied_index_changed.broadcast();
                _stats.sm_load_snapshot++;
            },
            [this] (const removed_from_config&) -> future<> {
                // If the node is no longer part of a config and no longer the leader
                // it may never know the status of entries it submitted.
                drop_waiters();
                co_return;
            }
            ), v);

            signal_applied();
        }
    } catch(stop_apply_fiber& ex) {
        // the fiber is aborted
    } catch (...) {
        handle_background_error("applier");
    }
    co_return;
}

term_t server_impl::get_current_term() const {
    return _fsm->get_current_term();
}

future<> server_impl::wait_for_apply(index_t idx, abort_source* as) {
    if (as && as->abort_requested()) {
        throw request_aborted();
    }

    check_not_aborted();

    if (idx > _applied_idx) {
        // The index is not applied yet. Wait for it.
        // This will be signalled when read_idx is applied
        auto it = _awaited_indexes.emplace(idx, awaited_index{{}, {}});
        if (as) {
            it->second.abort = as->subscribe([this, it] () noexcept {
                it->second.promise.set_exception(request_aborted());
                _awaited_indexes.erase(it);
            });
            assert(it->second.abort);
        }
        co_await it->second.promise.get_future();
    }
}

future<read_barrier_reply> server_impl::execute_read_barrier(server_id from, seastar::abort_source* as) {
    check_not_aborted();

    logger.trace("[{}] execute_read_barrier start", _id);

    std::optional<std::pair<read_id, index_t>> rid;
    try {
        rid = _fsm->start_read_barrier(from);
        if (!rid) {
            // cannot start a barrier yet
            return make_ready_future<read_barrier_reply>(std::monostate{});
        }
    } catch (not_a_leader& err) {
        return make_ready_future<read_barrier_reply>(err);
    }
    logger.trace("[{}] execute_read_barrier read id is {} for commit idx {}",
        _id, rid->first, rid->second);
    if (as && as->abort_requested()) {
        return make_exception_future<read_barrier_reply>(request_aborted());
    }
    _reads.push_back({rid->first, rid->second, {}, {}});
    auto read = std::prev(_reads.end());
    if (as) {
        read->abort = as->subscribe([this, read] () noexcept {
            read->promise.set_exception(request_aborted());
            _reads.erase(read);
        });
        assert(read->abort);
    }
    return read->promise.get_future();
}

future<read_barrier_reply> server_impl::get_read_idx(server_id leader, seastar::abort_source* as) {
    if (_id == leader) {
        return execute_read_barrier(_id, as);
    } else {
        return _rpc->execute_read_barrier_on_leader(leader);
    }
}

future<> server_impl::read_barrier(seastar::abort_source* as) {
    logger.trace("[{}] read_barrier start", _id);
    index_t read_idx;

    co_await do_on_leader_with_retries(as, [&](server_id& leader) -> future<stop_iteration> {
        auto applied = _applied_idx;
        read_barrier_reply res;
        try {
            res = co_await get_read_idx(leader, as);
        } catch (const transport_error& e) {
            logger.trace("[{}] read_barrier on {} resulted in {}; retrying", _id, leader, e);
            leader = server_id{};
            co_return stop_iteration::no;
        }
        if (std::holds_alternative<std::monostate>(res)) {
            // the leader is not ready to answer because it did not
            // committed any entries yet, so wait for any entry to be
            // committed (if non were since start of the attempt) and retry.
            logger.trace("[{}] read_barrier leader not ready", _id);
            co_await wait_for_apply(++applied, as);
            co_return stop_iteration::no;
        }
        if (std::holds_alternative<raft::not_a_leader>(res)) {
            leader = std::get<not_a_leader>(res).leader;
            co_return stop_iteration::no;
        }
        read_idx = std::get<index_t>(res);
        co_return stop_iteration::yes;
    });

    logger.trace("[{}] read_barrier read index {}, applied index {}", _id, read_idx, _applied_idx);
    co_return co_await wait_for_apply(read_idx, as);
}

void server_impl::abort_snapshot_transfer(server_id id) {
    auto it = _snapshot_transfers.find(id);
    if (it != _snapshot_transfers.end()) {
        auto& [f, as, tid] = it->second;
        logger.trace("[{}] Request abort of snapshot transfer to {}", _id, id);
        as.request_abort();
        _aborted_snapshot_transfers.emplace(tid, std::move(f));
        _snapshot_transfers.erase(it);
    }
}

void server_impl::abort_snapshot_transfers() {
    for (auto&& [id, t] : _snapshot_transfers) {
        logger.trace("[{}] Request abort of snapshot transfer to {}", _id, id);
        t.as.request_abort();
        _aborted_snapshot_transfers.emplace(t.id, std::move(t.f));
    }
    _snapshot_transfers.clear();
}

void server_impl::check_not_aborted() {
    if (_aborted) {
        throw stopped_error(*_aborted);
    }
}

void server_impl::handle_background_error(const char* fiber_name) {
    _is_alive = false;
    const auto e = std::current_exception();
    logger.error("[{}] {} fiber stopped because of the error: {}", _id, fiber_name, e);
    if (_config.on_background_error) {
        _config.on_background_error(e);
    }
}

future<> server_impl::abort(sstring reason) {
    _is_alive = false;
    _aborted = std::move(reason);
    logger.trace("[{}]: abort() called", _id);
    _fsm->stop();
    _sm_events.broken();

    // IO and applier fibers may update waiters and start new snapshot
    // transfers, so abort them first
    _apply_entries.abort(std::make_exception_ptr(stop_apply_fiber()));
    co_await seastar::when_all_succeed(std::move(_io_status), std::move(_applier_status)).discard_result();

    // Start RPC abort before aborting snapshot applications or destroying entry waiters.
    // After calling `_rpc->abort()` no new snapshot applications should be started or new waiters created
    // (see `rpc::abort()` comment and `_aborted` flag).
    auto abort_rpc = _rpc->abort();
    auto abort_sm = _state_machine->abort();
    auto abort_persistence = _persistence->abort();

    // Abort snapshot applications before waiting for `abort_rpc`,
    // since the RPC implementation may wait for snapshot applications to finish.
    for (auto&& [_, f] : _snapshot_application_done) {
        f.set_exception(std::runtime_error("Snapshot application aborted"));
    }

    // Destroy entry waiters before waiting for `abort_rpc`,
    // since the RPC implementation may wait for forwarded `modify_config` calls to finish
    // (and `modify_config` does not finish until the configuration entry is committed or an error occurs).
    for (auto& ac: _awaited_commits) {
        ac.second.done.set_exception(stopped_error(*_aborted));
    }
    for (auto& aa: _awaited_applies) {
        aa.second.done.set_exception(stopped_error(*_aborted));
    }
    _awaited_commits.clear();
    _awaited_applies.clear();
    if (_non_joint_conf_commit_promise) {
        std::exchange(_non_joint_conf_commit_promise, std::nullopt)->promise.set_exception(stopped_error(*_aborted));
    }

    // Complete all read attempts with not_a_leader
    for (auto& r: _reads) {
        r.promise.set_value(raft::not_a_leader{server_id{}});
    }
    _reads.clear();

    // Abort all read_barriers with an exception
    for (auto& i : _awaited_indexes) {
        i.second.promise.set_exception(stopped_error(*_aborted));
    }
    _awaited_indexes.clear();

    co_await seastar::when_all_succeed(std::move(abort_rpc), std::move(abort_sm), std::move(abort_persistence)).discard_result();

    if (_leader_promise) {
        _leader_promise->set_exception(stopped_error(*_aborted));
    }
    if (_tick_promise) {
        _tick_promise->set_exception(stopped_error(*_aborted));
    }

    if (_state_change_promise) {
        _state_change_promise->set_exception(stopped_error(*_aborted));
    }

    abort_snapshot_transfers();

    auto snp_futures = _aborted_snapshot_transfers | boost::adaptors::map_values;

    auto append_futures = _append_request_status | boost::adaptors::map_values |  boost::adaptors::transformed([] (append_request_queue& a) -> future<>& { return a.f; });

    auto all_futures = boost::range::join(snp_futures, append_futures);

    std::array<future<>, 1> gate{_do_on_leader_gate.close()};

    auto all_with_gate = boost::range::join(all_futures, gate);

    co_await seastar::when_all_succeed(all_with_gate.begin(), all_with_gate.end()).discard_result();
}

bool server_impl::is_alive() const {
    return _is_alive;
}

future<> server_impl::set_configuration(config_member_set c_new, seastar::abort_source* as) {
    check_not_aborted();
    const auto& cfg = _fsm->get_configuration();
    // 4.1 Cluster membership changes. Safety.
    // When the leader receives a request to add or remove a server
    // from its current configuration (C old ), it appends the new
    // configuration (C new ) as an entry in its log and replicates
    // that entry using the normal Raft mechanism.
    auto [joining, leaving] = cfg.diff(c_new);
    if (joining.size() == 0 && leaving.size() == 0) {
        co_return;
    }

    _stats.add_config++;

    if (_non_joint_conf_commit_promise) {
        logger.warn("[{}] set_configuration: a configuration change is still in progress (at index: {}, config: {})",
            _id, _fsm->log_last_conf_idx(), cfg);
        throw conf_change_in_progress{};
    }

    const auto& e = _fsm->add_entry(raft::configuration{std::move(c_new)});

    // We've just submitted a joint configuration to be committed.
    // Once the FSM discovers a committed joint configuration,
    // it appends a corresponding non-joint entry.
    // By waiting for the joint configuration first we ensure
    // that the next non-joint configuration we get from fsm in io_fiber
    // would be the one corresponding to our joint configuration,
    // no matter if the leader changed in the meantime.

    auto f = _non_joint_conf_commit_promise.emplace().promise.get_future();
    if (as) {
        _non_joint_conf_commit_promise->abort = as->subscribe([this] () noexcept {
            // If we're inside this callback, the subscription wasn't destroyed yet.
            // The subscription is destroyed when the field is reset, so if we're here, the field must be engaged.
            assert(_non_joint_conf_commit_promise);
            // Whoever resolves the promise must reset the field. Thus, if we're here, the promise is not resolved.
            std::exchange(_non_joint_conf_commit_promise, std::nullopt)->promise.set_exception(request_aborted{});
        });
    }

    try {
        co_await wait_for_entry({.term = e.term, .idx = e.idx}, wait_type::committed, as);
    } catch (...) {
        _non_joint_conf_commit_promise.reset();
        // We need to 'observe' possible exceptions in f, otherwise they will be
        // considered unhandled and cause a warning.
        (void)f.handle_exception([id = _id] (auto e) {
            logger.trace("[{}] error while waiting for non-joint configuration to be committed: {}", id, e);
        });
        throw;
    }
    co_await std::move(f);
}

raft::configuration
server_impl::get_configuration() const {
    return _fsm->get_configuration();
}

void server_impl::register_metrics() {
    namespace sm = seastar::metrics;
    _metrics.add_group("raft", {
        sm::make_total_operations("add_entries", _stats.add_command,
             sm::description("how many entries were added on this node"), {server_id_label(_id), log_entry_type("command")}),
        sm::make_total_operations("add_entries", _stats.add_dummy,
             sm::description("how many entries were added on this node"), {server_id_label(_id), log_entry_type("dummy")}),
        sm::make_total_operations("add_entries", _stats.add_config,
             sm::description("how many entries were added on this node"), {server_id_label(_id), log_entry_type("config")}),

        sm::make_total_operations("messages_received", _stats.append_entries_received,
             sm::description("how many messages were received"), {server_id_label(_id), message_type("append_entries")}),
        sm::make_total_operations("messages_received", _stats.append_entries_reply_received,
             sm::description("how many messages were received"), {server_id_label(_id), message_type("append_entries_reply")}),
        sm::make_total_operations("messages_received", _stats.request_vote_received,
             sm::description("how many messages were received"), {server_id_label(_id), message_type("request_vote")}),
        sm::make_total_operations("messages_received", _stats.request_vote_reply_received,
             sm::description("how many messages were received"), {server_id_label(_id), message_type("request_vote_reply")}),
        sm::make_total_operations("messages_received", _stats.timeout_now_received,
             sm::description("how many messages were received"), {server_id_label(_id), message_type("timeout_now")}),
        sm::make_total_operations("messages_received", _stats.read_quorum_received,
             sm::description("how many messages were received"), {server_id_label(_id), message_type("read_quorum")}),
        sm::make_total_operations("messages_received", _stats.read_quorum_reply_received,
             sm::description("how many messages were received"), {server_id_label(_id), message_type("read_quorum_reply")}),

        sm::make_total_operations("messages_sent", _stats.append_entries_sent,
             sm::description("how many messages were send"), {server_id_label(_id), message_type("append_entries")}),
        sm::make_total_operations("messages_sent", _stats.append_entries_reply_sent,
             sm::description("how many messages were sent"), {server_id_label(_id), message_type("append_entries_reply")}),
        sm::make_total_operations("messages_sent", _stats.vote_request_sent,
             sm::description("how many messages were sent"), {server_id_label(_id), message_type("request_vote")}),
        sm::make_total_operations("messages_sent", _stats.vote_request_reply_sent,
             sm::description("how many messages were sent"), {server_id_label(_id), message_type("request_vote_reply")}),
        sm::make_total_operations("messages_sent", _stats.install_snapshot_sent,
             sm::description("how many messages were sent"), {server_id_label(_id), message_type("install_snapshot")}),
        sm::make_total_operations("messages_sent", _stats.snapshot_reply_sent,
             sm::description("how many messages were sent"), {server_id_label(_id), message_type("snapshot_reply")}),
        sm::make_total_operations("messages_sent", _stats.timeout_now_sent,
             sm::description("how many messages were sent"), {server_id_label(_id), message_type("timeout_now")}),
        sm::make_total_operations("messages_sent", _stats.read_quorum_sent,
             sm::description("how many messages were sent"), {server_id_label(_id), message_type("read_quorum")}),
        sm::make_total_operations("messages_sent", _stats.read_quorum_reply_sent,
             sm::description("how many messages were sent"), {server_id_label(_id), message_type("read_quorum_reply")}),

        sm::make_total_operations("waiter_awoken", _stats.waiters_awoken,
             sm::description("how many waiters got result back"), {server_id_label(_id)}),
        sm::make_total_operations("waiter_dropped", _stats.waiters_dropped,
             sm::description("how many waiters did not get result back"), {server_id_label(_id)}),
        sm::make_total_operations("polls", _stats.polls,
             sm::description("how many time raft state machine was polled"), {server_id_label(_id)}),
        sm::make_total_operations("store_term_and_vote", _stats.store_term_and_vote,
             sm::description("how many times term and vote were persisted"), {server_id_label(_id)}),
        sm::make_total_operations("store_snapshot", _stats.store_snapshot,
             sm::description("how many snapshot were persisted"), {server_id_label(_id)}),
        sm::make_total_operations("sm_load_snapshot", _stats.sm_load_snapshot,
             sm::description("how many times user state machine was reloaded with a snapshot"), {server_id_label(_id)}),
        sm::make_total_operations("truncate_persisted_log", _stats.truncate_persisted_log,
             sm::description("how many times log was truncated on storage"), {server_id_label(_id)}),
        sm::make_total_operations("persisted_log_entries", _stats.persisted_log_entries,
             sm::description("how many log entries were persisted"), {server_id_label(_id)}),
        sm::make_total_operations("queue_entries_for_apply", _stats.queue_entries_for_apply,
             sm::description("how many log entries were queued to be applied"), {server_id_label(_id)}),
        sm::make_total_operations("applied_entries", _stats.applied_entries,
             sm::description("how many log entries were applied"), {server_id_label(_id)}),
        sm::make_total_operations("snapshots_taken", _stats.snapshots_taken,
             sm::description("how many time the user's state machine was snapshotted"), {server_id_label(_id)}),

        sm::make_gauge("in_memory_log_size", [this] { return _fsm->in_memory_log_size(); },
                       sm::description("size of in-memory part of the log"), {server_id_label(_id)}),
        sm::make_gauge("log_memory_usage", [this] { return _fsm->log_memory_usage(); },
                       sm::description("memory usage of in-memory part of the log in bytes"), {server_id_label(_id)}),
    });
}

void server_impl::wait_until_candidate() {
    while (_fsm->is_follower()) {
        _fsm->tick();
    }
}

// Wait until candidate is either leader or reverts to follower
future<> server_impl::wait_election_done() {
    while (_fsm->is_candidate()) {
        co_await yield();
    };
}

future<> server_impl::wait_log_idx_term(std::pair<index_t, term_t> idx_log) {
    while (_fsm->log_last_term() < idx_log.second || _fsm->log_last_idx() < idx_log.first) {
        co_await seastar::sleep(5us);
    }
}

std::pair<index_t, term_t> server_impl::log_last_idx_term() {
    return {_fsm->log_last_idx(), _fsm->log_last_term()};
}

bool server_impl::is_leader() {
    return _fsm->is_leader();
}

raft::server_id server_impl::current_leader() const {
    return _fsm->current_leader();
}

void server_impl::elapse_election() {
    while (_fsm->election_elapsed() < ELECTION_TIMEOUT) {
        _fsm->tick();
    }
}

void server_impl::tick() {
    _fsm->tick();

    if (_tick_promise && !_aborted) {
        std::exchange(_tick_promise, std::nullopt)->set_value();
    }
}

raft::server_id server_impl::id() const {
    return _id;
}

void server_impl::set_applier_queue_max_size(size_t queue_max_size) {
    _apply_entries.set_max_size(queue_max_size);
}

const server_address_set& server_impl::get_rpc_config() const {
    return _current_rpc_config;
}

void server_impl::add_to_rpc_config(server_address srv) {
    _current_rpc_config.emplace(std::move(srv));
}

void server_impl::remove_from_rpc_config(const server_address& srv) {
    _current_rpc_config.erase(srv);
}

future<> server_impl::stepdown(logical_clock::duration timeout) {
    if (_stepdown_promise) {
        return make_exception_future<>(std::logic_error("Stepdown is already in progress"));
    }
    try {
        _fsm->transfer_leadership(timeout);
    } catch (...) {
        return make_exception_future<>(std::current_exception());
    }
    _stepdown_promise = promise<>();
    return _stepdown_promise->get_future();
}

size_t server_impl::max_command_size() const {
    return _config.max_command_size;
}

std::unique_ptr<server> create_server(server_id uuid, std::unique_ptr<rpc> rpc,
    std::unique_ptr<state_machine> state_machine, std::unique_ptr<persistence> persistence,
    seastar::shared_ptr<failure_detector> failure_detector, server::configuration config) {
    assert(uuid != raft::server_id{utils::UUID(0, 0)});
    return std::make_unique<raft::server_impl>(uuid, std::move(rpc), std::move(state_machine),
        std::move(persistence), failure_detector, config);
}

std::ostream& operator<<(std::ostream& os, const server_impl& s) {
    fmt::print(os, "[id: {}, fsm ()]\n", s._id, s._fsm);
    return os;
}

} // end of namespace raft
