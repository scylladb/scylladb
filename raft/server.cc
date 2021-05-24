/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */
#include "server.hh"

#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <map>
#include <seastar/core/sleep.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/pipe.hh>
#include <seastar/core/metrics.hh>

#include "fsm.hh"
#include "log.hh"

using namespace std::chrono_literals;

namespace raft {

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

    // server interface
    future<> add_entry(command command, wait_type type);
    future<snapshot_reply> apply_snapshot(server_id from, install_snapshot snp) override;
    future<> set_configuration(server_address_set c_new) override;
    future<> start() override;
    future<> abort() override;
    term_t get_current_term() const override;
    future<> read_barrier() override;
    void wait_until_candidate() override;
    future<> wait_election_done() override;
    future<> wait_log_idx(index_t) override;
    index_t log_last_idx();
    void elapse_election() override;
    bool is_leader() override;
    void tick() override;
private:
    std::unique_ptr<rpc> _rpc;
    std::unique_ptr<state_machine> _state_machine;
    std::unique_ptr<persistence> _persistence;
    seastar::shared_ptr<failure_detector> _failure_detector;
    // Protocol deterministic finite-state machine
    std::unique_ptr<fsm> _fsm;
    // id of this server
    server_id _id;
    server::configuration _config;

    struct stop_apply_fiber{}; // exception to send when apply fiber is needs to be stopepd
    queue<std::vector<log_entry_ptr>> _apply_entries = queue<std::vector<log_entry_ptr>>(10);

    struct stats {
        uint64_t add_command = 0;
        uint64_t add_dummy = 0;
        uint64_t add_config = 0;
        uint64_t append_entries_received = 0;
        uint64_t append_entries_reply_received = 0;
        uint64_t request_vote_received = 0;
        uint64_t request_vote_reply_received = 0;
        uint64_t waiters_awaken = 0;
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
    } _stats;

    struct op_status {
        term_t term; // term the entry was added with
        promise<> done; // notify when done here
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

    // An id of last loaded snapshot into a state machine
    snapshot_id _last_loaded_snapshot_id;

    // Called to commit entries (on a leader or otherwise).
    void notify_waiters(std::map<index_t, op_status>& waiters, const std::vector<log_entry_ptr>& entries);

    // Drop waiter that we lost track of, can happen due to a snapshot transfer,
    // or a leader removed from cluster while some entries added on it are uncommitted.
    void drop_waiters(std::optional<index_t> idx = {});

    // This fiber processes FSM output by doing the following steps in order:
    //  - persist the current term and vote
    //  - persist unstable log entries on disk.
    //  - send out messages
    future<> io_fiber(index_t stable_idx);

    // This fiber runs in the background and applies committed entries.
    future<> applier_fiber();

    template <typename T> future<> add_entry_internal(T command, wait_type type);
    template <typename Message> future<> send_message(server_id id, Message m);

    // Abort all snapshot transfers.
    // Called when no longer a leader or on shutdown
    void abort_snapshot_transfers();

    // Apply a dummy entry. Dummy entry is not propagated to the
    // state machine, but waiting for it to be "applied" ensures
    // all previous entries are applied as well.
    // Resolves when the entry is committed.
    // The function has to be called on the leader, throws otherwise
    // May fail because of an internal error or because the leader
    // has changed and the entry was replaced by another one,
    // submitted to the new leader.
    future<> apply_dummy_entry();

    // Send snapshot in the background and notify FSM about the result.
    void send_snapshot(server_id id, install_snapshot&& snp);

    future<> _applier_status = make_ready_future<>();
    future<> _io_status = make_ready_future<>();

    void register_metrics();
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

    friend std::ostream& operator<<(std::ostream& os, const server_impl& s);
};

server_impl::server_impl(server_id uuid, std::unique_ptr<rpc> rpc,
        std::unique_ptr<state_machine> state_machine, std::unique_ptr<persistence> persistence,
        seastar::shared_ptr<failure_detector> failure_detector, server::configuration config) :
                    _rpc(std::move(rpc)), _state_machine(std::move(state_machine)),
                    _persistence(std::move(persistence)), _failure_detector(failure_detector),
                    _id(uuid), _config(config) {
    set_rpc_server(_rpc.get());
    if (_config.snapshot_threshold > _config.max_log_size) {
        throw config_error("snapshot_threshold has to be smaller than max_log_size");
    }
}

future<> server_impl::start() {
    register_metrics();
    auto [term, vote] = co_await _persistence->load_term_and_vote();
    auto snapshot  = co_await _persistence->load_snapshot();
    auto snp_id = snapshot.id;
    auto log_entries = co_await _persistence->load_log();
    auto log = raft::log(std::move(snapshot), std::move(log_entries));
    raft::configuration rpc_config = log.get_configuration();
    index_t stable_idx = log.stable_idx();
    _fsm = std::make_unique<fsm>(_id, term, vote, std::move(log), *_failure_detector,
                                 fsm_config {
                                     .append_request_threshold = _config.append_request_threshold,
                                     .max_log_size = _config.max_log_size,
                                     .enable_prevoting = _config.enable_prevoting
                                 });

    if (snp_id) {
        co_await _state_machine->load_snapshot(snp_id);
        _last_loaded_snapshot_id = snp_id;
    }

    if (!rpc_config.current.empty()) {
        // Update RPC address map from the latest configuration (either from
        // the log or the snapshot)
        //
        // Account both for current and previous configurations since
        // the last configuration idx can point to the joint configuration entry.
        rpc_config.current.merge(rpc_config.previous);
        for (const auto& addr: rpc_config.current) {
            add_to_rpc_config(addr);
            _rpc->add_server(addr.id, addr.info);
        }
    }

    // start fiber to persist entries added to in-memory log
    _io_status = io_fiber(stable_idx);
    // start fiber to apply committed entries
    _applier_status = applier_fiber();

    co_return;
}

template <typename T>
future<> server_impl::add_entry_internal(T command, wait_type type) {
    logger.trace("An entry is submitted on a leader");

    // Wait for a new slot to become available
    co_await _fsm->wait_max_log_size();

    logger.trace("An entry proceeds after wait");

    const log_entry& e = _fsm->add_entry(std::move(command));

    auto& container = type == wait_type::committed ? _awaited_commits : _awaited_applies;

    // This will track the commit/apply status of the entry
    auto [it, inserted] = container.emplace(e.idx, op_status{e.term, promise<>()});
    assert(inserted);
    co_return co_await it->second.done.get_future();
}

future<> server_impl::add_entry(command command, wait_type type) {
    _stats.add_command++;
    return add_entry_internal(std::move(command), type);
}

future<> server_impl::apply_dummy_entry() {
    _stats.add_dummy++;
    return add_entry_internal(log_entry::dummy(), wait_type::applied);
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
        _stats.waiters_awaken++;
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
            _stats.waiters_awaken++;
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

template <typename Message>
future<> server_impl::send_message(server_id id, Message m) {
    return std::visit([this, id] (auto&& m) {
        using T = std::decay_t<decltype(m)>;
        if constexpr (std::is_same_v<T, append_reply>) {
            _stats.append_entries_reply_sent++;
            return _rpc->send_append_entries_reply(id, m);
        } else if constexpr (std::is_same_v<T, append_request>) {
            _stats.append_entries_sent++;
            return _rpc->send_append_entries(id, m);
        } else if constexpr (std::is_same_v<T, vote_request>) {
            _stats.vote_request_sent++;
            return _rpc->send_vote_request(id, m);
        } else if constexpr (std::is_same_v<T, vote_reply>) {
            _stats.vote_request_reply_sent++;
            return _rpc->send_vote_reply(id, m);
        } else if constexpr (std::is_same_v<T, timeout_now>) {
            _stats.timeout_now_sent++;
            return _rpc->send_timeout_now(id, m);
        } else if constexpr (std::is_same_v<T, install_snapshot>) {
            _stats.install_snapshot_sent++;
            // Send in the background.
            send_snapshot(id, std::move(m));
            return make_ready_future<>();
        } else if constexpr (std::is_same_v<T, snapshot_reply>) {
            _stats.snapshot_reply_sent++;
            assert(_snapshot_application_done.contains(id));
            // Send a reply to install_snapshot after
            // snapshot application is done.
            _snapshot_application_done[id].set_value(std::move(m));
            _snapshot_application_done.erase(id);
            // ... and do not wait for it here.
            return make_ready_future<>();
        } else {
            static_assert(!sizeof(T*), "not all message types are handled");
            return make_ready_future<>();
        }
    }, std::move(m));
}

static configuration_diff diff_address_sets(const server_address_set& prev, const server_address_set& current) {
    configuration_diff result;
    for (const auto& s : current) {
        if (!prev.contains(s)) {
            result.joining.insert(s);
        }
    }
    for (const auto& s : prev) {
        if (!current.contains(s)) {
            result.leaving.insert(s);
        }
    }
    return result;
}

future<> server_impl::io_fiber(index_t last_stable) {
    logger.trace("[{}] io_fiber start", _id);
    try {
        while (true) {
            auto batch = co_await _fsm->poll_output();
            _stats.polls++;

            if (batch.term_and_vote) {
                // Current term and vote are always persisted
                // together. A vote may change independently of
                // term, but it's safe to update both in this
                // case.
                co_await _persistence->store_term_and_vote(batch.term_and_vote->first, batch.term_and_vote->second);
                _stats.store_term_and_vote++;
            }

            if (batch.snp) {
                logger.trace("[{}] io_fiber storing snapshot {}", _id, batch.snp->id);
                // Persist the snapshot
                co_await _persistence->store_snapshot(*batch.snp, _config.snapshot_trailing);
                _stats.store_snapshot++;
                // If this is locally generated snapshot there is no need to
                // load it.
                if (_last_loaded_snapshot_id != batch.snp->id) {
                    // Apply it to the state machine
                    logger.trace("[{}] io_fiber applying snapshot {}", _id, batch.snp->id);
                    co_await _state_machine->load_snapshot(batch.snp->id);
                    _state_machine->drop_snapshot(_last_loaded_snapshot_id);
                    drop_waiters(batch.snp->idx);
                    _last_loaded_snapshot_id = batch.snp->id;
                    _stats.sm_load_snapshot++;
                }
            }

            if (batch.log_entries.size()) {
                auto& entries = batch.log_entries;

                if (last_stable >= entries[0]->idx) {
                    co_await _persistence->truncate_log(entries[0]->idx);
                    _stats.truncate_persisted_log++;
                }

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
            configuration_diff rpc_diff;
            if (batch.rpc_configuration) {
                const server_address_set& current_rpc_config = get_rpc_config();
                rpc_diff = diff_address_sets(get_rpc_config(), *batch.rpc_configuration);
                for (const auto& addr: rpc_diff.joining) {
                    add_to_rpc_config(addr);
                    _rpc->add_server(addr.id, addr.info);
                }
            }

            if (batch.messages.size()) {
                // After entries are persisted we can send messages.
                co_await seastar::parallel_for_each(std::move(batch.messages), [this] (std::pair<server_id, rpc_message>& message) {
                    return send_message(message.first, std::move(message.second)).then_wrapped([this, &message] (future<>&& f) {
                        if (f.failed()) {
                            // Not being able to send a message is not a critical error
                            logger.debug("[{}] io_fiber failed to send a message to {}: {}", _id, message.first, f.get_exception());
                        }
                        return make_ready_future();
                    });
                });
            }

            if (batch.rpc_configuration) {
                for (const auto& addr: rpc_diff.leaving) {
                    remove_from_rpc_config(addr);
                    _rpc->remove_server(addr.id);
                }
            }

            // Process committed entries.
            if (batch.committed.size()) {
                notify_waiters(_awaited_commits, batch.committed);
                _stats.queue_entries_for_apply += batch.committed.size();
                co_await _apply_entries.push_eventually(std::move(batch.committed));
            }

            if (!_fsm->is_leader()) {
                if (!_current_rpc_config.contains(server_address{_id})) {
                    // If the node is no longer part of a config and no longer the leader
                    // it will never know the status of entries it submitted
                    drop_waiters();
                }
                // request aborts of snapshot transfers
                abort_snapshot_transfers();
            }
        }
    } catch (seastar::broken_condition_variable&) {
        // Log fiber is stopped explicitly.
    } catch (stop_apply_fiber&) {
        // Log fiber is stopped explicitly
    } catch (...) {
        logger.error("[{}] io fiber stopped because of the error: {}", _id, std::current_exception());
    }
    co_return;
}

void server_impl::send_snapshot(server_id dst, install_snapshot&& snp) {
    seastar::abort_source as;
    uint64_t id = _next_snapshot_transfer_id++;
    future<> f = _rpc->send_snapshot(dst, std::move(snp), as).then_wrapped([this, dst, id] (future<snapshot_reply> f) {
        if (_aborted_snapshot_transfers.erase(id)) {
            // The transfer was aborted
            f.ignore_ready_future();
            return;
        }
        _snapshot_transfers.erase(dst);
        auto reply = raft::snapshot_reply{.current_term = _fsm->get_current_term(), .success = false};
        if (f.failed()) {
            logger.error("[{}] Transferring snapshot to {} failed with: {}", _id, dst, f.get_exception());
        } else {
            logger.trace("[{}] Transferred snapshot to {}", _id, dst);
            reply = f.get();
        }
        _fsm->step(dst, std::move(reply));
    });
    auto res = _snapshot_transfers.emplace(dst, snapshot_transfer{std::move(f), std::move(as), id});
    assert(res.second);
}

future<snapshot_reply> server_impl::apply_snapshot(server_id from, install_snapshot snp) {
    _fsm->step(from, std::move(snp));
    // Only one snapshot can be received at a time from each node
    assert(! _snapshot_application_done.contains(from));
    return _snapshot_application_done[from].get_future();
}

future<> server_impl::applier_fiber() {
    logger.trace("applier_fiber start");
    size_t applied_since_snapshot = 0;

    try {
        while (true) {
            auto batch = co_await _apply_entries.pop_eventually();

            applied_since_snapshot += batch.size();

            std::vector<command_cref> commands;
            commands.reserve(batch.size());

            index_t last_idx = batch.back()->idx;

            boost::range::copy(
                    batch |
                    boost::adaptors::filtered([] (log_entry_ptr& entry) { return std::holds_alternative<command>(entry->data); }) |
                    boost::adaptors::transformed([] (log_entry_ptr& entry) { return std::cref(std::get<command>(entry->data)); }),
                    std::back_inserter(commands));

            auto size = commands.size();
            co_await _state_machine->apply(std::move(commands));
            _stats.applied_entries += size;
            notify_waiters(_awaited_applies, batch);

            if (applied_since_snapshot >= _config.snapshot_threshold) {
                snapshot snp;
                snp.term = get_current_term();
                snp.idx = last_idx;
                logger.trace("[{}] applier fiber taking snapshot term={}, idx={}", _id, snp.term, snp.idx);
                snp.id = co_await _state_machine->take_snapshot();
                _last_loaded_snapshot_id = snp.id;
                _fsm->apply_snapshot(snp, _config.snapshot_trailing);
                applied_since_snapshot = 0;
                _stats.snapshots_taken++;
            }
        }
    } catch(stop_apply_fiber& ex) {
        // the fiber is aborted
    } catch (...) {
        logger.error("[{}] applier fiber stopped because of the error: {}", _id, std::current_exception());
    }
    co_return;
}

term_t server_impl::get_current_term() const {
    return _fsm->get_current_term();
}

future<> server_impl::read_barrier() {
    if (_fsm->can_read()) {
        co_return;
    }

    co_await apply_dummy_entry();
    co_return;
}

void server_impl::abort_snapshot_transfers() {
    for (auto&& [id, t] : _snapshot_transfers) {
        logger.trace("[{}] Request abort of snapshot transfer to {}", _id, id);
        t.as.request_abort();
        _aborted_snapshot_transfers.emplace(t.id, std::move(t.f));
    }
    _snapshot_transfers.clear();
}

future<> server_impl::abort() {
    logger.trace("abort() called");
    _fsm->stop();
    _apply_entries.abort(std::make_exception_ptr(stop_apply_fiber()));

    // IO and applier fibers may update waiters and start new snapshot
    // transfers, so abort() them first
    co_await seastar::when_all_succeed(std::move(_io_status), std::move(_applier_status),
                        _rpc->abort(), _state_machine->abort(), _persistence->abort()).discard_result();

    for (auto& ac: _awaited_commits) {
        ac.second.done.set_exception(stopped_error());
    }
    for (auto& aa: _awaited_applies) {
        aa.second.done.set_exception(stopped_error());
    }
    _awaited_commits.clear();
    _awaited_applies.clear();

    for (auto&& [_, f] : _snapshot_application_done) {
        f.set_exception(std::runtime_error("Snapshot application aborted"));
    }

    abort_snapshot_transfers();

    auto snp_futures = _aborted_snapshot_transfers | boost::adaptors::map_values;

    co_await seastar::when_all_succeed(snp_futures.begin(), snp_futures.end());
}

future<> server_impl::set_configuration(server_address_set c_new) {
    // 4.1 Cluster membership changes. Safety.
    // When the leader receives a request to add or remove a server
    // from its current configuration (C old ), it appends the new
    // configuration (C new ) as an entry in its log and replicates
    // that entry using the normal Raft mechanism.
    auto [joining, leaving] = _fsm->get_configuration().diff(c_new);
    if (joining.size() == 0 && leaving.size() == 0) {
        co_return;
    }
    _stats.add_config++;
    co_return co_await add_entry_internal(raft::configuration{std::move(c_new)}, wait_type::committed);
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

        sm::make_total_operations("waiter_awaken", _stats.waiters_awaken,
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
        co_await later();
    };
}

future<> server_impl::wait_log_idx(index_t idx) {
    while (_fsm->log_last_idx() < idx) {
        co_await seastar::sleep(5us);
    }
}

index_t server_impl::log_last_idx() {
    return _fsm->log_last_idx();
}

bool server_impl::is_leader() {
    return _fsm->is_leader();
}

void server_impl::elapse_election() {
    while (_fsm->election_elapsed() < ELECTION_TIMEOUT) {
        _fsm->tick();
    }
}

void server_impl::tick() {
    _fsm->tick();
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

std::unique_ptr<server> create_server(server_id uuid, std::unique_ptr<rpc> rpc,
    std::unique_ptr<state_machine> state_machine, std::unique_ptr<persistence> persistence,
    seastar::shared_ptr<failure_detector> failure_detector, server::configuration config) {
    assert(uuid != raft::server_id{utils::UUID(0, 0)});
    return std::make_unique<raft::server_impl>(uuid, std::move(rpc), std::move(state_machine),
        std::move(persistence), failure_detector, config);
}

std::ostream& operator<<(std::ostream& os, const server_impl& s) {
    os << "[id: " << s._id << ", fsm (" << s._fsm << ")]\n";
    return os;
}

} // end of namespace raft
