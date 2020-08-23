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

#include <map>
#include <ranges>
#include <seastar/core/sleep.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/pipe.hh>

#include "fsm.hh"
#include "log.hh"

using namespace std::chrono_literals;

namespace raft {

class server_impl : public rpc_server, public server {
public:
    explicit server_impl(server_id uuid, std::unique_ptr<rpc> rpc,
        std::unique_ptr<state_machine> state_machine, std::unique_ptr<storage> storage,
        seastar::shared_ptr<failure_detector> failure_detector);

    server_impl(server_impl&&) = delete;

    ~server_impl() {}

    // rpc_server interface
    void append_entries(server_id from, append_request_recv append_request) override;
    void append_entries_reply(server_id from, append_reply reply) override;
    void request_vote(server_id from, vote_request vote_request) override;
    void request_vote_reply(server_id from, vote_reply vote_reply) override;

    // server interface
    future<> add_entry(command command, wait_type type);
    future<> apply_snapshot(server_id from, install_snapshot snp) override;
    future<> add_server(server_id id, bytes node_info, clock_type::duration timeout) override;
    future<> remove_server(server_id id, clock_type::duration timeout) override;
    future<> start() override;
    future<> abort() override;
    term_t get_current_term() const override;
    future<> read_barrier() override;
    void make_me_leader() override;
private:
    std::unique_ptr<rpc> _rpc;
    std::unique_ptr<state_machine> _state_machine;
    std::unique_ptr<storage> _storage;
    seastar::shared_ptr<failure_detector> _failure_detector;
    // Protocol deterministic finite-state machine
    std::unique_ptr<fsm> _fsm;
    // id of this server
    server_id _id;
    seastar::timer<lowres_clock> _ticker;

    seastar::pipe<std::vector<log_entry_ptr>> _apply_entries = seastar::pipe<std::vector<log_entry_ptr>>(10);

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

    // Called to commit entries (on a leader or otherwise).
    void notify_waiters(std::map<index_t, op_status>& waiters, const std::vector<log_entry_ptr>& entries);

    // This fiber process fsm output, by doing the following steps in that order:
    //  - persists current term and voter
    //  - persists unstable log entries on disk.
    //  - sends out messages
    future<> io_fiber(index_t stable_idx);

    // This fiber runs in the background and applies committed entries.
    future<> applier_fiber();

    template <typename T> future<> add_entry_internal(T command, wait_type type);
    template <typename Message> future<> send_message(server_id id, Message m);

    // Apply a dummy entry. Dummy entry is not propagated to the
    // state machine, but waiting for it to be "applied" ensures
    // all previous entries are applied as well.
    // Resolves when the entry is committed.
    // The function has to be called on the leader, throws otherwise
    // May fail because of an internal error or because the leader
    // has changed and the entry was replaced by another one,
    // submitted to the new leader.
    future<> apply_dummy_entry();

    future<> _applier_status = make_ready_future<>();
    future<> _io_status = make_ready_future<>();

    friend std::ostream& operator<<(std::ostream& os, const server_impl& s);
};

server_impl::server_impl(server_id uuid, std::unique_ptr<rpc> rpc,
        std::unique_ptr<state_machine> state_machine, std::unique_ptr<storage> storage,
        seastar::shared_ptr<failure_detector> failure_detector) :
                    _rpc(std::move(rpc)), _state_machine(std::move(state_machine)),
                    _storage(std::move(storage)), _failure_detector(failure_detector),
                    _id(uuid) {
    set_rpc_server(_rpc.get());
}

future<> server_impl::start() {
    auto [term, vote] = co_await _storage->load_term_and_vote();
    auto snapshot  = co_await _storage->load_snapshot();
    auto log_entries = co_await _storage->load_log();
    auto log = raft::log(std::move(snapshot), std::move(log_entries));
    index_t stable_idx = log.stable_idx();
    _fsm = std::make_unique<fsm>(_id, term, vote, std::move(log), *_failure_detector);
    assert(_fsm->get_current_term() != term_t(0));

    // start fiber to persist entries added to in-memory log
    _io_status = io_fiber(stable_idx);
    // start fiber to apply committed entries
    _applier_status = applier_fiber();

    _ticker.arm_periodic(100ms);
    _ticker.set_callback([this] {
        _fsm->tick();
    });

    co_return;
}

template <typename T>
future<> server_impl::add_entry_internal(T command, wait_type type) {
    logger.trace("An entry is submitted on a leader");

    // lock access to the raft log while it is been updated

    // @todo: ensure the reference to the entry is stable between
    // yields, before removing _log_lock.
    const log_entry& e = _fsm->add_entry(std::move(command));

    auto& container = type == wait_type::committed ? _awaited_commits : _awaited_applies;

    // This will track the commit/apply status of the entry
    auto [it, inserted] = container.emplace(e.idx, op_status{e.term, promise<>()});
    assert(inserted);
    return it->second.done.get_future();
}

future<> server_impl::add_entry(command command, wait_type type) {
    return add_entry_internal(std::move(command), type);
}

future<> server_impl::apply_dummy_entry() {
    return add_entry_internal(log_entry::dummy(), wait_type::applied);
}
void server_impl::append_entries(server_id from, append_request_recv append_request) {
    _fsm->step(from, std::move(append_request));
}

void server_impl::append_entries_reply(server_id from, append_reply reply) {
    _fsm->step(from, std::move(reply));
}

void server_impl::request_vote(server_id from, vote_request vote_request) {
    _fsm->step(from, std::move(vote_request));
}

void server_impl::request_vote_reply(server_id from, vote_reply vote_reply) {
    _fsm->step(from, std::move(vote_reply));
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
        // it means that notification is out of order which is prohinited
        assert(entry_idx >= first_idx);

        waiters.erase(it);
        if (status.term == entries[entry_idx - first_idx]->term) {
            status.done.set_value();
        } else {
            // term does not match which means that between the entry was submitted
            // and committed there was a leadership change and the entry was replaced.
            status.done.set_exception(dropped_entry());
        }
    }
}

template <typename Message>
future<> server_impl::send_message(server_id id, Message m) {
    return std::visit([this, id] (auto&& m) {
        using T = std::decay_t<decltype(m)>;
        if constexpr (std::is_same_v<T, append_reply>) {
            return _rpc->send_append_entries_reply(id, m);
        } else if constexpr (std::is_same_v<T, append_request_send>) {
            return _rpc->send_append_entries(id, m);
        } else if constexpr (std::is_same_v<T, vote_request>) {
            return _rpc->send_vote_request(id, m);
        } else if constexpr (std::is_same_v<T, vote_reply>) {
            return _rpc->send_vote_reply(id, m);
        } else {
            static_assert(!sizeof(Message*), "not all message types are handled");
            return make_ready_future<>();
        }
    }, std::move(m));
}

future<> server_impl::io_fiber(index_t last_stable) {
    logger.trace("[{}] io_fiber start", _id);
    try {
        while (true) {
            auto batch = co_await _fsm->poll_output();

            if (batch.term != term_t{}) {
                // Current term and vote are always persisted
                // together. A vote may change independently of
                // term, but it's safe to update both in this
                // case.
                co_await _storage->store_term_and_vote(batch.term, batch.vote);
            }

            if (batch.log_entries.size()) {
                auto& entries = batch.log_entries;

                if (last_stable >= entries[0]->idx) {
                    co_await _storage->truncate_log(entries[0]->idx);
                }

                // Combine saving and truncating into one call?
                // will require storage to keep track of last idx
                co_await _storage->store_log_entries(entries);

                last_stable = (*entries.crbegin())->idx;
            }

            if (batch.messages.size()) {
                // after entries are persisted we can send messages
                co_await seastar::parallel_for_each(std::move(batch.messages), [this] (std::pair<server_id, rpc_message>& message) {
                    return send_message(message.first, std::move(message.second));
                });
            }

            // process committed entries
            if (batch.committed.size()) {
                notify_waiters(_awaited_commits, batch.committed);
                co_await _apply_entries.writer.write(std::move(batch.committed));
            }
        }
    } catch (seastar::broken_condition_variable&) {
        // log fiber is stopped explicitly.
    } catch (...) {
        logger.error("[{}] io fiber stopped because of the error: {}", _id, std::current_exception());
    }
    co_return;
}

future<> server_impl::apply_snapshot(server_id from, install_snapshot snp) {
    return make_ready_future<>();
}

future<> server_impl::applier_fiber() {
    logger.trace("applier_fiber start");
    try {
        while (true) {
            auto opt_batch = co_await _apply_entries.reader.read();
            if (!opt_batch) {
                // EOF
                break;
            }
            std::vector<command_cref> commands;
            commands.reserve(opt_batch->size());

            std::ranges::copy(
                    *opt_batch |
                    std::views::filter([] (log_entry_ptr& entry) { return std::holds_alternative<command>(entry->data); }) |
                    std::views::transform([] (log_entry_ptr& entry) { return std::cref(std::get<command>(entry->data)); }),
                    std::back_inserter(commands));

            co_await _state_machine->apply(std::move(commands));
            notify_waiters(_awaited_applies, *opt_batch);
        }
    } catch (...) {
        logger.error("applier fiber {} stopped because of the error: {}", _id, std::current_exception());
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

future<> server_impl::abort() {
    logger.trace("abort() called");
    _fsm->stop();
    {
        // there is not explicit close for the pipe!
        auto tmp = std::move(_apply_entries.writer);
    }
    for (auto& ac: _awaited_commits) {
        ac.second.done.set_exception(stopped_error());
    }
    for (auto& aa: _awaited_applies) {
        aa.second.done.set_exception(stopped_error());
    }
    _awaited_commits.clear();
    _awaited_applies.clear();
    _ticker.cancel();

    return seastar::when_all_succeed(std::move(_io_status), std::move(_applier_status),
            _rpc->abort(), _state_machine->abort(), _storage->abort()).discard_result();
}

future<> server_impl::add_server(server_id id, bytes node_info, clock_type::duration timeout) {
    return make_ready_future<>();
}

// Removes a server from the cluster. If the server is not a member
// of the cluster does nothing. Can be called on a leader only
// otherwise throws not_a_leader.
// Cannot be called until previous add/remove server completes
// otherwise conf_change_in_progress exception is returned.
future<> server_impl::remove_server(server_id id, clock_type::duration timeout) {
    return make_ready_future<>();
}

void server_impl::make_me_leader() {
    _fsm->become_leader();
}

std::unique_ptr<server> create_server(server_id uuid, std::unique_ptr<rpc> rpc,
    std::unique_ptr<state_machine> state_machine, std::unique_ptr<storage> storage,
    seastar::shared_ptr<failure_detector> failure_detector) {
    return std::make_unique<raft::server_impl>(uuid, std::move(rpc), std::move(state_machine),
        std::move(storage), failure_detector);
}

std::ostream& operator<<(std::ostream& os, const server_impl& s) {
    os << "[id: " << s._id << ", fsm (" << s._fsm << ")]\n";
    return os;
}

} // end of namespace raft
