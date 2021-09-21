/*
 * Copyright (C) 2021-present ScyllaDB
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

#include <seastar/core/reactor.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/util/defer.hh>

#include "raft/server.hh"
#include "raft/logical_clock.hh"
#include "serializer.hh"
#include "serializer_impl.hh"
#include "idl/uuid.dist.hh"
#include "idl/uuid.dist.impl.hh"

#include "test/lib/random_utils.hh"
#include "test/raft/logical_timer.hh"
#include "test/raft/ticker.hh"
#include "test/raft/generator.hh"

#include "to_string.hh"

using namespace seastar;
using namespace std::chrono_literals;

seastar::logger tlogger("randomized_nemesis_test");

// A direct translaction of a mathematical definition of a state machine
// (see e.g. Wikipedia) as a C++ concept. Implementations of this concept
// do not store the state, they only define the types, the transition function
// (which is a pure function), and the initial state (which is a constant).
template <typename M> concept PureStateMachine =
requires (typename M::state_t s, typename M::input_t i) {
    // The type of all possible states.
    typename M::state_t;

    // The type of all possible inputs (commands).
    typename M::input_t;

    // The type of all possible outputs.
    typename M::output_t;

    // The transition function (a pure function - no side effects). It takes a state
    // and an input, and returns the next state and the output produced
    // by applying the input to the given state.
    { M::delta(s, i) } -> std::same_as<std::pair<typename M::state_t, typename M::output_t>>;

    // The initial state, of type `state_t`.
    M::init;
    requires std::is_same_v<const typename M::state_t, decltype(M::init)>;
};

// Used to uniquely identify commands passed into `apply` in order to return
// the outputs of these commands. See `impure_state_machine` and `call`.
using cmd_id_t = utils::UUID;

// A set of in-memory snapshots maintained by a single Raft server.
// The different parts of the server (the state machine, persistence,
// rpc) will share a single `snapshots_t`.
template <typename State>
using snapshots_t = std::unordered_map<raft::snapshot_id, State>;

// To replicate a state machine, our Raft implementation requires it to
// be represented with the `raft::state_machine` interface.
//
// `impure_state_machine` is an implementation of `raft::state_machine`
// that wraps a `PureStateMachine`. It keeps a variable of type `state_t`
// representing the current state. In `apply` it deserializes the given
// command into `input_t`, uses the transition (`delta`) function to
// produce the next state and output, replaces its current state with the
// obtained state and returns the output (more on that below); it does so
// sequentially for every given command. We can think of `PureStateMachine`
// as the actual state machine - the business logic, and `impure_state_machine`
// as the ``boilerplate'' that allows the pure machine to be replicated
// by Raft and communicate with the external world.
//
// The interface also requires maintainance of snapshots. We use the
// `snapshots_t` introduced above; `impure_state_machine` keeps a reference to `snapshots_t`
// because it will share it with an implementation of `raft::persistence`.
template <PureStateMachine M>
class impure_state_machine : public raft::state_machine {
    typename M::state_t _val;
    snapshots_t<typename M::state_t>& _snapshots;

    // Used to ensure that when `abort()` returns there are
    // no more in-progress methods running on this object.
    seastar::gate _gate;

    // To obtain output from an applied command, the client (see `call`)
    // first allocates a channel in this data structure by calling `with_output_channel`
    // and makes the returned command ID a part of the command passed to Raft.
    // When (if) we eventually apply the command, we use the ID to find the output channel
    // here and push the output to the client waiting on the other end.
    // The channel is allocated only on the local server where `with_output_channel`
    // was called; other replicas of the state machine will therefore not find the ID
    // in their instances of `_output_channels` so they just drop the output.
    std::unordered_map<cmd_id_t, promise<typename M::output_t>> _output_channels;

public:
    impure_state_machine(snapshots_t<typename M::state_t>& snapshots)
        : _val(M::init), _snapshots(snapshots) {}

    future<> apply(std::vector<raft::command_cref> cmds) override {
        co_await with_gate(_gate, [this, cmds = std::move(cmds)] () mutable -> future<> {
            for (auto& cref : cmds) {
                _gate.check();

                auto is = ser::as_input_stream(cref);
                auto cmd_id = ser::deserialize(is, boost::type<cmd_id_t>{});
                auto input = ser::deserialize(is, boost::type<typename M::input_t>{});
                auto [new_state, output] = M::delta(std::move(_val), std::move(input));
                _val = std::move(new_state);

                auto it = _output_channels.find(cmd_id);
                if (it != _output_channels.end()) {
                    // We are on the leader server where the client submitted the command
                    // and waits for the output. Send it to them.
                    it->second.set_value(std::move(output));
                    _output_channels.erase(it);
                } else {
                    // This is not the leader on which the command was submitted,
                    // or it is but the client already gave up on us and deallocated the channel.
                    // In any case we simply drop the output.
                }

                co_await coroutine::maybe_yield();
            }
        });
    }

    future<raft::snapshot_id> take_snapshot() override {
        auto id = raft::snapshot_id::create_random_id();
        assert(_snapshots.emplace(id, _val).second);
        co_return id;
    }

    void drop_snapshot(raft::snapshot_id id) override {
        _snapshots.erase(id);
    }

    future<> load_snapshot(raft::snapshot_id id) override {
        auto it = _snapshots.find(id);
        assert(it != _snapshots.end()); // dunno if the snapshot can actually be missing
        _val = it->second;
        co_return;
    }

    future<> abort() override {
        return _gate.close();
    }

    struct output_channel_dropped : public raft::error {
        output_channel_dropped() : error("output channel dropped") {}
    };

    // Before sending a command to Raft, the client must obtain a command ID
    // and an output channel using this function.
    template <typename F>
    future<typename M::output_t> with_output_channel(F f) {
        return with_gate(_gate, [this, f = std::move(f)] () mutable -> future<typename M::output_t> {
            promise<typename M::output_t> p;
            auto fut = p.get_future();
            auto cmd_id = utils::make_random_uuid();
            assert(_output_channels.emplace(cmd_id, std::move(p)).second);

            auto guard = defer([this, cmd_id] {
                auto it = _output_channels.find(cmd_id);
                if (it != _output_channels.end()) {
                    it->second.set_exception(output_channel_dropped{});
                    _output_channels.erase(it);
                }
            });
            return f(cmd_id, std::move(fut)).finally([guard = std::move(guard)] {});
        });
    }
};

// TODO: serializable concept?
template <typename Input>
raft::command make_command(const cmd_id_t& cmd_id, const Input& input) {
    raft::command cmd;
    ser::serialize(cmd, cmd_id);
    ser::serialize(cmd, input);
    return cmd;
}

// TODO: handle other errors?
template <PureStateMachine M>
using call_result_t = std::variant<typename M::output_t, timed_out_error, raft::not_a_leader, raft::dropped_entry>;

// Sends a given `input` as a command to `server`, waits until the command gets replicated
// and applied on that server and returns the produced output.
//
// The wait time is limited using `timeout` which is a logical time point referring to the
// logical clock used by `timer`. Standard way to use is to pass `timer.now() + X_t`
// as the time point, where `X` is the maximum number of ticks that we wait for.
//
// `sm` must be a reference to the state machine owned by `server`.
//
// The `server` may currently be a follower, in which case it will return a `not_a_leader` error.
template <PureStateMachine M>
future<call_result_t<M>> call(
        typename M::input_t input,
        raft::logical_clock::time_point timeout,
        logical_timer& timer,
        raft::server& server,
        impure_state_machine<M>& sm) {
    using output_channel_dropped = typename impure_state_machine<M>::output_channel_dropped;
    return sm.with_output_channel([&, input = std::move(input), timeout] (cmd_id_t cmd_id, future<typename M::output_t> f) {
        return timer.with_timeout(timeout, [&] (typename M::input_t input, future<typename M::output_t> f) {
            return server.add_entry(
                    make_command(std::move(cmd_id), std::move(input)),
                    raft::wait_type::applied
            ).then_wrapped([output_f = std::move(f)] (future<> add_entry_f) mutable {
                if (add_entry_f.failed()) {
                    // We need to discard `output_f`; the only expected exception is:
                    (void)output_f.discard_result().handle_exception_type([] (const output_channel_dropped&) {});
                    std::rethrow_exception(add_entry_f.get_exception());
                }

                return std::move(output_f);
            });
        }(std::move(input), std::move(f)));
    }).then([] (typename M::output_t output) {
        return make_ready_future<call_result_t<M>>(std::move(output));
    }).handle_exception([] (std::exception_ptr eptr) {
        try {
            std::rethrow_exception(eptr);
        } catch (raft::not_a_leader e) {
            return make_ready_future<call_result_t<M>>(e);
        } catch (raft::dropped_entry e) {
            return make_ready_future<call_result_t<M>>(e);
        } catch (logical_timer::timed_out<typename M::output_t> e) {
            (void)e.get_future().discard_result()
                .handle_exception([] (std::exception_ptr eptr) {
                    try {
                        std::rethrow_exception(eptr);
                    } catch (const output_channel_dropped&) {
                    } catch (const raft::dropped_entry&) {
                    } catch (const raft::stopped_error&) {
                    }
                });
            return make_ready_future<call_result_t<M>>(timed_out_error{});
        }
    });
}

// Allows a Raft server to communicate with other servers.
// The implementation is mostly boilerplate. It assumes that there exists a method of message passing
// given by a `send_message_t` function (passed in the constructor) for sending and by the `receive`
// function for receiving messages.
//
// We also keep a reference to a `snapshots_t` set to be shared with the `impure_state_machine`
// on the same server. We access this set when we receive or send a snapshot message.
template <typename State>
class rpc : public raft::rpc {
    using reply_id_t = uint32_t;

    struct snapshot_message {
        raft::install_snapshot ins;
        State snapshot_payload;
        reply_id_t reply_id;
    };

    struct snapshot_reply_message {
        raft::snapshot_reply reply;
        reply_id_t reply_id;
    };

    struct execute_barrier_on_leader {
        reply_id_t reply_id;
    };

    struct execute_barrier_on_leader_reply {
        raft::read_barrier_reply reply;
        reply_id_t reply_id;
    };

public:
    using message_t = std::variant<
        snapshot_message,
        snapshot_reply_message,
        raft::append_request,
        raft::append_reply,
        raft::vote_request,
        raft::vote_reply,
        raft::timeout_now,
        raft::read_quorum,
        raft::read_quorum_reply,
        execute_barrier_on_leader,
        execute_barrier_on_leader_reply>;

    using send_message_t = std::function<void(raft::server_id dst, message_t)>;

private:
    snapshots_t<State>& _snapshots;

    logical_timer _timer;

    send_message_t _send;

    // Before we send a snapshot apply request we create a promise-future pair,
    // allocate a new ID, and put the promise here under that ID. We then send the ID
    // together with the request and wait on the future.
    // When (if) a reply returns, we take the ID from the reply (which is the same
    // as the ID in the corresponding request), take the promise under that ID
    // and push the reply through that promise.
    std::unordered_map<reply_id_t, std::variant<promise<raft::snapshot_reply>, promise<raft::read_barrier_reply>>> _reply_promises;
    reply_id_t _counter = 0;

    // Used to ensure that when `abort()` returns there are
    // no more in-progress methods running on this object.
    seastar::gate _gate;

public:
    rpc(snapshots_t<State>& snaps, send_message_t send)
        : _snapshots(snaps), _send(std::move(send)) {
    }

    // Message is delivered to us
    future<> receive(raft::server_id src, message_t payload) {
        assert(_client);
        auto& c = *_client;

        co_await std::visit(make_visitor(
        [&] (snapshot_message m) -> future<> {
            _snapshots.emplace(m.ins.snp.id, std::move(m.snapshot_payload));

            co_await with_gate(_gate, [&] () -> future<> {
                auto reply = co_await c.apply_snapshot(src, std::move(m.ins));

                _send(src, snapshot_reply_message{
                    .reply = std::move(reply),
                    .reply_id = m.reply_id
                });
            });
        },
        [this] (snapshot_reply_message m) -> future<> {
            auto it = _reply_promises.find(m.reply_id);
            if (it != _reply_promises.end()) {
            std::get<promise<raft::snapshot_reply>>(it->second).set_value(std::move(m.reply));
            }
            co_return;
        },
        [&] (raft::append_request m) -> future<> {
            c.append_entries(src, std::move(m));
            co_return;
        },
        [&] (raft::append_reply m) -> future<> {
            c.append_entries_reply(src, std::move(m));
            co_return;
        },
        [&] (raft::vote_request m) -> future<> {
            c.request_vote(src, std::move(m));
            co_return;
        },
        [&] (raft::vote_reply m) -> future<> {
            c.request_vote_reply(src, std::move(m));
            co_return;
        },
        [&] (raft::timeout_now m) -> future<> {
            c.timeout_now_request(src, std::move(m));
            co_return;
        },
        [&] (raft::read_quorum m) -> future<> {
            c.read_quorum_request(src, std::move(m));
            co_return;
        },
        [&] (raft::read_quorum_reply m) -> future<> {
            c.read_quorum_reply(src, std::move(m));
            co_return;
        },
        [&] (execute_barrier_on_leader m) -> future<> {
            co_await with_gate(_gate, [&] () -> future<> {
                auto reply = co_await c.execute_read_barrier(src);

                _send(src, execute_barrier_on_leader_reply{
                    .reply = std::move(reply),
                    .reply_id = m.reply_id
                });
            });
        },
        [this] (execute_barrier_on_leader_reply m) -> future<> {
            auto it = _reply_promises.find(m.reply_id);
            if (it != _reply_promises.end()) {
                std::get<promise<raft::read_barrier_reply>>(it->second).set_value(std::move(m.reply));
            }
            co_return;
        }
        ), std::move(payload));
    }

    // This is the only function of `raft::rpc` which actually expects a response.
    virtual future<raft::snapshot_reply> send_snapshot(raft::server_id dst, const raft::install_snapshot& ins, seastar::abort_source&) override {
        auto it = _snapshots.find(ins.snp.id);
        assert(it != _snapshots.end());

        auto id = _counter++;
        promise<raft::snapshot_reply> p;
        auto f = p.get_future();
        _reply_promises.emplace(id, std::move(p));
        auto guard = defer([this, id] { _reply_promises.erase(id); });

        _send(dst, snapshot_message{
            .ins = ins,
            .snapshot_payload = it->second,
            .reply_id = id
        });

        // The message receival function on the other side, when it receives the snapshot message,
        // will apply the snapshot and send `id` back to us in the snapshot reply message (see `receive`,
        // `snapshot_message` case). When we receive the reply, we shall find `id` in `_reply_promises`
        // and push the reply through the promise, which will resolve `f` (see `receive`, `snapshot_reply_message`
        // case).

        co_return co_await with_gate(_gate,
                [&, guard = std::move(guard), f = std::move(f)] () mutable -> future<raft::snapshot_reply> {
            // TODO configurable
            static const raft::logical_clock::duration send_snapshot_timeout = 20_t;

            // TODO: catch aborts from the abort_source as well
            try {
                co_return co_await _timer.with_timeout(_timer.now() + send_snapshot_timeout, std::move(f));
            } catch (logical_timer::timed_out<raft::snapshot_reply>& e) {
                // The future will probably get a broken_promise exception after we destroy the guard.
                (void)e.get_future().discard_result().handle_exception_type([] (const broken_promise&) {});
                throw timed_out_error{};
            }
            // co_await ensures that `guard` is destroyed before we leave `_gate`
        });
    }

    virtual future<raft::read_barrier_reply> execute_read_barrier_on_leader(raft::server_id dst) override {
        auto id = _counter++;
        promise<raft::read_barrier_reply> p;
        auto f = p.get_future();
        _reply_promises.emplace(id, std::move(p));
        auto guard = defer([this, id] { _reply_promises.erase(id); });

        _send(dst, execute_barrier_on_leader {
            .reply_id = id
        });

        co_return co_await with_gate(_gate,
                [&, guard = std::move(guard), f = std::move(f)] () mutable -> future<raft::read_barrier_reply> {
            // TODO configurable
            static const raft::logical_clock::duration execute_read_barrier_on_leader_timeout = 20_t;

            // TODO: catch aborts from the abort_source as well
            co_return co_await _timer.with_timeout(_timer.now() + execute_read_barrier_on_leader_timeout, std::move(f));
            // co_await ensures that `guard` is destroyed before we leave `_gate`
        });
    }

    virtual future<> send_append_entries(raft::server_id dst, const raft::append_request& m) override {
        _send(dst, m);
        co_return;
    }

    virtual void send_append_entries_reply(raft::server_id dst, const raft::append_reply& m) override {
        _send(dst, m);
    }

    virtual void send_vote_request(raft::server_id dst, const raft::vote_request& m) override {
        _send(dst, m);
    }

    virtual void send_vote_reply(raft::server_id dst, const raft::vote_reply& m) override {
        _send(dst, m);
    }

    virtual void send_timeout_now(raft::server_id dst, const raft::timeout_now& m) override {
        _send(dst, m);
    }

    virtual void send_read_quorum(raft::server_id dst, const raft::read_quorum& m) override {
        _send(dst, m);
    }

    virtual void send_read_quorum_reply(raft::server_id dst, const raft::read_quorum_reply& m) override {
        _send(dst, m);
    }

    virtual void add_server(raft::server_id, raft::server_info) override {
    }

    virtual void remove_server(raft::server_id) override {
    }

    virtual future<> abort() override {
        return _gate.close();
    }

    void tick() {
        _timer.tick();
    }
};

template <typename State>
class persistence : public raft::persistence {
    snapshots_t<State>& _snapshots;

    std::pair<raft::snapshot_descriptor, State> _stored_snapshot;
    std::pair<raft::term_t, raft::server_id> _stored_term_and_vote;

    // Invariants:
    // 1. for each entry except the first, the raft index is equal to the raft index of the previous entry plus one.
    // 2. the index of the first entry is <= _stored_snapshot.first.idx + 1.
    // 3. the index of the last entry is >= _stored_snapshot.first.idx.
    // Informally, the last two invariants say that the stored log intersects or ``touches'' the snapshot ``on the right side''.
    raft::log_entries _stored_entries;

    // Returns an iterator to the entry in `_stored_entries` whose raft index is `idx` if the entry exists.
    // If all entries in `_stored_entries` have greater indexes, returns the first one.
    // If all entries have smaller indexes, returns end().
    raft::log_entries::iterator find(raft::index_t idx) {
        // The correctness of this depends on the `_stored_entries` invariant.
        auto b = _stored_entries.begin();
        if (b == _stored_entries.end() || (*b)->idx >= idx) {
            return b;
        }
        return b + std::min((idx - (*b)->idx).get_value(), _stored_entries.size());
    }

public:
    // If this is the first server of a cluster, it must be initialized with a singleton configuration
    // containing opnly this server's ID which must be also provided here as `init_config_id`.
    // Otherwise it must be initialized with an empty configuration (it will be added to the cluster
    // through a configuration change) and `init_config_id` must be `nullopt`.
    persistence(snapshots_t<State>& snaps, std::optional<raft::server_id> init_config_id, State init_state)
        : _snapshots(snaps)
        , _stored_snapshot(
                raft::snapshot_descriptor{
                    .config = init_config_id ? raft::configuration{*init_config_id} : raft::configuration{}
                },
                std::move(init_state))
        , _stored_term_and_vote(raft::term_t{1}, raft::server_id{})
    {}

    virtual future<> store_term_and_vote(raft::term_t term, raft::server_id vote) override {
        _stored_term_and_vote = std::pair{term, vote};
        co_return;
    }

    virtual future<std::pair<raft::term_t, raft::server_id>> load_term_and_vote() override {
        co_return _stored_term_and_vote;
    }

    virtual future<> store_snapshot_descriptor(const raft::snapshot_descriptor& snap, size_t preserve_log_entries) override {
        // The snapshot's index cannot be smaller than the index of the first stored entry minus one;
        // that would create a ``gap'' in the log.
        assert(_stored_entries.empty() || snap.idx + 1 >= _stored_entries.front()->idx);

        auto it = _snapshots.find(snap.id);
        assert(it != _snapshots.end());
        _stored_snapshot = {snap, it->second};

        if (!_stored_entries.empty() && snap.idx > _stored_entries.back()->idx) {
            // Clear the log in order to not create a gap.
            _stored_entries.clear();
            co_return;
        }

        auto first_to_remain = snap.idx + 1 >= preserve_log_entries ? raft::index_t{snap.idx + 1 - preserve_log_entries} : raft::index_t{0};
        _stored_entries.erase(_stored_entries.begin(), find(first_to_remain));

        co_return;
    }

    virtual future<raft::snapshot_descriptor> load_snapshot_descriptor() override {
        auto [snap, state] = _stored_snapshot;
        _snapshots.insert_or_assign(snap.id, std::move(state));
        co_return snap;
    }

    virtual future<> store_log_entries(const std::vector<raft::log_entry_ptr>& entries) override {
        if (entries.empty()) {
            co_return;
        }

        // The raft server is supposed to provide entries in strictly increasing order,
        // hence the following assertions.
        if (_stored_entries.empty()) {
            assert(entries.front()->idx == _stored_snapshot.first.idx + 1);
        } else {
            assert(entries.front()->idx == _stored_entries.back()->idx + 1);
        }

        _stored_entries.push_back(entries[0]);
        for (size_t i = 1; i < entries.size(); ++i) {
            assert(entries[i]->idx == entries[i-1]->idx + 1);
            _stored_entries.push_back(entries[i]);
        }

        co_return;
    }

    virtual future<raft::log_entries> load_log() override {
        co_return _stored_entries;
    }

    virtual future<> truncate_log(raft::index_t idx) override {
        _stored_entries.erase(find(idx), _stored_entries.end());
        co_return;
    }

    virtual future<> abort() override {
        // There are no yields anywhere in our methods so no need to wait for anything.
        // We assume that our methods won't be called after `abort()`.
        // TODO: is this assumption correct?
        co_return;
    }
};

// A failure detector using heartbeats for deciding whether to convict a server
// as failed. We convict a server if we don't receive a heartbeat for a long enough time.
// `failure_detector` assumes a message-passing method given by a `send_heartbeat_t` function
// through the constructor for sending heartbeats and assumes that `receive_heartbeat` is called
// whenever another server sends a message to us.
// To decide who to send heartbeats to we use the ``current knowledge'' of servers in the network
// which is updated through `add_server` and `remove_server` functions.
class failure_detector : public raft::failure_detector {
public:
    using send_heartbeat_t = std::function<void(raft::server_id dst)>;

private:
    raft::logical_clock _clock;

    // The set of known servers, used to broadcast heartbeats.
    std::unordered_set<raft::server_id> _known;

    // The last time we received a heartbeat from a server.
    std::unordered_map<raft::server_id, raft::logical_clock::time_point> _last_heard;

    // The last time we sent a heartbeat.
    raft::logical_clock::time_point _last_beat;

    // How long from the last received heartbeat does it take to convict a node as dead.
    const raft::logical_clock::duration _convict_threshold;

    send_heartbeat_t _send_heartbeat;

public:
    failure_detector(raft::logical_clock::duration convict_threshold, send_heartbeat_t f)
        : _convict_threshold(convict_threshold), _send_heartbeat(std::move(f))
    {
        send_heartbeats();
        assert(_last_beat == _clock.now());
    }

    void receive_heartbeat(raft::server_id src) {
        assert(_known.contains(src));
        _last_heard[src] = std::max(_clock.now(), _last_heard[src]);
    }

    void tick() {
        _clock.advance();

        // TODO: make it adjustable
        static const raft::logical_clock::duration _heartbeat_period = 10_t;

        if (_last_beat + _heartbeat_period <= _clock.now()) {
            send_heartbeats();
        }
    }

    void send_heartbeats() {
        for (auto& dst : _known) {
            _send_heartbeat(dst);
        }
        _last_beat = _clock.now();
    }

    // We expect a server to be added through this function before we receive a heartbeat from it.
    void add_server(raft::server_id id) {
        _known.insert(id);
    }

    void remove_server(raft::server_id id) {
        _known.erase(id);
        _last_heard.erase(id);
    }

    bool is_alive(raft::server_id id) override {
        return _clock.now() < _last_heard[id] + _convict_threshold;
    }
};

// `network` is a simple priority queue of `event`s, where an `event` is a message associated
// with its planned delivery time. The queue uses a logical clock to decide when to deliver messages.
// It delives all messages whose associated times are smaller than the ``current time'', the latter
// determined by the number of `tick()` calls.
//
// Note: the actual delivery happens through a function that is passed in the `network` constructor.
// The function may return `false` (for whatever reason) denoting that it failed to deliver the message.
// In this case network will backup this message and retry the delivery on every later `tick` until
// it succeeds.
template <typename Payload>
class network {
public:
    // When the time comes to deliver a message we use this function.
    using deliver_t = std::function<void(raft::server_id src, raft::server_id dst, const Payload&)>;

private:
    struct message {
        raft::server_id src;
        raft::server_id dst;

        // shared ptr to implement duplication of messages
        lw_shared_ptr<Payload> payload;
    };

    struct event {
        raft::logical_clock::time_point time;
        message msg;
    };

    deliver_t _deliver;

    // A min-heap of event occurences compared by their time points.
    std::vector<event> _events;

    // Comparator for the `_events` min-heap.
    static bool cmp(const event& o1, const event& o2) {
        return o1.time > o2.time;
    }

    // A pair (dst, [src1, src2, ...]) in this set denotes that `dst`
    // does not receive messages from src1, src2, ...
    std::unordered_map<raft::server_id, std::unordered_set<raft::server_id>> _grudges;

    raft::logical_clock _clock;

    // How long does it take to deliver a message?
    // TODO: use a random distribution or let the user change this dynamically
    raft::logical_clock::duration _delivery_delay;

public:
    network(raft::logical_clock::duration delivery_delay, deliver_t f)
        : _deliver(std::move(f)), _delivery_delay(delivery_delay) {}

    void send(raft::server_id src, raft::server_id dst, Payload payload) {
        // Predict the delivery time in advance.
        // Our prediction may be wrong if a grudge exists at this expected moment of delivery.
        // Messages may also be reordered.
        auto delivery_time = _clock.now() + _delivery_delay;

        _events.push_back(event{delivery_time, message{src, dst, make_lw_shared<Payload>(std::move(payload))}});
        std::push_heap(_events.begin(), _events.end(), cmp);
    }

    void tick() {
        _clock.advance();
        deliver();
    }

    void add_grudge(raft::server_id src, raft::server_id dst) {
        _grudges[dst].insert(src);
    }

    void remove_grudge(raft::server_id src, raft::server_id dst) {
        _grudges[dst].erase(src);
    }

private:
    void deliver() {
        // Deliver every message whose time has come.
        while (!_events.empty() && _events.front().time <= _clock.now()) {
            auto& [_, m] = _events.front();
            if (!_grudges[m.dst].contains(m.src)) {
                _deliver(m.src, m.dst, *m.payload);
            } else {
                // A grudge means that we drop the message.
            }

            std::pop_heap(_events.begin(), _events.end(), cmp);
            _events.pop_back();
        }
    }
};

// A queue of messages that have arrived at a given Raft server and are waiting to be processed
// by that server (which may be a long computation, hence returning a `future<>`).
// Its purpose is to serve as a ``bridge'' between `network` and a server's `rpc` instance.
// The `network`'s delivery function will `push()` a message onto this queue and `receive_fiber()`
// will eventually forward the message to `rpc` by calling `rpc::receive()`.
// `push()` may fail if the queue is full, meaning that the queue expects the caller (`network`
// in our case) to retry later.
template <typename State>
class delivery_queue {
    struct delivery {
        raft::server_id src;
        typename rpc<State>::message_t payload;
    };

    struct aborted_exception {};

    seastar::queue<delivery> _queue;
    rpc<State>& _rpc;

    std::optional<future<>> _receive_fiber;

public:
    delivery_queue(rpc<State>& rpc)
        : _queue(std::numeric_limits<size_t>::max()), _rpc(rpc) {
    }

    ~delivery_queue() {
        assert(!_receive_fiber);
    }

    void push(raft::server_id src, const typename rpc<State>::message_t& p) {
        assert(_receive_fiber);
        bool pushed = _queue.push(delivery{src, p});
        // The queue is practically unbounded...
        assert(pushed);

        // If the queue is growing then the test infrastructure must have some kind of a liveness problem
        // (which may eventually cause OOM). Let's warn the user.
        if (_queue.size() > 100) {
            tlogger.warn("delivery_queue: large queue size ({})", _queue.size());
        }
    }

    // Start the receiving fiber.
    // Can be executed at most once. When restarting a ``crashed'' server, create a new queue.
    void start() {
        assert(!_receive_fiber);
        _receive_fiber = receive_fiber();
    }

    // Stop the receiving fiber (if it's running). The returned future resolves
    // when the fiber finishes. Must be called before destruction (unless the fiber was never started).
    future<> abort() {
        _queue.abort(std::make_exception_ptr(aborted_exception{}));
        if (_receive_fiber) {
            try {
                co_await *std::exchange(_receive_fiber, std::nullopt);
            } catch (const aborted_exception&) {}
        }
    }

private:
    future<> receive_fiber() {
        // TODO: configurable
        static const size_t _max_receive_concurrency = 20;

        std::vector<delivery> batch;
        while (true) {
            // TODO: there is most definitely a better way to do this, but let's assume this is good enough (for now...)
            // Unfortunately seastar does not yet have a multi-consumer queue implementation.
            batch.push_back(co_await _queue.pop_eventually());
            while (!_queue.empty() && batch.size() < _max_receive_concurrency) {
                batch.push_back(_queue.pop());
            }

            co_await parallel_for_each(batch, [&] (delivery& m) {
                return _rpc.receive(m.src, std::move(m.payload));
            });

            batch.clear();
        }
    }
};

using reconfigure_result_t = std::variant<std::monostate,
    timed_out_error, raft::not_a_leader, raft::dropped_entry, raft::commit_status_unknown, raft::conf_change_in_progress>;

future<reconfigure_result_t> reconfigure(
        const std::vector<raft::server_id>& ids,
        raft::logical_clock::time_point timeout,
        logical_timer& timer,
        raft::server& server) {
    raft::server_address_set config;
    for (auto id : ids) {
        config.insert(raft::server_address { .id = id });
    }

    try {
        co_await timer.with_timeout(timeout, [&server, config = std::move(config)] () {
            return server.set_configuration(std::move(config));
        }());
        co_return std::monostate{};
    } catch (raft::not_a_leader e) {
        co_return e;
    } catch (raft::dropped_entry e) {
        co_return e;
    } catch (raft::commit_status_unknown e) {
        co_return e;
    } catch (raft::conf_change_in_progress e) {
        co_return e;
    } catch (logical_timer::timed_out<void> e) {
        (void)e.get_future().discard_result()
            .handle_exception([] (std::exception_ptr eptr) {
                try {
                    std::rethrow_exception(eptr);
                } catch (const raft::dropped_entry&) {
                } catch (const raft::stopped_error&) {
                }
            });
        co_return timed_out_error{};
    }
}

// Contains a `raft::server` and other facilities needed for it and the underlying
// modules (persistence, rpc, etc.) to run, and to communicate with the external environment.
template <PureStateMachine M>
class raft_server {
    raft::server_id _id;

    std::unique_ptr<snapshots_t<typename M::state_t>> _snapshots;
    std::unique_ptr<delivery_queue<typename M::state_t>> _queue;
    std::unique_ptr<raft::server> _server;

    // The following objects are owned by _server:
    impure_state_machine<M>& _sm;
    rpc<typename M::state_t>& _rpc;

    bool _started = false;
    bool _stopped = false;

    // Used to ensure that when `abort()` returns there are
    // no more in-progress methods running on this object.
    seastar::gate _gate;

public:
    // Create a `raft::server` with the given `id` and all other facilities required
    // by the server (the state machine, RPC instance and so on). The server will use
    // `send_rpc` to send RPC messages to other servers and `fd` for failure detection.
    //
    // If this is the first server in the cluster, pass `first_server = true`; this will
    // cause the server to be created with a non-empty singleton configuration containing itself.
    // Otherwise, pass `first_server = false`; that server, in order to function, must be then added
    // by the existing cluster through a configuration change.
    //
    // The created server is not started yet; use `start` for that.
    static std::unique_ptr<raft_server> create(
            raft::server_id id,
            shared_ptr<failure_detector> fd,
            raft::server::configuration cfg,
            bool first_server,
            typename rpc<typename M::state_t>::send_message_t send_rpc) {
        using state_t = typename M::state_t;

        auto snapshots = std::make_unique<snapshots_t<state_t>>();
        auto sm = std::make_unique<impure_state_machine<M>>(*snapshots);
        auto rpc_ = std::make_unique<rpc<state_t>>(*snapshots, std::move(send_rpc));
        auto persistence_ = std::make_unique<persistence<state_t>>(*snapshots, first_server ? std::optional{id} : std::nullopt, M::init);
        auto queue = std::make_unique<delivery_queue<state_t>>(*rpc_);

        auto& sm_ref = *sm;
        auto& rpc_ref = *rpc_;

        auto server = raft::create_server(
                id, std::move(rpc_), std::move(sm), std::move(persistence_), std::move(fd),
                std::move(cfg));

        return std::make_unique<raft_server>(initializer{
            ._id = id,
            ._snapshots = std::move(snapshots),
            ._queue = std::move(queue),
            ._server = std::move(server),
            ._sm = sm_ref,
            ._rpc = rpc_ref
        });
    }

    ~raft_server() {
        assert(!_started || _stopped);
    }

    raft_server(const raft_server&&) = delete;
    raft_server(raft_server&&) = delete;

    // Start the server. Can be called at most once.
    //
    // TODO: implement server ``crashes'' and ``restarts''.
    // A crashed server needs a new delivery queue to be created in order to restart but must
    // reuse the previous `persistence`. Perhaps the delivery queue should be created in `start`?
    future<> start() {
        assert(!_started);
        _started = true;

        co_await _server->start();
        _queue->start();
    }

    // Stop the given server. Must be called before the server is destroyed
    // (unless it was never started in the first place).
    future<> abort() {
        auto f = _gate.close();
        // Abort everything before waiting on the gate close future
        // so currently running operations finish earlier.
        if (_started) {
            co_await _queue->abort();
            co_await _server->abort();
        }
        co_await std::move(f);
        _stopped = true;
    }

    void tick() {
        assert(_started);
        _rpc.tick();
        _server->tick();
    }

    future<call_result_t<M>> call(
            typename M::input_t input,
            raft::logical_clock::time_point timeout,
            logical_timer& timer) {
        assert(_started);
        return with_gate(_gate, [this, input = std::move(input), timeout, &timer] {
            return ::call(std::move(input), timeout, timer, *_server, _sm);
        });
    }

    future<reconfigure_result_t> reconfigure(
            const std::vector<raft::server_id>& ids,
            raft::logical_clock::time_point timeout,
            logical_timer& timer) {
        assert(_started);
        return with_gate(_gate, [this, &ids, timeout, &timer] {
            return ::reconfigure(ids, timeout, timer, *_server);
        });
    }

    bool is_leader() const {
        return _server->is_leader();
    }

    raft::server_id id() const {
        return _id;
    }

    void deliver(raft::server_id src, const typename rpc<typename M::state_t>::message_t& m) {
        assert(_started);
        _queue->push(src, m);
    }

private:
    struct initializer {
        raft::server_id _id;

        std::unique_ptr<snapshots_t<typename M::state_t>> _snapshots;
        std::unique_ptr<delivery_queue<typename M::state_t>> _queue;
        std::unique_ptr<raft::server> _server;

        impure_state_machine<M>& _sm;
        rpc<typename M::state_t>& _rpc;
    };

    raft_server(initializer i)
        : _id(i._id)
        , _snapshots(std::move(i._snapshots))
        , _queue(std::move(i._queue))
        , _server(std::move(i._server))
        , _sm(i._sm)
        , _rpc(i._rpc) {
    }

    friend std::unique_ptr<raft_server> std::make_unique<raft_server, raft_server::initializer>(initializer&&);
};

static raft::server_id to_raft_id(size_t id) {
    // Raft uses UUID 0 as special case.
    assert(id > 0);
    return raft::server_id{utils::UUID{0, id}};
}

struct environment_config {
    raft::logical_clock::duration network_delay;
    raft::logical_clock::duration fd_convict_threshold;
};

// A set of `raft_server`s connected by a `network`.
//
// The `network` is initialized with a message delivery function
// which notifies the destination's failure detector on each message
// and if the message contains an RPC payload, pushes it into the destination's
// `delivery_queue`.
//
// Needs to be periodically `tick()`ed which ticks the network
// and underlying servers.
template <PureStateMachine M>
class environment : public seastar::weakly_referencable<environment<M>> {
    using input_t = typename M::output_t;
    using state_t = typename M::state_t;
    using output_t = typename M::output_t;

    struct route {
        std::unique_ptr<raft_server<M>> _server;
        shared_ptr<failure_detector> _fd;
    };

    // Passed to newly created failure detectors.
    const raft::logical_clock::duration _fd_convict_threshold;

    // Used to deliver messages coming from the network to appropriate servers and their failure detectors.
    // Also keeps the servers and the failure detectors alive (owns them).
    // Before we show a Raft server to others we must add it to this map.
    std::unordered_map<raft::server_id, route> _routes;

    // Used to create a new ID in `new_server`.
    size_t _next_id = 1;

    // Engaged optional: RPC message, nullopt: heartbeat
    using message_t = std::optional<typename rpc<state_t>::message_t>;
    network<message_t> _network;

    bool _stopped = false;

    // Used to ensure that when `abort()` returns there are
    // no more in-progress methods running on this object.
    seastar::gate _gate;

public:
    environment(environment_config cfg)
            : _fd_convict_threshold(cfg.fd_convict_threshold), _network(cfg.network_delay,
        [this] (raft::server_id src, raft::server_id dst, const message_t& m) {
            auto& [s, fd] = _routes.at(dst);
            fd->receive_heartbeat(src);
            if (m) {
                s->deliver(src, *m);
            }
        }) {
    }

    ~environment() {
        assert(_routes.empty() || _stopped);
    }

    environment(const environment&) = delete;
    environment(environment&&) = delete;

    void tick_network() {
        _network.tick();
    }

    // TODO: adjustable/randomizable ticking ratios
    void tick_servers() {
        for (auto& [_, r] : _routes) {
            r._server->tick();
            r._fd->tick();
        }
    }

    // Creates and starts a server with a local (uniquely owned) failure detector,
    // connects it to the network and returns its ID.
    //
    // If `first == true` the server is created with a singleton configuration containing itself.
    // Otherwise it is created with an empty configuration. The user must explicitly ask for a configuration change
    // if they want to make a cluster (group) out of this server and other existing servers.
    // The user should be able to create multiple clusters by calling `new_server` multiple times with `first = true`.
    // (`first` means ``first in group'').
    future<raft::server_id> new_server(bool first, raft::server::configuration cfg = {}) {
        return with_gate(_gate, [this, first, cfg = std::move(cfg)] () -> future<raft::server_id> {
            auto id = to_raft_id(_next_id++);

            // TODO: in the future we want to simulate multiple raft servers running on a single ``node'',
            // sharing a single failure detector. We will then likely split `new_server` into two steps: `new_node` and `new_server`,
            // the first creating the failure detector for a node and wiring it up, the second creating a server on a given node.
            // We will also possibly need to introduce some kind of ``node IDs'' which `failure_detector` (and `network`)
            // will operate on (currently they operate on `raft::server_id`s, assuming a 1-1 mapping of server-to-node).
            auto fd = seastar::make_shared<failure_detector>(_fd_convict_threshold, [id, this] (raft::server_id dst) {
                _network.send(id, dst, std::nullopt);
            });

            auto srv = raft_server<M>::create(id, fd, std::move(cfg), first,
                    [id, this] (raft::server_id dst, typename rpc<state_t>::message_t m) {
                _network.send(id, dst, {std::move(m)});
            });

            co_await srv->start();

            // Add us to other servers' failure detectors.
            for (auto& [_, r] : _routes) {
                r._fd->add_server(id);
            }

            // Add other servers to our failure detector.
            for (auto& [id, _] : _routes) {
                fd->add_server(id);
            }

            _routes.emplace(id, route{std::move(srv), std::move(fd)});

            co_return id;
        });
    }

    raft_server<M>& get_server(raft::server_id id) {
        return *_routes.at(id)._server;
    }

    network<message_t>& get_network() {
        return _network;
    }

    // Must be called before we are destroyed unless `new_server` was never called.
    future<> abort() {
        // Close the gate before iterating over _routes to prevent concurrent modification by other methods.
        co_await _gate.close();
        for (auto& [_, r] : _routes) {
            co_await r._server->abort();
        }
        _stopped = true;
    }
};

template <PureStateMachine M, std::invocable<environment<M>&, ticker&> F>
auto with_env_and_ticker(environment_config cfg, F f) {
    return do_with(std::move(f), std::make_unique<environment<M>>(std::move(cfg)), std::make_unique<ticker>(tlogger),
            [] (F& f, std::unique_ptr<environment<M>>& env, std::unique_ptr<ticker>& t) {
        return f(*env, *t).finally([&env_ = env, &t_ = t] () mutable -> future<> {
            // move into coroutine body so they don't get destroyed with the lambda (on first co_await)
            auto& env = env_;
            auto& t = t_;

            co_await t->abort();
            co_await env->abort();
        });
    });
}

struct ExReg {
    // Replaces the state with `x` and returns the previous state.
    struct exchange { int32_t x; };

    // Returns the state.
    struct read {};

    // Return value for `exchange` or `read`.
    struct ret { int32_t x; };

    using state_t = int32_t;
    using input_t = std::variant<read, exchange>;
    using output_t = ret;

    static std::pair<state_t, output_t> delta(state_t curr, input_t input) {
        using res_t = std::pair<state_t, output_t>;

        return std::visit(make_visitor(
        [&curr] (const exchange& w) -> res_t {
            return {w.x, ret{curr}};
        },
        [&curr] (const read&) -> res_t {
            return {curr, ret{curr}};
        }
        ), input);
    }

    static const state_t init;
};

const ExReg::state_t ExReg::init = 0;

namespace ser {
    template <>
    struct serializer<ExReg::exchange> {
        template <typename Output>
        static void write(Output& buf, const ExReg::exchange& op) { serializer<int32_t>::write(buf, op.x); };

        template <typename Input>
        static ExReg::exchange read(Input& buf) { return { serializer<int32_t>::read(buf) }; }

        template <typename Input>
        static void skip(Input& buf) { serializer<int32_t>::skip(buf); }
    };

    template <>
    struct serializer<ExReg::read> {
        template <typename Output>
        static void write(Output& buf, const ExReg::read&) {};

        template <typename Input>
        static ExReg::read read(Input& buf) { return {}; }

        template <typename Input>
        static void skip(Input& buf) {}
    };
}

bool operator==(ExReg::ret a, ExReg::ret b) { return a.x == b.x; }

std::ostream& operator<<(std::ostream& os, const ExReg::ret& r) {
    return os << format("ret{{{}}}", r.x);
}

std::ostream& operator<<(std::ostream& os, const ExReg::read&) {
    return os << "read";
}

std::ostream& operator<<(std::ostream& os, const ExReg::exchange& e) {
    return os << format("xng{{{}}}", e.x);
}

// Wait until either one of `nodes` in `env` becomes a leader, or duration `d` expires according to `timer` (whichever happens first).
// If the leader is found, returns it. Otherwise throws a `logical_timer::timed_out` exception.
template <PureStateMachine M>
struct wait_for_leader {
    // FIXME: change into free function after clang bug #50345 is fixed
    future<raft::server_id> operator()(
            environment<M>& env,
            std::vector<raft::server_id> nodes,
            logical_timer& timer,
            raft::logical_clock::duration d) {
        auto l = co_await timer.with_timeout(timer.now() + d, [] (weak_ptr<environment<M>> env, std::vector<raft::server_id> nodes) -> future<raft::server_id> {
            while (true) {
                if (!env) {
                    co_return raft::server_id{};
                }

                auto it = std::find_if(nodes.begin(), nodes.end(), [&env] (raft::server_id id) { return env->get_server(id).is_leader(); });
                if (it != nodes.end()) {
                    co_return *it;
                }

                co_await seastar::later();
            }
        }(env.weak_from_this(), std::move(nodes)));

        assert(l != raft::server_id{});
        assert(env.get_server(l).is_leader());

        co_return l;
    }
};

SEASTAR_TEST_CASE(basic_test) {
    logical_timer timer;
    environment_config cfg {
        .network_delay = 5_t,
        .fd_convict_threshold = 50_t,
    };
    co_await with_env_and_ticker<ExReg>(cfg, [&timer] (environment<ExReg>& env, ticker& t) -> future<> {
        using output_t = typename ExReg::output_t;

        t.start({
            {1, [&] {
                env.tick_network();
                timer.tick();
            }},
            {10, [&] {
                env.tick_servers();
            }}
        }, 10'000);

        auto leader_id = co_await env.new_server(true);

        // Wait at most 1000 ticks for the server to elect itself as a leader.
        assert(co_await wait_for_leader<ExReg>{}(env, {leader_id}, timer, 1000_t) == leader_id);

        auto call = [&] (ExReg::input_t input, raft::logical_clock::duration timeout) {
            return env.get_server(leader_id).call(std::move(input),  timer.now() + timeout, timer);
        };

        auto eq = [] (const call_result_t<ExReg>& r, const output_t& expected) {
            return std::holds_alternative<output_t>(r) && std::get<output_t>(r) == expected;
        };

        for (int i = 1; i <= 100; ++i) {
            assert(eq(co_await call(ExReg::exchange{i}, 100_t), ExReg::ret{i - 1}));
        }

        tlogger.debug("100 exchanges - single server - passed");

        auto id2 = co_await env.new_server(false);
        auto id3 = co_await env.new_server(false);

        tlogger.debug("Started 2 more servers, changing configuration");

        assert(std::holds_alternative<std::monostate>(
            co_await env.get_server(leader_id).reconfigure({leader_id, id2, id3}, timer.now() + 100_t, timer)));

        tlogger.debug("Configuration changed");

        co_await call(ExReg::exchange{0}, 100_t);
        for (int i = 1; i <= 100; ++i) {
            assert(eq(co_await call(ExReg::exchange{i}, 100_t), ExReg::ret{i - 1}));
        }

        tlogger.debug("100 exchanges - three servers - passed");

        // concurrent calls
        std::vector<future<call_result_t<ExReg>>> futs;
        for (int i = 0; i < 100; ++i) {
            futs.push_back(call(ExReg::read{}, 100_t));
            co_await timer.sleep(2_t);
        }
        for (int i = 0; i < 100; ++i) {
            assert(eq(co_await std::move(futs[i]), ExReg::ret{100}));
        }

        tlogger.debug("100 concurrent reads - three servers - passed");
    });

    tlogger.debug("Finished");
}

// A snapshot was being taken with the wrong term (current term instead of the term at the snapshotted index).
// This is a regression test for that bug.
SEASTAR_TEST_CASE(snapshot_uses_correct_term_test) {
    logical_timer timer;
    environment_config cfg {
        .network_delay = 1_t,
        .fd_convict_threshold = 10_t,
    };
    co_await with_env_and_ticker<ExReg>(cfg, [&timer] (environment<ExReg>& env, ticker& t) -> future<> {
        t.start({
            {1, [&] {
                env.tick_network();
                timer.tick();
            }},
            {10, [&] {
                env.tick_servers();
            }}
        }, 10'000);


        auto id1 = co_await env.new_server(true,
                raft::server::configuration{
        // It's easier to catch the problem when we send entries one by one, not in batches.
                    .append_request_threshold = 1,
                });
        assert(co_await wait_for_leader<ExReg>{}(env, {id1}, timer, 1000_t) == id1);

        auto id2 = co_await env.new_server(true,
                raft::server::configuration{
                    .append_request_threshold = 1,
                });

        assert(std::holds_alternative<std::monostate>(
            co_await env.get_server(id1).reconfigure({id1, id2}, timer.now() + 100_t, timer)));

        // Append a bunch of entries
        for (int i = 1; i <= 10; ++i) {
            assert(std::holds_alternative<typename ExReg::ret>(
                co_await env.get_server(id1).call(ExReg::exchange{0}, timer.now() + 100_t, timer)));
        }

        assert(env.get_server(id1).is_leader());

        // Force a term increase by partitioning the network and waiting for the leader to step down
        tlogger.trace("add grudge");
        env.get_network().add_grudge(id2, id1);
        env.get_network().add_grudge(id1, id2);

        while (env.get_server(id1).is_leader()) {
            co_await seastar::later();
        }

        tlogger.trace("remove grudge");
        env.get_network().remove_grudge(id2, id1);
        env.get_network().remove_grudge(id1, id2);

        auto l = co_await wait_for_leader<ExReg>{}(env, {id1, id2}, timer, 1000_t);
        tlogger.trace("last leader: {}", l);

        // Now the current term is greater than the term of the first couple of entries.
        // Join another server with a small snapshot_threshold.
        // The leader will send entries to this server one by one (due to small append_request_threshold),
        // so the joining server will apply entries one by one or in small batches (depends on the timing),
        // making it likely that it decides to take a snapshot at an entry with term lower than the current one.
        // If we are (un)lucky and we take a snapshot at the last appended entry, the node will refuse all
        // later append_entries requests due to non-matching term at the last appended entry. Note: due to this
        // requirement, the test is nondeterministic and doesn't always catch the bug (it depends on a race
        // between applier_fiber and io_fiber), but it does catch it in a significant number of runs.
        // It's also a lot easier to catch this in dev than in debug, for instance.
        // If we catch the bug, the reconfigure request below will time out.

        auto id3 = co_await env.new_server(false,
                raft::server::configuration{
                    .snapshot_threshold = 5,
                    .snapshot_trailing = 2,
                });
        assert(std::holds_alternative<std::monostate>(
            co_await env.get_server(l).reconfigure({l, id3}, timer.now() + 1000_t, timer)));
    });
}

// Regression test for the following bug: when we took a snapshot, we forgot to save the configuration.
// This caused each node in the cluster to eventually forget the cluster configuration.
SEASTAR_TEST_CASE(snapshotting_preserves_config_test) {
    logical_timer timer;
    environment_config cfg {
        .network_delay = 1_t,
        .fd_convict_threshold = 10_t,
    };
    co_await with_env_and_ticker<ExReg>(cfg, [&timer] (environment<ExReg>& env, ticker& t) -> future<> {
        t.start({
            {1, [&] {
                env.tick_network();
                timer.tick();
            }},
            {10, [&] {
                env.tick_servers();
            }}
        }, 10'000);


        auto id1 = co_await env.new_server(true,
                raft::server::configuration{
                    .snapshot_threshold = 5,
                    .snapshot_trailing = 1,
                });
        assert(co_await wait_for_leader<ExReg>{}(env, {id1}, timer, 1000_t) == id1);

        auto id2 = co_await env.new_server(false,
                raft::server::configuration{
                    .snapshot_threshold = 5,
                    .snapshot_trailing = 1,
                });

        assert(std::holds_alternative<std::monostate>(
            co_await env.get_server(id1).reconfigure({id1, id2}, timer.now() + 100_t, timer)));

        // Append a bunch of entries
        for (int i = 1; i <= 10; ++i) {
            assert(std::holds_alternative<typename ExReg::ret>(
                co_await env.get_server(id1).call(ExReg::exchange{0}, timer.now() + 100_t, timer)));
        }

        assert(env.get_server(id1).is_leader());

        // Partition the network, forcing the leader to step down.
        tlogger.trace("add grudge");
        env.get_network().add_grudge(id2, id1);
        env.get_network().add_grudge(id1, id2);

        while (env.get_server(id1).is_leader()) {
            co_await seastar::later();
        }

        tlogger.trace("remove grudge");
        env.get_network().remove_grudge(id2, id1);
        env.get_network().remove_grudge(id1, id2);

        // With the bug this would timeout, the cluster is unable to elect a leader without the configuration.
        auto l = co_await wait_for_leader<ExReg>{}(env, {id1, id2}, timer, 1000_t);
        tlogger.trace("last leader: {}", l);
    });
}

// Given a function `F` which takes a `raft::server_id` argument and returns a variant type
// which contains `not_a_leader`, repeatedly calls `F` until it returns something else than
// `not_a_leader` or until we reach a limit, whichever happens first.
// The maximum number of calls until we give up is specified by `bounces`.
// The initial `raft::server_id` argument provided to `F` is specified as an argument
// to this function (`srv_id`). If the initial call returns `not_a_leader`, then:
// - if the result contained a different leader ID, we will use it in the next call,
//   sleeping for `known_leader_delay` first,
// - otherwise we will take the next ID from the `known` set, sleeping for
//   `unknown_leader_delay` first.
// The returned result contains the result of the last call to `F` and the last
// server ID passed to `F`.
template <typename F>
struct bouncing {
    using future_type = std::invoke_result_t<F, raft::server_id>;
    using value_type = typename future_type::value_type;

    static_assert(boost::mp11::mp_contains<value_type, raft::not_a_leader>::value);

    F _f;

    bouncing(F f) : _f(std::move(f)) {}

    // FIXME: change this into a free function after clang bug #50345 is fixed.
    future<std::pair<value_type, raft::server_id>> operator()(
            logical_timer& timer,
            std::unordered_set<raft::server_id> known,
            raft::server_id srv_id,
            size_t bounces,
            raft::logical_clock::duration known_leader_delay,
            raft::logical_clock::duration unknown_leader_delay
            ) {
        auto it = known.find(srv_id);
        while (true) {
            auto res = co_await _f(srv_id);

            if (auto n_a_l = std::get_if<raft::not_a_leader>(&res); n_a_l && bounces) {
                --bounces;
                if (n_a_l->leader) {
                    assert(n_a_l->leader != srv_id);
                    co_await timer.sleep(known_leader_delay);
                    srv_id = n_a_l->leader;
                } else {
                    co_await timer.sleep(unknown_leader_delay);
                    assert(!known.empty());
                    if (it == known.end() || ++it == known.end()) {
                        it = known.begin();
                    }
                    srv_id = *it;
                }
                continue;
            }

            co_return std::pair{res, srv_id};
        }
    }
};

// An operation representing a call to the Raft cluster with a specific state machine input.
// We may bounce a number of times if the server returns `not_a_leader` before giving up.
template <PureStateMachine M>
struct raft_call {
    typename M::input_t input;
    raft::logical_clock::duration timeout;

    using result_type = call_result_t<M>;

    struct state_type {
        environment<M>& env;

        // The set of servers that may be part of the current configuration.
        // Sometimes we don't know the exact configuration, e.g. after a failed configuration change.
        const std::unordered_set<raft::server_id>& known;

        logical_timer& timer;
    };

    future<result_type> execute(state_type& s, const operation::context& ctx) {
        // TODO a stable contact point used by a given thread would be preferable;
        // the thread would switch only if necessary (the contact point left the configuration).
        // Currently we choose the contact point randomly each time.
        assert(s.known.size() > 0);
        static std::mt19937 engine{0};

        auto it = s.known.begin();
        std::advance(it, std::uniform_int_distribution<size_t>{0, s.known.size() - 1}(engine));
        auto contact = *it;

        tlogger.debug("db call start inp {} tid {} start time {} current time {} contact {}", input, ctx.thread, ctx.start, s.timer.now(), contact);

        auto [res, last] = co_await bouncing{[input = input, timeout = s.timer.now() + timeout, &timer = s.timer, &env = s.env] (raft::server_id id) {
            return env.get_server(id).call(input, timeout, timer);
        }}(s.timer, s.known, contact, 6, 10_t, 10_t);
        tlogger.debug("db call end inp {} tid {} start time {} current time {} last contact {}", input, ctx.thread, ctx.start, s.timer.now(), last);

        co_return res;
    }

    friend std::ostream& operator<<(std::ostream& os, const raft_call& c) {
        return os << format("raft_call{{input:{},timeout:{}}}", c.input, c.timeout);
    }
};

// An operation that partitions the network in half.
// During the partition, no server from one half can contact any server from the other;
// the partition is symmetric.
// For odd number of nodes, ensures that the current leader (if there is one) is in the minority.
template <PureStateMachine M>
class network_majority_grudge {
    raft::logical_clock::duration _duration;

public:
    struct state_type {
        environment<M>& env;
        const std::unordered_set<raft::server_id>& known;
        logical_timer& timer;
        std::mt19937 rnd;
    };

    using result_type = std::monostate;

    network_majority_grudge(raft::logical_clock::duration d) : _duration(d) {
        static_assert(operation::Executable<network_majority_grudge<M>>);
    }

    future<result_type> execute(state_type& s, const operation::context& ctx) {
        std::vector<raft::server_id> nodes{s.known.begin(), s.known.end()};
        std::shuffle(nodes.begin(), nodes.end(), s.rnd);

        auto mid = nodes.begin() + (nodes.size() / 2);
        if (nodes.size() % 2) {
            // Odd number of nodes, let's ensure that the leader (if there is one) is in the minority
            auto it = std::find_if(mid, nodes.end(), [&env = s.env] (raft::server_id id) { return env.get_server(id).is_leader(); });
            if (it != nodes.end()) {
                std::swap(*nodes.begin(), *it);
            }
        }

        // Note: creating the grudges has O(n^2) complexity, where n is the cluster size.
        // May be problematic for (very) large clusters.
        for (auto x = nodes.begin(); x != mid; ++x) {
            for (auto y = mid; y != nodes.end(); ++y) {
                s.env.get_network().add_grudge(*x, *y);
                s.env.get_network().add_grudge(*y, *x);
            }
        }

        tlogger.debug("network_majority_grudge start tid {} start time {} current time {} duration {} grudge: {} vs {}",
                ctx.thread, ctx.start, s.timer.now(),
                _duration,
                std::vector<raft::server_id>{nodes.begin(), mid},
                std::vector<raft::server_id>{mid, nodes.end()});

        co_await s.timer.sleep(_duration);

        tlogger.debug("network_majority_grudge end tid {} start time {} current time {}", ctx.thread, ctx.start, s.timer.now());

        // Some servers in `nodes` may already be gone at this point but network doesn't care.
        // It's safe to call `remove_grudge`.
        for (auto x = nodes.begin(); x != mid; ++x) {
            for (auto y = mid; y != nodes.end(); ++y) {
                s.env.get_network().remove_grudge(*x, *y);
                s.env.get_network().remove_grudge(*y, *x);
            }
        }

        co_return std::monostate{};
    }

    friend std::ostream& operator<<(std::ostream& os, const network_majority_grudge& p) {
        return os << format("network_majority_grudge{{duration:{}}}", p._duration);
    }
};

std::ostream& operator<<(std::ostream& os, const std::monostate&) {
    return os << "";
}

template <typename T, typename... Ts>
std::ostream& operator<<(std::ostream& os, const std::variant<T, Ts...>& v) {
    std::visit([&os] (auto& arg) { os << arg; }, v);
    return os;
}

namespace operation {

std::ostream& operator<<(std::ostream& os, const thread_id& tid) {
    return os << format("thread_id{{{}}}", tid.id);
}

} // namespace operation

// An immutable sequence of integers.
class append_seq {
public:
    using elem_t = int32_t;

private:
    // This represents the sequence of integers from _seq->begin() to _seq->begin() + _end.
    // The underlying vector *_seq may however be shared by other instances of `append_seq`.
    // If only one instance is appending, the operation is O(1). However, each subsequent
    // append performed by another instance sharing this vector must perform a copy.

    lw_shared_ptr<std::vector<elem_t>> _seq; // always engaged
    size_t _end; // <= _seq.size()
    elem_t _digest; // sum of all elements modulo `magic`

    static const elem_t magic = 54313;

public:
    append_seq(std::vector<elem_t> v) : _seq{make_lw_shared<std::vector<elem_t>>(std::move(v))}, _end{_seq->size()}, _digest{0} {
        for (auto x : *_seq) {
            _digest = digest_append(_digest, x);
        }
    }

    static elem_t digest_append(elem_t d, elem_t x) {
        assert(0 <= d < magic);

        auto y = (d + x) % magic;
        assert(digest_remove(y, x) == d);
        return y;
    }

    static elem_t digest_remove(elem_t d, elem_t x) {
        assert(0 <= d < magic);
        auto y = (d - x) % magic;
        return y < 0 ? y + magic : y;
    }

    elem_t digest() const {
        return _digest;
    }

    append_seq append(elem_t x) const {
        assert(_seq);
        assert(_end <= _seq->size());

        auto seq = _seq;
        if (_end < seq->size()) {
            // The shared sequence was already appended beyond _end by someone else.
            // We need to copy everything so we don't break the other guy.
            seq = make_lw_shared<std::vector<elem_t>>(seq->begin(), seq->begin() + _end);
        }

        seq->push_back(x);
        return {std::move(seq), _end + 1, digest_append(_digest, x)};
    }

    elem_t operator[](size_t idx) const {
        assert(_seq);
        assert(idx < _end);
        assert(_end <= _seq->size());
        return (*_seq)[idx];
    }

    bool empty() const {
        return _end == 0;
    }

    std::pair<append_seq, elem_t> pop() {
        assert(_seq);
        assert(_end <= _seq->size());
        assert(0 < _end);

        return {{_seq, _end - 1, digest_remove(_digest, (*_seq)[_end - 1])}, (*_seq)[_end - 1]};
    }

    friend std::ostream& operator<<(std::ostream& os, const append_seq& s) {
        // TODO: don't copy the elements
        std::vector<elem_t> v{s._seq->begin(), s._seq->begin() + s._end};
        return os << format("v {} _end {}", v, s._end);
    }

private:
    append_seq(lw_shared_ptr<std::vector<elem_t>> seq, size_t end, elem_t d)
        : _seq(std::move(seq)), _end(end), _digest(d) {}
};

struct AppendReg {
    struct append { int32_t x; };
    struct ret { int32_t x; append_seq prev; };

    using state_t = append_seq;
    using input_t = append;
    using output_t = ret;

    static std::pair<state_t, output_t> delta(const state_t& curr, input_t input) {
        return {curr.append(input.x), {input.x, curr}};
    }

    static thread_local const state_t init;
};

thread_local const AppendReg::state_t AppendReg::init{{0}};

namespace ser {
    template <>
    struct serializer<AppendReg::append> {
        template <typename Output>
        static void write(Output& buf, const AppendReg::append& op) { serializer<int32_t>::write(buf, op.x); };

        template <typename Input>
        static AppendReg::append read(Input& buf) { return { serializer<int32_t>::read(buf) }; }

        template <typename Input>
        static void skip(Input& buf) { serializer<int32_t>::skip(buf); }
    };
}

// TODO: do some useful logging in case of consistency violation
struct append_reg_model {
    using elem_t = typename append_seq::elem_t;

    struct entry {
        elem_t elem;
        elem_t digest;
    };

    std::vector<entry> seq{{0, 0}};
    std::unordered_map<elem_t, size_t> index{{0, 0}};
    std::unordered_set<elem_t> banned;
    std::unordered_set<elem_t> returned;
    std::unordered_set<elem_t> in_progress;

    void invocation(elem_t x) {
        assert(!index.contains(x));
        assert(!in_progress.contains(x));
        in_progress.insert(x);
    }

    void return_success(elem_t x, append_seq prev) {
        assert(!returned.contains(x));
        assert(x != 0);
        assert(!prev.empty());
        completion(x, std::move(prev));
        returned.insert(x);
    }

    void return_failure(elem_t x) {
        assert(!index.contains(x));
        assert(in_progress.contains(x));
        banned.insert(x);
        in_progress.erase(x);
    }

private:
    void completion(elem_t x, append_seq prev) {
        if (prev.empty()) {
            assert(x == 0);
            return;
        }

        assert(x != 0);
        assert(!banned.contains(x));
        assert(in_progress.contains(x) || index.contains(x));

        auto [prev_prev, prev_x] = prev.pop();

        if (auto it = index.find(x); it != index.end()) {
            // This element was already completed.
            auto idx = it->second;
            assert(0 < idx);
            assert(idx < seq.size());

            assert(prev_x == seq[idx - 1].elem);
            assert(prev.digest() == seq[idx - 1].digest);

            return;
        }

        // A new completion.
        // First, recursively complete the previous elements...
        completion(prev_x, std::move(prev_prev));

        // Check that the existing tail matches our tail.
        assert(!seq.empty());
        assert(prev_x == seq.back().elem);
        assert(prev.digest() == seq.back().digest);

        // All previous elements were completed, so the new element belongs at the end.
        index.emplace(x, seq.size());
        seq.push_back(entry{x, append_seq::digest_append(seq.back().digest, x)});
        in_progress.erase(x);
    }
};

std::ostream& operator<<(std::ostream& os, const AppendReg::append& a) {
    return os << format("append{{{}}}", a.x);
}

std::ostream& operator<<(std::ostream& os, const AppendReg::ret& r) {
    return os << format("ret{{{}, {}}}", r.x, r.prev);
}

SEASTAR_TEST_CASE(basic_generator_test) {
    using op_type = operation::invocable<operation::either_of<
            raft_call<AppendReg>,
            network_majority_grudge<AppendReg>
        >>;
    using history_t = utils::chunked_vector<std::variant<op_type, operation::completion<op_type>>>;

    static_assert(operation::Invocable<op_type>);

    logical_timer timer;
    environment_config cfg {
        .network_delay = 3_t,
        .fd_convict_threshold = 50_t,
    };
    co_await with_env_and_ticker<AppendReg>(cfg, [&cfg, &timer] (environment<AppendReg>& env, ticker& t) -> future<> {
        t.start({
            {1, [&] {
                env.tick_network();
                timer.tick();
            }},
            {10, [&] {
                env.tick_servers();
            }}
        }, 200'000);

        auto leader_id = co_await env.new_server(true);

        // Wait for the server to elect itself as a leader.
        assert(co_await wait_for_leader<AppendReg>{}(env, {leader_id}, timer, 1000_t) == leader_id);


        size_t no_servers = 5;
        std::unordered_set<raft::server_id> servers{leader_id};
        for (size_t i = 1; i < no_servers; ++i) {
            servers.insert(co_await env.new_server(false));
        }

        assert(std::holds_alternative<std::monostate>(
            co_await env.get_server(leader_id).reconfigure(
                std::vector<raft::server_id>{servers.begin(), servers.end()}, timer.now() + 100_t, timer)));

        auto threads = operation::make_thread_set(servers.size() + 1);
        auto nemesis_thread = some(threads);

        auto seed = tests::random::get_int<int32_t>();

        // TODO: make it dynamic based on the current configuration
        std::unordered_set<raft::server_id>& known = servers;

        raft_call<AppendReg>::state_type db_call_state {
            .env = env,
            .known = known,
            .timer = timer
        };

        network_majority_grudge<AppendReg>::state_type network_majority_grudge_state {
            .env = env,
            .known = known,
            .timer = timer,
            .rnd = std::mt19937{seed}
        };

        auto init_state = op_type::state_type{std::move(db_call_state), std::move(network_majority_grudge_state)};

        using namespace generator;

        // For reference to ``real life'' suppose 1_t ~= 10ms. Then:
        // 10_t (server tick) ~= 100ms
        // network delay = 3_t ~= 30ms
        // election timeout = 10 server ticks = 100_t ~= 1s
        // thus, to enforce leader election, need a majority to convict the current leader for > 100_t ~= 1s,
        // failure detector convict threshold = 50 srv ticks = 500_t ~= 5s
        // so need to partition for > 600_t ~= 6s
        // choose network partition duration uniformly from [600_t-600_t/3, 600_t+600_t/3] = [400_t, 800_t]
        // ~= [4s, 8s] -> ~1/2 partitions should cause an election
        // we will set request timeout 600_t ~= 6s and partition every 1200_t ~= 12s

        auto gen = op_limit(500,
            pin(nemesis_thread,
                stagger(seed, timer.now() + 200_t, 1200_t, 1200_t,
                    random(seed, [] (std::mt19937& engine) {
                        static std::uniform_int_distribution<raft::logical_clock::rep> dist{400, 800};
                        return op_type{network_majority_grudge<AppendReg>{raft::logical_clock::duration{dist(engine)}}};
                    })
                ),
                stagger(seed, timer.now(), 0_t, 50_t,
                    sequence(1, [] (int32_t i) {
                        assert(i > 0);
                        return op_type{raft_call<AppendReg>{AppendReg::append{i}, 200_t}};
                    })
                )
            )
        );

        class consistency_checker {
            append_reg_model _model;

        public:
            consistency_checker() : _model{} {}

            void operator()(op_type o) {
                tlogger.debug("invocation {}", o);

                if (auto call_op = std::get_if<raft_call<AppendReg>>(&o.op)) {
                    _model.invocation(call_op->input.x);
                }
            }

            void operator()(operation::completion<op_type> c) {
                auto res = std::get_if<op_type::result_type>(&c.result);
                assert(res);

                if (auto call_res = std::get_if<raft_call<AppendReg>::result_type>(res)) {
                    std::visit(make_visitor(
                    [this] (AppendReg::output_t& out) {
                        tlogger.debug("completion x: {} prev digest: {}", out.x, out.prev.digest());

                        _model.return_success(out.x, std::move(out.prev));
                    },
                    [] (raft::not_a_leader& e) {
                        // TODO: this is a definite failure, mark it
                        // _model.return_failure(...)
                    },
                    [] (raft::commit_status_unknown& e) {
                        // TODO assert: only allowed if reconfigurations happen?
                        // assert(false); TODO debug this
                    },
                    [] (auto&) { }
                    ), *call_res);
                } else {
                    tlogger.debug("completion {}", c);
                }
            }
        };

        history_t history;
        interpreter<op_type, decltype(gen), consistency_checker> interp{
            std::move(gen), std::move(threads), 1_t, std::move(init_state), timer,
            consistency_checker{}};
        co_await interp.run();

        // All network partitions are healed, this should succeed:
        auto last_leader = co_await wait_for_leader<AppendReg>{}(env, std::vector<raft::server_id>{servers.begin(), servers.end()}, timer, 10000_t)
                .handle_exception_type([] (logical_timer::timed_out<raft::server_id>) -> raft::server_id {
            tlogger.error("Failed to find a leader after 10000 ticks at the end of test (network partitions are healed).");
            assert(false);
        });

        // Should also succeed
        auto last_res = co_await env.get_server(last_leader).call(AppendReg::append{-1}, timer.now() + 10000_t, timer);
        if (!std::holds_alternative<typename AppendReg::ret>(last_res)) {
            tlogger.error(
                    "Expected success on the last call in the test (after electing a leader; network partitions are healed)."
                    " Got: {}", last_res);
            assert(false);
        }
    });
}
