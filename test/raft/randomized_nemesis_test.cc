/*
 * Copyright (C) 2021 ScyllaDB
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

#include <seastar/testing/test_case.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/future-util.hh>
#include <seastar/util/defer.hh>

#include "raft/server.hh"
#include "raft/logical_clock.hh"
#include "serializer.hh"
#include "serializer_impl.hh"
#include "idl/uuid.dist.hh"
#include "idl/uuid.dist.impl.hh"

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

constexpr raft::logical_clock::duration operator "" _t(unsigned long long ticks) {
    return raft::logical_clock::duration{ticks};
}

// A wrapper around `raft::logical_clock` that allows scheduling events to happen after a certain number of ticks.
class logical_timer {
    struct scheduled_impl {
    private:
        bool _resolved = false;

        virtual void do_resolve() = 0;

    public:
        virtual ~scheduled_impl() { }

        void resolve() {
            if (!_resolved) {
                do_resolve();
                _resolved = true;
            }
        }

        void mark_resolved() { _resolved = true; }
        bool resolved() { return _resolved; }
    };

    struct scheduled {
        raft::logical_clock::time_point _at;
        seastar::shared_ptr<scheduled_impl> _impl;

        void resolve() { _impl->resolve(); }
        bool resolved() { return _impl->resolved(); }
    };

    raft::logical_clock _clock;

    // A min-heap of `scheduled` events sorted by `_at`.
    std::vector<scheduled> _scheduled;

    // Comparator for the `_scheduled` min-heap.
    static bool cmp(const scheduled& a, const scheduled& b) {
        return a._at > b._at;
    }

public:
    logical_timer() = default;

    logical_timer(const logical_timer&) = delete;
    logical_timer(logical_timer&&) = default;

    // Returns the current logical time (number of `tick()`s since initialization).
    raft::logical_clock::time_point now() {
        return _clock.now();
    }

    // Tick the internal clock.
    // Resolve all events whose scheduled times arrive after that tick.
    void tick() {
        _clock.advance();
        while (!_scheduled.empty() && _scheduled.front()._at <= _clock.now()) {
            _scheduled.front().resolve();
            std::pop_heap(_scheduled.begin(), _scheduled.end(), cmp);
            _scheduled.pop_back();
        }
    }

    // Given a future `f` and a logical time point `tp`, returns a future that resolves
    // when either `f` resolves or the time point `tp` arrives (according to the number of `tick()` calls),
    // whichever comes first.
    //
    // Note: if `tp` comes first, that doesn't cancel any in-progress computations behind `f`.
    // `f` effectively becomes a discarded future.
    // Note: there is a possibility that the internal heap will grow unbounded if we call `with_timeout`
    // more often than we `tick`, so don't do that. It is recommended to call at least one
    // `tick` per one `with_timeout` call (on average in the long run).
    template <typename... T>
    future<T...> with_timeout(raft::logical_clock::time_point tp, future<T...> f) {
        if (f.available()) {
            return f;
        }

        struct sched : public scheduled_impl {
            promise<T...> _p;

            virtual ~sched() override { }
            virtual void do_resolve() override {
                _p.set_exception(std::make_exception_ptr(timed_out_error()));
            }
        };

        auto s = make_shared<sched>();
        auto res = s->_p.get_future();

        _scheduled.push_back(scheduled{
            ._at = tp,
            ._impl = s
        });
        std::push_heap(_scheduled.begin(), _scheduled.end(), cmp);

        (void)f.then_wrapped([s = std::move(s)] (auto&& f) mutable {
            if (s->resolved()) {
                f.ignore_ready_future();
            } else {
                f.forward_to(std::move(s->_p));
                s->mark_resolved();
                // tick() will (eventually) clear the `_scheduled` entry.
            }
        });

        return res;
    }

    // Returns a future that resolves after a number of `tick()`s represented by `d`.
    // Example usage: `sleep(20_t)` resolves after 20 `tick()`s.
    // Note: analogous remark applies as for `with_timeout`, i.e. make sure to call at least one `tick`
    // per one `sleep` call on average.
    future<> sleep(raft::logical_clock::duration d) {
        if (d == raft::logical_clock::duration{0}) {
            return make_ready_future<>();
        }

        struct sched : public scheduled_impl {
            promise<> _p;
            virtual ~sched() override {}
            virtual void do_resolve() override { _p.set_value(); }
        };

        auto s = make_shared<sched>();
        auto f = s->_p.get_future();
        _scheduled.push_back(scheduled{
            ._at = now() + d,
            ._impl = std::move(s)
        });
        std::push_heap(_scheduled.begin(), _scheduled.end(), cmp);

        return f;
    }
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
                } else {
                    // This is not the leader on which the command was submitted,
                    // or it is but the client already gave up on us and deallocated the channel.
                    // In any case we simply drop the output.
                }

                co_await make_ready_future<>(); // maybe yield
            }
        });
    }

    future<raft::snapshot_id> take_snapshot() override {
        auto id = raft::snapshot_id{utils::make_random_uuid()};
        _snapshots[id] = _val;
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

    // Before sending a command to Raft, the client must obtain a command ID
    // and an output channel using this function.
    template <typename F>
    future<typename M::output_t> with_output_channel(F f) {
        return with_gate(_gate, [this, f = std::move(f)] () mutable -> future<typename M::output_t> {
            promise<typename M::output_t> p;
            auto fut = p.get_future();
            auto cmd_id = utils::make_random_uuid();
            assert(_output_channels.emplace(cmd_id, std::move(p)).second);

            auto guard = defer([this, cmd_id] { _output_channels.erase(cmd_id); });
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
using call_result_t = std::variant<typename M::output_t, timed_out_error, raft::not_a_leader>;

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
    return sm.with_output_channel([&, input = std::move(input), timeout] (cmd_id_t cmd_id, future<typename M::output_t> f) {
        return timer.with_timeout(timeout, [&] (typename M::input_t input, future<typename M::output_t> f) {
            return server.add_entry(
                    make_command(std::move(cmd_id), std::move(input)),
                    raft::wait_type::applied
            ).then([f = std::move(f)] () mutable {
                return std::move(f);
            });
        }(std::move(input), std::move(f)));
    }).then([] (typename M::output_t output) {
        return make_ready_future<call_result_t<M>>(std::move(output));
    }).handle_exception([] (std::exception_ptr eptr) {
        try {
            std::rethrow_exception(eptr);
        } catch (raft::not_a_leader e) {
            return make_ready_future<call_result_t<M>>(e);
        } catch (timed_out_error e) {
            return make_ready_future<call_result_t<M>>(e);
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

public:
    using message_t = std::variant<
        snapshot_message,
        snapshot_reply_message,
        raft::append_request,
        raft::append_reply,
        raft::vote_request,
        raft::vote_reply,
        raft::timeout_now>;

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
    std::unordered_map<reply_id_t, promise<raft::snapshot_reply>> _reply_promises;
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
                it->second.set_value(std::move(m.reply));
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
        }
        ), std::move(payload));
    }

    // This is the only function of `raft::rpc` which actually expects a response.
    virtual future<raft::snapshot_reply> send_snapshot(raft::server_id dst, const raft::install_snapshot& ins, seastar::abort_source&) override {
        auto it = _snapshots.find(ins.snp.id);
        assert(it != _snapshots.end());

        auto id = _counter++;
        auto f = _reply_promises[id].get_future();
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
            co_return co_await _timer.with_timeout(_timer.now() + send_snapshot_timeout, std::move(f));
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

template <typename State, State init_state>
class persistence : public raft::persistence {
    snapshots_t<State>& _snapshots;

    std::pair<raft::snapshot, State> _stored_snapshot;
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
    persistence(snapshots_t<State>& snaps, std::optional<raft::server_id> init_config_id)
        : _snapshots(snaps)
        , _stored_snapshot(
                raft::snapshot{
                    .config = init_config_id ? raft::configuration{*init_config_id} : raft::configuration{}
                },
                init_state)
        , _stored_term_and_vote(raft::term_t{1}, raft::server_id{})
    {}

    virtual future<> store_term_and_vote(raft::term_t term, raft::server_id vote) override {
        _stored_term_and_vote = std::pair{term, vote};
        co_return;
    }

    virtual future<std::pair<raft::term_t, raft::server_id>> load_term_and_vote() override {
        co_return _stored_term_and_vote;
    }

    virtual future<> store_snapshot(const raft::snapshot& snap, size_t preserve_log_entries) override {
        // The snapshot's index cannot be smaller than the index of the first stored entry minus one;
        // that would create a ``gap'' in the log.
        assert(_stored_entries.empty() || snap.idx + 1 >= _stored_entries.front()->idx);

        auto it = _snapshots.find(snap.id);
        assert(it != _snapshots.end());
        _stored_snapshot = {snap, it->second};

        auto first_to_remain = snap.idx + 1 >= preserve_log_entries ? raft::index_t{snap.idx + 1 - preserve_log_entries} : raft::index_t{0};
        _stored_entries.erase(_stored_entries.begin(), find(first_to_remain));

        co_return;
    }

    virtual future<raft::snapshot> load_snapshot() override {
        auto [snap, state] = _stored_snapshot;
        _snapshots[snap.id] = std::move(state);
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

    send_heartbeat_t _send_heartbeat;

public:
    failure_detector(send_heartbeat_t f)
        : _send_heartbeat(std::move(f))
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
        // TODO: make it adjustable
        static const raft::logical_clock::duration _convict_threshold = 50_t;

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

public:
    network(deliver_t f)
        : _deliver(std::move(f)) {}

    void send(raft::server_id src, raft::server_id dst, Payload payload) {
        // Predict the delivery time in advance.
        // Our prediction may be wrong if a grudge exists at this expected moment of delivery.
        // Messages may also be reordered.
        // TODO: scale with number of msgs already in transit and payload size?
        // TODO: randomize the delivery time
        auto delivery_time = _clock.now() + 5_t;

        _events.push_back(event{delivery_time, message{src, dst, make_lw_shared<Payload>(std::move(payload))}});
        std::push_heap(_events.begin(), _events.end(), cmp);
    }

    void tick() {
        _clock.advance();
        deliver();
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
            bool first_server,
            typename rpc<typename M::state_t>::send_message_t send_rpc) {
        using state_t = typename M::state_t;

        auto snapshots = std::make_unique<snapshots_t<state_t>>();
        auto sm = std::make_unique<impure_state_machine<M>>(*snapshots);
        auto rpc_ = std::make_unique<rpc<state_t>>(*snapshots, std::move(send_rpc));
        auto persistence_ = std::make_unique<persistence<state_t, M::init>>(*snapshots, first_server ? std::optional{id} : std::nullopt);
        auto queue = std::make_unique<delivery_queue<state_t>>(*rpc_);

        auto& sm_ref = *sm;
        auto& rpc_ref = *rpc_;

        auto server = raft::create_server(
                id, std::move(rpc_), std::move(sm), std::move(persistence_), std::move(fd),
                raft::server::configuration{});

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

    future<> set_configuration(raft::server_address_set c) {
        assert(_started);
        return with_gate(_gate, [this, c = std::move(c)] {
            return _server->set_configuration(std::move(c));
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
class environment {
    using input_t = typename M::output_t;
    using state_t = typename M::state_t;
    using output_t = typename M::output_t;

    struct route {
        std::unique_ptr<raft_server<M>> _server;
        shared_ptr<failure_detector> _fd;
    };

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
    environment()
            : _network(
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

    // TODO: adjustable/randomizable ticking ratios
    void tick() {
        _network.tick();
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
    future<raft::server_id> new_server(bool first) {
        return with_gate(_gate, [this, first] () -> future<raft::server_id> {
            auto id = to_raft_id(_next_id++);

            // TODO: in the future we want to simulate multiple raft servers running on a single ``node'',
            // sharing a single failure detector. We will then likely split `new_server` into two steps: `new_node` and `new_server`,
            // the first creating the failure detector for a node and wiring it up, the second creating a server on a given node.
            // We will also possibly need to introduce some kind of ``node IDs'' which `failure_detector` (and `network`)
            // will operate on (currently they operate on `raft::server_id`s, assuming a 1-1 mapping of server-to-node).
            auto fd = seastar::make_shared<failure_detector>([id, this] (raft::server_id dst) {
                _network.send(id, dst, std::nullopt);
            });

            auto srv = raft_server<M>::create(id, fd, first,
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

// Calls the given function (``tick''s) as fast as the Seastar reactor allows and yields between each call.
// May be provided a limit for the number of calls; crashes if the limit is reached before the ticker
// is `abort()`ed.
// Call `start()` to start the ticking.
class ticker {
    bool _stop = false;
    std::optional<future<>> _ticker;

public:
    ticker() = default;
    ticker(const ticker&) = delete;
    ticker(ticker&&) = delete;

    ~ticker() {
        assert(!_ticker);
    }

    void start(noncopyable_function<void()> on_tick, uint64_t limit = std::numeric_limits<uint64_t>::max()) {
        assert(!_ticker);
        _ticker = tick(std::move(on_tick), limit);
    }

    future<> abort() {
        if (_ticker) {
            _stop = true;
            co_await *std::exchange(_ticker, std::nullopt);
        }
    }

private:
    future<> tick(noncopyable_function<void()> on_tick, uint64_t limit) {
        for (uint64_t i = 0; i < limit; ++i) {
            if (_stop) {
                tlogger.debug("ticker: finishing after {} ticks", i);
                co_return;
            }
            on_tick();
            co_await seastar::later();
        }

        tlogger.error("ticker: limit reached");
        assert(false);
    }
};

template <PureStateMachine M>
future<> with_env_and_ticker(noncopyable_function<future<>(environment<M>&, ticker&)> f) {
    auto env = std::make_unique<environment<M>>();
    auto t = std::make_unique<ticker>();
    return f(*env, *t).finally([env_ = std::move(env), t_ = std::move(t)] () mutable -> future<> {
        // move into coroutine body so they don't get destroyed with the lambda (on first co_await)
        auto env = std::move(env_);
        auto t = std::move(t_);
        co_await t->abort();
        co_await env->abort();
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

    static const state_t init = 0;
};

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

SEASTAR_TEST_CASE(basic_test) {
    logical_timer timer;
    co_await with_env_and_ticker<ExReg>([&timer] (environment<ExReg>& env, ticker& t) -> future<> {
        using output_t = typename ExReg::output_t;

        t.start([&] {
            env.tick();
            timer.tick();
        }, 10'000);

        auto leader_id = co_await env.new_server(true);

        // Wait at most 100 ticks for the server to elect itself as a leader.
        co_await timer.with_timeout(timer.now() + 100_t, ([&] () -> future<> {
            while (true) {
                if (env.get_server(leader_id).is_leader()) {
                    co_return;
                }
                co_await seastar::later();
            }
        })());

        assert(env.get_server(leader_id).is_leader());

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

        co_await env.get_server(leader_id).set_configuration({{.id = leader_id}, {.id = id2}, {.id = id3}});

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
