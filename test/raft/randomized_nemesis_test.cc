/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include <fmt/ranges.h>
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

#include "direct_failure_detector/failure_detector.hh"
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
#include "test/raft/helpers.hh"


namespace std {

// fmt::formatter<std::variant<Ts...> requires that the none of Ts... has
// fallback formatter, so we cannot use it before ditching operator<< of
// all element types.
template <typename T, typename... Ts>
std::ostream& operator<<(std::ostream& os, const std::variant<T, Ts...>& v) {
    std::visit([&os] (auto& arg) { fmt::print(os, "{}", arg); }, v);
    return os;
}

} // namespace std

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
// The interface also requires maintenance of snapshots. We use the
// `snapshots_t` introduced above; `impure_state_machine` keeps a reference to `snapshots_t`
// because it will share it with an implementation of `raft::persistence`.
template <PureStateMachine M>
class impure_state_machine : public raft::state_machine {
    raft::server_id _id;

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
    impure_state_machine(raft::server_id id, snapshots_t<typename M::state_t>& snapshots)
        : _id(id), _val(M::init), _snapshots(snapshots) {}

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
        SCYLLA_ASSERT(_snapshots.emplace(id, _val).second);
        tlogger.trace("{}: took snapshot id {} val {}", _id, id, _val);
        co_return id;
    }

    void drop_snapshot(raft::snapshot_id id) override {
        _snapshots.erase(id);
    }

    future<> load_snapshot(raft::snapshot_id id) override {
        auto it = _snapshots.find(id);
        SCYLLA_ASSERT(it != _snapshots.end()); // dunno if the snapshot can actually be missing
        tlogger.trace("{}: loading snapshot id {} prev val {} new val {}", _id, id, _val, it->second);
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
            SCYLLA_ASSERT(_output_channels.emplace(cmd_id, std::move(p)).second);

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

    const typename M::state_t& state() const {
        return _val;
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
using call_result_t = std::variant<typename M::output_t, timed_out_error, raft::not_a_leader, raft::dropped_entry, raft::commit_status_unknown, raft::stopped_error, raft::not_a_member>;

// Wait for a future `f` to finish, but keep the result inside a `future`.
// Works for `future<void>` as well as for `future<T>`.
template <Future F>
future<F> wait(F f) {
    // FIXME: using lambda as workaround for clang bug #50345
    auto impl = [] (F f) -> future<F> {
        struct container { F f; };
        container c = co_await f.then_wrapped([] (F f) { return container{std::move(f)}; });
        SCYLLA_ASSERT(c.f.available());
        co_return std::move(c.f);
    };

    return impl(std::move(f));
}

template <std::invocable<abort_source&> F>
static futurize_t<std::invoke_result_t<F, abort_source&>>
with_timeout(logical_timer& t, raft::logical_clock::time_point tp, F&& fun) {
    using future_t = futurize_t<std::invoke_result_t<F, abort_source&>>;

    // FIXME: using lambda as workaround for clang bug #50345
    auto impl = [] (logical_timer& t, raft::logical_clock::time_point tp, F&& fun) -> future_t {
        abort_source timeout_as;

        // Using lambda here as workaround for seastar#1005
        future_t f = futurize_invoke([fun = std::move(fun)] (abort_source& as) mutable { return std::forward<F>(fun)(as); }, timeout_as);

        auto sleep_and_abort = [] (raft::logical_clock::time_point tp, abort_source& timeout_as, logical_timer& t) -> future<> {
            co_await t.sleep_until(tp, timeout_as);
            if (!timeout_as.abort_requested()) {
                // We resolved before `f`. Abort the operation.
                timeout_as.request_abort();
            }
        }(tp, timeout_as, t);

        f = co_await wait(std::move(f));

        if (!timeout_as.abort_requested()) {
            // `f` has already resolved, but abort the sleep.
            timeout_as.request_abort();
        }

        // Wait on the sleep as well (it should return shortly, being aborted) so we don't discard the future.
        try {
            co_await std::move(sleep_and_abort);
        } catch (const sleep_aborted&) {
            // Expected (if `f` resolved first or we were externally aborted).
        } catch (...) {
            // There should be no other exceptions, but just in case... log it and discard,
            // we want to propagate exceptions from `f`, not from sleep.
            tlogger.error("unexpected exception from sleep_and_abort", std::current_exception());
        }

        // The future is available but cannot use `f.get()` as it doesn't handle void futures.
        co_return co_await std::move(f);
    };

    return impl(t, tp, std::forward<F>(fun));
}

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
    using input_t = typename M::input_t;
    using output_t = typename M::output_t;

    return sm.with_output_channel([&, input = std::move(input), timeout] (cmd_id_t cmd_id, future<output_t> f) {
        return with_timeout(timer, timeout, std::bind_front([&] (input_t input, future<output_t> f, abort_source& as) {
            return server.add_entry(
                    make_command(std::move(cmd_id), std::move(input)),
                    raft::wait_type::applied,
                    &as
            ).then_wrapped([output_f = std::move(f)] (future<> add_entry_f) mutable {
                if (add_entry_f.failed()) {
                    // We need to discard `output_f`; the only expected exception is:
                    (void)output_f.discard_result().handle_exception_type([] (const output_channel_dropped&) {});
                    std::rethrow_exception(add_entry_f.get_exception());
                }

                return std::move(output_f);
            });
        }, std::move(input), std::move(f)));
    }).then([] (output_t output) {
        return make_ready_future<call_result_t<M>>(std::move(output));
    }).handle_exception([] (std::exception_ptr eptr) {
        try {
            std::rethrow_exception(eptr);
        } catch (raft::not_a_leader& e) {
            return make_ready_future<call_result_t<M>>(e);
        } catch (raft::not_a_member& e) {
            return make_ready_future<call_result_t<M>>(e);
        } catch (raft::dropped_entry& e) {
            return make_ready_future<call_result_t<M>>(e);
        } catch (raft::commit_status_unknown& e) {
            return make_ready_future<call_result_t<M>>(e);
        } catch (raft::stopped_error& e) {
            return make_ready_future<call_result_t<M>>(e);
        } catch (raft::request_aborted&) {
            return make_ready_future<call_result_t<M>>(timed_out_error{});
        } catch (seastar::timed_out_error& e) {
            return make_ready_future<call_result_t<M>>(e);
        } catch (broken_promise&) {
            // FIXME: workaround for #9688
            return make_ready_future<call_result_t<M>>(raft::stopped_error{});
        } catch (...) {
            tlogger.error("unexpected exception from call: {}", std::current_exception());
            SCYLLA_ASSERT(false);
        }
    });
}

template <PureStateMachine M>
using read_result_t = std::variant<typename M::state_t, timed_out_error, raft::stopped_error>;

// Performs a linearizable read by calling a `read_barrier` and then reading the local state of the server's state machine.
// Only to be used in forwarding mode.
// See `call` for the meanings of `timeout`, `timer`, `server` and `sm`.
template <PureStateMachine M>
future<read_result_t<M>> read(
        raft::logical_clock::time_point timeout,
        logical_timer& timer,
        raft::server& server,
        impure_state_machine<M>& sm) {
    // FIXME: using lambda as workaround for clang bug #50345.
    auto impl = [] (raft::logical_clock::time_point timeout, logical_timer& timer,
                    raft::server& server, impure_state_machine<M>& sm) -> future<read_result_t<M>> {
        try {
            co_await with_timeout(timer, timeout, [&] (abort_source& as) {
                return server.read_barrier(&as);
            });

            co_return sm.state();
        } catch (raft::stopped_error e) {
            co_return e;
        } catch (seastar::timed_out_error e) {
            co_return e;
        } catch (raft::request_aborted&) {
            co_return timed_out_error{};
        } catch (...) {
            tlogger.error("unexpected exception from `read`: {}", std::current_exception());
            SCYLLA_ASSERT(false);
        }
    };

    return impl(timeout, timer, server, sm);
}

// Allows a Raft server to communicate with other servers.
// The implementation is mostly boilerplate. It assumes that there exists a method of message passing
// given by a `send_message_t` function (passed in the constructor) for sending and by the `receive`
// function for receiving messages.
//
// We also keep a reference to a `snapshots_t` set to be shared with the `impure_state_machine`
// on the same server. We access this set when we receive or send a snapshot message.
//
// The `on_server_update` function passed into the constructor is called when servers
// are added or removed when our cluster configuration changes.
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

    struct add_entry_message {
        raft::command cmd;
        reply_id_t reply_id;
    };

    struct add_entry_reply_message {
        raft::add_entry_reply reply;
        reply_id_t reply_id;
    };

    struct modify_config_message {
        std::vector<raft::config_member> add;
        std::vector<raft::server_id> del;
        reply_id_t reply_id;
    };

    struct ping_message {
        reply_id_t reply_id;
    };

    struct ping_reply {
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
        execute_barrier_on_leader_reply,
        add_entry_message,
        add_entry_reply_message,
        modify_config_message,
        ping_message,
        ping_reply
        >;

    using send_message_t = std::function<void(raft::server_id dst, message_t)>;
    using on_server_update_t = std::function<void(raft::server_id, bool)>;

private:
    raft::server_id _id;

    snapshots_t<State>& _snapshots;

    logical_timer _timer;

    send_message_t _send;
    on_server_update_t _on_server_update;

    // Before we send a snapshot apply request we create a promise-future pair,
    // allocate a new ID, and put the promise here under that ID. We then send the ID
    // together with the request and wait on the future.
    // When (if) a reply returns, we take the ID from the reply (which is the same
    // as the ID in the corresponding request), take the promise under that ID
    // and push the reply through that promise.
    using reply_promise = std::variant<
        promise<raft::snapshot_reply>,
        promise<raft::read_barrier_reply>,
        promise<raft::add_entry_reply>,
        promise<>
    >;
    std::unordered_map<reply_id_t, reply_promise> _reply_promises;

    // Used to ensure that when `abort()` returns there are
    // no more in-progress methods running on this object.
    seastar::gate _gate;

    size_t _snapshot_applications = 0;
    size_t _read_barrier_executions = 0;
    size_t _add_entry_executions = 0;
    size_t _modify_config_executions = 0;

    template <typename F>
    auto with_gate(F&& f) -> decltype(f()) {
        return seastar::with_gate(_gate, std::forward<F>(f))
            .handle_exception_type([] (const gate_closed_exception&) -> decltype(f()) {
                throw raft::stopped_error{};
            });
    }

    static reply_id_t new_reply_id() {
        static size_t counter = 0;
        return counter++;
    }

public:
    rpc(raft::server_id id, snapshots_t<State>& snaps, send_message_t send, on_server_update_t on_server_update)
        : _id(id), _snapshots(snaps), _send(std::move(send)), _on_server_update(std::move(on_server_update)) {
    }

    // Message is delivered to us.
    // The caller must ensure that `abort()` wasn't called yet.
    void receive(raft::server_id src, message_t payload) {
        SCYLLA_ASSERT(!_gate.is_closed());
        SCYLLA_ASSERT(_client);
        auto& c = *_client;

        std::visit(make_visitor(
        [&] (snapshot_message m) {
            static const size_t max_concurrent_snapshot_applications = 5; // TODO: configurable
            if (_snapshot_applications >= max_concurrent_snapshot_applications) {
                tlogger.warn(
                    "{}: cannot apply snapshot from {} (id: {}) due to too many concurrent requests, dropping it",
                    _id, src, m.ins.snp.id);
                // Should we send some message back instead?
                return;
            }

            ++_snapshot_applications;
            (void)[] (rpc& self, raft::server_id src, snapshot_message m, gate::holder holder) -> future<> {
                try {
                    self._snapshots.emplace(m.ins.snp.id, std::move(m.snapshot_payload));
                    auto reply = co_await self._client->apply_snapshot(src, std::move(m.ins));

                    self._send(src, snapshot_reply_message{
                        .reply = std::move(reply),
                        .reply_id = m.reply_id
                    });
                } catch (...) {
                    tlogger.warn("{}: exception when applying snapshot from {}: {}", self._id, src, std::current_exception());
                }

                --self._snapshot_applications;
            }(*this, src, std::move(m), _gate.hold());
        },
        [this] (snapshot_reply_message m) {
            auto it = _reply_promises.find(m.reply_id);
            if (it != _reply_promises.end()) {
                std::get<promise<raft::snapshot_reply>>(it->second).set_value(std::move(m.reply));
            }
        },
        [&] (raft::append_request m) {
            c.append_entries(src, std::move(m));
        },
        [&] (raft::append_reply m) {
            c.append_entries_reply(src, std::move(m));
        },
        [&] (raft::vote_request m) {
            c.request_vote(src, std::move(m));
        },
        [&] (raft::vote_reply m) {
            c.request_vote_reply(src, std::move(m));
        },
        [&] (raft::timeout_now m) {
            c.timeout_now_request(src, std::move(m));
        },
        [&] (raft::read_quorum m) {
            c.read_quorum_request(src, std::move(m));
        },
        [&] (raft::read_quorum_reply m) {
            c.read_quorum_reply(src, std::move(m));
        },
        [&] (execute_barrier_on_leader m) {
            static const size_t max_concurrent_read_barrier_executions = 100; // TODO: configurable
            if (_read_barrier_executions >= max_concurrent_read_barrier_executions) {
                tlogger.warn(
                    "{}: cannot execute read barrier for {} due to too many concurrent requests, dropping it",
                    _id, src);
                // Should we send some message back instead?
                return;
            }

            ++_read_barrier_executions;
            (void)[] (rpc& self, raft::server_id src, execute_barrier_on_leader m, gate::holder holder) -> future<> {
                try {
                    auto reply = co_await self._client->execute_read_barrier(src, nullptr);

                    self._send(src, execute_barrier_on_leader_reply{
                        .reply = std::move(reply),
                        .reply_id = m.reply_id
                    });
                } catch (...) {
                    tlogger.warn("{}: exception when executing read barrier for {}: {}", self._id, src, std::current_exception());
                }

                --self._read_barrier_executions;
            }(*this, src, std::move(m), _gate.hold());
        },
        [this] (execute_barrier_on_leader_reply m) {
            auto it = _reply_promises.find(m.reply_id);
            if (it != _reply_promises.end()) {
                std::get<promise<raft::read_barrier_reply>>(it->second).set_value(std::move(m.reply));
            }
        },
        [&] (add_entry_message m) {
            static const size_t max_concurrent_add_entry_executions = 100; // TODO: configurable
            if (_add_entry_executions >= max_concurrent_add_entry_executions) {
                tlogger.warn(
                    "{}: cannot execute add_entry for {} due to too many concurrent requests, dropping it",
                    _id, src);
                // Should we send some message back instead?
                return;
            }

            ++_add_entry_executions;
            (void)[] (rpc& self, raft::server_id src, add_entry_message m, gate::holder holder) -> future<> {
                try {
                    auto reply = co_await self._client->execute_add_entry(src, std::move(m.cmd), nullptr);

                    self._send(src, add_entry_reply_message{
                        .reply = std::move(reply),
                        .reply_id = m.reply_id
                    });
                } catch (...) {
                    tlogger.warn("{}: exception when executing add_entry for {}: {}", self._id, src, std::current_exception());
                }

                --self._add_entry_executions;
            }(*this, src, std::move(m), _gate.hold());
        },
        [this] (add_entry_reply_message m) {
            auto it = _reply_promises.find(m.reply_id);
            if (it != _reply_promises.end()) {
                std::get<promise<raft::add_entry_reply>>(it->second).set_value(std::move(m.reply));
            }
        },
        [&] (modify_config_message m) {
            static const size_t max_concurrent_modify_config_executions = 100; // TODO: configurable
            if (_modify_config_executions >= max_concurrent_modify_config_executions) {
                tlogger.warn(
                    "{}: cannot execute modify_config for {} due to too many concurrent requests, dropping it",
                    _id, src);
                // Should we send some message back instead?
                return;
            }

            ++_modify_config_executions;
            (void)[] (rpc& self, raft::server_id src, modify_config_message m, gate::holder holder) -> future<> {
                try {
                    auto reply = co_await self._client->execute_modify_config(src, std::move(m.add), std::move(m.del), nullptr);

                    self._send(src, add_entry_reply_message{
                        .reply = std::move(reply),
                        .reply_id = m.reply_id
                    });
                } catch (...) {
                    tlogger.warn("{}: exception when executing modify_config for {}: {}", self._id, src, std::current_exception());
                }

                --self._modify_config_executions;
            }(*this, src, std::move(m), _gate.hold());
        },
        [&] (ping_message m) {
            _send(src, ping_reply {
                .reply_id = m.reply_id
            });
        },
        [this] (ping_reply m) {
            auto it = _reply_promises.find(m.reply_id);
            if (it != _reply_promises.end()) {
                std::get<promise<>>(it->second).set_value();
                _reply_promises.erase(it);
            }
        }
        ), std::move(payload));
    }

    struct snapshot_not_found {
        raft::snapshot_id id;
    };

    virtual future<raft::snapshot_reply> send_snapshot(raft::server_id dst, const raft::install_snapshot& ins, seastar::abort_source&) override {
        co_return co_await with_gate([&] () -> future<raft::snapshot_reply> {
            auto it = _snapshots.find(ins.snp.id);
            if (it == _snapshots.end()) {
                throw snapshot_not_found{ .id = ins.snp.id };
            }

            auto id = new_reply_id();
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

    virtual future<raft::add_entry_reply> send_add_entry(raft::server_id dst, const raft::command& cmd) override {
        co_return co_await with_gate([&] () -> future<raft::add_entry_reply> {
            auto id = new_reply_id();
            promise<raft::add_entry_reply> p;
            auto f = p.get_future();
            _reply_promises.emplace(id, std::move(p));
            auto guard = defer([this, id] { _reply_promises.erase(id); });

            _send(dst, add_entry_message{
                .cmd = cmd,
                .reply_id = id
            });

            static const raft::logical_clock::duration send_add_entry_timeout = 20_t;

            try {
                co_return co_await _timer.with_timeout(_timer.now() + send_add_entry_timeout, std::move(f));
            } catch (logical_timer::timed_out<raft::add_entry_reply>& e) {
                (void) e.get_future().discard_result().handle_exception_type([] (const broken_promise&) { });
                throw timed_out_error{};
            }
        });
    }
    virtual future<raft::add_entry_reply> send_modify_config(raft::server_id dst,
                const std::vector<raft::config_member>& add,
                const std::vector<raft::server_id>& del) override {
        co_return co_await with_gate([&] () -> future<raft::add_entry_reply> {
            auto id = new_reply_id();
            promise<raft::add_entry_reply> p;
            auto f = p.get_future();
            _reply_promises.emplace(id, std::move(p));
            auto guard = defer([this, id] { _reply_promises.erase(id); });

            _send(dst, modify_config_message{
                .add = add,
                .del = del,
                .reply_id = id
            });

            static const raft::logical_clock::duration send_modify_config_timeout = 200_t;

            try {
                co_return co_await _timer.with_timeout(_timer.now() + send_modify_config_timeout, std::move(f));
            } catch (logical_timer::timed_out<raft::add_entry_reply>& e) {
                (void) e.get_future().discard_result().handle_exception_type([] (const broken_promise&) { });
                throw timed_out_error{};
            }
        });
    }
    virtual future<raft::read_barrier_reply> execute_read_barrier_on_leader(raft::server_id dst) override {
        co_return co_await with_gate([&] () -> future<raft::read_barrier_reply> {
            auto id = new_reply_id();
            promise<raft::read_barrier_reply> p;
            auto f = p.get_future();
            _reply_promises.emplace(id, std::move(p));
            auto guard = defer([this, id] { _reply_promises.erase(id); });

            _send(dst, execute_barrier_on_leader {
                .reply_id = id
            });

            // TODO configurable
            static const raft::logical_clock::duration execute_read_barrier_on_leader_timeout = 20_t;

            // TODO: catch aborts from the abort_source as well
            try {
                co_return co_await _timer.with_timeout(_timer.now() + execute_read_barrier_on_leader_timeout, std::move(f));
            } catch (logical_timer::timed_out<raft::read_barrier_reply>& e) {
                (void) e.get_future().discard_result().handle_exception_type([] (const broken_promise&) { });
                throw timed_out_error{};
            }
            // co_await ensures that `guard` is destroyed before we leave `_gate`
        });
    }

    future<> ping(raft::server_id dst, abort_source& as) {
        co_await with_gate([&] () -> future<> {
            auto id = new_reply_id();
            promise<> p;
            auto f = p.get_future();
            _reply_promises.emplace(id, std::move(p));
            auto guard = defer([this, id] { _reply_promises.erase(id); });
            auto sub = as.subscribe([this, id] () noexcept {
                auto it = _reply_promises.find(id);
                if (it == _reply_promises.end()) {
                    // We already had a response when the abort got called.
                    return;
                }

                std::get<promise<>>(it->second).set_exception(std::make_exception_ptr(abort_requested_exception{}));
                // Erase the promise immediately so ping_reply doesn't try to set it.
                _reply_promises.erase(it);
            });

            if (!sub) {
                // Destroy the future before the promise to prevent 'exceptional future ignored'
                auto _ = std::move(f);

                throw abort_requested_exception{};
            }

            _send(dst, ping_message {
                .reply_id = id
            });

            co_await std::move(f);
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

    virtual void on_configuration_change(raft::server_address_set add,
        raft::server_address_set del) override {
        for (const auto& addr: add) {
            _on_server_update(addr.id, true);
        }
        for (const auto& addr: del) {
            _on_server_update(addr.id, false);
        }
    }

    virtual future<> abort() override {
        return _gate.close();
    }

    void tick() {
        _timer.tick();
    }
};

template <typename State>
class persistence {
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
        return b + std::min(size_t((idx - (*b)->idx).value()), _stored_entries.size());
    }

public:
    // If this is the first server of a cluster, it must be initialized with a singleton configuration
    // containing opnly this server's ID which must be also provided here as `init_config_id`.
    // Otherwise it must be initialized with an empty configuration (it will be added to the cluster
    // through a configuration change) and `init_config_id` must be `nullopt`.
    persistence(std::optional<raft::server_id> init_config_id, State init_state)
        : _stored_snapshot(
                raft::snapshot_descriptor{
                    .config = init_config_id ? config_from_ids({*init_config_id}) : raft::configuration{}
                },
                std::move(init_state))
        , _stored_term_and_vote(raft::term_t{1}, raft::server_id{})
    {}

    void store_term_and_vote(raft::term_t term, raft::server_id vote) {
        _stored_term_and_vote = std::pair{term, vote};
    }

    std::pair<raft::term_t, raft::server_id> load_term_and_vote() {
        return _stored_term_and_vote;
    }

    void store_snapshot(const raft::snapshot_descriptor& snap, State snap_data, size_t preserve_log_entries) {
        // The snapshot's index cannot be smaller than the index of the first stored entry minus one;
        // that would create a ``gap'' in the log.
        SCYLLA_ASSERT(_stored_entries.empty() || snap.idx + raft::index_t{1} >= _stored_entries.front()->idx);

        _stored_snapshot = {snap, std::move(snap_data)};

        if (!_stored_entries.empty() && snap.idx > _stored_entries.back()->idx) {
            // Clear the log in order to not create a gap.
            _stored_entries.clear();
            return;
        }

        raft::index_t first_to_remain = snap.idx + raft::index_t{1};
        if (first_to_remain.value() >= preserve_log_entries) {
            first_to_remain -= raft::index_t{preserve_log_entries};
        } else {
            first_to_remain = raft::index_t{0};
        }
        _stored_entries.erase(_stored_entries.begin(), find(first_to_remain));
    }

    std::pair<raft::snapshot_descriptor, State> load_snapshot() {
        return _stored_snapshot;
    }

    void store_log_entries(const std::vector<raft::log_entry_ptr>& entries) {
        if (entries.empty()) {
            return;
        }

        // The raft server is supposed to provide entries in strictly increasing order,
        // hence the following assertions.
        if (_stored_entries.empty()) {
            SCYLLA_ASSERT(entries.front()->idx == _stored_snapshot.first.idx + raft::index_t{1});
        } else {
            SCYLLA_ASSERT(entries.front()->idx == _stored_entries.back()->idx + raft::index_t{1});
        }

        _stored_entries.push_back(entries[0]);
        for (size_t i = 1; i < entries.size(); ++i) {
            SCYLLA_ASSERT(entries[i]->idx == entries[i-1]->idx + raft::index_t{1});
            _stored_entries.push_back(entries[i]);
        }
    }

    raft::log_entries load_log() {
        return _stored_entries;
    }

    void truncate_log(raft::index_t idx) {
        _stored_entries.erase(find(idx), _stored_entries.end());
    }
};

template <typename State>
class persistence_proxy : public raft::persistence {
    snapshots_t<State>& _snapshots;
    lw_shared_ptr<::persistence<State>> _persistence;

public:
    persistence_proxy(snapshots_t<State>& snaps, lw_shared_ptr<::persistence<State>> persistence)
        : _snapshots(snaps)
        , _persistence(std::move(persistence))
    {}

    virtual future<> store_term_and_vote(raft::term_t term, raft::server_id vote) override {
        _persistence->store_term_and_vote(term, vote);
        co_return;
    }

    virtual future<std::pair<raft::term_t, raft::server_id>> load_term_and_vote() override {
        co_return _persistence->load_term_and_vote();
    }

    virtual future<> store_commit_idx(raft::index_t) override {
        co_return;
    }

    virtual future<raft::index_t> load_commit_idx() override {
        co_return raft::index_t{0};
    }

    // Stores not only the snapshot descriptor but also the corresponding snapshot.
    virtual future<> store_snapshot_descriptor(const raft::snapshot_descriptor& snap, size_t preserve_log_entries) override {
        auto it = _snapshots.find(snap.id);
        SCYLLA_ASSERT(it != _snapshots.end());

        _persistence->store_snapshot(snap, it->second, preserve_log_entries);
        co_return;
    }

    // Loads not only the snapshot descriptor but also the corresponding snapshot.
    virtual future<raft::snapshot_descriptor> load_snapshot_descriptor() override {
        auto [snap, state] = _persistence->load_snapshot();
        _snapshots.insert_or_assign(snap.id, std::move(state));
        co_return snap;
    }

    virtual future<> store_log_entries(const std::vector<raft::log_entry_ptr>& entries) override {
        _persistence->store_log_entries(entries);
        co_return;
    }

    virtual future<raft::log_entries> load_log() override {
        co_return _persistence->load_log();
    }

    virtual future<> truncate_log(raft::index_t idx) override {
        _persistence->truncate_log(idx);
        co_return;
    }

    virtual future<> abort() override {
        // There are no yields anywhere in our methods so no need to wait for anything.
        // We assume that our methods won't be called after `abort()`.
        // TODO: is this assumption correct?
        co_return;
    }
};

template <typename State>
class direct_fd_pinger final : public direct_failure_detector::pinger {
    ::rpc<State>& _rpc;

public:
    direct_fd_pinger(::rpc<State>& rpc)
            : _rpc(rpc) {
        SCYLLA_ASSERT(this_shard_id() == 0);
    }

    // Can be called on any shard.
    future<bool> ping(direct_failure_detector::pinger::endpoint_id id, abort_source& as) override {
        try {
            co_await invoke_abortable_on(0, [this, id] (abort_source& as) {
                return _rpc.ping(raft::server_id{id}, as);
            }, as);
        } catch (raft::stopped_error&) {
            co_return false;
        }
        co_return true;
    }
};

class direct_fd_clock final : public direct_failure_detector::clock {
    // We use `logical_timer` for an implementation of `sleep_until`
    // (for simplicity of implementation we route the sleep to shard 0),
    // but we also need a separate atomic _ticks counter because we need a `now` function callable from every shard.
    // The timer is ticked in synchrony with _ticks.
    logical_timer _timer;
    std::atomic<int64_t> _ticks{0};

public:
    direct_fd_clock() {
        SCYLLA_ASSERT(this_shard_id() == 0);
    }

    void tick() {
        _timer.tick();
        ++_ticks;
    }

    direct_failure_detector::clock::timepoint_t now() noexcept override {
        return _ticks;
    }

    future<> sleep_until(direct_failure_detector::clock::timepoint_t tp, abort_source& as) override {
        try {
            co_await invoke_abortable_on(0, [this, tp] (abort_source& as) {
                auto start = now();
                if (tp <= start) {
                    return make_ready_future<>();
                }

                // Translate direct_failure_detector timepoint `tp` to a `logical_timer` timepoint,
                // using the fact that they are ticked in synchrony.
                auto diff = tp - start;
                auto timer_start = _timer.now();
                auto timer_tp = timer_start + raft::logical_clock::duration{diff};

                return _timer.sleep_until(timer_tp, as);

                // When this sleep finishes, we know that timer_tp <= _timer.now().
                // Thus timer_tp - timer_start <= _timer.now() - timer_start.
                // _timer.now() - timer_start == now() - start (because _ticks is incremented in synchrony with _timer.tick()).
                // Thus timer_tp - timer_start <= now() - start.
                // But timer_tp = timer_start + (tp - start), so timer_tp - timer_start = tp - start,
                // hence tp - start <= now() - start,
                // hence tp <= now().
            }, as);
        } catch (abort_requested_exception&) {
            throw sleep_aborted{};
        }
    }
};

class direct_fd_listener : public raft::failure_detector, public direct_failure_detector::listener {
    raft::server_id _id;

    std::unordered_set<raft::server_id> _alive_set;

public:
    direct_fd_listener(raft::server_id id)
            : _id(id) {
    }

    future<> mark_alive(direct_failure_detector::pinger::endpoint_id ep) override {
        auto id = raft::server_id{ep};
        tlogger.trace("failure detector ({}): mark {} alive", _id, id);
        _alive_set.insert(id);
        return make_ready_future<>();
    }

    future<> mark_dead(direct_failure_detector::pinger::endpoint_id ep) override {
        auto id = raft::server_id{ep};
        tlogger.trace("failure detector ({}): mark {} dead", _id, id);
        _alive_set.erase(id);
        return make_ready_future<>();
    }

    bool is_alive(raft::server_id id) override {
        return _alive_set.contains(id);
    }
};

// `network` is a simple priority queue of `event`s, where an `event` is a message associated
// with its planned delivery time. The queue uses a logical clock to decide when to deliver messages.
// It delives all messages whose associated times are smaller than the ``current time'', the latter
// determined by the number of `tick()` calls.
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

    // A min-heap of event occurrences compared by their time points.
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
    std::uniform_int_distribution<raft::logical_clock::rep> _delivery_delay;
    std::mt19937 _rnd;

public:
    network(std::uniform_int_distribution<raft::logical_clock::rep> delivery_delay, std::mt19937 rnd, deliver_t f)
        : _deliver(std::move(f)), _delivery_delay(std::move(delivery_delay)), _rnd(std::move(rnd)) {}

    void send(raft::server_id src, raft::server_id dst, Payload payload) {
        // Predict the delivery time in advance.
        // Our prediction may be wrong if a grudge exists at this expected moment of delivery.
        // Messages may also be reordered.
        auto delivery_time = _clock.now() + raft::logical_clock::duration{_delivery_delay(_rnd)};

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

using reconfigure_result_t = std::variant<std::monostate,
    timed_out_error, raft::not_a_leader, raft::dropped_entry, raft::commit_status_unknown, raft::conf_change_in_progress, raft::stopped_error, raft::not_a_member>;

future<reconfigure_result_t> reconfigure(
        const std::vector<std::pair<raft::server_id, bool>>& ids,
        raft::logical_clock::time_point timeout,
        logical_timer& timer,
        raft::server& server) {
    raft::config_member_set config;
    for (auto [id, can_vote] : ids) {
        config.insert(raft::config_member{server_addr_from_id(id), can_vote});
    }

    try {
        co_await with_timeout(timer, timeout, [&server, config = std::move(config)] (abort_source& as) {
            return server.set_configuration(std::move(config), &as);
        });
        co_return std::monostate{};
    } catch (raft::not_a_leader e) {
        co_return e;
    } catch (raft::dropped_entry e) {
        co_return e;
    } catch (raft::commit_status_unknown e) {
        co_return e;
    } catch (raft::conf_change_in_progress e) {
        co_return e;
    } catch (broken_promise&) {
        // FIXME: workaround for #9688
        co_return raft::stopped_error{};
    } catch (raft::stopped_error e) {
        co_return e;
    } catch (raft::request_aborted&) {
        co_return timed_out_error{};
    } catch (...) {
        tlogger.error("unexpected exception from set_configuration: {}", std::current_exception());
        SCYLLA_ASSERT(false);
    }
}

future<reconfigure_result_t> modify_config(
        const std::vector<std::pair<raft::server_id, bool>>& added,
        std::vector<raft::server_id> deleted,
        raft::logical_clock::time_point timeout,
        logical_timer& timer,
        raft::server& server) {
    std::vector<raft::config_member> added_set;
    for (auto [id, can_vote] : added) {
        added_set.push_back(raft::config_member{server_addr_from_id(id), can_vote});
    }

    try {
        co_await with_timeout(timer, timeout, [&server, added_set = std::move(added_set), deleted = std::move(deleted)] (abort_source& as) mutable {
            return server.modify_config(std::move(added_set), std::move(deleted), &as);
        });
        co_return std::monostate{};
    } catch (raft::not_a_leader e) {
        co_return e;
    } catch (raft::not_a_member e) {
        co_return e;
    } catch (raft::dropped_entry e) {
        co_return e;
    } catch (raft::commit_status_unknown e) {
        co_return e;
    } catch (raft::conf_change_in_progress e) {
        co_return e;
    } catch (raft::stopped_error e) {
        co_return e;
    } catch (raft::request_aborted&) {
        co_return timed_out_error{};
    } catch (seastar::timed_out_error e) {
        co_return e;
    } catch (...) {
        tlogger.error("unexpected exception from modify_config: {}", std::current_exception());
        SCYLLA_ASSERT(false);
    }
}

// Contains a `raft::server` and other facilities needed for it and the underlying
// modules (persistence, rpc, etc.) to run, and to communicate with the external environment.
template <PureStateMachine M>
class raft_server {
    raft::server_id _id;

    std::unique_ptr<snapshots_t<typename M::state_t>> _snapshots;
    std::unique_ptr<raft::server> _server;

    // _sm and _rpc are owned by _server:
    impure_state_machine<M>& _sm;
    rpc<typename M::state_t>& _rpc;

    std::unique_ptr<sharded<direct_failure_detector::failure_detector>> _fd_service;
    std::unique_ptr<direct_fd_pinger<typename M::state_t>> _fd_pinger;
    std::unique_ptr<direct_fd_clock> _fd_clock;
    shared_ptr<direct_fd_listener> _fd_listener;

    raft::logical_clock::duration _fd_convict_threshold;

    bool _started = false;
    bool _stopped = false;

    // Used to ensure that when `abort()` returns there are
    // no more in-progress methods running on this object.
    seastar::gate _gate;

    std::optional<direct_failure_detector::subscription> _fd_subscription;

public:
    // Create a `raft::server` with the given `id` and all other facilities required
    // by the server (the state machine, RPC instance and so on). The server will use
    // `send_rpc` to send RPC messages to other servers and `fd` for failure detection.
    //
    // The server is started with `persistence` as its underlying persistent storage.
    // This can be used to simulate a server that is restarting by giving it a `persistence`
    // that was previously used by a different instance of `raft_server<M>` (but make sure
    // they had the same `id` and that the previous instance is no longer using this
    // `persistence`).
    //
    // The created server is not started yet; use `start` for that.
    static std::unique_ptr<raft_server> create(
            raft::server_id id,
            lw_shared_ptr<persistence<typename M::state_t>> persistence,
            raft::logical_clock::duration fd_convict_threshold,
            raft::server::configuration cfg,
            typename rpc<typename M::state_t>::send_message_t send_rpc) {
        using state_t = typename M::state_t;

        auto fd_service = std::make_unique<sharded<direct_failure_detector::failure_detector>>();
        auto update_fd_server = [&fd = *fd_service] (raft::server_id id, bool added) {
            if (!fd.local_is_initialized()) {
                // We're stopping.
                return;
            }

            auto ep = id.uuid();
            if (added) {
                fd.local().add_endpoint(ep);
            } else {
                fd.local().remove_endpoint(ep);
            }
        };

        auto snapshots = std::make_unique<snapshots_t<state_t>>();
        auto sm = std::make_unique<impure_state_machine<M>>(id, *snapshots);
        auto rpc_ = std::make_unique<rpc<state_t>>(id, *snapshots, std::move(send_rpc), std::move(update_fd_server));
        auto persistence_ = std::make_unique<persistence_proxy<state_t>>(*snapshots, std::move(persistence));

        auto fd_pinger = std::make_unique<direct_fd_pinger<state_t>>(*rpc_);
        auto fd_clock = std::make_unique<direct_fd_clock>();
        auto fd_listener = make_shared<direct_fd_listener>(id);

        auto& sm_ref = *sm;
        auto& rpc_ref = *rpc_;

        auto server = raft::create_server(
                id, std::move(rpc_), std::move(sm), std::move(persistence_), fd_listener,
                std::move(cfg));

        return std::make_unique<raft_server>(initializer{
            ._id = id,
            ._snapshots = std::move(snapshots),
            ._server = std::move(server),
            ._sm = sm_ref,
            ._rpc = rpc_ref,
            ._fd_service = std::move(fd_service),
            ._fd_pinger = std::move(fd_pinger),
            ._fd_clock = std::move(fd_clock),
            ._fd_listener = std::move(fd_listener),
            ._fd_convict_threshold = fd_convict_threshold
        });
    }

    ~raft_server() {
        SCYLLA_ASSERT(!_started || _stopped);
    }

    raft_server(const raft_server&&) = delete;
    raft_server(raft_server&&) = delete;

    // Start the server. Can be called at most once.
    future<> start() {
        // TODO: make it adjustable
        static const raft::logical_clock::duration fd_ping_period = 10_t;
        static const raft::logical_clock::duration fd_ping_timeout = 30_t;

        SCYLLA_ASSERT(!_started);
        _started = true;

        // _fd_service must be started before raft server,
        // because as soon as raft server is started, it may start adding endpoints to the service.
        // _fd_service is using _server's RPC, but not until the first endpoint is added.
        co_await _fd_service->start(std::ref(*_fd_pinger), std::ref(*_fd_clock), fd_ping_period.count(), fd_ping_timeout.count());
        _fd_subscription.emplace(co_await _fd_service->local().register_listener(*_fd_listener, _fd_convict_threshold.count()));
        co_await _server->start();
    }

    // Stop the given server. Must be called before the server is destroyed
    // (unless it was never started in the first place).
    future<> abort() {
        auto f = _gate.close();
        // Abort everything before waiting on the gate close future
        // so currently running operations finish earlier.
        if (_started) {
            // Stop _fd_service before _server because _fd_service is using _server's RPC.
            // _server may try to add/remove endpoints after _fd_service is stopped but it's allowed.
            _fd_subscription = std::nullopt;
            co_await _fd_service->stop();
            co_await _server->abort();

            {
                std::vector<raft::snapshot_id> snapshot_ids;
                snapshot_ids.reserve(_snapshots->size());
                for (const auto& p: *_snapshots) {
                    snapshot_ids.push_back(p.first);
                }
                BOOST_TEST_INFO(format("snapshot ids: [{}]", snapshot_ids));
                BOOST_CHECK_LE(snapshot_ids.size(), 2);
            }
        }
        co_await std::move(f);
        _stopped = true;
    }

    void tick() {
        SCYLLA_ASSERT(_started);
        _rpc.tick();
        _server->tick();
        _fd_clock->tick();
    }

    future<call_result_t<M>> call(
            typename M::input_t input,
            raft::logical_clock::time_point timeout,
            logical_timer& timer) {
        SCYLLA_ASSERT(_started);
        try {
            co_return co_await with_gate(_gate, [this, input = std::move(input), timeout, &timer] {
                return ::call(std::move(input), timeout, timer, *_server, _sm);
            });
        } catch (const gate_closed_exception&) {
            co_return raft::stopped_error{};
        }
    }

    future<read_result_t<M>> read(
            raft::logical_clock::time_point timeout,
            logical_timer& timer) {
        SCYLLA_ASSERT(_started);
        try {
            co_return co_await with_gate(_gate, [this, timeout, &timer] {
                return ::read(timeout, timer, *_server, _sm);
            });
        } catch (const gate_closed_exception&) {
            co_return raft::stopped_error{};
        }
    }

    future<reconfigure_result_t> reconfigure(
            const std::vector<std::pair<raft::server_id, bool>>& ids,
            raft::logical_clock::time_point timeout,
            logical_timer& timer) {
        SCYLLA_ASSERT(_started);
        try {
            co_return co_await with_gate(_gate, [this, &ids, timeout, &timer] {
                return ::reconfigure(ids, timeout, timer, *_server);
            });
        } catch (const gate_closed_exception&) {
            co_return raft::stopped_error{};
        }
    }

    future<reconfigure_result_t> modify_config(
            const std::vector<std::pair<raft::server_id, bool>>& added,
            std::vector<raft::server_id> deleted,
            raft::logical_clock::time_point timeout,
            logical_timer& timer) {
        SCYLLA_ASSERT(_started);
        try {
            co_return co_await with_gate(_gate, [this, &added, deleted = std::move(deleted), timeout, &timer] {
                return ::modify_config(added, std::move(deleted), timeout, timer, *_server);
            });
        } catch (const gate_closed_exception&) {
            co_return raft::stopped_error{};
        }
    }

    bool is_leader() const {
        return _server->is_leader();
    }

    raft::server_id id() const {
        return _id;
    }

    const typename M::state_t& state() const {
        return _sm.state();
    }

    raft::configuration get_configuration() const {
        return _server->get_configuration();
    }

    void deliver(raft::server_id src, const typename rpc<typename M::state_t>::message_t& m) {
        SCYLLA_ASSERT(_started);
        if (!_gate.is_closed()) {
            _rpc.receive(src, m);
        }
    }

    raft::server* get_server() {
        return _server.get();
    }

private:
    struct initializer {
        raft::server_id _id;

        std::unique_ptr<snapshots_t<typename M::state_t>> _snapshots;
        std::unique_ptr<raft::server> _server;

        impure_state_machine<M>& _sm;
        rpc<typename M::state_t>& _rpc;

        std::unique_ptr<sharded<direct_failure_detector::failure_detector>> _fd_service;
        std::unique_ptr<direct_fd_pinger<typename M::state_t>> _fd_pinger;
        std::unique_ptr<direct_fd_clock> _fd_clock;
        shared_ptr<direct_fd_listener> _fd_listener;
        raft::logical_clock::duration _fd_convict_threshold;
    };

    raft_server(initializer i)
        : _id(i._id)
        , _snapshots(std::move(i._snapshots))
        , _server(std::move(i._server))
        , _sm(i._sm)
        , _rpc(i._rpc)
        , _fd_service(std::move(i._fd_service))
        , _fd_pinger(std::move(i._fd_pinger))
        , _fd_clock(std::move(i._fd_clock))
        , _fd_listener(std::move(i._fd_listener))
        , _fd_convict_threshold(i._fd_convict_threshold)
    {}

    friend std::unique_ptr<raft_server> std::make_unique<raft_server, raft_server::initializer>(initializer&&);
};

struct environment_config {
    std::mt19937 rnd;
    std::uniform_int_distribution<raft::logical_clock::rep> network_delay;
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

    // Invariant: if `_server` is engaged then it uses `_persistence` and `_fd`
    // underneath and is initialized using `_cfg`.
    struct route {
        raft::server::configuration _cfg;
        lw_shared_ptr<persistence<state_t>> _persistence;
        std::unique_ptr<raft_server<M>> _server;
    };

    // Passed to newly created failure detectors.
    const raft::logical_clock::duration _fd_convict_threshold;

    // Used to deliver messages coming from the network to appropriate servers and their failure detectors.
    // Also keeps the servers and the failure detectors alive (owns them).
    // Before we show a Raft server to others we must add it to this map.
    std::unordered_map<raft::server_id, route> _routes;

    // Used to create a new ID in `new_server`.
    size_t _next_id = 0;

    using message_t = typename rpc<state_t>::message_t;
    network<message_t> _network;

    bool _stopped = false;

    // Used to ensure that when `abort()` returns there are
    // no more in-progress methods running on this object.
    seastar::gate _gate;

    // Used to implement `crash`.
    //
    // We cannot destroy a server immediately in order to simulate a crash:
    // there may be fibers running that use the server's internals.
    // We move these 'crashed' servers into continuations attached to this fiber
    // and abort them there before destruction.
    future<> _crash_fiber = make_ready_future<>();

    // Servers that are aborting in the background (in `_crash_fiber`).
    // We need these pointers so we keep ticking the servers
    // (in general, `abort()` requires the server to be ticked in order to finish).
    // One downside of this is that ticks may cause the servers to output traces.
    // Hopefully these crashing servers abort quickly so they don't stay too long
    // and make the logs unreadable...
    std::unordered_set<raft_server<M>*> _crashing_servers;

public:
    environment(environment_config cfg)
            : _fd_convict_threshold(cfg.fd_convict_threshold)
            , _network(std::move(cfg.network_delay), std::move(cfg.rnd),
        [this] (raft::server_id src, raft::server_id dst, const message_t& m) {
            auto& n = _routes.at(dst);
            SCYLLA_ASSERT(n._persistence);

            if (n._server) {
                n._server->deliver(src, m);
            }
        }) {
    }

    ~environment() {
        SCYLLA_ASSERT(_routes.empty() || _stopped);
    }

    environment(const environment&) = delete;
    environment(environment&&) = delete;

    void tick_network() {
        _network.tick();
    }

    template <std::invocable<raft::server_id, raft_server<M>*> F>
    void for_each_server(F&& f) {
        for (auto& [id, r]: _routes) {
            f(id, r._server.get());
        }
    }

    // Call this periodically so `abort()` can finish for 'crashed' servers.
    void tick_crashing_servers() {
        for (auto& srv: _crashing_servers) {
            srv->tick();
        }
    }

    void tick_servers() {
        for_each_server([] (raft::server_id, raft_server<M>* srv) {
            if (srv) {
                srv->tick();
            }
        });

        tick_crashing_servers();
    }

    // A 'node' is a container for a Raft server, its storage ('persistence') and failure detector.
    // At a given point in time at most one Raft server instance can be running on a node.
    // Different instances may be running at different points in time, but they will all have
    // the same ID (returned by `new_node`) and will reuse the same storage and failure detector
    // (this can be used to simulate a server that is restarting).
    //
    // The storage is initialized when the node is created and will be used by the first started server.
    // If `first == true` the storage is created with a singleton server configuration containing only
    // the ID returned from the function. Otherwise it is created with an empty configuration
    // (a server started on this node will have to be joined to an existing cluster in this case).
    raft::server_id new_node(bool first, raft::server::configuration cfg) {
        _gate.check();

        auto id = to_raft_id(_next_id++);
        auto [it, inserted] = _routes.emplace(id, route{
            ._cfg = std::move(cfg),
            ._persistence = make_lw_shared<persistence<state_t>>(first ? std::optional{id} : std::nullopt, M::init),
            ._server = nullptr,
        });
        SCYLLA_ASSERT(inserted);

        return id;
    }

    // Starts a server on node `id`.
    // Assumes node with `id` exists (i.e. an earlier `new_node` call returned `id`) and that no server is running on node `id`.
    future<> start_server(raft::server_id id) {
        return with_gate(_gate, [this, id] () -> future<> {
            auto& n = _routes.at(id);
            SCYLLA_ASSERT(n._persistence);
            SCYLLA_ASSERT(!n._server);

            lw_shared_ptr<raft_server<M>*> this_srv_addr = make_lw_shared<raft_server<M>*>(nullptr);
            auto srv = raft_server<M>::create(id, n._persistence, _fd_convict_threshold, n._cfg,
                    [id, this_srv_addr, &n, this] (raft::server_id dst, typename rpc<state_t>::message_t m) {
                // Allow the message out only if we are still the currently running server on this node.
                if (*this_srv_addr == n._server.get()) {
                    _network.send(id, dst, {std::move(m)});
                }
            });
            *this_srv_addr = srv.get();

            co_await srv->start();
            n._server = std::move(srv);
        });
    }

    // Creates a new node, connects it to the network, starts a server on it and returns its ID.
    //
    // If `first == true` the node is created with a singleton configuration containing only its ID.
    // Otherwise it is created with an empty configuration. The user must explicitly ask for a configuration change
    // if they want to make a cluster (group) out of this server and other existing servers.
    // The user should be able to create multiple clusters by calling `new_server` multiple times with `first = true`.
    // (`first` means ``first in group'').
    future<raft::server_id> new_server(bool first, raft::server::configuration cfg = {}) {
        auto id = new_node(first, std::move(cfg));
        // not using co_await here due to miscompile
        return start_server(id).then([id] () { return id; });
    }

    // Gracefully stop a running server.
    // Assumes a server is currently running on the node `id`.
    // When the future resolves, a new server may be started on this node. It will reuse the storage
    // of the previously running server (so the Raft log etc. will be preserved).
    future<> stop(raft::server_id id) {
        return with_gate(_gate, [this, id] () -> future<> {
            auto& n = _routes.at(id);
            SCYLLA_ASSERT(n._persistence);
            SCYLLA_ASSERT(n._server);

            co_await n._server->abort();
            n._server = nullptr;
        });
    }

    // Immediately stop a running server.
    // Assumes a server is currently running on the node `id`.
    // A new server may be started on this node when the function returns. It will reuse the storage
    // of the previously running server (so the Raft log etc. will be preserved).
    void crash(raft::server_id id) {
        _gate.check();

        auto& n = _routes.at(id);
        SCYLLA_ASSERT(n._persistence);
        SCYLLA_ASSERT(n._server);

        // Let the 'crashed' server continue working on its copy of persistence;
        // none of that work will be seen by later servers restarted on this node
        // since they'll use a separate copy.
        n._persistence = make_lw_shared<persistence<state_t>>(*n._persistence);
        // Setting `n._server` to nullptr cuts out the network access both for the server and failure detector.
        // Even though the server will continue running for some time (in order to be gracefully aborted),
        // none of that work will be seen by the rest of the environment. From others' point of view
        // the server is immediately gone.
        auto srv = std::exchange(n._server, nullptr);
        _crashing_servers.insert(srv.get());

        auto f = std::bind_front([] (environment<M>& self, std::unique_ptr<raft_server<M>> srv) -> future<> {
            tlogger.trace("crash fiber: aborting {}", srv->id());
            co_await srv->abort();
            tlogger.trace("crash fiber: finished aborting {}", srv->id());
            self._crashing_servers.erase(srv.get());
            // abort() ensures there are no in-progress calls on the server, so we can destroy it.
        }, std::ref(*this), std::move(srv));

        // Cannot do `.then(std::move(f))`, because that would try to use `f()`, which is ill-formed (seastar#1005).
        _crash_fiber = _crash_fiber.then([f = std::move(f)] () mutable { return std::move(f)(); });
    }

    bool is_leader(raft::server_id id) {
        auto& n = _routes.at(id);
        if (!n._server) {
            return false;
        }
        return n._server->is_leader();
    }

    future<call_result_t<M>> call(
            raft::server_id id,
            typename M::input_t input,
            raft::logical_clock::time_point timeout,
            logical_timer& timer) {
        auto& n = _routes.at(id);
        if (!n._server) {
            // A 'remote' caller doesn't know in general if the server is down or just slow to respond.
            // Simulate this by timing out the call.
            co_await timer.sleep_until(timeout);
            co_return timed_out_error{};
        }

        auto srv = n._server.get();
        auto res = co_await srv->call(std::move(input), timeout, timer);

        if (srv != n._server.get()) {
            // The server stopped while the call was happening.
            // As above, we simulate a 'remote' call by timing it out in this case.
            co_await timer.sleep_until(timeout);
            co_return timed_out_error{};
        }
        co_return res;
    }

    future<read_result_t<M>> read(
            raft::server_id id,
            raft::logical_clock::time_point timeout,
            logical_timer& timer) {
        auto& n = _routes.at(id);
        if (!n._server) {
            // As in `call`.
            co_await timer.sleep_until(timeout);
            co_return timed_out_error{};
        }

        auto srv = n._server.get();
        auto res = co_await srv->read(timeout, timer);

        if (srv != n._server.get()) {
            // As in `call`.
            co_await timer.sleep_until(timeout);
            co_return timed_out_error{};
        }
        co_return res;
    }

    future<reconfigure_result_t> reconfigure(
            raft::server_id id,
            const std::vector<std::pair<raft::server_id, bool>>& ids,
            raft::logical_clock::time_point timeout,
            logical_timer& timer) {
        auto& n = _routes.at(id);
        if (!n._server) {
            // A 'remote' caller doesn't know in general if the server is down or just slow to respond.
            // Simulate this by timing out the call.
            co_await timer.sleep_until(timeout);
            co_return timed_out_error{};
        }

        auto srv = n._server.get();
        auto res = co_await srv->reconfigure(ids, timeout, timer);

        if (srv != n._server.get()) {
            // The server stopped while the call was happening.
            // As above, we simulate a 'remote' call by timing it out in this case.
            co_await timer.sleep_until(timeout);
            co_return timed_out_error{};
        }
        co_return res;
    }

    future<reconfigure_result_t> reconfigure(
            raft::server_id id,
            const std::vector<raft::server_id>& ids,
            raft::logical_clock::time_point timeout,
            logical_timer& timer) {
        std::vector<std::pair<raft::server_id, bool>> ids_voters;
        for (auto srv: ids) {
            ids_voters.emplace_back(srv, true);
        }
        co_return co_await reconfigure(id, ids_voters, timeout, timer);
    }

    future<reconfigure_result_t> modify_config(
            raft::server_id id,
            const std::vector<std::pair<raft::server_id, bool>>& added,
            std::vector<raft::server_id> deleted,
            raft::logical_clock::time_point timeout,
            logical_timer& timer) {
        auto& n = _routes.at(id);
        if (!n._server) {
            // A 'remote' caller doesn't know in general if the server is down or just slow to respond.
            // Simulate this by timing out the call.
            co_await timer.sleep_until(timeout);
            co_return timed_out_error{};
        }

        auto srv = n._server.get();
        auto res = co_await srv->modify_config(added, std::move(deleted), timeout, timer);

        if (srv != n._server.get()) {
            // The server stopped while the call was happening.
            // As above, we simulate a 'remote' call by timing it out in this case.
            co_await timer.sleep_until(timeout);
            co_return timed_out_error{};
        }
        co_return res;
    }

    future<reconfigure_result_t> modify_config(
            raft::server_id id,
            const std::vector<raft::server_id>& added,
            std::vector<raft::server_id> deleted,
            raft::logical_clock::time_point timeout,
            logical_timer& timer) {
        std::vector<std::pair<raft::server_id, bool>> added_voters;
        for (auto srv: added) {
            added_voters.emplace_back(srv, true);
        }
        co_return co_await modify_config(id, added_voters, std::move(deleted), timeout, timer);
    }

    std::optional<raft::configuration> get_configuration(raft::server_id id) {
        auto& n = _routes.at(id);
        if (!n._server) {
            return std::nullopt;
        }
        return n._server->get_configuration();
    }

    network<message_t>& get_network() {
        return _network;
    }

    // Must be called before we are destroyed unless `new_server` was never called.
    future<> abort() {
        // Close the gate before iterating over _routes to prevent concurrent modification by other methods.
        co_await _gate.close();
        for (auto& [_, r] : _routes) {
            if (r._server) {
                co_await r._server->abort();
                r._server = nullptr;
            }
        }
        co_await std::move(_crash_fiber);
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

            // We abort the environment before the ticker as the environment may require time to advance
            // in order to finish (e.g. some operations may need to timeout).
            tlogger.info("aborting environment");
            co_await env->abort();
            tlogger.info("environment aborted, aborting ticker");
            co_await t->abort();
            tlogger.info("ticker aborted");
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

// Wait until either one of `nodes` in `env` becomes a leader, or time point `timeout` is reached according to `timer` (whichever happens first).
// If the leader is found, returns it. Otherwise throws a `logical_timer::timed_out` exception.
//
// Note: the returned node may have been a leader the moment we found it, but may have just stepped down
// the moment we return it. It may be useful to call this function multiple times during cluster
// stabilization periods in order to find a node that will successfully answer calls.
template <PureStateMachine M>
struct wait_for_leader {
    // FIXME: change into free function after clang bug #50345 is fixed
    future<raft::server_id> operator()(
            environment<M>& env,
            std::vector<raft::server_id> nodes,
            logical_timer& timer,
            raft::logical_clock::time_point timeout) {
        auto l = co_await timer.with_timeout(timeout, [] (weak_ptr<environment<M>> env, std::vector<raft::server_id> nodes) -> future<raft::server_id> {
            while (true) {
                if (!env) {
                    co_return raft::server_id{};
                }

                auto it = std::find_if(nodes.begin(), nodes.end(), [&env] (raft::server_id id) { return env->is_leader(id); });
                if (it != nodes.end()) {
                    co_return *it;
                }

                co_await seastar::yield();
            }
        }(env.weak_from_this(), std::move(nodes)));

        SCYLLA_ASSERT(l != raft::server_id{});

        // Note: `l` may no longer be a leader at this point if there was a yield at the `co_await` above
        // and `l` decided to step down, was restarted, or just got removed from the configuration.

        co_return l;
    }
};

future<> ping_shards() {
    if (smp::count == 1) {
        return seastar::yield();
    }

    return parallel_for_each(boost::irange(0u, smp::count), [] (shard_id s) {
        return smp::submit_to(s, [](){});
    });
}

SEASTAR_TEST_CASE(basic_test) {
    logical_timer timer;
    environment_config cfg {
        .rnd{0},
        .network_delay{5, 5},
        .fd_convict_threshold = 50_t,
    };
    co_await with_env_and_ticker<ExReg>(cfg, [&timer] (environment<ExReg>& env, ticker& t) -> future<> {
        using output_t = typename ExReg::output_t;

        t.start([&] (uint64_t tick) -> future<> {
            env.tick_network();
            timer.tick();
            if (tick % 10 == 0) {
                env.tick_servers();
            }
            return ping_shards();
        }, 10'000);

        auto leader_id = co_await env.new_server(true);

        // Wait at most 1000 ticks for the server to elect itself as a leader.
        SCYLLA_ASSERT(co_await wait_for_leader<ExReg>{}(env, {leader_id}, timer, timer.now() + 1000_t) == leader_id);

        auto call = [&] (ExReg::input_t input, raft::logical_clock::duration timeout) {
            return env.call(leader_id, std::move(input),  timer.now() + timeout, timer);
        };

        auto eq = [] (const call_result_t<ExReg>& r, const output_t& expected) {
            return std::holds_alternative<output_t>(r) && std::get<output_t>(r) == expected;
        };

        for (int i = 1; i <= 100; ++i) {
            SCYLLA_ASSERT(eq(co_await call(ExReg::exchange{i}, 100_t), ExReg::ret{i - 1}));
        }

        tlogger.debug("100 exchanges - single server - passed");

        auto id2 = co_await env.new_server(false);
        auto id3 = co_await env.new_server(false);

        tlogger.debug("Started 2 more servers, changing configuration");

        SCYLLA_ASSERT(std::holds_alternative<std::monostate>(
            co_await env.reconfigure(leader_id, {leader_id, id2, id3}, timer.now() + 100_t, timer)));

        tlogger.debug("Configuration changed");

        co_await call(ExReg::exchange{0}, 100_t);
        for (int i = 1; i <= 100; ++i) {
            SCYLLA_ASSERT(eq(co_await call(ExReg::exchange{i}, 100_t), ExReg::ret{i - 1}));
        }

        tlogger.debug("100 exchanges - three servers - passed");

        // concurrent calls
        std::vector<future<call_result_t<ExReg>>> futs;
        for (int i = 0; i < 100; ++i) {
            futs.push_back(call(ExReg::read{}, 100_t));
            co_await timer.sleep(2_t);
        }
        for (int i = 0; i < 100; ++i) {
            SCYLLA_ASSERT(eq(co_await std::move(futs[i]), ExReg::ret{100}));
        }

        tlogger.debug("100 concurrent reads - three servers - passed");
    });

    tlogger.debug("Finished");
}

SEASTAR_TEST_CASE(test_frequent_snapshotting) {
    auto seed = tests::random::get_int<int32_t>();
    std::mt19937 random_engine{seed};

    logical_timer timer;
    environment_config cfg {
        .rnd{random_engine},
        .network_delay{0, 6},
        .fd_convict_threshold = 50_t,
    };
    co_await with_env_and_ticker<ExReg>(cfg, [&timer] (environment<ExReg>& env, ticker& t) -> future<> {
        using output_t = typename ExReg::output_t;

        t.start([&] (uint64_t tick) -> future<> {
            env.tick_network();
            timer.tick();
            if (tick % 10 == 0) {
                env.tick_servers();
            }
            return ping_shards();
        }, 10'000);
        const auto server_config = raft::server::configuration {
            .snapshot_threshold = 1,
            .snapshot_threshold_log_size = 150,
            .snapshot_trailing = 5,
            .snapshot_trailing_size= 75,
            .max_log_size = 300,
            .enable_forwarding = true,
            .max_command_size = 30
        };

        auto leader_id = co_await env.new_server(true, server_config);

        auto call = [&] (ExReg::input_t input, raft::logical_clock::duration timeout) {
            return env.call(leader_id, std::move(input),  timer.now() + timeout, timer);
        };

        auto eq = [] (const call_result_t<ExReg>& r, const output_t& expected) {
            return std::holds_alternative<output_t>(r) && std::get<output_t>(r) == expected;
        };

        // Wait at most 1000 ticks for the server to elect itself as a leader.
        SCYLLA_ASSERT(co_await wait_for_leader<ExReg>{}(env, {leader_id}, timer, timer.now() + 1000_t) == leader_id);

        auto id2 = co_await env.new_server(false, server_config);
        auto id3 = co_await env.new_server(false, server_config);

        env.for_each_server([](raft::server_id, raft_server<ExReg>* srv) {
            srv->get_server()->set_applier_queue_max_size(1);
        });

        tlogger.debug("Started 2 more servers, changing configuration");

        SCYLLA_ASSERT(std::holds_alternative<std::monostate>(
                co_await env.reconfigure(leader_id, {leader_id, id2, id3}, timer.now() + 100_t, timer)));

        tlogger.debug("Configuration changed");

        co_await call(ExReg::exchange{0}, 100_t);
        for (int i = 1; i <= 100; ++i) {
            SCYLLA_ASSERT(eq(co_await call(ExReg::exchange{i}, 100_t), ExReg::ret{i - 1}));
        }

        tlogger.debug("100 exchanges - three servers - passed");

        // concurrent calls
        std::vector<future<call_result_t<ExReg>>> futs;
        for (int i = 0; i < 100; ++i) {
            futs.push_back(call(ExReg::read{}, 100_t));
            co_await timer.sleep(2_t);
        }
        for (int i = 0; i < 100; ++i) {
            SCYLLA_ASSERT(eq(co_await std::move(futs[i]), ExReg::ret{100}));
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
        .rnd{0},
        .network_delay{1, 1},
        .fd_convict_threshold = 10_t,
    };
    co_await with_env_and_ticker<ExReg>(cfg, [&timer] (environment<ExReg>& env, ticker& t) -> future<> {
        t.start([&] (uint64_t tick) {
            env.tick_network();
            timer.tick();
            if (tick % 10 == 0) {
                env.tick_servers();
            }
            return ping_shards();
        }, 10'000);

        auto id1 = co_await env.new_server(true,
                raft::server::configuration{
        // It's easier to catch the problem when we send entries one by one, not in batches.
                    .append_request_threshold = 1,
                });
        SCYLLA_ASSERT(co_await wait_for_leader<ExReg>{}(env, {id1}, timer, timer.now() + 1000_t) == id1);

        auto id2 = co_await env.new_server(false,
                raft::server::configuration{
                    .append_request_threshold = 1,
                });

        SCYLLA_ASSERT(std::holds_alternative<std::monostate>(
            co_await env.reconfigure(id1, {id1, id2}, timer.now() + 100_t, timer)));

        // Append a bunch of entries
        for (int i = 1; i <= 10; ++i) {
            SCYLLA_ASSERT(std::holds_alternative<typename ExReg::ret>(
                co_await env.call(id1, ExReg::exchange{0}, timer.now() + 100_t, timer)));
        }

        SCYLLA_ASSERT(env.is_leader(id1));

        // Force a term increase by partitioning the network and waiting for the leader to step down
        tlogger.trace("add grudge");
        env.get_network().add_grudge(id2, id1);
        env.get_network().add_grudge(id1, id2);

        while (env.is_leader(id1)) {
            co_await seastar::yield();
        }

        tlogger.trace("remove grudge");
        env.get_network().remove_grudge(id2, id1);
        env.get_network().remove_grudge(id1, id2);

        auto l = co_await wait_for_leader<ExReg>{}(env, {id1, id2}, timer, timer.now() + 1000_t);
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
        SCYLLA_ASSERT(std::holds_alternative<std::monostate>(
            co_await env.reconfigure(l, {l, id3}, timer.now() + 1000_t, timer)));
    });
}

// Regression test for the following bug: when we took a snapshot, we forgot to save the configuration.
// This caused each node in the cluster to eventually forget the cluster configuration.
SEASTAR_TEST_CASE(snapshotting_preserves_config_test) {
    logical_timer timer;
    environment_config cfg {
        .rnd{0},
        .network_delay{1, 1},
        .fd_convict_threshold = 10_t,
    };
    co_await with_env_and_ticker<ExReg>(cfg, [&timer] (environment<ExReg>& env, ticker& t) -> future<> {
        t.start([&] (uint64_t tick) {
            env.tick_network();
            timer.tick();
            if (tick % 10 == 0) {
                env.tick_servers();
            }
            return ping_shards();
        }, 10'000);

        auto id1 = co_await env.new_server(true,
                raft::server::configuration{
                    .snapshot_threshold = 5,
                    .snapshot_trailing = 1,
                });
        SCYLLA_ASSERT(co_await wait_for_leader<ExReg>{}(env, {id1}, timer, timer.now() + 1000_t) == id1);

        auto id2 = co_await env.new_server(false,
                raft::server::configuration{
                    .snapshot_threshold = 5,
                    .snapshot_trailing = 1,
                });

        SCYLLA_ASSERT(std::holds_alternative<std::monostate>(
            co_await env.reconfigure(id1, {id1, id2}, timer.now() + 100_t, timer)));

        // Append a bunch of entries
        for (int i = 1; i <= 10; ++i) {
            SCYLLA_ASSERT(std::holds_alternative<typename ExReg::ret>(
                co_await env.call(id1, ExReg::exchange{0}, timer.now() + 100_t, timer)));
        }

        SCYLLA_ASSERT(env.is_leader(id1));

        // Partition the network, forcing the leader to step down.
        tlogger.trace("add grudge");
        env.get_network().add_grudge(id2, id1);
        env.get_network().add_grudge(id1, id2);

        while (env.is_leader(id1)) {
            co_await seastar::yield();
        }

        tlogger.trace("remove grudge");
        env.get_network().remove_grudge(id2, id1);
        env.get_network().remove_grudge(id1, id2);

        // With the bug this would timeout, the cluster is unable to elect a leader without the configuration.
        auto l = co_await wait_for_leader<ExReg>{}(env, {id1, id2}, timer, timer.now() + 1000_t);
        tlogger.trace("last leader: {}", l);
    });
}

// Regression test for #9981.
SEASTAR_TEST_CASE(removed_follower_with_forwarding_learns_about_removal) {
    logical_timer timer;
    environment_config cfg {
        .rnd{0},
        .network_delay{1, 1},
        .fd_convict_threshold = 10_t,
    };
    co_await with_env_and_ticker<ExReg>(cfg, [&timer] (environment<ExReg>& env, ticker& t) -> future<> {
        t.start([&] (uint64_t tick) {
            env.tick_network();
            timer.tick();
            if (tick % 10 == 0) {
                env.tick_servers();
            }
            return ping_shards();
        }, 10'000);

        raft::server::configuration cfg {
            .enable_forwarding = true,
        };

        auto id1 = co_await env.new_server(true, cfg);
        SCYLLA_ASSERT(co_await wait_for_leader<ExReg>{}(env, {id1}, timer, timer.now() + 1000_t) == id1);

        auto id2 = co_await env.new_server(false, cfg);
        SCYLLA_ASSERT(std::holds_alternative<std::monostate>(
            co_await env.reconfigure(id1, {id1, id2}, timer.now() + 100_t, timer)));

        // Server 2 forwards the entry that removes it to server 1.
        // We want server 2 to eventually learn from server 1 that it was removed,
        // so the call finishes (no timeout).
        SCYLLA_ASSERT(std::holds_alternative<std::monostate>(
            co_await env.modify_config(id2, std::vector<raft::server_id>{}, {id2}, timer.now() + 100_t, timer)));
    });
}

// Regression test for #10010, #11235.
SEASTAR_TEST_CASE(remove_leader_with_forwarding_finishes) {
    auto seed = tests::random::get_int<int32_t>();
    std::mt19937 random_engine{seed};

    logical_timer timer;
    environment_config cfg {
            .rnd{seed},
            .network_delay{0, 6},
            .fd_convict_threshold = 50_t,
    };

    co_await with_env_and_ticker<ExReg>(cfg, [&] (environment<ExReg>& env, ticker& t) -> future<> {
        t.start([&, dist = std::uniform_int_distribution<size_t>(0, 9)] (uint64_t tick) mutable {
            env.tick_network();
            timer.tick();
            env.for_each_server([&] (raft::server_id, raft_server<ExReg>* srv) {
                // Tick each server with probability 1/10.
                // Thus each server is ticked, on average, once every 10 timer/network ticks.
                // On the other hand, we now have servers running at different speeds.
                if (srv && dist(random_engine) == 0) {
                    srv->tick();
                }
            });
            return ping_shards();
        }, 20'000);

        raft::server::configuration cfg {
                .enable_forwarding = true,
        };

        auto id1 = co_await env.new_server(true, cfg);
        SCYLLA_ASSERT(co_await wait_for_leader<ExReg>{}(env, {id1}, timer, timer.now() + 1000_t) == id1);
        auto id2 = co_await env.new_server(false, cfg);
        SCYLLA_ASSERT(std::holds_alternative<std::monostate>(
                co_await env.reconfigure(id1, {id1, id2}, timer.now() + 200_t, timer)));
        // Server 2 forwards the entry that removes server 1 to server 1.
        // We want server 2 to either learn from server 1 about the removal,
        // or become a leader and learn from itself; in both cases the call should finish (no timeout).
        auto result = co_await env.modify_config(id2, std::vector<raft::server_id>{}, {id1}, timer.now() + 200_t, timer);
        tlogger.info("env.modify_config result {}", result);
        SCYLLA_ASSERT(std::holds_alternative<std::monostate>(result));
    });
}

// Given a function `F` which takes a `raft::server_id` argument and returns a variant type
// which contains `not_a_leader`, repeatedly calls `F` until it returns something else than
// `not_a_leader` or until we reach a limit, whichever happens first.
// The maximum number of calls until we give up is specified by `bounces`.
// The initial `raft::server_id` argument provided to `F` is specified as an argument
// to this function (`srv_id`). If the initial call returns `not_a_leader`, then:
// - if the result contained a different leader ID and we didn't already try that ID,
//   we will use it in the next call, sleeping for `known_leader_delay` first,
// - otherwise we will take the next ID from the `known` set, sleeping for
//   `unknown_leader_delay` first; no ID will be tried twice.
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
        tlogger.trace("bouncing call: starting with {}", srv_id);
        std::unordered_set<raft::server_id> tried;
        while (true) {
            auto res = co_await _f(srv_id);
            tried.insert(srv_id);
            known.erase(srv_id);

            if (auto n_a_l = std::get_if<raft::not_a_leader>(&res); n_a_l && bounces) {
                --bounces;

                if (n_a_l->leader) {
                    if (n_a_l->leader == srv_id || !tried.contains(n_a_l->leader)) {
                        co_await timer.sleep(known_leader_delay);
                        tlogger.trace("bouncing call: got `not_a_leader` from {}, rerouting to {}", srv_id, n_a_l->leader);
                        srv_id = n_a_l->leader;
                        continue;
                    }
                }

                if (!known.empty()) {
                    auto prev = srv_id;
                    srv_id = *known.begin();
                    if (n_a_l->leader) {
                        tlogger.trace("bouncing call: got `not_a_leader` from {}, rerouted to {}, but already tried it; trying {}",
                                prev, n_a_l->leader, srv_id);
                    } else {
                        tlogger.trace("bouncing call: got `not_a_leader` from {}, no reroute, trying {}", prev, srv_id);
                    }
                    continue;
                }
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
        SCYLLA_ASSERT(s.known.size() > 0);
        static std::mt19937 engine{0};

        auto it = s.known.begin();
        std::advance(it, std::uniform_int_distribution<size_t>{0, s.known.size() - 1}(engine));
        auto contact = *it;

        tlogger.debug("db call start inp {} tid {} start time {} current time {} contact {}", input, ctx.thread, ctx.start, s.timer.now(), contact);

        auto [res, last] = co_await bouncing{[input = input, timeout = s.timer.now() + timeout, &timer = s.timer, &env = s.env] (raft::server_id id) {
            return env.call(id, input, timeout, timer);
        }}(s.timer, s.known, contact, 6, 10_t, 10_t);
        tlogger.debug("db call end inp {} tid {} start time {} current time {} last contact {}", input, ctx.thread, ctx.start, s.timer.now(), last);

        co_return res;
    }
};

template <PureStateMachine M>
struct fmt::formatter<raft_call<M>> : fmt::formatter<string_view> {
    auto format(const raft_call<M>& r, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "raft_call{{input:{}, timeout:{}}}", r.input, r.timeout);
    }
};

// An operation representing a linearizable read from a Raft server.
// To be used only in forwarding mode. Doesn't bounce.
template <PureStateMachine M>
struct raft_read {
    int32_t read_id;
    raft::logical_clock::duration timeout;

    using result_type = std::pair<int32_t, read_result_t<M>>;

    struct state_type {
        environment<M>& env;
        const std::unordered_set<raft::server_id>& known;
        logical_timer& timer;
    };

    future<result_type> execute(state_type& s, const operation::context& ctx) {
        SCYLLA_ASSERT(s.known.size() > 0);
        static std::mt19937 engine{0};

        auto it = s.known.begin();
        std::advance(it, std::uniform_int_distribution<size_t>{0, s.known.size() - 1}(engine));
        auto contact = *it;

        tlogger.debug("read start tid {} start time {} current time {} contact {}", ctx.thread, ctx.start, s.timer.now(), contact);
        auto res = co_await s.env.read(contact, s.timer.now() + timeout, s.timer);
        tlogger.debug("read end tid {} start time {} current time {} contact {}", ctx.thread, ctx.start, s.timer.now(), contact);

        co_return result_type{read_id, std::move(res)};
    }
};

template <PureStateMachine M>
struct fmt::formatter<raft_read<M>> : fmt::formatter<string_view> {
    auto format(const raft_read<M>& r, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "raft_read{{id:{}, timeout:{}}}", r.read_id, r.timeout);
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
            auto it = std::find_if(mid, nodes.end(), [&env = s.env] (raft::server_id id) { return env.is_leader(id); });
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

    friend fmt::formatter<network_majority_grudge>;
};

template <PureStateMachine M>
struct fmt::formatter<network_majority_grudge<M>> : fmt::formatter<string_view> {
    auto format(const network_majority_grudge<M>& p, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "network_majority_grudge{{duration:{}}}", p._duration);
    }
};

// Must be executed sequentially.
template <PureStateMachine M>
struct reconfiguration {
    raft::logical_clock::duration timeout;

    struct state_type {
        const std::vector<raft::server_id> all_servers;
        environment<M>& env;
        // a subset of all_servers that we modify;
        // the set of servers which may potentially be in the current configuration
        std::unordered_set<raft::server_id>& known;
        logical_timer& timer;
        std::mt19937 rnd;
    };

    using result_type = reconfigure_result_t;

    future<result_type> execute_modify_config(
            state_type& s, const operation::context& ctx, std::vector<raft::server_id> nodes, size_t members_end, size_t voters_end) {
        std::vector<std::pair<raft::server_id, bool>> added;
        for (size_t i = 0; i < voters_end; ++i) {
            added.emplace_back(nodes[i], true);
        }
        for (size_t i = voters_end; i < members_end; ++i) {
            added.emplace_back(nodes[i], false);
        }

        std::vector<raft::server_id> removed {nodes.begin() + members_end, nodes.end()};
        auto contact = *s.known.begin();

        tlogger.debug("reconfig modify_config start add {} remove {} start tid {} start time {} current time {} contact {}",
                added, removed, ctx.thread, ctx.start, s.timer.now(), contact);

        SCYLLA_ASSERT(s.known.size() > 0);
        auto [res, last] = co_await bouncing{
                [&added, &removed, timeout = s.timer.now() + timeout, &timer = s.timer, &env = s.env] (raft::server_id id) {
            return env.modify_config(id, added, removed, timeout, timer);
        }}(s.timer, s.known, contact, 10, 10_t, 10_t);

        std::visit(make_visitor(
        [&, last = last] (std::monostate) {
            tlogger.debug("reconfig successful known {} added {} removed {} by {}", s.known, added, removed, last);
            s.known.merge(std::unordered_set<raft::server_id>{nodes.begin(), nodes.begin() + members_end});
            for (auto id: removed) {
                s.known.erase(id);
            }
        },
        [&, last = last] (raft::not_a_leader& e) {
            tlogger.debug("reconfig failed, not a leader: {} tried to add {}, remove {} by {}", e, added, removed, last);
        },
        [&, last = last] (auto& e) {
            s.known.merge(std::unordered_set<raft::server_id>{nodes.begin(), nodes.begin() + members_end});
            tlogger.debug("reconfig failed: {}, tried to add {}, remove {}, after merge {} by {}", e, added, removed, s.known, last);
        }
        ), res);

        tlogger.debug("reconfig modify_config end add {} remove {} start tid {} start time {} current time {} last contact {}",
                added, removed, ctx.thread, ctx.start, s.timer.now(), last);

        co_return res;
    }

    future<result_type> execute_reconfigure(
            state_type& s, const operation::context& ctx, std::vector<raft::server_id> nodes, size_t members_end, size_t voters_end) {
        std::vector<std::pair<raft::server_id, bool>> nodes_voters;
        nodes_voters.reserve(members_end);
        for (size_t i = 0; i < voters_end; ++i) {
            nodes_voters.emplace_back(nodes[i], true);
        }
        for (size_t i = voters_end; i < members_end; ++i) {
            nodes_voters.emplace_back(nodes[i], false);
        }

        auto contact = *s.known.begin();

        tlogger.debug("reconfig set_configuration start nodes {} start tid {} start time {} current time {} contact {}",
                nodes_voters, ctx.thread, ctx.start, s.timer.now(), contact);

        SCYLLA_ASSERT(s.known.size() > 0);
        auto [res, last] = co_await bouncing{[&nodes_voters, timeout = s.timer.now() + timeout, &timer = s.timer, &env = s.env] (raft::server_id id) {
            return env.reconfigure(id, nodes_voters, timeout, timer);
        }}(s.timer, s.known, contact, 10, 10_t, 10_t);

        std::visit(make_visitor(
        [&, last = last] (std::monostate) {
            tlogger.debug("reconfig successful from {} to {} by {}", s.known, nodes_voters, last);
            s.known = std::unordered_set<raft::server_id>{nodes.begin(), nodes.begin() + members_end};
            // TODO: include the old leader as well in case it's not part of the new config?
            // it may remain a leader for some time...
        },
        [&, last = last] (raft::not_a_leader& e) {
            tlogger.debug("reconfig failed, not a leader: {} tried {} by {}", e, nodes_voters, last);
        },
        [&, last = last] (auto& e) {
            s.known.merge(std::unordered_set<raft::server_id>{nodes.begin(), nodes.begin() + members_end});
            tlogger.debug("reconfig failed: {}, tried {} after merge {} by {}", e, nodes_voters, s.known, last);
        }
        ), res);

        tlogger.debug("reconfig set_configuration end nodes {} start tid {} start time {} current time {} last contact {}",
                nodes_voters, ctx.thread, ctx.start, s.timer.now(), last);

        co_return res;
    }

    future<result_type> execute(state_type& s, const operation::context& ctx) {
        static std::bernoulli_distribution bdist{0.5};

        SCYLLA_ASSERT(s.all_servers.size() > 1);
        std::vector<raft::server_id> nodes{s.all_servers.begin(), s.all_servers.end()};

        std::shuffle(nodes.begin(), nodes.end(), s.rnd);
        size_t members_end = std::uniform_int_distribution<size_t>{1, nodes.size()}(s.rnd);
        size_t voters_end = std::uniform_int_distribution<size_t>{1, members_end}(s.rnd);

        if (bdist(s.rnd)) {
            return execute_modify_config(s, ctx, std::move(nodes), members_end, voters_end);
        } else {
            return execute_reconfigure(s, ctx, std::move(nodes), members_end, voters_end);
        }
    }
};

template <PureStateMachine M>
struct fmt::formatter<reconfiguration<M>>: fmt::formatter<string_view> {
    auto format(const reconfiguration<M>& r, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "reconfiguration{{timeout:{}}}", r.timeout);
    }
};

// TODO: make stop_crash_result a nested class of stop_crash,
//   and print it using format_as(), once {fmt} v10 can be used
struct stop_crash_result {};

template <>
struct fmt::formatter<stop_crash_result>: fmt::formatter<string_view> {
    auto format(stop_crash_result, fmt::format_context& ctx) const {
        return ctx.out();
    }
};

template <PureStateMachine M>
struct stop_crash {
    raft::logical_clock::duration restart_delay;

    struct state_type {
        environment<M>& env;
        std::unordered_set<raft::server_id>& known;
        logical_timer& timer;
        std::mt19937 rnd;
    };

    using result_type = stop_crash_result;

    future<result_type> execute(state_type& s, const operation::context& ctx) {
        SCYLLA_ASSERT(s.known.size() > 0);
        auto it = s.known.begin();
        std::advance(it, std::uniform_int_distribution<size_t>{0, s.known.size() - 1}(s.rnd));
        auto srv = *it;

        static std::bernoulli_distribution bdist{0.5};
        if (bdist(s.rnd)) {
            tlogger.debug("Crashing server {}", srv);
            s.env.crash(srv);
        } else {
            tlogger.debug("Stopping server {}...", srv);
            co_await s.env.stop(srv);
            tlogger.debug("Server {} stopped", srv);
        }
        co_await s.timer.sleep(restart_delay);
        tlogger.debug("Restarting server {}", srv);
        co_await s.env.start_server(srv);

        co_return result_type{};
    }
};

template <PureStateMachine M>
struct fmt::formatter<stop_crash<M>>: fmt::formatter<string_view> {
    auto format(const stop_crash<M>& c, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "stop_crash{{delay:{}}}", c.restart_delay);
    }
};

template <> struct fmt::formatter<operation::thread_id>: fmt::formatter<string_view> {
    auto format(const operation::thread_id& tid, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "thread_id{{{}}}", tid.id);
    }
};

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

    static constexpr elem_t magic = 54313;

public:
    append_seq(std::vector<elem_t> v) : _seq{make_lw_shared<std::vector<elem_t>>(std::move(v))}, _end{_seq->size()}, _digest{0} {
        for (auto x : *_seq) {
            _digest = digest_append(_digest, x);
        }
    }

    static elem_t digest_append(elem_t d, elem_t x) {
        BOOST_REQUIRE_LE(0, d);
        BOOST_REQUIRE_LT(d, magic);

        auto y = (d + x) % magic;
        SCYLLA_ASSERT(digest_remove(y, x) == d);
        return y;
    }

    static elem_t digest_remove(elem_t d, elem_t x) {
        BOOST_REQUIRE_LE(0, d);
        BOOST_REQUIRE_LT(d, magic);

        auto y = (d - x) % magic;
        return y < 0 ? y + magic : y;
    }

    elem_t digest() const {
        return _digest;
    }

    append_seq append(elem_t x) const {
        SCYLLA_ASSERT(_seq);
        SCYLLA_ASSERT(_end <= _seq->size());

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
        SCYLLA_ASSERT(_seq);
        SCYLLA_ASSERT(idx < _end);
        SCYLLA_ASSERT(_end <= _seq->size());
        return (*_seq)[idx];
    }

    bool empty() const {
        return _end == 0;
    }

    size_t size() const {
        SCYLLA_ASSERT(_end <= _seq->size());
        return _end;
    }

    std::pair<append_seq, elem_t> pop() const {
        SCYLLA_ASSERT(_seq);
        SCYLLA_ASSERT(_end <= _seq->size());
        SCYLLA_ASSERT(0 < _end);

        return {{_seq, _end - 1, digest_remove(_digest, (*_seq)[_end - 1])}, (*_seq)[_end - 1]};
    }

    friend fmt::formatter<append_seq>;

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

template <> struct fmt::formatter<append_seq> : fmt::formatter<string_view> {
    auto format(const append_seq& s, fmt::format_context& ctx) const {
        // TODO: don't copy the elements
        std::vector<append_seq::elem_t> v{s._seq->begin(), s._seq->begin() + s._end};
        return fmt::format_to(ctx.out(), "seq({} _end {})", v, s._end);
    }
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

struct inconsistency {
    std::string what;
};

struct append_entry {
    using elem_t = typename append_seq::elem_t;
    elem_t elem;
    elem_t digest;
};

template <>
struct fmt::formatter<append_entry> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const append_entry& e, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", e.elem);
    }
};

std::ostream& operator<<(std::ostream& os, const append_entry& e) {
    return os << e.elem;
}

struct append_reg_model {
    using elem_t = typename append_entry::elem_t;
    using entry = append_entry;

    std::vector<entry> seq{{0, 0}};
    std::unordered_map<elem_t, size_t> index{{0, 0}};
    std::unordered_set<elem_t> banned;
    std::unordered_set<elem_t> returned;
    std::unordered_set<elem_t> in_progress;

    // For each read, the element observed at the end of the model sequence
    // at the moment the read has started.
    std::unordered_map<int32_t, elem_t> reads;

    void invocation(elem_t x) {
        SCYLLA_ASSERT(!index.contains(x));
        SCYLLA_ASSERT(!in_progress.contains(x));
        in_progress.insert(x);
    }

    void return_success(elem_t x, append_seq prev) {
        SCYLLA_ASSERT(!returned.contains(x));
        SCYLLA_ASSERT(x != 0);
        SCYLLA_ASSERT(!prev.empty());
        try {
            completion(x, prev);
        } catch (inconsistency& e) {
            e.what += format("\nwhen completing append: {}\nprev: {}\nmodel: {}", x, prev, seq);
            throw;
        }
        returned.insert(x);
    }

    void return_failure(elem_t x) {
        SCYLLA_ASSERT(!index.contains(x));
        SCYLLA_ASSERT(in_progress.contains(x));
        banned.insert(x);
        in_progress.erase(x);
    }

    void start_read(int32_t id) {
        auto [_, inserted] = reads.emplace(id, seq.back().elem);
        SCYLLA_ASSERT(inserted);
    }

    void read_success(int32_t id, append_seq result) {
        auto read = reads.find(id);
        SCYLLA_ASSERT(read != reads.end());

        size_t idx = 0;
        for (; idx < result.size(); ++idx) {
            if (result[idx] == read->second) {
                break;
            }
        }

        if (idx == result.size()) {
            throw inconsistency{format(
                "read {} observed last model elem {} at start not present in result: {}",
                id, read->second, result)};
        }

        try {
            auto [prev, x] = result.pop();
            completion(x, prev);
        } catch (inconsistency& e) {
            e.what += format(
                    "\nwhen completing read id: {}, last model elem at start: {}\nread result: {}",
                    id, read->second, result);
        }
    }

private:
    void completion(elem_t x, append_seq prev) {
        if (prev.empty()) {
            SCYLLA_ASSERT(x == 0);
            return;
        }

        SCYLLA_ASSERT(x != 0);
        SCYLLA_ASSERT(!banned.contains(x));
        SCYLLA_ASSERT(in_progress.contains(x) || index.contains(x));

        auto [prev_prev, prev_x] = prev.pop();

        if (auto it = index.find(x); it != index.end()) {
            // This element was already completed.
            auto idx = it->second;
            SCYLLA_ASSERT(0 < idx);
            SCYLLA_ASSERT(idx < seq.size());

            if (prev_x != seq[idx - 1].elem) {
                throw inconsistency{format(
                    "elem {} completed again (existing at idx {}), but prev elem does not match existing model"
                    "\nprev elem: {}\nmodel prev elem: {}\nprev: {} model up to idx: {}",
                    x, idx, prev_x, seq[idx - 1].elem, prev, std::vector<entry>{seq.begin(), seq.begin()+idx})};
            }

            if (prev.digest() != seq[idx - 1].digest) {
                auto err = format(
                    "elem {} completed again (existing at idx {}), but prev does not match existing model"
                    "\n prev: {}\nmodel up to idx: {}",
                    x, idx, prev, std::vector<entry>{seq.begin(), seq.begin()+idx});

                auto min_len = std::min(prev.size(), idx);
                for (size_t i = 0; i < min_len; ++i) {
                    if (prev[i] != seq[i].elem) {
                        err += format("\nmismatch at idx {} prev {} model {}", i, prev[i], seq[i].elem);
                    }
                }

                throw inconsistency{std::move(err)};
            }

            return;
        }

        // A new completion.
        // First, recursively complete the previous elements...
        completion(prev_x, std::move(prev_prev));

        // Check that the existing tail matches our tail.
        SCYLLA_ASSERT(!seq.empty());
        if (prev_x != seq.back().elem) {
            throw inconsistency{format(
                "new completion (elem: {}) but prev elem does not match existing model"
                "\nprev elem: {}\nmodel prev elem: {}\nprev: {}\n model: {}",
                x, prev_x, seq.back().elem, prev, seq)};
        }
        if (prev.digest() != seq.back().digest) {
            auto err = format(
                "new completion (elem: {}) but prev does not match existing model"
                "\nprev: {}\n model: {}",
                x, prev, seq);

            auto min_len = std::min(prev.size(), seq.size());
            for (size_t i = 0; i < min_len; ++i) {
                if (prev[i] != seq[i].elem) {
                    err += format("\nmismatch at idx {} prev {} model {}", i, prev[i], seq[i].elem);
                }
            }

            throw inconsistency{std::move(err)};
        }

        // All previous elements were completed, so the new element belongs at the end.
        index.emplace(x, seq.size());
        seq.push_back(entry{x, append_seq::digest_append(seq.back().digest, x)});
        in_progress.erase(x);
    }
};

template <> struct fmt::formatter<AppendReg::append> : fmt::formatter<string_view> {
    auto format(const AppendReg::append& a, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "append{{{}}}", a.x);
    }
};

template <> struct fmt::formatter<AppendReg::ret> : fmt::formatter<string_view> {
    auto format(const AppendReg::ret& r, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "ret{{{}, {}}}", r.x, r.prev);
    }
};

SEASTAR_TEST_CASE(basic_generator_test) {
    using op_type = operation::invocable<operation::either_of<
            raft_call<AppendReg>,
            raft_read<AppendReg>,
            network_majority_grudge<AppendReg>,
            reconfiguration<AppendReg>,
            stop_crash<AppendReg>
        >>;
    using history_t = utils::chunked_vector<std::variant<op_type, operation::completion<op_type>>>;

    static_assert(operation::Invocable<op_type>);

    auto seed = tests::random::get_int<int32_t>();
    std::mt19937 random_engine{seed};

    logical_timer timer;
    environment_config cfg {
        .rnd{random_engine},
        .network_delay{0, 6},
        .fd_convict_threshold = 50_t,
    };
    co_await with_env_and_ticker<AppendReg>(cfg, [&] (environment<AppendReg>& env, ticker& t) -> future<> {
        t.start([&, dist = std::uniform_int_distribution<size_t>(0, 9)] (uint64_t tick) mutable {
            env.tick_network();
            timer.tick();
            env.for_each_server([&] (raft::server_id, raft_server<AppendReg>* srv) {
                // Tick each server with probability 1/10.
                // Thus each server is ticked, on average, once every 10 timer/network ticks.
                // On the other hand, we now have servers running at different speeds.
                if (srv && dist(random_engine) == 0) {
                    srv->tick();
                }
            });
            env.tick_crashing_servers();
            return ping_shards();
        }, 200'000);

        std::bernoulli_distribution bdist{0.5};

        // With probability 1/2 enable forwarding: when we send a command to a follower, it automatically
        // forwards it to the known leader or waits for learning about a leader instead of returning
        // `not_a_leader`.
        bool forwarding = bdist(random_engine);

        // With probability 1/2, run the servers with a configuration which causes frequent snapshotting.
        // Note: with the default configuration we won't observe any snapshots at all, since the default
        // threshold is 1024 log commands and we perform only 500 ops.
        bool frequent_snapshotting = bdist(random_engine);

        bool nemesis_partitions = true;
        bool nemesis_reconfigurations = true;
        bool nemesis_crashes = true;

        // TODO: randomize the snapshot thresholds between different servers for more chaos.
        const auto max_command_size = 2 * sizeof(raft::log_entry);
        auto srv_cfg = frequent_snapshotting
            ? raft::server::configuration {
                .snapshot_threshold = 10,
                .snapshot_threshold_log_size = 3 * (max_command_size + sizeof(raft::log_entry)),
                .snapshot_trailing = 5,
                .snapshot_trailing_size = max_command_size + sizeof(raft::log_entry),
                .max_log_size = 5 * (max_command_size + sizeof(raft::log_entry)),
                .enable_forwarding = forwarding,
                .max_command_size = max_command_size
            }
            : raft::server::configuration {
                .enable_forwarding = forwarding,
            };

        tlogger.info("basic_generator_test: forwarding: {}, frequent snapshotting: {}", forwarding, frequent_snapshotting);

        auto leader_id = co_await env.new_server(true, srv_cfg);

        // Wait for the server to elect itself as a leader.
        SCYLLA_ASSERT(co_await wait_for_leader<AppendReg>{}(env, {leader_id}, timer, timer.now() + 1000_t) == leader_id);

        size_t no_all_servers = 10;
        std::vector<raft::server_id> all_servers{leader_id};
        for (size_t i = 1; i < no_all_servers; ++i) {
            all_servers.push_back(co_await env.new_server(false, srv_cfg));
        }

        size_t no_init_servers = 5;

        // `known_config` represents the set of servers that may potentially be in the cluster configuration.
        //
        // It is not possible to determine in general what the 'true' current configuration is (if even such notion
        // makes sense at all). Given a sequence of reconfiguration requests, assuming that all except possibly the last
        // requests have finished, then:
        // - if the last request has finished successfully, then the current configuration must be equal
        //   to the one chosen in the last request;
        // - but if it hasn't finished yet, or it finished with a failure, the current configuration may contain servers
        //   from the one chosen in the last request or from the previously known set of servers.
        //
        // The situation is even worse considering that requests may never 'finish', i.e. we may never get a response
        // to a reconfiguration request (in which case we eventually timeout). These requests may in theory execute
        // at any point in the future. We take a practical approach when updating `known_config`: we assume
        // that our timeouts for reconfiguration requests are large enough so that if a reconfiguration request
        // has timed out, it has either already finished or it never will.
        // TODO: this may not be true and we may end up with `known_config` that does not contain the current leader
        // (not observed in practice yet though... I think) Come up with a better approach.
        std::unordered_set<raft::server_id> known_config;

        for (size_t i = 0; i < no_init_servers; ++i) {
            known_config.insert(all_servers[i]);
        }

        SCYLLA_ASSERT(std::holds_alternative<std::monostate>(
            co_await env.reconfigure(leader_id,
                std::vector<raft::server_id>{known_config.begin(), known_config.end()}, timer.now() + 100_t, timer)));

        auto threads = operation::make_thread_set(all_servers.size() + 3);
        auto [partition_thread, reconfig_thread, crash_thread] = take<3>(threads);


        raft_call<AppendReg>::state_type db_call_state {
            .env = env,
            .known = known_config,
            .timer = timer
        };

        raft_read<AppendReg>::state_type read_state {
            .env = env,
            .known = known_config,
            .timer = timer
        };

        network_majority_grudge<AppendReg>::state_type network_majority_grudge_state {
            .env = env,
            .known = known_config,
            .timer = timer,
            .rnd = std::mt19937{seed}
        };

        reconfiguration<AppendReg>::state_type reconfiguration_state {
            .all_servers = all_servers,
            .env = env,
            .known = known_config,
            .timer = timer,
            .rnd = std::mt19937{seed}
        };

        stop_crash<AppendReg>::state_type crash_state {
            .env = env,
            .known = known_config,
            .timer = timer,
            .rnd = std::mt19937{seed}
        };

        auto init_state = op_type::state_type{
            std::move(db_call_state),
            std::move(read_state),
            std::move(network_majority_grudge_state),
            std::move(reconfiguration_state),
            std::move(crash_state)
        };

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

        auto num_ops = 500;
        auto gen = op_limit(num_ops,
            pin(partition_thread,
                op_limit(nemesis_partitions ? num_ops : 0,
                    stagger(seed, timer.now() + 200_t, 1200_t, 1200_t,
                        random(seed, [] (std::mt19937& engine) {
                            static std::uniform_int_distribution<raft::logical_clock::rep> dist{400, 800};
                            return op_type{network_majority_grudge<AppendReg>{raft::logical_clock::duration{dist(engine)}}};
                        })
                    )
                ),
                pin(reconfig_thread,
                    op_limit(nemesis_reconfigurations ? num_ops : 0,
                        stagger(seed, timer.now() + 1000_t, 500_t, 500_t,
                            constant([] () { return op_type{reconfiguration<AppendReg>{500_t}}; })
                        )
                    ),
                    pin(crash_thread,
                        op_limit(nemesis_crashes ? num_ops : 0,
                            stagger(seed, timer.now() + 200_t, 100_t, 200_t,
                                random(seed, [] (std::mt19937& engine) {
                                    static std::uniform_int_distribution<raft::logical_clock::rep> dist{0, 100};
                                    return op_type{stop_crash<AppendReg>{raft::logical_clock::duration{dist(engine)}}};
                                })
                            )
                        ),
                        either(
                            stagger(seed, timer.now(), 0_t, 50_t,
                                sequence(1, [] (int32_t i) {
                                    SCYLLA_ASSERT(i > 0);
                                    return op_type{raft_call<AppendReg>{AppendReg::append{i}, 200_t}};
                                })
                            ),
                            op_limit(forwarding ? num_ops : 0 /* only produce raft_reads in forwarding mode */,
                                stagger(seed, timer.now(), 0_t, 200_t,
                                    sequence(1, [] (int32_t i) {
                                        return op_type{raft_read<AppendReg>{i, 200_t}};
                                    })
                                )
                            )
                        )
                    )
                )
            )
        );

        struct statistics {
            size_t invocations{0};
            size_t successes{0};
            size_t failures{0};
        };

        class consistency_checker {
            append_reg_model _model;
            statistics& _stats;

        public:
            consistency_checker(statistics& s) : _model{}, _stats(s) {}

            void operator()(op_type o) {
                tlogger.debug("invocation {}", o);

                if (auto call_op = std::get_if<raft_call<AppendReg>>(&o.op)) {
                    ++_stats.invocations;
                    _model.invocation(call_op->input.x);
                } else if (auto read_op = std::get_if<raft_read<AppendReg>>(&o.op)) {
                    ++_stats.invocations;
                    _model.start_read(read_op->read_id);
                }
            }

            void operator()(operation::completion<op_type> c) {
                auto res = std::get_if<op_type::result_type>(&c.result);
                SCYLLA_ASSERT(res);

                if (auto call_res = std::get_if<raft_call<AppendReg>::result_type>(res)) {
                    std::visit(make_visitor(
                    [this] (AppendReg::output_t& out) {
                        tlogger.debug("completion x: {} prev digest: {}", out.x, out.prev.digest());

                        ++_stats.successes;
                        _model.return_success(out.x, std::move(out.prev));
                    },
                    [this] (raft::not_a_leader& e) {
                        // TODO: this is a definite failure, mark it
                        // _model.return_failure(...)
                        ++_stats.failures;
                    },
                    [this] (raft::commit_status_unknown& e) {
                        // TODO SCYLLA_ASSERT: only allowed if reconfigurations happen?
                        // SCYLLA_ASSERT(false); TODO debug this
                        ++_stats.failures;
                    },
                    [this] (auto&) {
                        ++_stats.failures;
                    }
                    ), *call_res);
                } else if (auto read_res = std::get_if<raft_read<AppendReg>::result_type>(res)) {
                    std::visit(make_visitor(
                    [this, id = read_res->first] (AppendReg::state_t& s) {
                        tlogger.debug("read completion id: {} digest: {}", id, s.digest());

                        ++_stats.successes;
                        _model.read_success(id, std::move(s));
                    },
                    [this] (auto&) {
                        ++_stats.failures;
                    }
                    ), read_res->second);
                } else {
                    tlogger.debug("completion {}", c);
                }

                // TODO: check consistency of reconfiguration completions
                // (there's not much to check, but for example: we should not get back `conf_change_in_progress`
                //  if our last reconfiguration was successful?).
            }
        };

        statistics stats;
        history_t history;
        interpreter<op_type, decltype(gen), consistency_checker> interp{
            std::move(gen), std::move(threads), 1_t, std::move(init_state), timer,
            consistency_checker{stats}};
        try {
            co_await interp.run();
        } catch (inconsistency& e) {
            tlogger.error("inconsistency: {}", e.what);
            env.for_each_server([&] (raft::server_id id, raft_server<AppendReg>* srv) {
                if (srv) {
                    tlogger.info("server {} state machine state: {}", id, srv->state());
                } else {
                    tlogger.info("node {} currently missing server", id);
                }
            });

            SCYLLA_ASSERT(false);
        }

        tlogger.info("Finished generator run, time: {}, invocations: {}, successes: {}, failures: {}, total: {}",
                timer.now(), stats.invocations, stats.successes, stats.failures, stats.successes + stats.failures);

        // Liveness check: we must be able to obtain a final response after all the nemeses have stopped.
        // Due to possible multiple leaders at this point and the cluster stabilizing (for example there
        // may be no leader right now, the current leader may be stepping down etc.) we may need to try
        // sending requests multiple times to different servers to obtain the last result.

        auto limit = timer.now() + 10000_t;
        size_t cnt = 0;
        for (; timer.now() < limit; ++cnt) {
            tlogger.info("Trying to obtain last result: attempt number {}", cnt + 1);

            auto now = timer.now();
            auto leader = co_await wait_for_leader<AppendReg>{}(env,
                        std::vector<raft::server_id>{all_servers.begin(), all_servers.end()}, timer, limit)
                    .handle_exception_type([&timer, now] (logical_timer::timed_out<raft::server_id>) -> raft::server_id {
                tlogger.error("Failed to find a leader after {} ticks at the end of test.", timer.now() - now);
                SCYLLA_ASSERT(false);
            });

            if (env.is_leader(leader)) {
                tlogger.info("Leader {} found after {} ticks", leader, timer.now() - now);
            } else {
                tlogger.warn("Leader {} found after {} ticks, but suddenly lost leadership", leader, timer.now() - now);
                continue;
            }

            auto config = env.get_configuration(leader);
            SCYLLA_ASSERT(config);
            tlogger.info("Leader {} configuration: current {} previous {}", leader, config->current, config->previous);

            for (auto& s: all_servers) {
                if (env.is_leader(s) && s != leader) {
                    auto conf = env.get_configuration(s);
                    SCYLLA_ASSERT(conf);
                    tlogger.info("There is another leader: {}, configuration: current {} previous {}", s, conf->current, conf->previous);
                }
            }

            tlogger.info("From the clients' point of view, the possible cluster members are: {}", known_config);

            auto [res, last_attempted_server] = co_await bouncing{[&timer, &env] (raft::server_id id) {
                return env.call(id, AppendReg::append{-1}, timer.now() + 200_t, timer);
            }}(timer, known_config, leader, known_config.size() + 1, 10_t, 10_t);

            if (std::holds_alternative<typename AppendReg::ret>(res)) {
                tlogger.info("Obtained last result");
                tlogger.debug("Last result: {}", res);
                co_return;
            }

            tlogger.warn("Failed to obtain last result at end of test: {} returned by {}", res, last_attempted_server);
        }

        tlogger.error("Failed to obtain a final successful response at the end of the test. Number of attempts: {}", cnt);
        SCYLLA_ASSERT(false);
    });
}
