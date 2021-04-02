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
#include <seastar/util/defer.hh>

#include "raft/server.hh"
#include "raft/logical_clock.hh"
#include "serializer.hh"
#include "serializer_impl.hh"
#include "idl/uuid.dist.hh"
#include "idl/uuid.dist.impl.hh"

using namespace seastar;

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

SEASTAR_TEST_CASE(dummy_test) {
    return seastar::make_ready_future<>();
}
