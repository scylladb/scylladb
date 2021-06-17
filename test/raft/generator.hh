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

#pragma once

#include <unordered_set>

#include <seastar/util/variant_utils.hh>
#include "utils/chunked_vector.hh"

#include "test/raft/future_set.hh"

namespace operation {

// Operation execution thread identifier.
// See `Executable` context.
struct thread_id {
    bool operator==(const thread_id&) const = default;
    size_t id;
};

} // namespace operation

namespace std {
template<> struct hash<operation::thread_id> {
    size_t operator()(const operation::thread_id& tid) const {
        return hash<size_t>()(tid.id);
    }
};
} // namespace std

namespace operation {

using thread_set = std::unordered_set<thread_id>;

thread_id some(const thread_set& s) {
    assert(!s.empty());
    return *s.begin();
}

// Make a set of `n` threads.
thread_set make_thread_set(size_t n) {
    thread_set s;
    for (size_t i = 0; i < n; ++i) {
        s.insert(thread_id{i});
    }
    return s;
}

// Operation execution context.
// See `Executable` concept.
struct context {
    thread_id thread;
    raft::logical_clock::time_point start;
};

// An executable operation.
// Represents a computation which may cause side effects.
//
// The computation is performed through the `execute` function. It returns a result
// of type `result_type` (for computations that don't have a result, use `std::monostate` or equivalent).
//
// Different computations of the same type may share state of type `state_type`; for example,
// the state may contain database handles. A reference to the state is passed to `execute`.
//
// Each execution is performed on an abstract `thread' (represented by a `thread_id`)
// and has a logical start time point. The thread and start point together form
// the execution's `context` which is also passed as a reference to `execute`.
//
// Two operations may be called in parallel only if they are on different threads.
template <typename Op>
concept Executable = requires (Op o, typename Op::state_type& s, const context& ctx) {
    typename Op::result_type;
    typename Op::state_type;

    { o.execute(s, ctx) } -> std::same_as<future<typename Op::result_type>>;
};

// An operation which can specify a logical point in time when it's ready.
// We will say that such operations `understand time'.
template <typename Op>
concept HasReadyTime = requires(Op o) {
    { o.ready } -> std::same_as<std::optional<raft::logical_clock::time_point>>;
};

// An operation which can specify a specific thread on which it can be executed.
// We will say that such operations `understand threads`.
// If the operation specifies a thread, it won't be executed on any other thread.
template <typename Op>
concept HasThread = requires(Op o) {
    { o.thread } -> std::same_as<std::optional<thread_id>>;
};

// An operation which finished with an unexpected / unhandled exception.
// Generally it is recommended to handle any failures in the operation's `execute`
// function and return them in the result.
template <typename Op>
struct exceptional_result {
    Op op;
    std::exception_ptr eptr;
};

template <Executable Op>
using execute_result = std::variant<typename Op::result_type, exceptional_result<Op>>;

// A completion of an operation's execution.
// Contains the result, the logical time point when the operation finished
// and the thread on which the operation was executed.
template <Executable Op>
struct completion {
    execute_result<Op> result;
    raft::logical_clock::time_point time;
    thread_id thread;
};

template <typename Op>
concept Invocable = Executable<Op> && HasReadyTime<Op> && HasThread<Op>;

} // namespace operation

namespace generator {

// A generator run context.
// See `Generator` concept.
struct context {
    raft::logical_clock::time_point now;

    // free_threads must be a subset of all_threads
    const operation::thread_set& all_threads;
    const operation::thread_set& free_threads;
};

struct pending {};
struct finished {};

// Possible results for requesting an operation from a generator.
// See `Generator` concept.
//
// A successful fetch results in an operation (`Op`).
// The generator may also return `pending`, denoting that it doesn't have an operation
// ready yet but may in the future (e.g. when the time advances or the set of free threads changes),
// or `finished`, denoting that it has no more operations to return.
//
// Note: the order of types in the variant is part of the interface, i.e. the user may depend on it.
template <typename Op>
using op_ret = std::variant<Op, pending, finished>;

template <typename Op, typename G>
using op_and_gen = std::pair<op_ret<Op>, G>;

// A generator is a data structure that produces a sequence of operations.
// Fetch an operation using the `fetch_op` function.
// The type of produced operations is `operation_type`.
//
// The returned operation depends on the generator's state and a context provided
// when fetching the operation. The context contains:
// - A logical time point representing the current time; the time point should
//   be non-decreasing in subsequent `fetch_op` calls.
// - A set of threads used to execute the fetched operations.
// - A subset of that set containing the free threads, i.e. threads that are currently
//   not executing any operations.
//
// `fetch_op` may assume that there's at least one free thread;
// the user should not call `fetch_op` otherwise.
//
// Other than an operation, the `fetch_op` function may also return `pending` or `finished`.
// See comment on `op_ret` above for their meaning.
//
// Generators are purely functional data structures. When an operation is fetched,
// it doesn't change the generator's state; instead, it returns a new generator
// whose state represents the modified state after fetching the operation.
// This allows one to easily compose generators. The user of a generator may
// also decide not to use a fetched operation, but instead call `fetch_op` again later
// on the same generator when the situation changes (e.g. there's a new free thread).
//
// TODO: consider using `seastar::lazy_eval` or equivalent for the returned generator.
template <typename G>
concept Generator = requires(const G g, const context& ctx) {
    typename G::operation_type;

    // Fetch an operation.
    //
    // The implementation should be deterministic
    // (it should produce the same operation given the same generator and context).
    { g.fetch_op(ctx) } -> std::same_as<op_and_gen<typename G::operation_type, G>>;
};

template <typename G, typename Op>
concept GeneratorOf = Generator<G> && std::is_same_v<typename G::operation_type, Op>;

// Runs the provided generator, fetching operations from it and executing them on the provided
// set of threads until the generator is exhausted (returns `finished`).
//
// The interpreter assumes an externally ticked `logical_timer`. The time returned from the timer
// can be used by the interpreted generator. For example, the generator may decide not to return
// an operation until a later time point.
//
// The operation type is an `Invocable` meaning that the generator can specify through the operation's
// fields the logical time point when the operation is ready (see `HasReadyTime`) or the thread
// on which the operation can execute (see `HasThread`).
//
// The interpreter guarantees the generator the following:
// - Subsequent calls to `fetch_op` have non-decreasing values of `context::now`.
// - `context::all_threads` is a constant non-empty set.
// - `context::free_threads` is a non-empty subset of `context::all_threads`.
//
// The interpreter requires the following from the generator:
// - If the generator returns `pending`, then given a context satisfying `free_threads = all_threads`
//   and large enough `now` it must return an operation.
// - If the returned operation specifies a thread (the `thread` field is not `nullopt`),
//   the thread must be an element of `free_threads` that was provided in the context passed to `fetch_op`.
// - If the returned operation specifies a ready time (the `ready` field is not `nullopt`),
//   the time must be greater than `now` that was provided in the context passed to `fetch_op`.
//
// Given an operation `Op op`, interpreter guarantees the following to `op.execute(s, ctx)`
// for `Op::state_type& s` and `operation::context ctx`:
// - `op.execute` is called only once.
// - `s` refers to the same object for all `execute` calls (for different operations).
// - If `op.thread` was not `nullopt`, then `ctx.thread = *op.thread`.
// - If `op.ready` was not `nullopt`, then `ctx.start >= *op.ready`.
// - Two executions with the same `ctx.thread` are sequential
//   (the future returned by one of them resolved before the other has started).
//
// The interpreter requires `op.execute` to finish eventually.
//
// If the generator returns an operation which does not specify `ready` or the specified `ready`
// satisfies `*ready <= ctx.now`, the interpreter calls `execute` immediately
// (there is no reactor yield in between). Note: by the above requirements, since the operation
// was fetched, at least one thread is free; if the operation specified a `thread`,
// it belongs to the set of free threads; thus it is possible to execute the operation.
//
// If the returned `pending` or an operation which specified a `*ready > ctx.now`,
// the interpreter will retry the fetch later.
//
// Given an operation `op`, before calling `execute`, the interpreter records the operation
// by calling the given `Recorder`. After `execute` finishes, the interpreter records
// the corresponding completion.
//
// When the interpreter run finishes, it is guaranteed that:
// - all operation executions have completed,
// - the generator has returned `finished`,
// - every recorded operation has a corresponding recorded completion.
template <operation::Invocable Op, GeneratorOf<Op> G, typename Recorder>
requires std::invocable<Recorder, Op> && std::invocable<Recorder, operation::completion<Op>>
class interpreter {
    G _generator;

    const operation::thread_set _all_threads;
    operation::thread_set _free_threads; // a subset of _all_threads

    raft::logical_clock::duration _poll_timeout;
    const raft::logical_clock::duration _max_pending_interval;

    future_set<std::pair<operation::execute_result<Op>, operation::thread_id>> _invocations;
    typename Op::state_type _op_state;

    logical_timer& _timer;

    Recorder _record;

public:
    // Create an interpreter for the given generator, set of threads, and initial state.
    // Use `timer` as a source of logical time.
    //
    // If the generator returns `pending`, the interpreter will wait for operation completions
    // for at most `max_pending_interval` before trying to fetch an operation again.
    // It may retry the fetch earlier if it manages to get a completion in the meantime.
    //
    // Operation invocations and completions are recorded using the `record` function.
    interpreter(G gen, operation::thread_set threads, raft::logical_clock::duration max_pending_interval,
                typename Op::state_type init_op_state, logical_timer& timer, Recorder record)
        : _generator(std::move(gen))
        , _all_threads(threads)
        , _free_threads(std::move(threads))
        , _max_pending_interval(max_pending_interval)
        , _op_state(std::move(init_op_state))
        , _timer(timer)
        , _record(std::move(record))
    {
        assert(!_all_threads.empty());
        assert(_max_pending_interval > raft::logical_clock::duration{0});
    }

    // Run the interpreter and record all operation invocations and completions.
    // Can only be executed once.
    future<> run() {
        while (true) {
            // If there are any completions we want to record them as soon as possible
            // so we don't introduce false concurrency for anomaly analyzers
            // (concurrency makes the problem of looking for anomalies harder).
            if (auto r = co_await _invocations.poll(_timer, _poll_timeout)) {
                auto [res, tid] = std::move(*r);

                assert(_all_threads.contains(tid));
                assert(!_free_threads.contains(tid));
                _free_threads.insert(tid);

                _record(operation::completion<Op> {
                    .result = std::move(res),
                    .time = _timer.now(),
                    .thread = tid
                });

                // In the next iteration poll synchronously so if there will be no more completions
                // to immediately fetch, we move on to the next generator operation.
                _poll_timeout = raft::logical_clock::duration{0};
                continue;
            }

            // No completions to handle at this time.
            if (_free_threads.empty()) {
                // No point in fetching an operation from the generator even if there is any;
                // there is no thread to execute the operation at this time.
                _poll_timeout = _max_pending_interval;
                continue;
            }

            // There is a free thread, check the generator.
            auto now = _timer.now();
            auto [op, next_gen] = _generator.fetch_op(context {
                .now = now,
                .all_threads = _all_threads,
                .free_threads = _free_threads,
            });

            bool stop = false;
            std::visit(make_visitor(
            [&stop] (finished) {
                stop = true;
            },
            [this] (pending) {
                _poll_timeout = _max_pending_interval;
            },
            [this, now, next_gen = std::move(next_gen)] (Op op) mutable {
                if (!op.ready) {
                    op.ready = now;
                }

                if (now < *op.ready) {
                    _poll_timeout = *op.ready - now;
                } else {
                    if (!op.thread) {
                        // We ensured !_free_threads.empty() before fetching the operation.
                        op.thread = some(_free_threads);
                    }

                    assert(_free_threads.contains(*op.thread));
                    _free_threads.erase(*op.thread);

                    _record(op);

                    operation::context ctx { .thread = *op.thread, .start = now};
                    _invocations.add(invoke(std::move(op), _op_state, std::move(ctx)));

                    _generator = std::move(next_gen);
                    _poll_timeout = raft::logical_clock::duration{0};
                }
            }
            ), std::move(op));

            if (stop) {
                co_await exit();
                co_return;
            }
        }
    }

    ~interpreter() {
        // Ensured by `exit()`.
        assert(_invocations.empty());
    }

private:
    future<> exit() {
        auto fs = _invocations.release();
        co_await parallel_for_each(fs, [this] (future<std::pair<operation::execute_result<Op>, operation::thread_id>>& f) -> future<> {
            auto [res, tid] = co_await std::move(f);
            _record(operation::completion<Op>{
                .result = std::move(res),
                .time = _timer.now(),
                .thread = tid
            });
        });
    }

    // Not a static method nor free function due to clang bug #50345.
    future<std::pair<operation::execute_result<Op>, operation::thread_id>>
    invoke(Op op, typename Op::state_type& op_state, operation::context ctx) {
        try {
            co_return std::pair{operation::execute_result<Op>{co_await op.execute(op_state, ctx)}, ctx.thread};
        // TODO: consider failing in case of unknown/unexpected exception instead of storing it
        } catch (...) {
            co_return std::pair{operation::execute_result<Op>{
                    operation::exceptional_result<Op>{std::move(op), std::current_exception()}},
                ctx.thread};
        }
    }
};

} // namespace generator

namespace operation {

template <typename Op>
std::ostream& operator<<(std::ostream& os, const exceptional_result<Op>& r) {
    try {
        std::rethrow_exception(r.eptr);
    } catch (const std::exception& e) {
        return os << format("exceptional{{i:{}, ex:{}}}", r.op, e);
    }
}

template <operation::Executable Op>
std::ostream& operator<<(std::ostream& os, const completion<Op>& c) {
    return os << format("c{{r:{}, t:{}, tid:{}}}", c.result, c.time, c.thread);
}

} // namespace operation
