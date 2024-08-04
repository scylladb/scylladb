/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <unordered_set>
#include <random>

#include <boost/mp11/algorithm.hpp>
#include <boost/implicit_cast.hpp>

#include <fmt/std.h>

#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/util/variant_utils.hh>
#include "utils/assert.hh"
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
    SCYLLA_ASSERT(!s.empty());
    return *s.begin();
}

template <size_t... I>
auto take_impl(const std::vector<thread_id>& vec, std::index_sequence<I...>) {
    return std::make_tuple(vec[I]...);
}

template <size_t N>
auto take(const thread_set& s) {
    SCYLLA_ASSERT(N <= s.size());
    auto end = s.begin();
    std::advance(end, N);
    std::vector<thread_id> vec{s.begin(), end};
    return take_impl(vec, std::make_index_sequence<N>{});
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
    { o.ready } -> std::convertible_to<std::optional<raft::logical_clock::time_point>>;
};

// An operation which can specify a specific thread on which it can be executed.
// We will say that such operations `understand threads`.
// If the operation specifies a thread, it won't be executed on any other thread.
template <typename Op>
concept HasThread = requires(Op o) {
    { o.thread } -> std::convertible_to<std::optional<thread_id>>;
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

    raft::logical_clock::duration _poll_timeout{0};
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
        SCYLLA_ASSERT(!_all_threads.empty());
        SCYLLA_ASSERT(_max_pending_interval > raft::logical_clock::duration{0});
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

                SCYLLA_ASSERT(_all_threads.contains(tid));
                SCYLLA_ASSERT(!_free_threads.contains(tid));
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

                    SCYLLA_ASSERT(_free_threads.contains(*op.thread));
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
        SCYLLA_ASSERT(_invocations.empty());
    }

private:
    future<> exit() {
        auto fs = _invocations.release();
        co_await coroutine::parallel_for_each(fs, [this] (future<std::pair<operation::execute_result<Op>, operation::thread_id>>& f) -> future<> {
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

// Given an Executable operation type create an Invocable operation type.
template <Executable Op>
struct invocable : public Op {
    using typename Op::result_type;
    using typename Op::state_type;

    using Op::execute;
    using Op::Op;

    std::optional<raft::logical_clock::time_point> ready;
    std::optional<thread_id> thread;
};

// Given a non-empty set of Executable operation types, create an Executable operation type representing their union (sum type).
//
// The state type of the union is a product of the original state types,
// i.e. the state of the union contains the states of each operation.
// The result type is a union of the original result types.
//
// An operation of this type is an operation of one of the original types.
// The state provided to `execute` is a projection of the product state
// onto the state type corresponding to the original operation type.
template <Executable... Ops>
struct either_of {
    static_assert((std::is_same_v<Ops, Ops> || ...)); // pack is non-empty

    std::variant<Ops...> op;

    using result_type = std::variant<typename Ops::result_type...>;
    using state_type = std::tuple<typename Ops::state_type...>;

    future<result_type> execute(state_type& s, const context& ctx) {
        // `co_return co_await` instead of simply `return` to keep the lambda alive during `execute`
        co_return co_await boost::mp11::mp_with_index<std::tuple_size_v<state_type>>(op.index(),
            [&s, &ctx, this] (auto I) -> future<result_type> {
                co_return result_type{co_await std::get<I>(op).execute(std::get<I>(s), ctx)};
            });
    }

    template <Executable Op>
    requires (std::is_same_v<Op, Ops> || ...) // Ops contain Op
    either_of(Op o) : op(std::move(o)) {
        static_assert(Executable<either_of<Ops...>>);
    }
};

} // namespace operation

// Convert a copy/move constructible type to a copy/move assignable type.
template <typename T>
requires std::is_nothrow_move_constructible_v<T>
class assignable {
    // Always engaged. Stored in optional for its `emplace` member function.
    std::optional<T> _v;

public:
    assignable(T v) : _v(std::move(v)) {}
    assignable(const assignable&) = default;
    assignable(assignable&&) = default;

    assignable& operator=(assignable a) {
        // nothrow_move_constructible guarantees that this does not throw:
        _v.emplace(std::move(*a._v));
        return *this;
    }

    operator T&() { return *_v; }
    operator const T&() const { return *_v; }
};

namespace generator {

template <std::invocable<std::mt19937&> F>
requires std::is_nothrow_move_constructible_v<F>
struct random_gen {
    using operation_type = std::invoke_result_t<F, std::mt19937&>;

    op_and_gen<operation_type, random_gen> fetch_op(const context&) const {
        auto e = _engine;
        auto r = boost::implicit_cast<F>(_f)(e);
        return {std::move(r), random_gen{std::move(e), _f}};
    }

    std::mt19937 _engine;
    assignable<F> _f;
};

// Given an operation factory that uses a pseudo-random number generator to produce an operation,
// creates a generator that produces an infinite random sequence of operations from the factory.
template <std::invocable<std::mt19937&> F>
requires std::is_nothrow_move_constructible_v<F>
random_gen<F> random(int seed, F f) {
    static_assert(GeneratorOf<random_gen<F>, std::invoke_result_t<F, std::mt19937&>>);
    return random_gen<F>{std::mt19937{seed}, std::move(f)};
}

template <std::invocable<> F>
requires std::is_nothrow_move_constructible_v<F>
struct constant_gen {
    using operation_type = std::invoke_result_t<F>;

    op_and_gen<operation_type, constant_gen> fetch_op(const context&) const {
        return {boost::implicit_cast<F>(_f)(), *this};
    }

    assignable<F> _f;
};

// Given an operation factory of no arguments,
// creates a generator that produces an infinite constant sequence of operations.
template <std::invocable<> F>
requires std::is_nothrow_move_constructible_v<F>
constant_gen<F> constant(F f) {
    static_assert(GeneratorOf<constant_gen<F>, std::invoke_result_t<F>>);
    return constant_gen<F>{std::move(f)};
}

template <std::invocable<int32_t> F>
requires std::is_nothrow_move_constructible_v<F>
struct sequence_gen {
    using operation_type = std::invoke_result_t<F, int32_t>;

    op_and_gen<operation_type, sequence_gen> fetch_op(const context&) const {
        return {boost::implicit_cast<F>(_f)(_next), sequence_gen{_next + 1, _f}};
    }

    int32_t _next;
    assignable<F> _f;
};

// Given an operation factory using an integer to produce an operation,
// creates a generator that produces an infinite sequence of operations from the factory
// by passing consecutive integers starting from `start`.
template <std::invocable<int32_t> F>
requires std::is_nothrow_move_constructible_v<F>
sequence_gen<F> sequence(int32_t start, F f) {
    static_assert(GeneratorOf<sequence_gen<F>, std::invoke_result_t<F, int32_t>>);
    return sequence_gen<F>{start, std::move(f)};
}

template <Generator G>
struct op_limit_gen {
    using operation_type = typename G::operation_type;

    op_and_gen<operation_type, op_limit_gen> fetch_op(const context& ctx) const {
        if (!_remaining) {
            return {finished{}, *this};
        }

        auto [op, new_g] = _g.fetch_op(ctx);
        return {std::move(op), op_limit_gen{_remaining - 1, std::move(new_g)}};
    }

    size_t _remaining;
    G _g;
};

// Given a generator and an operation number `limit`,
// creates a generator which truncates the sequence of operations returned by the original generator
// to the first `limit` operations.
// If the original generator finishes earlier, `op_limit` has no observable effect.
template <Generator G>
op_limit_gen<G> op_limit(size_t limit, G g) {
    static_assert(GeneratorOf<op_limit_gen<G>, typename G::operation_type>);
    return op_limit_gen<G>{limit, std::move(g)};
}

template <Generator G, std::predicate<operation::thread_id> P>
requires operation::HasThread<typename G::operation_type>
struct on_threads_gen {
    using operation_type = typename G::operation_type;

    op_and_gen<operation_type, on_threads_gen> fetch_op(const context& ctx) const {
        operation::thread_set masked_all_threads;
        masked_all_threads.reserve(ctx.all_threads.size());
        for (auto& t : ctx.all_threads) {
            if (_p(t)) {
                masked_all_threads.insert(t);
            }
        }

        if (masked_all_threads.empty()) {
            return {finished{}, *this};
        }

        operation::thread_set masked_free_threads;
        masked_free_threads.reserve(ctx.free_threads.size());
        for (auto& t : ctx.free_threads) {
            if (_p(t)) {
                masked_free_threads.insert(t);
            }
        }

        if (masked_free_threads.empty()) {
            return {pending{}, *this};
        }

        auto [op, new_g] = _g.fetch_op(context {
            .now = ctx.now,
            .all_threads = masked_all_threads,
            .free_threads = masked_free_threads
        });

        if (auto i = std::get_if<operation_type>(&op)) {
            if (i->thread) {
                SCYLLA_ASSERT(masked_free_threads.contains(*i->thread));
            } else {
                // The underlying generator didn't assign a thread so we do it.
                i->thread = some(masked_free_threads);
            }
        }

        return {std::move(op), on_threads_gen{_p, std::move(new_g)}};
    }

    P _p;
    G _g;
};

// Given a generator whose operations understand threads and a predicate on threads,
// creates a generator which produces operations which always specify a thread
// and the specified threads always satisfy the predicate.
//
// Finishes when the original generator finishes or no thread satisfies the predicate.
// If some thread satisfies the predicate but no thread satisfying the predicate
// is currently free, returns `pending`.
//
// The underlying generator must respect `free_threads`, i.e. it must satisfy the following:
// if the returned operation specifies a thread, the thread must be one of the `free_threads` passed in the context.
template <Generator G, std::predicate<operation::thread_id> P>
requires operation::HasThread<typename G::operation_type>
on_threads_gen<G, P> on_threads(P p, G g) {
    static_assert(GeneratorOf<on_threads_gen<G, P>, typename G::operation_type>);
    return on_threads_gen<G, P>{std::move(p), std::move(g)};
}

// Compare two generator return values for which one is ``sooner''.
// Operations are sooner than `pending`, `pending` is sooner than `finished`.
// For two operations which both specify a ready time, their ready times are compared.
// If one of them doesn't specify a ready time, we assume it is to be executed ``as soon as possible'',
// so we assume that its ready time is the current time for comparison purposes (passed in `now`).
//
// The return type is `weak_ordering` where smaller represents sooner.
template <typename Op>
requires operation::HasReadyTime<Op>
std::weak_ordering sooner(const op_ret<Op>& r1, const op_ret<Op>& r2, raft::logical_clock::time_point now) {
    auto op1 = std::get_if<Op>(&r1);
    auto op2 = std::get_if<Op>(&r2);

    if (op1 && op2) {
        auto t1 = op1->ready ? *op1->ready : now;
        auto t2 = op2->ready ? *op2->ready : now;

        return t1 <=> t2;
    }

    // one or both of them is not an operation (it's pending or finished)
    // operation is sooner than pending which is sooner than finished
    // since operation index = 0, pending index = 1, finished index = 2, this works:
    return r1.index() <=> r2.index();
}

template <Generator G1, Generator G2>
requires std::is_same_v<typename G1::operation_type, typename G2::operation_type> && operation::HasReadyTime<typename G1::operation_type>
struct either_gen {
    using operation_type = typename G1::operation_type;

    op_and_gen<operation_type, either_gen> fetch_op(const context& ctx) const {
        auto [op1, new_g1] = _g1.fetch_op(ctx);
        auto [op2, new_g2] = _g2.fetch_op(ctx);

        auto ord = sooner(op1, op2, ctx.now);
        bool new_use_g1_on_tie = ord == std::weak_ordering::equivalent ? !_use_g1_on_tie : _use_g1_on_tie;

        if ((ord == std::weak_ordering::equivalent && _use_g1_on_tie) || ord == std::weak_ordering::less) {
            return {std::move(op1), either_gen{new_use_g1_on_tie, std::move(new_g1), _g2}};
        }

        return {std::move(op2), either_gen{new_use_g1_on_tie, _g1, std::move(new_g2)}};
    }

    // Which generator to use when a tie is detected
    bool _use_g1_on_tie = true;

    G1 _g1;
    G2 _g2;
};

// Given two generators producing the same type of operations which understand time,
// creates a generator whose each produced operation comes from one of the underlying generators.
//
// Between operations produced by both generators, chooses the one which is `sooner`.
// In particular finishes when both generators finish.
//
// On the first tie takes an operation from `g1`. On the second and subsequent ties
// takes an operation from the generator which was not used on the previous tie.
template <Generator G1, Generator G2>
requires std::is_same_v<typename G1::operation_type, typename G2::operation_type>
        && operation::HasReadyTime<typename G1::operation_type>
either_gen<G1, G2> either(G1 g1, G2 g2) {
    static_assert(GeneratorOf<either_gen<G1, G2>, typename G1::operation_type>);
    return either_gen<G1, G2>{._g1{std::move(g1)}, ._g2{std::move(g2)}};
}

// Given a thread and two generators producing the same type of operations which understand threads and time,
// creates a generator whose each produced operation comes from one of the underlying generators;
// furthermore, ensures that all operations from `g1` are assigned to the given thread while all operations
// from `g2` are assigned to other threads.
//
// Ties are resolved as in `either`.
//
// Assumes that underlying generators respect `free_threads` (as in `on_threads`).
template <Generator G1, Generator G2>
requires std::is_same_v<typename G1::operation_type, typename G2::operation_type>
    && operation::HasReadyTime<typename G1::operation_type>
    && operation::HasThread<typename G1::operation_type>
GeneratorOf<typename G1::operation_type> auto pin(operation::thread_id to, G1 g1, G2 g2) {
    // Not using lambda because we need the copy constructor
    struct on_pinned_t {
        operation::thread_id _to;
        bool operator()(const operation::thread_id& tid) const { return tid == _to; };
    } on_pinned {to};

    // Not using std::not_fn for the same reason
    struct out_of_pinned_t {
        operation::thread_id _to;
        bool operator()(const operation::thread_id& tid) const { return tid != _to; };
    } out_of_pinned {to};

    auto gen = either(
        on_threads(on_pinned, std::move(g1)),
        on_threads(out_of_pinned, std::move(g2))
    );

    static_assert(GeneratorOf<decltype(gen), typename G1::operation_type>);
    return gen;
}

template <Generator G>
requires operation::HasReadyTime<typename G::operation_type>
struct stagger_gen {
    using operation_type = typename G::operation_type;
    using distribution_type = std::uniform_int_distribution<raft::logical_clock::rep>;

    op_and_gen<operation_type, stagger_gen> fetch_op(const context& ctx) const {
        auto [res, new_g] = _g.fetch_op(ctx);

        if (auto op = std::get_if<operation_type>(&res)) {
            if (!op->ready || op->ready < _next_ready) {
                // Need to delay the operation.
                op->ready = _next_ready;
            }

            auto e = _engine;
            distribution_type delta{_delta.param()};
            auto next_ready = _next_ready + raft::logical_clock::duration{delta(e)};
            return {std::move(res), stagger_gen{
                std::move(e), delta,
                next_ready, std::move(new_g)}
            };
        }

        // pending or finished, nothing to do
        return {std::move(res), *this};
    }

    std::mt19937 _engine;
    distribution_type _delta;

    raft::logical_clock::time_point _next_ready;
    G _g;
};

// Given a generator whose operations understand time,
// a logical `start` time point, and an interval of logical time durations [`delta_low`, `delta_high`],
// produces a sequence of time points where each pair of subsequent time points differs by a time duration
// chosen in random uniformly from the [`delta_low`, `delta_high`] interval;
// then, for each operation in the sequence of operations produced by the underlying generator, modifies its `ready`
// time to be at least as late as the corresponding time point in the above sequence.
//
// For example, if we randomly choose the sequence of time points [10, 20, 30], and the `ready` times specified
// by operations from the underlying generator are [5, `nullopt`, 34], the sequence of operations produced
// by the new generator will be [10, 20, 34].
//
// In particular if `delta = delta_low = delta_high` and the underlying operations never specify `ready`,
// the produced operations are scheduled to execute every `delta` ticks.
template <Generator G>
requires operation::HasReadyTime<typename G::operation_type>
stagger_gen<G> stagger(
        int seed, raft::logical_clock::time_point start,
        raft::logical_clock::duration delta_low, raft::logical_clock::duration delta_high,
        G g) {
    static_assert(GeneratorOf<stagger_gen<G>, typename G::operation_type>);
    return stagger_gen<G>{ ._engine{seed}, ._delta{delta_low.count(), delta_high.count()}, ._next_ready{start}, ._g{std::move(g)} };
}

} // namespace generator

template <operation::Executable... Ops>
struct fmt::formatter<operation::either_of<Ops...>> : fmt::formatter<string_view> {
    auto format(operation::either_of<Ops...>& e, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", e.op);
    }
};

template <typename Op>
struct fmt::formatter<operation::exceptional_result<Op>> : fmt::formatter<string_view> {
    auto format(const operation::exceptional_result<Op>& r, fmt::format_context& ctx) const {
        try {
            std::rethrow_exception(r.eptr);
        } catch (const std::exception& e) {
            return fmt::format_to(ctx.out(), "exceptional{{i:{}, ex:{}}}", r.op, e);
        }
    }
};

template <operation::Executable Op>
struct fmt::formatter<operation::completion<Op>> : fmt::formatter<string_view> {
    auto format(const operation::completion<Op>& c, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "c{{r:{}, t:{}, tid:{}}}", c.result, c.time, c.thread);
    }
};

template <operation::Executable Op>
struct fmt::formatter<operation::invocable<Op>> : fmt::formatter<string_view> {
    auto format(const operation::invocable<Op>& op, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", static_cast<Op>(op));
    }
};
