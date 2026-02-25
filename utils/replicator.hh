/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include "utils/assert.hh"

#include <seastar/util/later.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/coroutine.hh>
#include "utils/on_internal_error.hh"

namespace {

// Mutation represents state and changes to state.
//
// Algebra operations are defined in the Container concept.
//
// Changes can be combined:
//
//   m3 = m1 + m2
//
// Changes are associative:
//
//   ((state + m1) + m2) == (state + (m1 + m2))
//
// They don't have to be commutative.
//
// There must be a neutral element:
//
//   m1 + Mutation() == m1
//
// And be idempotent:
//
//   m1 + m1 == m1
//
template <typename M>
concept Mutation =
    std::default_initializable<M> &&
    std::move_constructible<M> &&
    std::is_nothrow_move_constructible_v<M> &&
    std::is_nothrow_move_assignable_v<M> &&
    requires(M m, const M& a, const M& b)
{
    // Neutral element
    { M{} } -> std::same_as<M>;

    // Checks if neutral element.
    // M().empty() == true
    { a.empty() } -> std::convertible_to<bool>;

    { a == b } -> std::convertible_to<bool>;
};

// Holds state and defines algebra on Mutation and MutationFragment objects.
template <typename C, typename M>
concept Container =
    Mutation<M> && requires(C& c, M& dst, const M& src)
{
    // Applies src to state.
    //
    // The state should be compatible with Mutation, meaning
    // apply_locally() should behave as if state was Mutation m
    // and apply_locally(src) did C::apply(m, src).
    // Same exception guarantees as C::apply(m, src).
    { c.apply_locally(src) } -> std::same_as<void>;

    // Performs: dst = dst + src
    //
    // Idempotent:
    //
    //    apply(dst, dst) == dst
    //
    // Has a neutral element:
    //
    //    apply(dst, M()) == dst
    //
    // May throw, but must leave dst valid and include at least all updates it had before the call.
    // Subsequent C::apply(dst, src) after exception should give the same result as if exception was not thrown.
    { C::apply(dst, src) } -> std::same_as<void>;
};

// MutationFragment represents a subset of Mutation.
// There is always an implicit Mutation to which MutationFragment can be converted,
// and algebra between Mutation and MutationFragment works as if the latter was that corresponding Mutation.
template <typename MF, typename C, typename M>
concept MutationFragment =
    Mutation<M> &&
    Container<C, M> &&
    requires(C& c, M& dst, const MF& src, const std::exception_ptr& e)
{
    // Applies src to state.
    // Semantically equivalent to c.apply_locally(C::apply(M(), src))
    // Strong exception guarantees: either all of src is applied or none of it.
    { c.apply_locally(src) } -> std::same_as<void>;

    // Performs: dst = dst + src
    //
    // Must not throw if prepare_apply(dst, src) was called immediately before.
    { C::apply(dst, src) } -> std::same_as<void>;

    // Reserves space in dst for application of src so that C::apply(dst, src) doesn't throw.
    { C::prepare_apply(dst, src) } -> std::same_as<void>;

    { c.container() } -> std::same_as<seastar::sharded<C>&>;

    // Called when replication to other shards fails with an exception, giving
    // the container a chance to log it.
    // Replication will be retried.
    { c.on_replication_failed(e) } -> std::same_as<void>;
};

}

/// Implements efficient and reliable replication of complex data structures
/// from the owning shard to other shards.
///
/// All instance methods must be called on the same shard (the owning shard).
///
/// Changes are applied to the owning shard first and accumulated
/// in a side data structure for replication to other shards.
/// Replication happens in the background, and a single
/// smp call replicates all changes accumulated so far.
/// This way throughput of replication is not impacted
/// by latency of cross-shard calls. A bad solution would be
/// to serialize cross-shard calls for each change, which
/// can cause queues to accumulate if cross-shard latency is higher
/// than change arrival period.
///
/// Replication is reliable in a sense that no updates successfully
/// applied to the owning shard are lost, and will eventually be replicated
/// to other shards.
///
/// Changes are represented as Mutation objects, which must
/// be associative:
///
///   ((state + m1) + m2) == (state + (m1 + m2))
///
/// and idempotent:
///
///   m1 + m1 == m1
///
/// and have a null element:
///
///   m1 + Mutation() == m1
///
///   // Returns true iff null element
///   Mutation::empty() -> bool
///
/// Mutations do not have to be commutative.
///
/// Deletions must be represented as mutations so that they're not lost.
/// There is no safe garbage-collection implemented yet.
///
/// State is not kept here but in the external Container, and its model can extend the Mutation object,
/// but must be compactible with it. Applying a Mutation to the container should semantically be equivalent
/// to applying it to a Mutation which represents the state.
///
/// Must call and await stop() before destruction.
template<Mutation mutation_type, Container<mutation_type> container_type>
class replicator {
    mutation_type _to_replicate;
    uint64_t _version = 0; // Versioning of the owning shard changes.
    uint64_t _replicated_version = 0; // Highest version replicated to all shards.
    seastar::condition_variable _replicate_cv; // Signals changes of _to_replicate or _stopping.
    seastar::condition_variable _replicate_done_cv; // Signals changes of _replicated_version.
    bool _stopping = false;
    bool _stopped = false;
    container_type& _container;
    seastar::future<> _replicate_fiber;

private:
    seastar::future<> start_replicating() {
        mutation_type updates;
        while (true) {
            co_await _replicate_cv.when([&] {
                return !updates.empty() || !_to_replicate.empty() || _stopping;
            });

            if (_stopping) {
                break;
            }

            try {
                if (!updates.empty()) {
                    // Replication failed
                    co_await seastar::sleep(std::chrono::seconds(1));
                    container_type::apply(updates, _to_replicate);
                } else {
                    updates = std::move(_to_replicate);
                }
                auto updates_version = _version;
                _to_replicate = {};
                co_await _container.container().invoke_on_others([&](container_type& ctr) {
                    ctr.apply_locally(updates);
                });
                updates = {};
                _replicated_version = updates_version;
                _replicate_done_cv.broadcast();
            } catch (...) {
                _container.on_replication_failed(std::current_exception());
            }
        }
    }

public:
    explicit replicator(container_type& ctr)
        : _container(ctr)
        , _replicate_fiber(start_replicating())
    { }

    replicator(replicator&&) = delete; // "this" captured in start_replicating().

    ~replicator() {
        SCYLLA_ASSERT(_stopped);
    }

    // The change is applied locally immediately, and to other shards eventually.
    // If throws, no change is applied anywhere.
    // Changes are applied in the same order on all shards, so all shards
    // should eventually converge.
    template<MutationFragment<container_type, mutation_type> M>
    void apply_to_all(M m) {
        container_type::prepare_apply(_to_replicate, m); // ensure [1] below does not fail.
        _container.apply_locally(m); // Must not throw after this.
        std::invoke([&] () noexcept {
            container_type::apply(_to_replicate, std::move(m)); // [1]
            ++_version;
            _replicate_cv.signal();
        });
    }

    // Resolves when all updates already applied locally replicate everywhere.
    seastar::future<> barrier() {
        auto v = _version;
        co_await _replicate_done_cv.when([&] {
            return _replicated_version >= v;
        });
    }

    seastar::future<> stop() noexcept {
        if (_stopped) {
            co_return;
        }
        if (_stopping) {
            utils::on_fatal_internal_error("replicator::stop() called twice");
        }
        _stopping = true;
        _replicate_cv.signal();
        co_await std::move(_replicate_fiber);
        _stopped = true;
    }
};
