/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "reader_concurrency_semaphore.hh"
#include "readers/mutation_reader_fwd.hh"
#include "readers/mutation_reader.hh"
#include "tracing/trace_state.hh"
#include "seastarx.hh"
#include "locator/abstract_replication_strategy.hh"

/// Reader lifecycle policy for the mulitshard combining reader.
///
/// This policy is expected to make sure any additional resource the readers
/// might need is kept alive for the lifetime of the readers, not that
/// of the multishard reader. This is a very important distinction. As
/// destructors cannot return futures, the multishard reader will be
/// destroyed before all it's shard readers could stop properly. Hence it
/// is the duty of this policy to make sure all objects the shard readers
/// depend on stay alive until they are properly destroyed on their home
/// shards. Note that this also includes the passed in `range` and `slice`
/// parameters because although client code is required to keep them alive as
/// long as the top level reader lives, the shard readers might outlive the
/// multishard reader itself.
class reader_lifecycle_policy_v2 {
public:
    struct stopped_reader {
        reader_concurrency_semaphore::inactive_read_handle handle;
        mutation_reader::tracked_buffer unconsumed_fragments;
    };

public:
    /// Create an appropriate reader on the shard it is called on.
    ///
    /// Will be called when the multishard reader visits a shard for the
    /// first time or when a reader has to be recreated after having been
    /// evicted (while paused). This method should also enter gates, take locks
    /// or whatever is appropriate to make sure resources it is using on the
    /// remote shard stay alive, during the lifetime of the created reader.
    ///
    /// The \c permit parameter shall be obtained via `obtain_reader_permit()`
    virtual mutation_reader create_reader(
            schema_ptr schema,
            reader_permit permit,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            tracing::trace_state_ptr trace_state,
            mutation_reader::forwarding fwd_mr) = 0;

    /// Retrieves the read-range for the shard reader.
    ///
    /// That lives on the current shard. Returns nullptr if there is no
    /// reader on the current shard.
    virtual const dht::partition_range* get_read_range() const = 0;

    /// Updates the read-range of the shard reader.
    ///
    /// Gives the lifecycle-policy a chance to update its stored read-range (if
    /// the case). Called after any modification to the read range (typically
    /// after fast_forward_to()). The range is identical to the one the reader
    /// holds a reference to after the modification happened. When this method
    /// is called, it is safe to destroy the previous range instance.
    ///
    /// This method has to be called on the shard the reader lives on.
    virtual void update_read_range(lw_shared_ptr<const dht::partition_range> pr) = 0;

    /// Destroy the shard reader.
    ///
    /// Will be called when the multishard reader is being destroyed. It will be
    /// called for each of the shard readers.
    /// This method is expected to do a proper cleanup, that is, leave any gates,
    /// release any locks or whatever is appropriate for the shard reader.
    ///
    /// This method has to be called on the shard the reader lives on.
    /// This method will be called from a destructor so it cannot throw.
    virtual future<> destroy_reader(stopped_reader reader) noexcept = 0;

    /// Get the relevant semaphore for this read.
    ///
    /// The semaphore is used to register paused readers with as inactive
    /// readers. The semaphore then can evict these readers when resources are
    /// in-demand.
    /// The multishard reader will pause and resume readers via the `pause()`
    /// and `try_resume()` helper methods. Clients can resume any paused readers
    /// after the multishard reader is destroyed via the same helper methods.
    ///
    /// This method will be called on the shard where the relevant reader lives.
    virtual reader_concurrency_semaphore& semaphore() = 0;

    /// Obtain an admitted permit.
    ///
    /// The permit will be associated with the semaphore returned by
    /// `semaphore()`.
    ///
    /// This method will be called on the shard where the relevant reader lives.
    virtual future<reader_permit> obtain_reader_permit(schema_ptr schema, const char* const description, db::timeout_clock::time_point timeout,
            tracing::trace_state_ptr trace_ptr) = 0;
};

/// Make a multishard_combining_reader.
///
/// multishard_combining_reader takes care of reading a range from all shards
/// that own a subrange in the range. Shard reader are created on-demand, when
/// the shard is visited for the first time.
///
/// The read starts with a concurrency of one, that is the reader reads from a
/// single shard at a time. The concurrency is exponentially increased (to a
/// maximum of the number of shards) when a reader's buffer is empty after
/// moving the next shard. This condition is important as we only want to
/// increase concurrency for sparse tables that have little data and the reader
/// has to move between shards often. When concurrency is > 1, the reader
/// issues background read-aheads to the next shards so that by the time it
/// needs to move to them they have the data ready.
/// For dense tables (where we rarely cross shards) we rely on the
/// foreign_reader to issue sufficient read-aheads on its own to avoid blocking.
///
/// The readers' life-cycles are managed through the supplied lifecycle policy.
mutation_reader make_multishard_combining_reader_v2(
        shared_ptr<reader_lifecycle_policy_v2> lifecycle_policy,
        schema_ptr schema,
        locator::effective_replication_map_ptr erm,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        tracing::trace_state_ptr trace_state = nullptr,
        mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::no);

mutation_reader make_multishard_combining_reader_v2_for_tests(
        const dht::sharder& sharder,
        shared_ptr<reader_lifecycle_policy_v2> lifecycle_policy,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        tracing::trace_state_ptr trace_state = nullptr,
        mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::no);

