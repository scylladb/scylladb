/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "readers/mutation_reader.hh"
#include "readers/mutation_reader_fwd.hh"

// A mutation reader together with an upper bound on the set of positions of fragments
// that the reader will return. The upper bound does not need to be exact.
struct reader_and_upper_bound {
    mutation_reader reader;
    position_in_partition upper_bound;

    reader_and_upper_bound(mutation_reader r, position_in_partition bound)
        : reader(std::move(r)), upper_bound(std::move(bound)) {}
};

// A queue of mutation readers returning fragments with the same schema from the same single partition.
//
// Intuitively, the order of returned readers is such that the positions of the first fragments
// returned by the readers inside the partition (after `partition_start`) are ``mostly increasing''.
//
// More formally:
// 1. The queue contains a sequence of readers.
//    Each call to `pop` consumes a batch of readers from the sequence.
// 2. Each position-in-partition `b` corresponds to a prefix of the sequence of readers in the queue.
//    Let's call it `pref(b)`.
// 3. If `b1 <= b2`, then `pref(b1)` is a prefix of `pref(b2)`.
// 4. `pref(position_in_partition::after_all_clustered_rows())` is the entire sequence.
// 5. For each `b`, `pop(b)` returns only readers from `pref(b)`.
// 6. For each `b`, all readers that lie in the sequence after `pref(b)`
//    satisfy the following property:
//        the first fragment returned by the reader has a position greater than `b`.
//    In other words, if `pop(b)` returns no readers, then we can be sure that all readers
//    returned later by the queue return fragments with positions greater than `b`.
//
// Considering the above properties, a simple legal implementation of this interface would
// return all readers on the first call to `pop(after_all_clustered_rows())` and would not return
// any readers on `pop(b)` for `b < after_all_clustered_rows()`.
//
// Better implementations may use information about positions returned by the readers
// to return some readers earlier, but they must not break property 6.
// For example, the following scenario is illegal:
// 1. pop(for_key(10)) returns r1
// 2. pop(for_key(10)) returns no readers => all readers from pref(for_key(10)) have been popped
// 3. pop(for_key(20)) returns r2 => due to the previous step we know that r2 is not in pref(for_key(10))
// 4. the first fragment (excluding partition_start) returned by r2 has position for_key(10)
//        => illegal, because for_key(10) is not greater than for_key(10).
//        The first position returned by r2 must be after_key(10) or higher.
//
// With each reader also comes an upper bound on the set of positions of fragments that the reader will return.
class position_reader_queue {
public:
    virtual ~position_reader_queue() = 0;

    // `empty(b)` <=>
    //      we have popped all readers from `pref(b)` so `pop(b)`
    //      will not return any more readers.
    virtual bool empty(position_in_partition_view bound) const = 0;

    // Return the next batch of readers from `pref(b)`.
    virtual std::vector<reader_and_upper_bound> pop(position_in_partition_view bound) = 0;

    // Close all readers
    virtual future<> close() noexcept = 0;
};

mutation_reader make_clustering_combined_reader(schema_ptr schema,
        reader_permit,
        streamed_mutation::forwarding,
        std::unique_ptr<position_reader_queue>);
