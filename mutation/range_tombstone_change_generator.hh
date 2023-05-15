/*
 * Copyright (C) 2021 ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "mutation/mutation_fragment_v2.hh"
#include "range_tombstone_list.hh"

template<typename T>
concept RangeTombstoneChangeConsumer = std::invocable<T, range_tombstone_change>;

/// Generates range_tombstone_change fragments for a stream of range_tombstone fragments.
///
/// The input range_tombstones passed to consume() may be overlapping, but must be weakly ordered by position().
/// It's ok to pass consecutive range_tombstone objects with the same position.
///
/// Generated range_tombstone_change fragments will have strictly monotonic positions.
///
/// Example usage:
///
///   consume(range_tombstone(1, +inf, t));
///   flush(2, consumer);
///   consume(range_tombstone(2, +inf, t));
///   flush(3, consumer);
///   consume(range_tombstone(4, +inf, t));
///   consume(range_tombstone(4, 7, t));
///   flush(5, consumer);
///   flush(6, consumer);
///
class range_tombstone_change_generator {
    range_tombstone_list _range_tombstones;
    // All range_tombstone_change fragments with positions < than this have been emitted.
    position_in_partition _lower_bound = position_in_partition::before_all_clustered_rows();
    const schema& _schema;
public:
    range_tombstone_change_generator(const schema& s)
        : _range_tombstones(s)
        , _schema(s)
    { }

    // Discards deletion information for positions < lower_bound.
    // After this, the lowest position of emitted range_tombstone_change will be before_key(lower_bound).
    void trim(const position_in_partition& lower_bound) {
        position_in_partition::less_compare less(_schema);

        if (lower_bound.is_clustering_row()) {
            _lower_bound = position_in_partition::before_key(lower_bound.key());
        } else {
            _lower_bound = lower_bound;
        }

        while (!_range_tombstones.empty() && !less(lower_bound, _range_tombstones.begin()->end_position())) {
            _range_tombstones.pop(_range_tombstones.begin());
        }

        if (!_range_tombstones.empty() && less(_range_tombstones.begin()->position(), _lower_bound)) {
            // _range_tombstones.begin()->end_position() < lower_bound is guaranteed by previous loop.
            _range_tombstones.begin()->tombstone().set_start(_lower_bound);
        }
    }

    // Emits range_tombstone_change fragments with positions smaller than upper_bound
    // for accumulated range tombstones. If end_of_range = true, range tombstone
    // change fragments with position equal to upper_bound may also be emitted.
    // After this, only range_tombstones with positions >= upper_bound may be added,
    // which guarantees that they won't affect the output of this flush.
    //
    // If upper_bound == position_in_partition::after_all_clustered_rows(),
    // emits all remaining range_tombstone_changes.
    // No range_tombstones may be added after this.
    //
    // FIXME: respect preemption
    template<RangeTombstoneChangeConsumer C>
    void flush(const position_in_partition_view upper_bound, C consumer, bool end_of_range = false) {
        if (_range_tombstones.empty()) {
            return;
        }

        position_in_partition::tri_compare cmp(_schema);
        std::optional<range_tombstone> prev;
        const bool allow_eq = upper_bound.is_after_all_clustered_rows(_schema);
        const auto should_flush = [&] (position_in_partition_view pos) {
            const auto res = cmp(pos, upper_bound);
            if (allow_eq) {
                return res <= 0;
            } else {
                return res < 0;
            }
        };

        while (!_range_tombstones.empty() && should_flush(_range_tombstones.begin()->end_position())) {
            auto rt = _range_tombstones.pop(_range_tombstones.begin());

            if (prev && (cmp(prev->end_position(), rt.position()) < 0)) { // [1]
                // previous range tombstone not adjacent, emit gap.
                consumer(range_tombstone_change(prev->end_position(), tombstone()));
            }

            // Check if start of rt was already emitted, emit if not.
            if (cmp(rt.position(), _lower_bound) >= 0) {
                consumer(range_tombstone_change(rt.position(), rt.tomb));
            }

            // Delay emitting end bound in case it's adjacent with the next tombstone. See [1] and [2]
            prev = std::move(rt);
        }

        // If previous range tombstone not adjacent with current, emit gap.
        // It cannot get adjacent later because prev->end_position() < upper_bound,
        // so nothing == prev->end_position() can be added after this invocation.
        if (prev && (_range_tombstones.empty()
                     || (cmp(prev->end_position(), _range_tombstones.begin()->position()) < 0))) {
            consumer(range_tombstone_change(prev->end_position(), tombstone())); // [2]
        }

        // Emit the fragment for start bound of a range_tombstone which is overlapping with upper_bound,
        // unless no such fragment or already emitted.
        if (!_range_tombstones.empty()
            && (cmp(_range_tombstones.begin()->position(), upper_bound) < 0)
            && (cmp(_range_tombstones.begin()->position(), _lower_bound) >= 0)) {
            consumer(range_tombstone_change(
                    _range_tombstones.begin()->position(), _range_tombstones.begin()->tombstone().tomb));
        }

        // Close current tombstone (if any) at upper_bound if end_of_range is
        // set, so a sliced read will have properly closed range tombstone bounds
        // at each range end
        if (!_range_tombstones.empty()
                && end_of_range
                && (cmp(_range_tombstones.begin()->position(), upper_bound) < 0)) {
            consumer(range_tombstone_change(upper_bound, tombstone()));
        }

        _lower_bound = upper_bound;
    }

    void consume(range_tombstone rt) {
        _range_tombstones.apply(_schema, std::move(rt));
    }

    void reset() {
        _range_tombstones.clear();
        _lower_bound = position_in_partition::before_all_clustered_rows();
    }

    bool discardable() const {
        return _range_tombstones.empty();
    }
};
