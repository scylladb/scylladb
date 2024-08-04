/*
 * Copyright (C) 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/assert.hh"
#include "schema/schema.hh"
#include "query-request.hh"
#include "mutation/mutation_fragment.hh"
#include "mutation/mutation_fragment_v2.hh"

// Utility for in-order checking of overlap with position ranges.
class clustering_ranges_walker {
    const schema& _schema;
    const query::clustering_row_ranges& _ranges;
    boost::iterator_range<query::clustering_row_ranges::const_iterator> _current_range;
    bool _in_current; // next position is known to be >= _current_start
    bool _past_current; // next position is known to be >= _current_end
    bool _using_clustering_range; // Whether current range comes from _current_range
    bool _with_static_row;
    position_in_partition_view _current_start;
    position_in_partition_view _current_end;
    std::optional<position_in_partition> _trim;
    size_t _change_counter = 1;
    tombstone _tombstone;
private:
    bool advance_to_next_range() {
        _in_current = false;
        _past_current = false;
        if (_using_clustering_range) {
            if (!_current_range) {
                return false;
            }
            _current_range.advance_begin(1);
        }
        ++_change_counter;
        _using_clustering_range = true;
        if (!_current_range) {
            _current_end = _current_start = position_in_partition_view::after_all_clustered_rows();
            return false;
        }
        _current_start = position_in_partition_view::for_range_start(_current_range.front());
        _current_end = position_in_partition_view::for_range_end(_current_range.front());
        return true;
    }

    void set_current_positions() {
        _using_clustering_range = false;
         if (!_with_static_row) {
            if (!_current_range) {
                _current_start = position_in_partition_view::before_all_clustered_rows();
            } else {
                _current_start = position_in_partition_view::for_range_start(_current_range.front());
                _current_end = position_in_partition_view::for_range_end(_current_range.front());
                _using_clustering_range = true;
            }
        } else {
             // If the first range is contiguous with the static row, then advance _current_end as much as we can
             if (_current_range && !_current_range.front().start()) {
                 _current_end = position_in_partition_view::for_range_end(_current_range.front());
                 _using_clustering_range = true;
             }
        }
    }

public:
    clustering_ranges_walker(const schema& s, const query::clustering_row_ranges& ranges, bool with_static_row = true)
            : _schema(s)
            , _ranges(ranges)
            , _current_range(ranges)
            , _in_current(with_static_row)
            , _past_current(false)
            , _with_static_row(with_static_row)
            , _current_start(position_in_partition_view::for_static_row())
            , _current_end(position_in_partition_view::before_all_clustered_rows()) {
        set_current_positions();
    }

    clustering_ranges_walker(const clustering_ranges_walker&) = delete;
    clustering_ranges_walker(clustering_ranges_walker&&) = delete;

    clustering_ranges_walker& operator=(const clustering_ranges_walker&) = delete;
    clustering_ranges_walker& operator=(clustering_ranges_walker&&) = delete;

    using range_tombstones = utils::small_vector<range_tombstone_change, 1>;

    // Result of advancing to a given position.
    struct progress {
        // True iff the position is contained in requested ranges.
        bool contained;

        // Range tombstone changes to emit which reflect current range tombstone
        // trimmed to requested ranges, up to the advanced-to position (inclusive).
        //
        // It is guaranteed that the sequence of tombstones returned from all
        // advance_to() calls will be the same for a given ranges no matter at
        // which positions you call advance_to(), provided that you change
        // the current tombstone at the same positions.
        // Redundant changes will not be generated.
        // This is to support the guarantees of mutation_reader.
        range_tombstones rts;
    };

    // Excludes positions smaller than pos from the ranges.
    // pos should be monotonic.
    // No constraints between pos and positions passed to advance_to().
    //
    // After the invocation, when !out_of_range(), lower_bound() returns the smallest position still contained.
    //
    // After this, advance_to(lower_bound()) will always emit a range tombstone change for pos
    // if there is an active range tombstone and !out_of_range().
    void trim_front(position_in_partition pos) {
        position_in_partition::less_compare less(_schema);

        do {
            if (!less(_current_start, pos)) {
                break;
            }
            if (less(pos, _current_end)) {
                _trim = std::move(pos);
                _current_start = *_trim;
                _in_current = false;
                ++_change_counter;
                break;
            }
        } while (advance_to_next_range());
    }

    // Returns true if given position is contained.
    // Must be called with monotonic positions.
    // Idempotent.
    bool advance_to(position_in_partition_view pos) {
        return advance_to(pos, _tombstone).contained;
    }

    // Returns result of advancing over clustering restrictions.
    // Must be called with monotonic positions.
    //
    // The walker tracks current clustered tombstone.
    // The new_tombstone will be the current clustered tombstone after advancing, starting from pos (inclusive).
    // The returned progress object contains range_tombstone_change fragments which reflect changes of
    // the current clustered tombstone trimmed to the boundaries of requested ranges, up to the
    // advanced-to position (inclusive).
    progress advance_to(position_in_partition_view pos, tombstone new_tombstone) {
        position_in_partition::less_compare less(_schema);
        range_tombstones rts;

        auto prev_tombstone = _tombstone;
        _tombstone = new_tombstone;

        do {
            if (!_in_current && less(pos, _current_start)) {
                break;
            }

            if (!_in_current && prev_tombstone) {
                rts.push_back(range_tombstone_change(_current_start, prev_tombstone));
            }

            // All subsequent clustering keys are larger than the start of this
            // range so there is no need to check that again.
            _in_current = true;

            if (less(pos, _current_end)) {
                if (prev_tombstone != new_tombstone) {
                    rts.push_back(range_tombstone_change(pos, new_tombstone));
                }
                return progress{.contained = true, .rts = std::move(rts)};
            } else {
                if (!_past_current && prev_tombstone) {
                    rts.push_back(range_tombstone_change(_current_end, {}));
                }
                _past_current = true;
            }
        } while (advance_to_next_range());

        return progress{.contained = false, .rts = std::move(rts)};
    }

    // Returns true if the range expressed by start and end (as in position_range) overlaps
    // with clustering ranges.
    // Must be called with monotonic start position. That position must also be greater than
    // the last position passed to the other advance_to() overload.
    // Idempotent.
    // Breaks the tracking of current range tombstone, so don't use if you also use the advance_to()
    // overload which tracks tombstones.
    bool advance_to(position_in_partition_view start, position_in_partition_view end) {
        position_in_partition::less_compare less(_schema);

        do {
            if (!less(_current_start, end)) {
                break;
            }
            if (less(start, _current_end)) {
                return true;
            }
        } while (advance_to_next_range());

        return false;
    }

    // Returns true if the range tombstone expressed by start and end (as in position_range) overlaps
    // with clustering ranges.
    // No monotonicity restrictions on argument values across calls.
    // Does not affect lower_bound().
    // Idempotent.
    bool contains_tombstone(position_in_partition_view start, position_in_partition_view end) const {
        position_in_partition::less_compare less(_schema);

        if (_trim && !less(*_trim, end)) {
            return false;
        }

        for (const auto& rng : _current_range) {
            auto range_start = position_in_partition_view::for_range_start(rng);
            if (!less(range_start, end)) {
                return false;
            }
            auto range_end = position_in_partition_view::for_range_end(rng);
            if (less(start, range_end)) {
                return true;
            }
        }

        return false;
    }

    // Intersects rt with query ranges. The first overlap is returned and the rest is applied to dst.
    // If returns a disengaged optional, there is no overlap and nothing was applied to dst.
    // No monotonicity restrictions on argument values across calls.
    // Does not affect lower_bound().
    std::optional<range_tombstone> split_tombstone(range_tombstone rt, range_tombstone_stream& dst) const {
        position_in_partition::less_compare less(_schema);

        if (_trim && !rt.trim_front(_schema, *_trim)) {
            return std::nullopt;
        }

        std::optional<range_tombstone> first;

        for (const auto& rng : _current_range) {
            auto range_start = position_in_partition_view::for_range_start(rng);
            auto range_end = position_in_partition_view::for_range_end(rng);
            if (!less(rt.position(), range_start) && !less(range_end, rt.end_position())) {
                // Fully enclosed by this range.
                SCYLLA_ASSERT(!first);
                return std::move(rt);
            }
            auto this_range_rt = rt;
            if (this_range_rt.trim(_schema, range_start, range_end)) {
                if (first) {
                    dst.apply(std::move(this_range_rt));
                } else {
                    first = std::move(this_range_rt);
                }
            }
        }

        return first;
    }

    tombstone current_tombstone() const {
        return _tombstone;
    }

    void set_tombstone(tombstone t) {
        _tombstone = t;
    }

    // Returns true if advanced past all contained positions. Any later advance_to() until reset() will return false.
    bool out_of_range() const {
        return !_in_current && !_current_range;
    }

    // Resets the state of the walker so that advance_to() can be now called for new sequence of positions.
    // Any range trimmings still hold after this.
    void reset() {
        _current_range = _ranges;
        _in_current = _with_static_row;
        _past_current = false;
        _current_start = position_in_partition_view::for_static_row();
        _current_end = position_in_partition_view::before_all_clustered_rows();
        set_current_positions();
        ++_change_counter;
        if (_trim) {
            trim_front(*std::exchange(_trim, {}));
        }
    }

    // Can be called only when !out_of_range()
    position_in_partition_view lower_bound() const {
        return _current_start;
    }

    // When lower_bound() changes, this also does
    // Always > 0.
    size_t lower_bound_change_counter() const {
        return _change_counter;
    }
};
