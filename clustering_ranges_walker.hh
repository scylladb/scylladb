/*
 * Copyright (C) 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
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

#include "schema.hh"
#include "query-request.hh"
#include "mutation_fragment.hh"

// Utility for in-order checking of overlap with position ranges.
class clustering_ranges_walker {
    const schema& _schema;
    const query::clustering_row_ranges& _ranges;
    boost::iterator_range<query::clustering_row_ranges::const_iterator> _current_range;
    bool _in_current; // next position is known to be >= _current_start
    bool _with_static_row;
    position_in_partition_view _current_start;
    position_in_partition_view _current_end;
    std::optional<position_in_partition> _trim;
    size_t _change_counter = 1;
private:
    bool advance_to_next_range() {
        _in_current = false;
        if (!_current_start.is_static_row()) {
            if (!_current_range) {
                return false;
            }
            _current_range.advance_begin(1);
        }
        ++_change_counter;
        if (!_current_range) {
            _current_end = _current_start = position_in_partition_view::after_all_clustered_rows();
            return false;
        }
        _current_start = position_in_partition_view::for_range_start(_current_range.front());
        _current_end = position_in_partition_view::for_range_end(_current_range.front());
        return true;
    }

    void set_current_positions() {
         if (!_with_static_row) {
            if (!_current_range) {
                _current_start = position_in_partition_view::before_all_clustered_rows();
            } else {
                _current_start = position_in_partition_view::for_range_start(_current_range.front());
                _current_end = position_in_partition_view::for_range_end(_current_range.front());
            }
        } else {
             // If the first range is contiguous with the static row, then advance _current_end as much as we can
             if (_current_range && !_current_range.front().start()) {
                 _current_end = position_in_partition_view::for_range_end(_current_range.front());
             }
        }
    }

public:
    clustering_ranges_walker(const schema& s, const query::clustering_row_ranges& ranges, bool with_static_row = true)
            : _schema(s)
            , _ranges(ranges)
            , _current_range(ranges)
            , _in_current(with_static_row)
            , _with_static_row(with_static_row)
            , _current_start(position_in_partition_view::for_static_row())
            , _current_end(position_in_partition_view::before_all_clustered_rows()) {
        set_current_positions();
    }

    clustering_ranges_walker(const clustering_ranges_walker&) = delete;
    clustering_ranges_walker(clustering_ranges_walker&&) = delete;

    clustering_ranges_walker& operator=(const clustering_ranges_walker&) = delete;
    clustering_ranges_walker& operator=(clustering_ranges_walker&&) = delete;

    // Excludes positions smaller than pos from the ranges.
    // pos should be monotonic.
    // No constraints between pos and positions passed to advance_to().
    //
    // After the invocation, when !out_of_range(), lower_bound() returns the smallest position still contained.
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
        position_in_partition::less_compare less(_schema);

        do {
            if (!_in_current && less(pos, _current_start)) {
                break;
            }
            // All subsequent clustering keys are larger than the start of this
            // range so there is no need to check that again.
            _in_current = true;

            if (less(pos, _current_end)) {
                return true;
            }
        } while (advance_to_next_range());

        return false;
    }

    // Returns true if the range expressed by start and end (as in position_range) overlaps
    // with clustering ranges.
    // Must be called with monotonic start position. That position must also be greater than
    // the last position passed to the other advance_to() overload.
    // Idempotent.
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

    // Returns true if advanced past all contained positions. Any later advance_to() until reset() will return false.
    bool out_of_range() const {
        return !_in_current && !_current_range;
    }

    // Resets the state of the walker so that advance_to() can be now called for new sequence of positions.
    // Any range trimmings still hold after this.
    void reset() {
        _current_range = _ranges;
        _in_current = _with_static_row;
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

    // Returns the upper bound of the last range in provided ranges set
    position_in_partition_view uppermost_bound() const {
        return position_in_partition_view::for_range_end(_ranges.back());
    }

    // When lower_bound() changes, this also does
    // Always > 0.
    size_t lower_bound_change_counter() const {
        return _change_counter;
    }
};
