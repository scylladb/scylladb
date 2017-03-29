/*
 * Copyright (C) 2017 ScyllaDB
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
#include "streamed_mutation.hh"

// Utility for in-order checking of overlap with clustering ranges.
class clustering_ranges_walker {
    const schema& _schema;
    const query::clustering_row_ranges& _ranges;
    query::clustering_row_ranges::const_iterator _current;
    query::clustering_row_ranges::const_iterator _end;
    bool _in_current = false;
public:
    clustering_ranges_walker(const schema& s, const query::clustering_row_ranges& ranges)
        : _schema(s)
        , _ranges(ranges)
        , _current(ranges.begin())
        , _end(ranges.end())
    { }
    clustering_ranges_walker(clustering_ranges_walker&& o) noexcept
        : _schema(o._schema)
        , _ranges(o._ranges)
        , _current(o._current)
        , _end(o._end)
        , _in_current(o._in_current)
    { }
    clustering_ranges_walker& operator=(clustering_ranges_walker&& o) {
        if (this != &o) {
            this->~clustering_ranges_walker();
            new (this) clustering_ranges_walker(std::move(o));
        }
        return *this;
    }

    // Returns true if given position is contained.
    // Must be called with monotonic positions.
    // Idempotent.
    bool advance_to(position_in_partition_view pos) {
        position_in_partition::less_compare less(_schema);

        while (_current != _end) {
            if (!_in_current && _current->start()) {
                auto range_start = position_in_partition_view::for_range_start(*_current);
                if (less(pos, range_start)) {
                    return false;
                }
            }
            // All subsequent clustering keys are larger than the start of this
            // range so there is no need to check that again.
            _in_current = true;

            if (!_current->end()) {
                return true;
            }

            auto range_end = position_in_partition_view::for_range_end(*_current);
            if (less(pos, range_end)) {
                return true;
            }

            ++_current;
            _in_current = false;
        }

        return false;
    }

    // Returns true if the range expressed by start and end (as in position_range) overlaps
    // with clustering ranges.
    // Must be called with monotonic start position. That position must also be greater than
    // the last position passed to the other advance_to() overload.
    // Idempotent.
    bool advance_to(position_in_partition_view start, position_in_partition_view end) {
        position_in_partition::less_compare less(_schema);

        while (_current != _end) {
            position_in_partition_view range_start(position_in_partition_view::range_tag_t(), bound_view::from_range_start(*_current));
            if (less(end, range_start)) {
                return false;
            }

            position_in_partition_view range_end(position_in_partition_view::range_tag_t(), bound_view::from_range_end(*_current));
            if (less(start, range_end)) {
                return true;
            }

            ++_current;
            _in_current = false;
        }

        return false;
    }

    // Returns true if advanced past all contained positions. Any later advance_to() until reset() will return false.
    bool out_of_range() const {
        return _current == _end;
    }

    // Resets the state of the walker so that advance_to() can be now called for new sequence of positions.
    void reset() {
        _current = _ranges.begin();
        _in_current = false;
    }
};
