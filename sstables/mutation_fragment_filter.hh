/*
 * Copyright (C) 2018 ScyllaDB
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

#include "mutation_fragment.hh"
#include "clustering_ranges_walker.hh"

namespace sstables {

class mutation_fragment_filter {
    const schema& _schema;
    query::clustering_key_filter_ranges _ranges;
    clustering_ranges_walker _walker;
    // True when we visited all the ranges or when we're after _fwd_end
    bool _out_of_range = false;
    streamed_mutation::forwarding _fwd;

    /*
     * _fwd_end reflects the end of the window set by fast forward to.
     * If fast forwarding is not enabled then it is set to allow the whole range.
     * Otherwise it is initially set to static row.
     */
    position_in_partition _fwd_end;
    size_t _last_lower_bound_counter = 0;

    bool is_after_fwd_window(position_in_partition_view pos) const {
        return _fwd && !position_in_partition::less_compare(_schema)(pos, _fwd_end);
    }
public:
    mutation_fragment_filter(const schema& schema,
                             const query::partition_slice& slice,
                             const partition_key& pk,
                             streamed_mutation::forwarding fwd)
        : _schema(schema)
        , _ranges(query::clustering_key_filter_ranges::get_ranges(schema, slice, pk))
        , _walker(schema, _ranges.ranges(), schema.has_static_columns())
        , _fwd(fwd)
        , _fwd_end(fwd ? position_in_partition_view::before_all_clustered_rows()
                       : position_in_partition_view::after_all_clustered_rows())
    { }

    mutation_fragment_filter(const mutation_fragment_filter&) = delete;
    mutation_fragment_filter(mutation_fragment_filter&&) = delete;

    mutation_fragment_filter& operator=(const mutation_fragment_filter&) = delete;
    mutation_fragment_filter& operator=(mutation_fragment_filter&&) = delete;

    enum class result {
        ignore,
        emit,
        store_and_finish
    };

    result apply(const static_row& sr) {
        bool inside_requested_ranges = _walker.advance_to(sr.position());
        _out_of_range |= _walker.out_of_range();
        if (!inside_requested_ranges) {
            return result::ignore;
        } else {
            return result::emit;
        }
    }

    result apply(position_in_partition_view pos) {
        if (is_after_fwd_window(pos)) {
            // This happens only when fwd is set
            _out_of_range = true;
            return result::store_and_finish;
        }
        bool inside_requested_ranges = _walker.advance_to(pos);
        _out_of_range |= _walker.out_of_range();
        if (!inside_requested_ranges) {
            return result::ignore;
        }
        return result::emit;
    }

    result apply(const range_tombstone& rt) {
        bool inside_requested_ranges = _walker.advance_to(rt.position(), rt.end_position());
        _out_of_range |= _walker.out_of_range();
        if (!inside_requested_ranges) {
            return result::ignore;
        }
        if (is_after_fwd_window(rt.position())) {
            // This happens only when fwd is set
            _out_of_range = true;
            return result::store_and_finish;
        } else {
            return result::emit;
        }
    }

    bool out_of_range() const {
        return _out_of_range;
    }

    std::optional<position_in_partition_view> maybe_skip() {
        if (!is_current_range_changed()) {
            return {};
        }

        _last_lower_bound_counter = _walker.lower_bound_change_counter();
        return _walker.lower_bound();
    }

    /*
     * The method fast-forwards the current range to the passed position range.
     * Returned optional is engaged iff the input range overlaps with any of the
     * query ranges tracked by _walker.
     */
    std::optional<position_in_partition_view> fast_forward_to(position_range r) {
        assert(_fwd);
        _walker.trim_front(r.start());
        _fwd_end = std::move(r).end();
        _out_of_range = !_walker.advance_to(r.start(), _fwd_end);

        if (_out_of_range) {
            return {};
        }

        _last_lower_bound_counter = _walker.lower_bound_change_counter();
        return _walker.lower_bound();
    }

    /*
     * Tells if current range has changed since last reader fast-forwarding or skip
     */
    inline bool is_current_range_changed() const {
        return (_last_lower_bound_counter != _walker.lower_bound_change_counter());
    }


    position_in_partition_view lower_bound() const {
        return _walker.lower_bound();
    }

    position_in_partition_view uppermost_bound() const {
        return _walker.uppermost_bound();
    }
};

};   // namespace sstables
