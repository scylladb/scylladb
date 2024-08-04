/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/assert.hh"
#include "mutation/mutation_fragment.hh"
#include "clustering_ranges_walker.hh"
#include "clustering_key_filter.hh"
#include "readers/mutation_reader_fwd.hh"

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
                             query::clustering_key_filter_ranges ranges,
                             streamed_mutation::forwarding fwd)
        : _schema(schema)
        , _ranges(std::move(ranges))
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

    struct clustering_result {
        result action;
        clustering_ranges_walker::range_tombstones rts;
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

    clustering_result apply(position_in_partition_view pos, tombstone t) {
        if (is_after_fwd_window(pos)) {
            // This happens only when fwd is set
            _out_of_range = true;
            clustering_ranges_walker::progress pr = _walker.advance_to(_fwd_end, _walker.current_tombstone());
            if (_walker.current_tombstone()) {
                // Close range tombstone before EOS
                pr.rts.push_back(range_tombstone_change(_fwd_end, {}));
            }
            return clustering_result{
                .action = result::store_and_finish,
                .rts = std::move(pr.rts)
            };
        }
        clustering_ranges_walker::progress pr = _walker.advance_to(pos, t);
        if (!pr.contained) {
            _out_of_range |= _walker.out_of_range();
            return clustering_result{
                .action = result::ignore,
                .rts = std::move(pr.rts)
            };
        }
        return clustering_result{
            .action = result::emit,
            .rts = std::move(pr.rts)
        };
    }

    clustering_result apply(position_in_partition_view pos) {
        return apply(pos, _walker.current_tombstone());
    }

    void set_tombstone(tombstone t) {
        _walker.set_tombstone(t);
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
        SCYLLA_ASSERT(_fwd);
        _fwd_end = std::move(r).end();
        _out_of_range = !_walker.advance_to(r.start(), _fwd_end);

        // Must be after advance_to() so that advance_to() doesn't enter the range.
        // Doing so would cause us to not emit a range_tombstone_change for the beginning of the range.
        _walker.trim_front(r.start());

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

    tombstone current_tombstone() const {
        return _walker.current_tombstone();
    }

    position_in_partition_view lower_bound() const {
        return _walker.lower_bound();
    }
};

};   // namespace sstables
