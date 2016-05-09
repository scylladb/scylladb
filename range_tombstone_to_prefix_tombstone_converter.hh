/*
 * Copyright (C) 2016 ScyllaDB
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

#include "range_tombstone.hh"
#include "exceptions/exceptions.hh"
#include "to_string.hh"

/**
 * Class used to transform a set of range_tombstones into a set of
 * overlapping tombstones, in order to support sending mutations to
 * nodes that don't yet support range tombstones.
 */
class range_tombstone_to_prefix_tombstone_converter {
    class open_tombstone {
    public:
        clustering_key_prefix prefix;
        tombstone tomb;
        open_tombstone(clustering_key_prefix start, tombstone tomb)
                : prefix(std::move(start))
                , tomb(std::move(tomb))
        { }
        clustering_key_prefix&& release() {
            return std::move(prefix);
        }
        bool ends_with(const schema& s, const clustering_key_prefix& candidate) {
            return prefix.equal(s, candidate);
        }
        explicit operator sstring() const {
            return sprint("%s", *this);
        }
        friend std::ostream& operator<<(std::ostream& out, const open_tombstone& o) {
            return out << sprint("%s deletion %s", o.prefix, o.tomb);
        }
    };
    std::vector<open_tombstone> _open_tombstones;
    clustering_key_prefix _end_contiguous_delete;
    bound_kind _end_contiguous_bound = bound_kind::incl_end;
public:
    range_tombstone_to_prefix_tombstone_converter()
            : _end_contiguous_delete(std::vector<bytes>())
    { }
    void verify_no_open_tombstones() {
        // Verify that no range tombstone was left "open" (waiting to
        // be converted into a deletion of an entire row).
        // Should be called at the end of going through all range_tombstones.
        if (!_open_tombstones.empty()) {
            throw exceptions::unsupported_operation_exception(sprint(
                        "RANGE DELETE not implemented. Tried to convert, but row finished before we could finish the conversion. "
                        "Starts found: %s", _open_tombstones));
        }
    }

    std::experimental::optional<clustering_key_prefix> convert(const schema& s, const range_tombstone& rt) {
        auto end_bound = bound_view(_end_contiguous_delete, _end_contiguous_bound);
        if (!_open_tombstones.empty()) {
            // If the range tombstones are the result of Cassandra's splitting
            // overlapping tombstones into disjoint tombstones, they have to
            // be adjacent. If they are not, it is probably a bona-fide range
            // delete, which we don't support.
            if (!end_bound.equal(s, rt.start_bound())) {
                throw exceptions::unsupported_operation_exception(sprint(
                        "RANGE DELETE not implemented. Tried to convert but "
                        "found non-adjacent tombstones: %s and %s.",
                        end_bound, rt.start_bound()));
            }
        }
        if (_open_tombstones.empty() || rt.tomb.timestamp > _open_tombstones.back().tomb.timestamp) {
            if (rt.start_kind == bound_kind::excl_start) {
                throw exceptions::unsupported_operation_exception(sprint(
                            "Range tombstones with an exclusive lower bound cannot "
                            "be converted to prefix tombstones: %s", rt.start_bound()));
            }
            _open_tombstones.push_back(open_tombstone(rt.start, rt.tomb));
        } else if (rt.tomb.timestamp < _open_tombstones.back().tomb.timestamp) {
            // If the new range has an *earlier* timestamp than the open tombstone
            // it is supposedely covering, then our representation as two overlapping
            // tombstones would not be identical to the two disjoint tombstones.
            throw exceptions::unsupported_operation_exception(sprint(
                        "RANGE DELETE not implemented. Tried to convert but "
                        "found range starting at %s which cannot close a "
                        "row because of decreasing timestamp %d.",
                        _open_tombstones.back(), rt.tomb.timestamp));
        } else if (rt.tomb.deletion_time != _open_tombstones.back().tomb.deletion_time) {
            // timestamps are equal, but deletion_times are not
            throw exceptions::unsupported_operation_exception(sprint(
                        "RANGE DELETE not implemented. Couldn't convert range "
                        "%s,%s into row %s. Both had same timestamp %s but "
                        "different deletion_time %s.",
                        rt.start_bound(), rt.end_bound(), _open_tombstones.back(),
                        rt.tomb.timestamp,
                        rt.tomb.deletion_time.time_since_epoch().count()));
        }
        std::experimental::optional<clustering_key_prefix> ret;
        if (_open_tombstones.back().ends_with(s, rt.end)) {
            ret = _open_tombstones.back().release();
            _open_tombstones.pop_back();
        }
        if (!_open_tombstones.empty()) {
            _end_contiguous_delete = rt.end;
            _end_contiguous_bound = rt.end_kind;
        }
        return ret;
    }
};
