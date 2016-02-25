/*
 * Copyright 2015 Cloudius Systems
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

#include "types.hh"
#include "atomic_cell.hh"
#include "query-request.hh"
#include "query-result.hh"

#include "idl/uuid.dist.hh"
#include "idl/keys.dist.hh"
#include "idl/query.dist.hh"
#include "serializer_impl.hh"
#include "serialization_visitors.hh"
#include "idl/query.dist.impl.hh"
#include "idl/keys.dist.impl.hh"
#include "idl/uuid.dist.impl.hh"

namespace query {


class result::partition_writer {
    ser::after_qr_partition__key _w;
    const partition_slice& _slice;
    // We are tasked with keeping track of the range
    // as well, since we are the primary "context"
    // when iterating "inside" a partition
    const clustering_row_ranges& _ranges;
    ser::query_result__partitions& _pw;
    ser::vector_position _pos;
    bool _static_row_added = false;
public:
    partition_writer(
        const partition_slice& slice,
        const clustering_row_ranges& ranges,
        ser::query_result__partitions& pw,
        ser::vector_position pos,
        ser::after_qr_partition__key w)
        : _w(std::move(w))
        , _slice(slice)
        , _ranges(ranges)
        , _pw(pw)
        , _pos(std::move(pos))
    { }

    ser::after_qr_partition__key start() {
        return std::move(_w);
    }

    // Cancels the whole partition element.
    // Can be called at any stage of writing before this element is finalized.
    // Do not use this writer after that.
    void retract() {
        _pw.rollback(_pos);
    }

    const clustering_row_ranges& ranges() const {
        return _ranges;
    }
    const partition_slice& slice() const {
        return _slice;
    }
};

class result::builder {
    bytes_ostream _out;
    const partition_slice& _slice;
    ser::query_result__partitions _w;
public:
    builder(const partition_slice& slice)
        : _slice(slice)
        , _w(ser::writer_of_query_result(_out).start_partitions())
    { }
    builder(builder&&) = delete; // _out is captured by reference

    // Starts new partition and returns a builder for its contents.
    // Invalidates all previously obtained builders
    partition_writer add_partition(const schema& s, const partition_key& key) {
        auto pos = _w.pos();
        // fetch the row range for this partition already.
        auto& ranges = _slice.row_ranges(s, key);
        auto after_key = [this, pw = _w.add(), &key] () mutable {
            if (_slice.options.contains<partition_slice::option::send_partition_key>()) {
                return std::move(pw).write_key(key);
            } else {
                return std::move(pw).skip_key();
            }
        }();
        return partition_writer(_slice, ranges, _w, std::move(pos), std::move(after_key));
    }

    result build() {
        std::move(_w).end_partitions().end_query_result();
        return result(std::move(_out));
    }
};

}
