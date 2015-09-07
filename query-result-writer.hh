/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include "types.hh"
#include "atomic_cell.hh"
#include "query-request.hh"
#include "query-result.hh"

// Refer to query-result.hh for the query result format

namespace query {

class result::row_writer {
    bytes_ostream& _w;
    const partition_slice& _slice;
    bytes_ostream::place_holder<uint32_t> _size_ph;
    size_t _start_pos;
public:
    row_writer(
        const partition_slice& slice,
        bytes_ostream& w,
        bytes_ostream::place_holder<uint32_t> size_ph)
        : _w(w)
        , _slice(slice)
        , _size_ph(size_ph)
        , _start_pos(w.size())
    { }

    void add_empty() {
        // FIXME: store this in a bitmap
        _w.write<int8_t>(false);
    }

    void add(::atomic_cell_view c) {
        // FIXME: store this in a bitmap
        _w.write<int8_t>(true);
        assert(c.is_live());
        if (_slice.options.contains<partition_slice::option::send_timestamp_and_expiry>()) {
            _w.write(c.timestamp());
            if (c.is_live_and_has_ttl()) {
                _w.write<gc_clock::rep>(c.expiry().time_since_epoch().count());
            } else {
                _w.write<gc_clock::rep>(std::numeric_limits<gc_clock::rep>::max());
            }
        }
        _w.write_blob(c.value());
    }

    void add(collection_mutation::view v) {
        // FIXME: store this in a bitmap
        _w.write<int8_t>(true);
        _w.write_blob(v.data);
    }

    void finish() {
        auto row_size = _w.size() - _start_pos;
        assert((uint32_t)row_size == row_size);
        _w.set(_size_ph, (uint32_t)row_size);
    }
};

// Call finish() or retract() when done.
class result::partition_writer {
    bytes_ostream& _w;
    const partition_slice& _slice;
    bytes_ostream::place_holder<uint32_t> _count_ph;
    bytes_ostream::position _pos;
    uint32_t _row_count = 0;
    bool _static_row_added = false;
public:
    partition_writer(
        const partition_slice& slice,
        bytes_ostream::place_holder<uint32_t> count_ph,
        bytes_ostream::position pos,
        bytes_ostream& w)
        : _w(w)
        , _slice(slice)
        , _count_ph(count_ph)
        , _pos(pos)
    { }

    row_writer add_row(const clustering_key& key) {
        if (_slice.options.contains<partition_slice::option::send_clustering_key>()) {
            _w.write_blob(key);
        }
        ++_row_count;
        auto size_placeholder = _w.write_place_holder<uint32_t>();
        return row_writer(_slice, _w, size_placeholder);
    }

    // Call before any add_row()
    row_writer add_static_row() {
        assert(!_static_row_added); // Static row can be added only once
        assert(!_row_count); // Static row must be added before clustered rows
        _static_row_added = true;
        auto size_placeholder = _w.write_place_holder<uint32_t>();
        return row_writer(_slice, _w, size_placeholder);
    }

    uint32_t row_count() const {
        return _row_count;
    }

    void finish() {
        _w.set(_count_ph, _row_count);

        // The partition is live. If there are no clustered rows, there
        // must be something live in the static row, which counts as one row.
        _row_count = std::max<uint32_t>(_row_count, 1);

    }

    void retract() {
        _row_count = 0;
        _w.retract(_pos);
    }

    const partition_slice& slice() const {
        return _slice;
    }
};

class result::builder {
    bytes_ostream _w;
    const partition_slice& _slice;
public:
    builder(const partition_slice& slice) : _slice(slice) { }

    // Starts new partition and returns a builder for its contents.
    // Invalidates all previously obtained builders
    partition_writer add_partition(const partition_key& key) {
        auto pos = _w.pos();
        auto count_place_holder = _w.write_place_holder<uint32_t>();
        if (_slice.options.contains<partition_slice::option::send_partition_key>()) {
            _w.write_blob(key);
        }
        return partition_writer(_slice, count_place_holder, pos, _w);
    }

    result build() {
        return result(std::move(_w));
    };

    const partition_slice& slice() const {
        return _slice;
    }
};

}
