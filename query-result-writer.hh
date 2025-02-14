/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "types/types.hh"
#include "query-request.hh"
#include "query-result.hh"
#include "utils/digest_algorithm.hh"
#include "utils/digester.hh"
#include "full_position.hh"
#include "mutation/tombstone.hh"
#include "idl/query.dist.hh"
#include "idl/query.dist.impl.hh"

namespace query {

class result::partition_writer {
    result_request _request;
    ser::after_qr_partition__key<bytes_ostream> _w;
    const partition_slice& _slice;
    // We are tasked with keeping track of the range
    // as well, since we are the primary "context"
    // when iterating "inside" a partition
    const clustering_row_ranges& _ranges;
    ser::query_result__partitions<bytes_ostream>& _pw;
    ser::vector_position _pos;
    digester& _digest;
    digester _digest_pos;
    uint64_t& _row_count;
    uint32_t& _partition_count;
    api::timestamp_type& _last_modified;
public:
    partition_writer(
        result_request request,
        const partition_slice& slice,
        const clustering_row_ranges& ranges,
        ser::query_result__partitions<bytes_ostream>& pw,
        ser::vector_position pos,
        ser::after_qr_partition__key<bytes_ostream> w,
        digester& digest,
        uint64_t& row_count,
        uint32_t& partition_count,
        api::timestamp_type& last_modified)
        : _request(request)
        , _w(std::move(w))
        , _slice(slice)
        , _ranges(ranges)
        , _pw(pw)
        , _pos(std::move(pos))
        , _digest(digest)
        , _digest_pos(digest)
        , _row_count(row_count)
        , _partition_count(partition_count)
        , _last_modified(last_modified)
    { }

    bool requested_digest() const {
        return _request != result_request::only_result;
    }

    bool requested_result() const {
        return _request != result_request::only_digest;
    }

    ser::after_qr_partition__key<bytes_ostream> start() {
        return std::move(_w);
    }

    // Cancels the whole partition element.
    // Can be called at any stage of writing before this element is finalized.
    // Do not use this writer after that.
    void retract() {
        _digest = _digest_pos;
        _pw.rollback(_pos);
    }

    const clustering_row_ranges& ranges() const {
        return _ranges;
    }
    const partition_slice& slice() const {
        return _slice;
    }
    digester& digest() {
        return _digest;
    }
    uint64_t& row_count() {
        return _row_count;
    }
    uint32_t& partition_count() {
        return _partition_count;
    }
    api::timestamp_type& last_modified() {
        return _last_modified;
    }

};

class result::builder {
    bytes_ostream _out;
    const partition_slice& _slice;
    ser::query_result__partitions<bytes_ostream> _w;
    result_request _request;
    uint64_t _row_count = 0;
    uint32_t _partition_count = 0;
    api::timestamp_type _last_modified = api::missing_timestamp;
    short_read _short_read;
    digester _digest;
    result_memory_accounter _memory_accounter;
    const uint64_t _tombstone_limit = query::max_tombstones;
    uint64_t _tombstones = 0;
public:
    builder(const partition_slice& slice, result_options options, result_memory_accounter memory_accounter, uint64_t tombstone_limit)
        : _slice(slice)
        , _w(ser::writer_of_query_result<bytes_ostream>(_out).start_partitions())
        , _request(options.request)
        , _digest(digester(options.digest_algo))
        , _memory_accounter(std::move(memory_accounter))
        , _tombstone_limit(tombstone_limit)
    { }
    builder(builder&&) = delete; // _out is captured by reference

    void mark_as_short_read() { _short_read = short_read::yes; }
    short_read is_short_read() const { return _short_read; }

    result_memory_accounter& memory_accounter() { return _memory_accounter; }

    stop_iteration bump_and_check_tombstone_limit() {
        ++_tombstones;
        if (_tombstones < _tombstone_limit) {
            return stop_iteration::no;
        }
        if (!_slice.options.contains<partition_slice::option::allow_short_read>()) {
            // The read is unpaged, we cannot interrupt it early without failing it.
            // Better let it continue.
            return stop_iteration::no;
        }
        return stop_iteration::yes;
    }

    const partition_slice& slice() const { return _slice; }

    uint64_t row_count() const {
        return _row_count;
    }

    uint32_t partition_count() const {
        return _partition_count;
    }

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
        if (_request != result_request::only_result) {
            _digest.feed_hash(key, s);
        }
        return partition_writer(_request, _slice, ranges, _w, std::move(pos), std::move(after_key), _digest, _row_count,
                                _partition_count, _last_modified);
    }

    result build(std::optional<full_position> last_pos = {}) {
        std::move(_w).end_partitions().end_query_result();
        switch (_request) {
        case result_request::only_result:
            return result(std::move(_out), _short_read, _row_count, _partition_count, std::move(last_pos), std::move(_memory_accounter).done());
        case result_request::only_digest: {
            bytes_ostream buf;
            ser::writer_of_query_result<bytes_ostream>(buf).start_partitions().end_partitions().end_query_result();
            return result(std::move(buf), result_digest(_digest.finalize_array()), _last_modified, _short_read, {}, {}, std::move(last_pos));
        }
        case result_request::result_and_digest:
            return result(std::move(_out), result_digest(_digest.finalize_array()),
                          _last_modified, _short_read, _row_count, _partition_count, std::move(last_pos), std::move(_memory_accounter).done());
        }
        abort();
    }
};

}

class row;
class static_row;
class clustering_row;
class range_tombstone_change;

// Adds mutation to query::result.
class mutation_querier {
    const schema& _schema;
    query::result_memory_accounter& _memory_accounter;
    ser::qr_partition__static_row__cells<bytes_ostream> _static_cells_wr;
    query::result::partition_writer _pw;
    bool _live_data_in_static_row{};
    uint64_t _live_clustering_rows = 0;
    std::optional<ser::qr_partition__rows<bytes_ostream>> _rows_wr;
private:
    void query_static_row(const row& r, tombstone current_tombstone);
    void prepare_writers();
public:
    mutation_querier(const schema& s, query::result::partition_writer pw,
                     query::result_memory_accounter& memory_accounter);
    void consume(tombstone) { }
    // Requires that sr.has_any_live_data()
    stop_iteration consume(static_row&& sr, tombstone current_tombstone);
    // Requires that cr.has_any_live_data()
    stop_iteration consume(clustering_row&& cr, row_tombstone current_tombstone);
    stop_iteration consume(range_tombstone_change&&) { return stop_iteration::no; }
    uint64_t consume_end_of_stream();
};

class query_result_builder {
    const schema& _schema;
    query::result::builder& _rb;
    std::optional<mutation_querier> _mutation_consumer;
    // We need to remember that we requested stop, to mark the read as short in the end.
    stop_iteration _stop;
public:
    query_result_builder(const schema& s, query::result::builder& rb) noexcept;

    void consume_new_partition(const dht::decorated_key& dk);
    void consume(tombstone t);
    stop_iteration consume(static_row&& sr, tombstone t, bool);
    stop_iteration consume(clustering_row&& cr, row_tombstone t, bool);
    stop_iteration consume(range_tombstone_change&& rtc);
    stop_iteration consume_end_of_partition();
    void consume_end_of_stream();
};
