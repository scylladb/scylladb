/*
 * Copyright (C) 2018-present ScyllaDB
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

#include "dht/i_partitioner.hh"
#include "schema_fwd.hh"
#include "mutation_fragment.hh"
#include "sstables/shared_sstable.hh"
#include "reader_permit.hh"
#include "db/view/row_locking.hh"
#include <seastar/core/abort_source.hh>
#include "mutation.hh"

class evictable_reader_handle;

namespace db::view {

/*
 * A consumer that pushes materialized view updates for each consumed mutation.
 * It is expected to be run in seastar::async threaded context through consume_in_thread()
 */
class view_updating_consumer {
public:
    // We prefer flushing on partition boundaries, so at the end of a partition,
    // we flush on reaching the soft limit. Otherwise we continue accumulating
    // data. We flush mid-partition if we reach the hard limit.
    static const size_t buffer_size_soft_limit;
    static const size_t buffer_size_hard_limit;

private:
    schema_ptr _schema;
    reader_permit _permit;
    const seastar::abort_source* _as;
    evictable_reader_handle& _staging_reader_handle;
    circular_buffer<mutation> _buffer;
    mutation* _m{nullptr};
    size_t _buffer_size{0};
    noncopyable_function<future<row_locker::lock_holder>(mutation)> _view_update_pusher;

private:
    void do_flush_buffer();
    void maybe_flush_buffer_mid_partition();

public:
    // Push updates with a custom pusher. Mainly for tests.
    view_updating_consumer(schema_ptr schema, reader_permit permit, const seastar::abort_source& as, evictable_reader_handle& staging_reader_handle,
            noncopyable_function<future<row_locker::lock_holder>(mutation)> view_update_pusher)
            : _schema(std::move(schema))
            , _permit(std::move(permit))
            , _as(&as)
            , _staging_reader_handle(staging_reader_handle)
            , _view_update_pusher(std::move(view_update_pusher))
    { }

    view_updating_consumer(schema_ptr schema, reader_permit permit, table& table, std::vector<sstables::shared_sstable> excluded_sstables, const seastar::abort_source& as,
            evictable_reader_handle& staging_reader_handle);

    view_updating_consumer(view_updating_consumer&&) = default;

    view_updating_consumer& operator=(view_updating_consumer&&) = delete;

    void consume_new_partition(const dht::decorated_key& dk) {
        _buffer.emplace_back(_schema, dk, mutation_partition(_schema));
        _m = &_buffer.back();
    }

    void consume(tombstone t) {
        _m->partition().apply(std::move(t));
    }

    stop_iteration consume(static_row&& sr) {
        if (_as->abort_requested()) {
            return stop_iteration::yes;
        }
        _buffer_size += sr.memory_usage(*_schema);
        _m->partition().apply(*_schema, mutation_fragment(*_schema, _permit, std::move(sr)));
        maybe_flush_buffer_mid_partition();
        return stop_iteration::no;
    }

    stop_iteration consume(clustering_row&& cr) {
        if (_as->abort_requested()) {
            return stop_iteration::yes;
        }
        _buffer_size += cr.memory_usage(*_schema);
        _m->partition().apply(*_schema, mutation_fragment(*_schema, _permit, std::move(cr)));
        maybe_flush_buffer_mid_partition();
        return stop_iteration::no;
    }

    stop_iteration consume(range_tombstone&& rt) {
        if (_as->abort_requested()) {
            return stop_iteration::yes;
        }
        _buffer_size += rt.memory_usage(*_schema);
        _m->partition().apply(*_schema, mutation_fragment(*_schema, _permit, std::move(rt)));
        maybe_flush_buffer_mid_partition();
        return stop_iteration::no;
    }

    // Expected to be run in seastar::async threaded context (consume_in_thread())
    stop_iteration consume_end_of_partition() {
        if (_as->abort_requested()) {
            return stop_iteration::yes;
        }
        if (_buffer_size >= buffer_size_soft_limit) {
            do_flush_buffer();
        }
        return stop_iteration::no;
    }

    stop_iteration consume_end_of_stream() {
        if (!_buffer.empty()) {
            do_flush_buffer();
        }
        return stop_iteration(_as->abort_requested());
    }
};

}

