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

#include "streamed_mutation.hh"

static inline bool has_ck_selector(const query::clustering_row_ranges& ranges) {
    // Like PK range, an empty row range, should be considered an "exclude all" restriction
    return ranges.empty() || std::any_of(ranges.begin(), ranges.end(), [](auto& r) {
        return !r.is_full();
    });
}

enum class emit_only_live_rows {
    no,
    yes,
};

enum class compact_for_sstables {
    no,
    yes,
};

/*
template<typename T>
concept bool CompactedMutationsConsumer() {
    return requires(T obj, tombstone t, const dht::decorated_key& dk, static_row sr,
        clustering_row cr, range_tombstone_begin rtb, range_tombstone_end rte, bool is_alive)
    {
        obj.consume_new_partition(dk);
        obj.consume(t);
        obj.consume(std::move(sr), is_alive);
        obj.consume(std::move(cr), is_alive);
        obj.consume(std::move(rtb));
        obj.consume(std::move(rte));
        obj.consume_end_of_partition();
        obj.consume_end_of_stream();
    };
}
*/
// emit_only_live::yes will cause compact_for_query to emit only live
// static and clustering rows. It doesn't affect the way range tombstones are
// emitted.
template<emit_only_live_rows OnlyLive, compact_for_sstables SSTableCompaction, typename Consumer>
class compact_mutation {
    const schema& _schema;
    gc_clock::time_point _query_time;
    gc_clock::time_point _gc_before;
    std::function<api::timestamp_type(const dht::decorated_key&)> _get_max_purgeable;
    api::timestamp_type _max_purgeable = api::max_timestamp;
    const query::partition_slice& _slice;
    uint32_t _row_limit{};
    uint32_t _partition_limit{};
    uint32_t _partition_row_limit{};

    Consumer _consumer;
    tombstone _partition_tombstone;
    tombstone _current_tombstone;

    bool _static_row_live{};
    uint32_t _rows_in_current_partition;
    uint32_t _current_partition_limit;
    bool _empty_partition{};
    const dht::decorated_key* _dk;
    bool _has_ck_selector{};
    bool _current_tombstone_emitted{};
private:
    static constexpr bool only_live() {
        return OnlyLive == emit_only_live_rows::yes;
    }
    static constexpr bool sstable_compaction() {
        return SSTableCompaction == compact_for_sstables::yes;
    }

    void partition_is_not_empty() {
        if (_empty_partition) {
            _empty_partition = false;
            _consumer.consume_new_partition(*_dk);
            auto pt = _partition_tombstone;
            if (!can_purge_tombstone(pt)) {
                _consumer.consume(pt);
            }
        }
    }

    bool can_purge_tombstone(const tombstone& t) {
        return (!sstable_compaction() || t.timestamp < _max_purgeable) && t.deletion_time < _gc_before;
    };
public:
    compact_mutation(const schema& s, gc_clock::time_point query_time, const query::partition_slice& slice, uint32_t limit,
              uint32_t partition_limit, Consumer consumer)
        : _schema(s)
        , _query_time(query_time)
        , _gc_before(query_time - s.gc_grace_seconds())
        , _slice(slice)
        , _row_limit(limit)
        , _partition_limit(partition_limit)
        , _partition_row_limit(_slice.options.contains(query::partition_slice::option::distinct) ? 1 : slice.partition_row_limit())
        , _consumer(std::move(consumer))
    {
        static_assert(!sstable_compaction(), "This constructor cannot be used for sstable compaction.");
    }

    compact_mutation(const schema& s, gc_clock::time_point compaction_time, Consumer consumer,
                     std::function<api::timestamp_type(const dht::decorated_key&)> get_max_purgeable)
        : _schema(s)
        , _query_time(compaction_time)
        , _gc_before(_query_time - s.gc_grace_seconds())
        , _get_max_purgeable(std::move(get_max_purgeable))
        , _slice(query::full_slice)
        , _consumer(std::move(consumer))
    {
        static_assert(sstable_compaction(), "This constructor can only be used for sstable compaction.");
        static_assert(!only_live(), "SSTable compaction cannot be run with emit_only_live_rows::yes.");
    }

    void consume_new_partition(const dht::decorated_key& dk) {
        auto& pk = dk.key();
        _dk = &dk;
        _has_ck_selector = has_ck_selector(_slice.row_ranges(_schema, pk));
        _empty_partition = true;
        _rows_in_current_partition = 0;
        _static_row_live = false;
        _current_tombstone = { };
        _partition_tombstone = { };
        _current_partition_limit = std::min(_row_limit, _partition_row_limit);
        if (sstable_compaction()) {
            _max_purgeable = _get_max_purgeable(dk);
        }
    }

    void consume(tombstone t) {
        _partition_tombstone = t;
        _current_tombstone = t;
        if (!only_live() && !can_purge_tombstone(t)) {
            partition_is_not_empty();
        }
    }

    stop_iteration consume(static_row&& sr) {
        bool is_live = sr.cells().compact_and_expire(_schema, column_kind::static_column,
                                                     _partition_tombstone,
                                                     _query_time, _max_purgeable, _gc_before);
        _static_row_live = is_live;
        if (is_live || (!only_live() && !sr.empty())) {
            partition_is_not_empty();
            _consumer.consume(std::move(sr), is_live);
        }
        return stop_iteration::no;
    }

    stop_iteration consume(clustering_row&& cr) {
        auto t = _current_tombstone;
        t.apply(cr.tomb());
        if (cr.tomb() <= _current_tombstone || can_purge_tombstone(cr.tomb())) {
            cr.remove_tombstone();
        }
        bool is_live = cr.marker().compact_and_expire(t, _query_time, _max_purgeable, _gc_before);
        is_live |= cr.cells().compact_and_expire(_schema, column_kind::regular_column, t, _query_time, _max_purgeable, _gc_before);
        if (only_live() && is_live) {
            partition_is_not_empty();
            _consumer.consume(std::move(cr), true);
            if (++_rows_in_current_partition == _current_partition_limit) {
                return stop_iteration::yes;
            }
        } else if (!only_live()) {
            if (is_live) {
                if (!sstable_compaction() && _rows_in_current_partition == _current_partition_limit) {
                    return stop_iteration::yes;
                }
                _rows_in_current_partition++;
            }
            if (!cr.empty()) {
                partition_is_not_empty();
                _consumer.consume(std::move(cr), is_live);
            }
        }
        return stop_iteration::no;
    }

    stop_iteration consume(range_tombstone_begin&& rt) {
        _current_tombstone.apply(rt.tomb());
        if (!can_purge_tombstone(rt.tomb()) && rt.tomb() > _partition_tombstone) {
            partition_is_not_empty();
            _consumer.consume(std::move(rt));
            _current_tombstone_emitted = true;
        }
        return stop_iteration::no;
    }

    stop_iteration consume(range_tombstone_end&& rt) {
        if (_current_tombstone_emitted) {
            _consumer.consume(std::move(rt));
            _current_tombstone_emitted = false;
        }
        _current_tombstone = _partition_tombstone;
        return stop_iteration::no;
    }

    stop_iteration consume_end_of_partition() {
        if (!_empty_partition) {
            // #589 - Do not add extra row for statics unless we did a CK range-less query.
            // See comment in query
            if (_rows_in_current_partition == 0 && _static_row_live && !_has_ck_selector) {
                ++_rows_in_current_partition;
            }

            _row_limit -= _rows_in_current_partition;
            _partition_limit -= 1;
            _consumer.consume_end_of_partition();
            if (!sstable_compaction()) {
                return _row_limit && _partition_limit ? stop_iteration::no : stop_iteration::yes;
            }
        }
        return stop_iteration::no;
    }

    auto consume_end_of_stream() {
        return _consumer.consume_end_of_stream();
    }
};

template<emit_only_live_rows only_live, typename Consumer>
struct compact_for_query : compact_mutation<only_live, compact_for_sstables::no, Consumer> {
    using compact_mutation<only_live, compact_for_sstables::no, Consumer>::compact_mutation;
};

template<typename Consumer>
struct compact_for_compaction : compact_mutation<emit_only_live_rows::no, compact_for_sstables::yes, Consumer> {
    using compact_mutation<emit_only_live_rows::no, compact_for_sstables::yes, Consumer>::compact_mutation;
};