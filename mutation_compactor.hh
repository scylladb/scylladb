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
        clustering_row cr, range_tombstone rt, tombstone current_tombstone, bool is_alive)
    {
        obj.consume_new_partition(dk);
        obj.consume(t);
        { obj.consume(std::move(sr), current_tombstone, is_alive) } ->stop_iteration;
        { obj.consume(std::move(cr), current_tombstone, is_alive) } ->stop_iteration;
        { obj.consume(std::move(rt)) } ->stop_iteration;
        { obj.consume_end_of_partition() } ->stop_iteration;
        obj.consume_end_of_stream();
    };
}
*/
// emit_only_live::yes will cause compact_for_query to emit only live
// static and clustering rows. It doesn't affect the way range tombstones are
// emitted.
template<emit_only_live_rows OnlyLive, compact_for_sstables SSTableCompaction, typename CompactedMutationsConsumer>
class compact_mutation {
    const schema& _schema;
    gc_clock::time_point _query_time;
    gc_clock::time_point _gc_before;
    std::function<api::timestamp_type(const dht::decorated_key&)> _get_max_purgeable;
    can_gc_fn _can_gc;
    api::timestamp_type _max_purgeable = api::missing_timestamp;
    const query::partition_slice& _slice;
    uint32_t _row_limit{};
    uint32_t _partition_limit{};
    uint32_t _partition_row_limit{};

    CompactedMutationsConsumer _consumer;
    range_tombstone_accumulator _range_tombstones;

    bool _static_row_live{};
    uint32_t _rows_in_current_partition;
    uint32_t _current_partition_limit;
    bool _empty_partition{};
    const dht::decorated_key* _dk;
    bool _has_ck_selector{};
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
            auto pt = _range_tombstones.get_partition_tombstone();
            if (pt && !can_purge_tombstone(pt)) {
                _consumer.consume(pt);
            }
        }
    }

    bool can_purge_tombstone(const tombstone& t) {
        return t.deletion_time < _gc_before && can_gc(t);
    };

    bool can_purge_tombstone(const row_tombstone& t) {
        return t.max_deletion_time() < _gc_before && can_gc(t.tomb());
    };

    bool can_gc(tombstone t) {
        if (!sstable_compaction()) {
            return true;
        }
        if (!t) {
            return false;
        }
        if (_max_purgeable == api::missing_timestamp) {
            _max_purgeable = _get_max_purgeable(*_dk);
        }
        return t.timestamp < _max_purgeable;
    };
public:
    compact_mutation(compact_mutation&&) = delete; // Because 'this' is captured

    compact_mutation(const schema& s, gc_clock::time_point query_time, const query::partition_slice& slice, uint32_t limit,
              uint32_t partition_limit, CompactedMutationsConsumer consumer)
        : _schema(s)
        , _query_time(query_time)
        , _gc_before(saturating_subtract(query_time, s.gc_grace_seconds()))
        , _can_gc(always_gc)
        , _slice(slice)
        , _row_limit(limit)
        , _partition_limit(partition_limit)
        , _partition_row_limit(_slice.options.contains(query::partition_slice::option::distinct) ? 1 : slice.partition_row_limit())
        , _consumer(std::move(consumer))
        , _range_tombstones(s, _slice.options.contains(query::partition_slice::option::reversed))
    {
        static_assert(!sstable_compaction(), "This constructor cannot be used for sstable compaction.");
    }

    compact_mutation(const schema& s, gc_clock::time_point compaction_time, CompactedMutationsConsumer consumer,
                     std::function<api::timestamp_type(const dht::decorated_key&)> get_max_purgeable)
        : _schema(s)
        , _query_time(compaction_time)
        , _gc_before(saturating_subtract(_query_time, s.gc_grace_seconds()))
        , _get_max_purgeable(std::move(get_max_purgeable))
        , _can_gc([this] (tombstone t) { return can_gc(t); })
        , _slice(s.full_slice())
        , _consumer(std::move(consumer))
        , _range_tombstones(s, false)
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
        _range_tombstones.clear();
        _current_partition_limit = std::min(_row_limit, _partition_row_limit);
        _max_purgeable = api::missing_timestamp;
    }

    void consume(tombstone t) {
        _range_tombstones.set_partition_tombstone(t);
        if (!only_live() && !can_purge_tombstone(t)) {
            partition_is_not_empty();
        }
    }

    stop_iteration consume(static_row&& sr) {
        auto current_tombstone = _range_tombstones.get_partition_tombstone();
        bool is_live = sr.cells().compact_and_expire(_schema, column_kind::static_column,
                                                     row_tombstone(current_tombstone),
                                                     _query_time, _can_gc, _gc_before);
        _static_row_live = is_live;
        if (is_live || (!only_live() && !sr.empty())) {
            partition_is_not_empty();
            return _consumer.consume(std::move(sr), current_tombstone, is_live);
        }
        return stop_iteration::no;
    }

    stop_iteration consume(clustering_row&& cr) {
        auto current_tombstone = _range_tombstones.tombstone_for_row(cr.key());
        auto t = cr.tomb();
        if (t.tomb() <= current_tombstone || can_purge_tombstone(t)) {
            cr.remove_tombstone();
        }
        t.apply(current_tombstone);
        bool is_live = cr.marker().compact_and_expire(t.tomb(), _query_time, _can_gc, _gc_before);
        is_live |= cr.cells().compact_and_expire(_schema, column_kind::regular_column, t, _query_time, _can_gc, _gc_before);
        if (only_live() && is_live) {
            partition_is_not_empty();
            auto stop = _consumer.consume(std::move(cr), t, true);
            if (++_rows_in_current_partition == _current_partition_limit) {
                return stop_iteration::yes;
            }
            return stop;
        } else if (!only_live()) {
            auto stop = stop_iteration::no;
            if (!cr.empty()) {
                partition_is_not_empty();
                stop = _consumer.consume(std::move(cr), t, is_live);
            }
            if (!sstable_compaction() && is_live && ++_rows_in_current_partition == _current_partition_limit) {
                return stop_iteration::yes;
            }
            return stop;
        }
        return stop_iteration::no;
    }

    stop_iteration consume(range_tombstone&& rt) {
        _range_tombstones.apply(rt);
        // FIXME: drop tombstone if it is fully covered by other range tombstones
        if (!can_purge_tombstone(rt.tomb) && rt.tomb > _range_tombstones.get_partition_tombstone()) {
            partition_is_not_empty();
            return _consumer.consume(std::move(rt));
        }
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
            _partition_limit -= _rows_in_current_partition > 0;
            auto stop = _consumer.consume_end_of_partition();
            if (!sstable_compaction()) {
                return _row_limit && _partition_limit && stop != stop_iteration::yes
                       ? stop_iteration::no : stop_iteration::yes;
            }
        }
        return stop_iteration::no;
    }

    auto consume_end_of_stream() {
        return _consumer.consume_end_of_stream();
    }
};

template<emit_only_live_rows only_live, typename CompactedMutationsConsumer>
struct compact_for_query : compact_mutation<only_live, compact_for_sstables::no, CompactedMutationsConsumer> {
    using compact_mutation<only_live, compact_for_sstables::no, CompactedMutationsConsumer>::compact_mutation;
};

template<typename CompactedMutationsConsumer>
struct compact_for_compaction : compact_mutation<emit_only_live_rows::no, compact_for_sstables::yes, CompactedMutationsConsumer> {
    using compact_mutation<emit_only_live_rows::no, compact_for_sstables::yes, CompactedMutationsConsumer>::compact_mutation;
};
