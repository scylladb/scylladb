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

#include "mutation_fragment.hh"

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

GCC6_CONCEPT(
    template<typename T>
    concept bool CompactedFragmentsConsumer = requires(T obj, tombstone t, const dht::decorated_key& dk, static_row sr,
            clustering_row cr, range_tombstone rt, tombstone current_tombstone, row_tombstone current_row_tombstone, bool is_alive) {
        obj.consume_new_partition(dk);
        obj.consume(t);
        { obj.consume(std::move(sr), current_tombstone, is_alive) } -> stop_iteration;
        { obj.consume(std::move(cr), current_row_tombstone, is_alive) } -> stop_iteration;
        { obj.consume(std::move(rt)) } -> stop_iteration;
        { obj.consume_end_of_partition() } -> stop_iteration;
        obj.consume_end_of_stream();
    };
)

struct detached_compaction_state {
    ::partition_start partition_start;
    std::optional<::static_row> static_row;
    std::deque<range_tombstone> range_tombstones;
};

// emit_only_live::yes will cause compact_for_query to emit only live
// static and clustering rows. It doesn't affect the way range tombstones are
// emitted.
template<emit_only_live_rows OnlyLive, compact_for_sstables SSTableCompaction>
class compact_mutation_state {
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

    range_tombstone_accumulator _range_tombstones;

    bool _static_row_live{};
    uint32_t _rows_in_current_partition;
    uint32_t _current_partition_limit;
    bool _empty_partition{};
    const dht::decorated_key* _dk{};
    dht::decorated_key _last_dk;
    bool _has_ck_selector{};

    std::optional<static_row> _last_static_row;
private:
    static constexpr bool only_live() {
        return OnlyLive == emit_only_live_rows::yes;
    }
    static constexpr bool sstable_compaction() {
        return SSTableCompaction == compact_for_sstables::yes;
    }

    template <typename Consumer>
    void partition_is_not_empty(Consumer& consumer) {
        if (_empty_partition) {
            _empty_partition = false;
            consumer.consume_new_partition(*_dk);
            auto pt = _range_tombstones.get_partition_tombstone();
            if (pt && !can_purge_tombstone(pt)) {
                consumer.consume(pt);
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
    struct parameters {
        static constexpr emit_only_live_rows only_live = OnlyLive;
        static constexpr compact_for_sstables sstable_compaction = SSTableCompaction;
    };

    compact_mutation_state(compact_mutation_state&&) = delete; // Because 'this' is captured

    compact_mutation_state(const schema& s, gc_clock::time_point query_time, const query::partition_slice& slice, uint32_t limit,
              uint32_t partition_limit)
        : _schema(s)
        , _query_time(query_time)
        , _gc_before(saturating_subtract(query_time, s.gc_grace_seconds()))
        , _can_gc(always_gc)
        , _slice(slice)
        , _row_limit(limit)
        , _partition_limit(partition_limit)
        , _partition_row_limit(_slice.options.contains(query::partition_slice::option::distinct) ? 1 : slice.partition_row_limit())
        , _range_tombstones(s, _slice.options.contains(query::partition_slice::option::reversed))
        , _last_dk({dht::token(), partition_key::make_empty()})
    {
        static_assert(!sstable_compaction(), "This constructor cannot be used for sstable compaction.");
    }

    compact_mutation_state(const schema& s, gc_clock::time_point compaction_time,
            std::function<api::timestamp_type(const dht::decorated_key&)> get_max_purgeable)
        : _schema(s)
        , _query_time(compaction_time)
        , _gc_before(saturating_subtract(_query_time, s.gc_grace_seconds()))
        , _get_max_purgeable(std::move(get_max_purgeable))
        , _can_gc([this] (tombstone t) { return can_gc(t); })
        , _slice(s.full_slice())
        , _range_tombstones(s, false)
        , _last_dk({dht::token(), partition_key::make_empty()})
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
        _last_static_row.reset();
    }

    template <typename Consumer>
    GCC6_CONCEPT(
        requires CompactedFragmentsConsumer<Consumer>
    )
    void consume(tombstone t, Consumer& consumer) {
        _range_tombstones.set_partition_tombstone(t);
        if (!only_live() && !can_purge_tombstone(t)) {
            partition_is_not_empty(consumer);
        }
    }

    template <typename Consumer>
    GCC6_CONCEPT(
        requires CompactedFragmentsConsumer<Consumer>
    )
    stop_iteration consume(static_row&& sr, Consumer& consumer) {
        _last_static_row = static_row(_schema, sr);
        auto current_tombstone = _range_tombstones.get_partition_tombstone();
        bool is_live = sr.cells().compact_and_expire(_schema, column_kind::static_column,
                                                     row_tombstone(current_tombstone),
                                                     _query_time, _can_gc, _gc_before);
        _static_row_live = is_live;
        if (is_live || (!only_live() && !sr.empty())) {
            partition_is_not_empty(consumer);
            return consumer.consume(std::move(sr), current_tombstone, is_live);
        }
        return stop_iteration::no;
    }

    template <typename Consumer>
    GCC6_CONCEPT(
        requires CompactedFragmentsConsumer<Consumer>
    )
    stop_iteration consume(clustering_row&& cr, Consumer& consumer) {
        auto current_tombstone = _range_tombstones.tombstone_for_row(cr.key());
        auto t = cr.tomb();
        if (t.tomb() <= current_tombstone || can_purge_tombstone(t)) {
            cr.remove_tombstone();
        }
        t.apply(current_tombstone);
        bool is_live = cr.marker().compact_and_expire(t.tomb(), _query_time, _can_gc, _gc_before);
        is_live |= cr.cells().compact_and_expire(_schema, column_kind::regular_column, t, _query_time, _can_gc, _gc_before, cr.marker());
        if (only_live() && is_live) {
            partition_is_not_empty(consumer);
            auto stop = consumer.consume(std::move(cr), t, true);
            if (++_rows_in_current_partition == _current_partition_limit) {
                return stop_iteration::yes;
            }
            return stop;
        } else if (!only_live()) {
            auto stop = stop_iteration::no;
            if (!cr.empty()) {
                partition_is_not_empty(consumer);
                stop = consumer.consume(std::move(cr), t, is_live);
            }
            if (!sstable_compaction() && is_live && ++_rows_in_current_partition == _current_partition_limit) {
                return stop_iteration::yes;
            }
            return stop;
        }
        return stop_iteration::no;
    }

    template <typename Consumer>
    GCC6_CONCEPT(
        requires CompactedFragmentsConsumer<Consumer>
    )
    stop_iteration consume(range_tombstone&& rt, Consumer& consumer) {
        _range_tombstones.apply(rt);
        // FIXME: drop tombstone if it is fully covered by other range tombstones
        if (!can_purge_tombstone(rt.tomb) && rt.tomb > _range_tombstones.get_partition_tombstone()) {
            partition_is_not_empty(consumer);
            return consumer.consume(std::move(rt));
        }
        return stop_iteration::no;
    }

    template <typename Consumer>
    GCC6_CONCEPT(
        requires CompactedFragmentsConsumer<Consumer>
    )
    stop_iteration consume_end_of_partition(Consumer& consumer) {
        if (!_empty_partition) {
            // #589 - Do not add extra row for statics unless we did a CK range-less query.
            // See comment in query
            if (_rows_in_current_partition == 0 && _static_row_live && !_has_ck_selector) {
                ++_rows_in_current_partition;
            }

            _row_limit -= _rows_in_current_partition;
            _partition_limit -= _rows_in_current_partition > 0;
            auto stop = consumer.consume_end_of_partition();
            if (!sstable_compaction()) {
                return _row_limit && _partition_limit && stop != stop_iteration::yes
                       ? stop_iteration::no : stop_iteration::yes;
            }
        }
        return stop_iteration::no;
    }

    template <typename Consumer>
    GCC6_CONCEPT(
        requires CompactedFragmentsConsumer<Consumer>
    )
    auto consume_end_of_stream(Consumer& consumer) {
        if (_dk) {
            _last_dk = *_dk;
            _dk = &_last_dk;
        }
        return consumer.consume_end_of_stream();
    }

    /// The decorated key of the partition the compaction is positioned in.
    /// Can be null if the compaction wasn't started yet.
    const dht::decorated_key* current_partition() const {
        return _dk;
    }

    /// Reset limits and query-time to the new page's ones and re-emit the
    /// partition-header and static row if there are clustering rows or range
    /// tombstones left in the partition.
    template <typename Consumer>
    GCC6_CONCEPT(
        requires CompactedFragmentsConsumer<Consumer>
    )
    void start_new_page(uint32_t row_limit,
            uint32_t partition_limit,
            gc_clock::time_point query_time,
            mutation_fragment::kind next_fragment_kind,
            Consumer& consumer) {
        _empty_partition = true;
        _static_row_live = false;
        _row_limit = row_limit;
        _partition_limit = partition_limit;
        _rows_in_current_partition = 0;
        _current_partition_limit = std::min(_row_limit, _partition_row_limit);
        _query_time = query_time;
        _gc_before = saturating_subtract(query_time, _schema.gc_grace_seconds());

        if ((next_fragment_kind == mutation_fragment::kind::clustering_row || next_fragment_kind == mutation_fragment::kind::range_tombstone)
                && _last_static_row) {
            // Stopping here would cause an infinite loop so ignore return value.
            consume(*std::exchange(_last_static_row, {}), consumer);
        }
    }

    bool are_limits_reached() const {
        return _row_limit == 0 || _partition_limit == 0;
    }

    /// Detach the internal state of the compactor
    ///
    /// The state is represented by the last seen partition header, static row
    /// and active range tombstones. Replaying these fragments through a new
    /// compactor will result in the new compactor being in the same state *this
    /// is (given the same outside parameters of course). Practically this
    /// allows the compaction state to be stored in the compacted reader.
    detached_compaction_state detach_state() && {
        partition_start ps(std::move(_last_dk), _range_tombstones.get_partition_tombstone());
        return {std::move(ps), std::move(_last_static_row), std::move(_range_tombstones).range_tombstones()};
    }
};

template<emit_only_live_rows OnlyLive, compact_for_sstables SSTableCompaction, typename Consumer>
GCC6_CONCEPT(
    requires CompactedFragmentsConsumer<Consumer>
)
class compact_mutation {
    lw_shared_ptr<compact_mutation_state<OnlyLive, SSTableCompaction>> _state;
    Consumer _consumer;

public:
    compact_mutation(const schema& s, gc_clock::time_point query_time, const query::partition_slice& slice, uint32_t limit,
              uint32_t partition_limit, Consumer consumer)
        : _state(make_lw_shared<compact_mutation_state<OnlyLive, SSTableCompaction>>(s, query_time, slice, limit, partition_limit))
        , _consumer(std::move(consumer)) {
    }

    compact_mutation(const schema& s, gc_clock::time_point compaction_time, Consumer consumer,
                     std::function<api::timestamp_type(const dht::decorated_key&)> get_max_purgeable)
        : _state(make_lw_shared<compact_mutation_state<OnlyLive, SSTableCompaction>>(s, compaction_time, get_max_purgeable))
        , _consumer(std::move(consumer)) {
    }

    compact_mutation(lw_shared_ptr<compact_mutation_state<OnlyLive, SSTableCompaction>> state, Consumer consumer)
        : _state(std::move(state))
        , _consumer(std::move(consumer)) {
    }

    void consume_new_partition(const dht::decorated_key& dk) {
        _state->consume_new_partition(dk);
    }

    void consume(tombstone t) {
        _state->consume(std::move(t), _consumer);
    }

    stop_iteration consume(static_row&& sr) {
        return _state->consume(std::move(sr), _consumer);
    }

    stop_iteration consume(clustering_row&& cr) {
        return _state->consume(std::move(cr), _consumer);
    }

    stop_iteration consume(range_tombstone&& rt) {
        return _state->consume(std::move(rt), _consumer);
    }

    stop_iteration consume_end_of_partition() {
        return _state->consume_end_of_partition(_consumer);
    }

    auto consume_end_of_stream() {
        return _state->consume_end_of_stream(_consumer);
    }
};

template<emit_only_live_rows only_live, typename Consumer>
GCC6_CONCEPT(
    requires CompactedFragmentsConsumer<Consumer>
)
struct compact_for_query : compact_mutation<only_live, compact_for_sstables::no, Consumer> {
    using compact_mutation<only_live, compact_for_sstables::no, Consumer>::compact_mutation;
};

template<emit_only_live_rows OnlyLive>
using compact_for_query_state = compact_mutation_state<OnlyLive, compact_for_sstables::no>;
using compact_for_mutation_query_state = compact_for_query_state<emit_only_live_rows::no>;
using compact_for_data_query_state = compact_for_query_state<emit_only_live_rows::yes>;

template<typename Consumer>
GCC6_CONCEPT(
    requires CompactedFragmentsConsumer<Consumer>
)
struct compact_for_compaction : compact_mutation<emit_only_live_rows::no, compact_for_sstables::yes, Consumer> {
    using compact_mutation<emit_only_live_rows::no, compact_for_sstables::yes, Consumer>::compact_mutation;
};
