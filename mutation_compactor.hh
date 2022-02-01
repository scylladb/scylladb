/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "compaction/compaction_garbage_collector.hh"
#include "mutation_fragment.hh"
#include "range_tombstone_assembler.hh"
#include "tombstone_gc.hh"

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

enum class compactor_output_format {
    v1,
    v2
};

template<typename T>
concept CompactedFragmentsConsumer = requires(T obj, tombstone t, const dht::decorated_key& dk, static_row sr,
        clustering_row cr, range_tombstone rt, tombstone current_tombstone, row_tombstone current_row_tombstone, bool is_alive) {
    obj.consume_new_partition(dk);
    obj.consume(t);
    { obj.consume(std::move(sr), current_tombstone, is_alive) } -> std::same_as<stop_iteration>;
    { obj.consume(std::move(cr), current_row_tombstone, is_alive) } -> std::same_as<stop_iteration>;
    { obj.consume(std::move(rt)) } -> std::same_as<stop_iteration>;
    { obj.consume_end_of_partition() } -> std::same_as<stop_iteration>;
    obj.consume_end_of_stream();
};

template<typename T>
concept CompactedFragmentsConsumerV2 = requires(T obj, tombstone t, const dht::decorated_key& dk, static_row sr,
        clustering_row cr, range_tombstone_change rtc, tombstone current_tombstone, row_tombstone current_row_tombstone, bool is_alive) {
    obj.consume_new_partition(dk);
    obj.consume(t);
    { obj.consume(std::move(sr), current_tombstone, is_alive) } -> std::same_as<stop_iteration>;
    { obj.consume(std::move(cr), current_row_tombstone, is_alive) } -> std::same_as<stop_iteration>;
    { obj.consume(std::move(rtc)) } -> std::same_as<stop_iteration>;
    { obj.consume_end_of_partition() } -> std::same_as<stop_iteration>;
    obj.consume_end_of_stream();
};

// TODO: I want to make this choose the right concept for OutputFormat but
// probably not worth the effort for the (hopefully) brief time for which we
// have to support both.
template<typename T, compactor_output_format OutputFormat>
concept CompactedFragmentsConsumerWithVersion = CompactedFragmentsConsumer<T> || CompactedFragmentsConsumerV2<T>;

struct detached_compaction_state {
    ::partition_start partition_start;
    std::optional<::static_row> static_row;
    std::optional<range_tombstone_change> current_tombstone;
};

class noop_compacted_fragments_consumer {
public:
    void consume_new_partition(const dht::decorated_key& dk) {}
    void consume(tombstone t) {}
    stop_iteration consume(static_row&& sr, tombstone, bool) { return stop_iteration::no; }
    stop_iteration consume(clustering_row&& cr, row_tombstone, bool) { return stop_iteration::no; }
    stop_iteration consume(range_tombstone&& rt) { return stop_iteration::no; }
    stop_iteration consume(range_tombstone_change&& rtc) { return stop_iteration::no; }
    stop_iteration consume_end_of_partition() { return stop_iteration::no; }
    void consume_end_of_stream() {}
};

class mutation_compactor_garbage_collector : public compaction_garbage_collector {
    const schema& _schema;
    column_kind _kind;
    std::optional<clustering_key> _ckey;
    row_tombstone _tomb;
    row_marker _marker;
    row _row;

public:
    explicit mutation_compactor_garbage_collector(const schema& schema)
        : _schema(schema) {
    }
    void start_collecting_static_row() {
        _kind = column_kind::static_column;
    }
    void start_collecting_clustering_row(clustering_key ckey) {
        _kind = column_kind::regular_column;
        _ckey = std::move(ckey);
    }
    void collect(row_tombstone tomb) {
        _tomb = tomb;
    }
    virtual void collect(column_id id, atomic_cell cell) override {
        _row.apply(_schema.column_at(_kind, id), std::move(cell));
    }
    virtual void collect(column_id id, collection_mutation_description mut) override {
        if (mut.tomb || !mut.cells.empty()) {
            const auto& cdef = _schema.column_at(_kind, id);
            _row.apply(cdef, mut.serialize(*cdef.type));
        }
    }
    virtual void collect(row_marker marker) override {
        _marker = marker;
    }
    template <typename Consumer>
    void consume_static_row(Consumer&& consumer) {
        if (!_row.empty()) {
            consumer(static_row(std::move(_row)));
            _row = {};
        }
    }
    template <typename Consumer>
    void consume_clustering_row(Consumer&& consumer) {
        if (_tomb || !_marker.is_missing() || !_row.empty()) {
            consumer(clustering_row(std::move(*_ckey), _tomb, _marker, std::move(_row)));
            _ckey.reset();
            _tomb = {};
            _marker = {};
            _row = {};
        }
    }
};

struct compaction_stats {
    struct row_stats {
        uint64_t live = 0;
        uint64_t dead = 0;

        void operator+=(bool is_live) {
            live += is_live;
            dead += !is_live;
        }
        uint64_t total() const {
            return live + dead;
        }
    };

    uint64_t partitions = 0;
    row_stats static_rows;
    row_stats clustering_rows;
    uint64_t range_tombstones = 0;
};

// emit_only_live::yes will cause compact_for_query to emit only live
// static and clustering rows. It doesn't affect the way range tombstones are
// emitted.
template<emit_only_live_rows OnlyLive, compact_for_sstables SSTableCompaction, compactor_output_format OutputFormat = compactor_output_format::v1>
class compact_mutation_state {
    const schema& _schema;
    gc_clock::time_point _query_time;
    std::function<api::timestamp_type(const dht::decorated_key&)> _get_max_purgeable;
    can_gc_fn _can_gc;
    api::timestamp_type _max_purgeable = api::missing_timestamp;
    std::optional<gc_clock::time_point> _gc_before;
    const query::partition_slice& _slice;
    uint64_t _row_limit{};
    uint32_t _partition_limit{};
    uint64_t _partition_row_limit{};

    tombstone _partition_tombstone;
    range_tombstone_assembler _rt_assembler;

    bool _static_row_live{};
    uint64_t _rows_in_current_partition;
    uint32_t _current_partition_limit;
    bool _empty_partition{};
    bool _empty_partition_in_gc_consumer{};
    const dht::decorated_key* _dk{};
    dht::decorated_key _last_dk;
    bool _return_static_content_on_partition_with_no_rows{};

    std::optional<static_row> _last_static_row;
    position_in_partition _last_clustering_pos;
    // Currently active tombstone, can be different than the tombstone emitted to
    // the regular consumer (_current_emitted_tombstone) because even purged
    // tombstone that are not emitted are still applied to data when compacting.
    tombstone _effective_tombstone;
    // Track last emitted tombstone to regular and gc consumers respectively.
    // Used to determine whether any active tombstones need closing at EOS.
    tombstone _current_emitted_tombstone;
    tombstone _current_emitted_gc_tombstone;

    std::unique_ptr<mutation_compactor_garbage_collector> _collector;

    compaction_stats _stats;
private:
    template <typename Consumer, typename GCConsumer>
    requires CompactedFragmentsConsumerWithVersion<Consumer, OutputFormat> && CompactedFragmentsConsumerWithVersion<GCConsumer, OutputFormat>
    stop_iteration do_consume(range_tombstone&& rt, Consumer& consumer, GCConsumer& gc_consumer) {
        if (rt.tomb <= _partition_tombstone) {
            return stop_iteration::no;
        }
        if (can_purge_tombstone(rt.tomb)) {
            partition_is_not_empty_for_gc_consumer(gc_consumer);
            return gc_consumer.consume(std::move(rt));
        } else {
            partition_is_not_empty(consumer);
            return consumer.consume(std::move(rt));
        }
    }
    template <typename Consumer, typename GCConsumer>
    requires CompactedFragmentsConsumerWithVersion<Consumer, OutputFormat> && CompactedFragmentsConsumerWithVersion<GCConsumer, OutputFormat>
    stop_iteration do_consume(range_tombstone_change&& rtc, Consumer& consumer, GCConsumer& gc_consumer) {
        stop_iteration gc_consumer_stop = stop_iteration::no;
        stop_iteration consumer_stop = stop_iteration::no;
        if (rtc.tombstone() <= _partition_tombstone) {
            rtc.set_tombstone({});
        }
        _effective_tombstone = rtc.tombstone();
        const auto can_purge = rtc.tombstone() && can_purge_tombstone(rtc.tombstone());
        if (can_purge || _current_emitted_gc_tombstone) {
            partition_is_not_empty_for_gc_consumer(gc_consumer);
            auto tomb = can_purge ? rtc.tombstone() : tombstone{};
            _current_emitted_gc_tombstone = tomb;
            gc_consumer_stop = gc_consumer.consume(range_tombstone_change(rtc.position(), tomb));
            if (can_purge) {
                rtc.set_tombstone({});
            }
        }
        // If we have a previous active tombstone we emit the current one even if it is purged.
        if (!can_purge || _current_emitted_tombstone) {
            partition_is_not_empty(consumer);
            _current_emitted_tombstone = rtc.tombstone();
            consumer_stop = consumer.consume(std::move(rtc));
        }
        return gc_consumer_stop || consumer_stop;
    }
    template <typename Consumer, typename GCConsumer>
    tombstone tombstone_for_row(const clustering_key& ckey, Consumer& consumer, GCConsumer& gc_consumer) {
        if constexpr (OutputFormat == compactor_output_format::v2) {
            return std::max(_partition_tombstone, _effective_tombstone);
        } else {
            if (_rt_assembler.needs_flush()) {
                if (auto rt_opt = _rt_assembler.flush(_schema, position_in_partition::after_key(ckey))) {
                    do_consume(std::move(*rt_opt), consumer, gc_consumer);
                }
            }
            return std::max(_partition_tombstone, _rt_assembler.get_current_tombstone());
        }
    }
    static constexpr bool only_live() {
        return OnlyLive == emit_only_live_rows::yes;
    }
    static constexpr bool sstable_compaction() {
        return SSTableCompaction == compact_for_sstables::yes;
    }

    template <typename GCConsumer>
    void partition_is_not_empty_for_gc_consumer(GCConsumer& gc_consumer) {
        if (_empty_partition_in_gc_consumer) {
            _empty_partition_in_gc_consumer = false;
            gc_consumer.consume_new_partition(*_dk);
            auto pt = _partition_tombstone;
            if (pt && can_purge_tombstone(pt)) {
                gc_consumer.consume(pt);
            }
        }
    }

    template <typename Consumer>
    void partition_is_not_empty(Consumer& consumer) {
        if (_empty_partition) {
            _empty_partition = false;
            ++_stats.partitions;
            consumer.consume_new_partition(*_dk);
            auto pt = _partition_tombstone;
            if (pt && !can_purge_tombstone(pt)) {
                consumer.consume(pt);
            }
        }
    }

    bool can_purge_tombstone(const tombstone& t) {
        return can_gc(t) && t.deletion_time < get_gc_before();
    };

    bool can_purge_tombstone(const row_tombstone& t) {
        return can_gc(t.tomb()) && t.max_deletion_time() < get_gc_before();
    };

    gc_clock::time_point get_gc_before() {
        if (_gc_before) {
            return _gc_before.value();
        } else {
            if (_dk) {
                _gc_before = ::get_gc_before_for_key(_schema.shared_from_this(), *_dk, _query_time);
                return _gc_before.value();
            } else {
                return gc_clock::time_point::min();
            }
        }
    }

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

    compact_mutation_state(const schema& s, gc_clock::time_point query_time, const query::partition_slice& slice, uint64_t limit,
              uint32_t partition_limit)
        : _schema(s)
        , _query_time(query_time)
        , _can_gc(always_gc)
        , _slice(slice)
        , _row_limit(limit)
        , _partition_limit(partition_limit)
        , _partition_row_limit(_slice.options.contains(query::partition_slice::option::distinct) ? 1 : slice.partition_row_limit())
        , _last_dk({dht::token(), partition_key::make_empty()})
        , _last_clustering_pos(position_in_partition::before_all_clustered_rows())
    {
        static_assert(!sstable_compaction(), "This constructor cannot be used for sstable compaction.");
    }

    compact_mutation_state(const schema& s, gc_clock::time_point compaction_time,
            std::function<api::timestamp_type(const dht::decorated_key&)> get_max_purgeable)
        : _schema(s)
        , _query_time(compaction_time)
        , _get_max_purgeable(std::move(get_max_purgeable))
        , _can_gc([this] (tombstone t) { return can_gc(t); })
        , _slice(s.full_slice())
        , _last_dk({dht::token(), partition_key::make_empty()})
        , _last_clustering_pos(position_in_partition::before_all_clustered_rows())
        , _collector(std::make_unique<mutation_compactor_garbage_collector>(_schema))
    {
        static_assert(sstable_compaction(), "This constructor can only be used for sstable compaction.");
        static_assert(!only_live(), "SSTable compaction cannot be run with emit_only_live_rows::yes.");
    }

    void consume_new_partition(const dht::decorated_key& dk) {
        auto& pk = dk.key();
        _dk = &dk;
        _return_static_content_on_partition_with_no_rows =
            _slice.options.contains(query::partition_slice::option::always_return_static_content) ||
            !has_ck_selector(_slice.row_ranges(_schema, pk));
        _empty_partition = true;
        _empty_partition_in_gc_consumer = true;
        _rows_in_current_partition = 0;
        _static_row_live = false;
        _partition_tombstone = {};
        _rt_assembler.reset();
        _current_partition_limit = std::min(_row_limit, _partition_row_limit);
        _max_purgeable = api::missing_timestamp;
        _gc_before = std::nullopt;
        _last_static_row.reset();
        _last_clustering_pos = position_in_partition::before_all_clustered_rows();
        _effective_tombstone = {};
        _current_emitted_tombstone = {};
        _current_emitted_gc_tombstone = {};
    }

    template <typename Consumer, typename GCConsumer>
    requires CompactedFragmentsConsumerWithVersion<Consumer, OutputFormat> && CompactedFragmentsConsumerWithVersion<GCConsumer, OutputFormat>
    void consume(tombstone t, Consumer& consumer, GCConsumer& gc_consumer) {
        _partition_tombstone = t;
        if (!only_live()) {
            if (can_purge_tombstone(t)) {
                partition_is_not_empty_for_gc_consumer(gc_consumer);
            } else {
                partition_is_not_empty(consumer);
            }
         }
    }

    template <typename Consumer>
    requires CompactedFragmentsConsumerWithVersion<Consumer, OutputFormat>
    void force_partition_not_empty(Consumer& consumer) {
        partition_is_not_empty(consumer);
    }

    template <typename Consumer, typename GCConsumer>
    requires CompactedFragmentsConsumerWithVersion<Consumer, OutputFormat> && CompactedFragmentsConsumerWithVersion<GCConsumer, OutputFormat>
    stop_iteration consume(static_row&& sr, Consumer& consumer, GCConsumer& gc_consumer) {
        _last_static_row = static_row(_schema, sr);
        auto current_tombstone = _partition_tombstone;
        if constexpr (sstable_compaction()) {
            _collector->start_collecting_static_row();
        }
        auto gc_before = get_gc_before();
        bool is_live = sr.cells().compact_and_expire(_schema, column_kind::static_column, row_tombstone(current_tombstone),
                _query_time, _can_gc, gc_before, _collector.get());
        _stats.static_rows += is_live;
        if constexpr (sstable_compaction()) {
            _collector->consume_static_row([this, &gc_consumer, current_tombstone] (static_row&& sr_garbage) {
                partition_is_not_empty_for_gc_consumer(gc_consumer);
                // We are passing only dead (purged) data so pass is_live=false.
                gc_consumer.consume(std::move(sr_garbage), current_tombstone, false);
            });
        } else {
            if (can_purge_tombstone(current_tombstone)) {
                current_tombstone = {};
            }
        }
        _static_row_live = is_live;
        if (is_live || (!only_live() && !sr.empty())) {
            partition_is_not_empty(consumer);
            return consumer.consume(std::move(sr), current_tombstone, is_live);
        }
        return stop_iteration::no;
    }

    template <typename Consumer, typename GCConsumer>
    requires CompactedFragmentsConsumerWithVersion<Consumer, OutputFormat> && CompactedFragmentsConsumerWithVersion<GCConsumer, OutputFormat>
    stop_iteration consume(clustering_row&& cr, Consumer& consumer, GCConsumer& gc_consumer) {
        if (!sstable_compaction()) {
            _last_clustering_pos = cr.position();
        }
        auto current_tombstone = tombstone_for_row(cr.key(), consumer, gc_consumer);
        auto t = cr.tomb();
        t.apply(current_tombstone);

        if constexpr (sstable_compaction()) {
            _collector->start_collecting_clustering_row(cr.key());
        }

        {
            const auto rt = cr.tomb();
            if (rt.tomb() <= current_tombstone) {
                cr.remove_tombstone();
            } else if (can_purge_tombstone(rt)) {
                if constexpr (sstable_compaction()) {
                    _collector->collect(rt);
                }
                cr.remove_tombstone();
            }
        }
        auto gc_before = get_gc_before();
        bool is_live = cr.marker().compact_and_expire(t.tomb(), _query_time, _can_gc, gc_before, _collector.get());
        is_live |= cr.cells().compact_and_expire(_schema, column_kind::regular_column, t, _query_time, _can_gc, gc_before, cr.marker(),
                _collector.get());
        _stats.clustering_rows += is_live;

        if constexpr (sstable_compaction()) {
            _collector->consume_clustering_row([this, &gc_consumer, t] (clustering_row&& cr_garbage) {
                partition_is_not_empty_for_gc_consumer(gc_consumer);
                // We are passing only dead (purged) data so pass is_live=false.
                gc_consumer.consume(std::move(cr_garbage), t, false);
            });
        } else {
            if (can_purge_tombstone(t)) {
                t = {};
            }
        }

        auto consume_row = [&] () mutable {
            partition_is_not_empty(consumer);
            return consumer.consume(std::move(cr), t, is_live);
        };

        if (only_live() && is_live) {
            auto stop = consume_row();
            if (++_rows_in_current_partition == _current_partition_limit) {
                return stop_iteration::yes;
            }
            return stop;
        } else if (!only_live()) {
            auto stop = stop_iteration::no;
            if (!cr.empty()) {
                stop = consume_row();
            }
            if (!sstable_compaction() && is_live && ++_rows_in_current_partition == _current_partition_limit) {
                return stop_iteration::yes;
            }
            return stop;
        }
        return stop_iteration::no;
    }

    template <typename Consumer, typename GCConsumer>
    requires CompactedFragmentsConsumerWithVersion<Consumer, OutputFormat> && CompactedFragmentsConsumerWithVersion<GCConsumer, OutputFormat>
    stop_iteration consume(range_tombstone_change&& rtc, Consumer& consumer, GCConsumer& gc_consumer) {
        if (!sstable_compaction()) {
            _last_clustering_pos = rtc.position();
        }
        ++_stats.range_tombstones;
        if constexpr (OutputFormat == compactor_output_format::v1) {
            _effective_tombstone = rtc.tombstone();
            if (auto rt_opt = _rt_assembler.consume(_schema, std::move(rtc))) {
                return do_consume(std::move(*rt_opt), consumer, gc_consumer);
            }
        } else {
            do_consume(std::move(rtc), consumer, gc_consumer);
        }
        return stop_iteration::no;
    }

    template <typename Consumer, typename GCConsumer>
    requires CompactedFragmentsConsumerWithVersion<Consumer, OutputFormat> && CompactedFragmentsConsumerWithVersion<GCConsumer, OutputFormat>
    stop_iteration consume_end_of_partition(Consumer& consumer, GCConsumer& gc_consumer) {
        if (_effective_tombstone) {
            auto rtc = range_tombstone_change(position_in_partition::after_key(_last_clustering_pos), tombstone{});
            if constexpr (OutputFormat == compactor_output_format::v1) {
                if (auto rt_opt = _rt_assembler.consume(_schema, std::move(rtc))) {
                    do_consume(std::move(*rt_opt), consumer, gc_consumer);
                }
                _rt_assembler.on_end_of_stream();
            } else {
                // do_consume() overwrites _effective_tombstone with {}, so save and restore it.
                auto prev_tombstone = _effective_tombstone;
                do_consume(std::move(rtc), consumer, gc_consumer);
                _effective_tombstone = prev_tombstone;
            }
        }
        if (!_empty_partition_in_gc_consumer) {
            gc_consumer.consume_end_of_partition();
        }
        if (!_empty_partition) {
            // #589 - Do not add extra row for statics unless we did a CK range-less query.
            // See comment in query
            if (_rows_in_current_partition == 0 && _static_row_live &&
                    _return_static_content_on_partition_with_no_rows) {
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

    template <typename Consumer, typename GCConsumer>
    requires CompactedFragmentsConsumerWithVersion<Consumer, OutputFormat> && CompactedFragmentsConsumerWithVersion<GCConsumer, OutputFormat>
    auto consume_end_of_stream(Consumer& consumer, GCConsumer& gc_consumer) {
        if (_dk) {
            _last_dk = *_dk;
            _dk = &_last_dk;
        }
        if constexpr (std::is_same_v<std::result_of_t<decltype(&GCConsumer::consume_end_of_stream)(GCConsumer&)>, void>) {
            gc_consumer.consume_end_of_stream();
            return consumer.consume_end_of_stream();
        } else {
            return std::pair(consumer.consume_end_of_stream(), gc_consumer.consume_end_of_stream());
        }
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
    requires CompactedFragmentsConsumerWithVersion<Consumer, OutputFormat>
    void start_new_page(uint64_t row_limit,
            uint32_t partition_limit,
            gc_clock::time_point query_time,
            partition_region next_fragment_region,
            Consumer& consumer) {
        _empty_partition = true;
        _static_row_live = false;
        _row_limit = row_limit;
        _partition_limit = partition_limit;
        _rows_in_current_partition = 0;
        _current_partition_limit = std::min(_row_limit, _partition_row_limit);
        _query_time = query_time;
        _stats = {};

        noop_compacted_fragments_consumer nc;

        if (next_fragment_region == partition_region::clustered && _last_static_row) {
            // Stopping here would cause an infinite loop so ignore return value.
            consume(*std::exchange(_last_static_row, {}), consumer, nc);
        }
        if (_effective_tombstone) {
            auto rtc = range_tombstone_change(position_in_partition_view::after_key(_last_clustering_pos), _effective_tombstone);
            if constexpr (OutputFormat == compactor_output_format::v2) {
                do_consume(std::move(rtc), consumer, nc);
            } else if (auto rt_opt = _rt_assembler.consume(_schema, std::move(rtc))) {
                do_consume(std::move(*rt_opt), consumer, nc);
            }
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
        partition_start ps(std::move(_last_dk), _partition_tombstone);
        if (_effective_tombstone) {
            return {std::move(ps), std::move(_last_static_row), range_tombstone_change(position_in_partition_view::after_key(_last_clustering_pos), _effective_tombstone)};
        } else {
            return {std::move(ps), std::move(_last_static_row), std::optional<range_tombstone_change>{}};
        }
    }

    const compaction_stats& stats() const { return _stats; }
};

template<emit_only_live_rows OnlyLive, compact_for_sstables SSTableCompaction, typename Consumer, typename GCConsumer>
requires CompactedFragmentsConsumer<Consumer> && CompactedFragmentsConsumer<GCConsumer>
class compact_mutation {
    lw_shared_ptr<compact_mutation_state<OnlyLive, SSTableCompaction>> _state;
    Consumer _consumer;
    // Garbage Collected Consumer
    GCConsumer _gc_consumer;

public:
    compact_mutation(const schema& s, gc_clock::time_point query_time, const query::partition_slice& slice, uint64_t limit,
              uint32_t partition_limit, Consumer consumer, GCConsumer gc_consumer = GCConsumer())
        : _state(make_lw_shared<compact_mutation_state<OnlyLive, SSTableCompaction>>(s, query_time, slice, limit, partition_limit))
        , _consumer(std::move(consumer))
        , _gc_consumer(std::move(gc_consumer)) {
    }

    compact_mutation(const schema& s, gc_clock::time_point compaction_time,
            std::function<api::timestamp_type(const dht::decorated_key&)> get_max_purgeable,
            Consumer consumer, GCConsumer gc_consumer = GCConsumer())
        : _state(make_lw_shared<compact_mutation_state<OnlyLive, SSTableCompaction>>(s, compaction_time, get_max_purgeable))
        , _consumer(std::move(consumer))
        , _gc_consumer(std::move(gc_consumer)) {
    }

    compact_mutation(lw_shared_ptr<compact_mutation_state<OnlyLive, SSTableCompaction>> state, Consumer consumer,
                     GCConsumer gc_consumer = GCConsumer())
        : _state(std::move(state))
        , _consumer(std::move(consumer))
        , _gc_consumer(std::move(gc_consumer)) {
    }

    void consume_new_partition(const dht::decorated_key& dk) {
        _state->consume_new_partition(dk);
    }

    void consume(tombstone t) {
        _state->consume(std::move(t), _consumer, _gc_consumer);
    }

    stop_iteration consume(static_row&& sr) {
        return _state->consume(std::move(sr), _consumer, _gc_consumer);
    }

    stop_iteration consume(clustering_row&& cr) {
        return _state->consume(std::move(cr), _consumer, _gc_consumer);
    }

    stop_iteration consume(range_tombstone_change&& rtc) {
        return _state->consume(std::move(rtc), _consumer, _gc_consumer);
    }

    stop_iteration consume_end_of_partition() {
        return _state->consume_end_of_partition(_consumer, _gc_consumer);
    }

    auto consume_end_of_stream() {
        return _state->consume_end_of_stream(_consumer, _gc_consumer);
    }
};

template<emit_only_live_rows OnlyLive, compact_for_sstables SSTableCompaction, typename Consumer, typename GCConsumer>
requires CompactedFragmentsConsumerV2<Consumer> && CompactedFragmentsConsumerV2<GCConsumer>
class compact_mutation_v2 {
    lw_shared_ptr<compact_mutation_state<OnlyLive, SSTableCompaction, compactor_output_format::v2>> _state;
    Consumer _consumer;
    // Garbage Collected Consumer
    GCConsumer _gc_consumer;

public:
    compact_mutation_v2(const schema& s, gc_clock::time_point query_time, const query::partition_slice& slice, uint64_t limit,
              uint32_t partition_limit, Consumer consumer, GCConsumer gc_consumer = GCConsumer())
        : _state(make_lw_shared<compact_mutation_state<OnlyLive, SSTableCompaction, compactor_output_format::v2>>(s, query_time, slice, limit, partition_limit))
        , _consumer(std::move(consumer))
        , _gc_consumer(std::move(gc_consumer)) {
    }

    compact_mutation_v2(const schema& s, gc_clock::time_point compaction_time,
            std::function<api::timestamp_type(const dht::decorated_key&)> get_max_purgeable,
            Consumer consumer, GCConsumer gc_consumer = GCConsumer())
        : _state(make_lw_shared<compact_mutation_state<OnlyLive, SSTableCompaction, compactor_output_format::v2>>(s, compaction_time, get_max_purgeable))
        , _consumer(std::move(consumer))
        , _gc_consumer(std::move(gc_consumer)) {
    }

    compact_mutation_v2(lw_shared_ptr<compact_mutation_state<OnlyLive, SSTableCompaction, compactor_output_format::v2>> state, Consumer consumer,
                     GCConsumer gc_consumer = GCConsumer())
        : _state(std::move(state))
        , _consumer(std::move(consumer))
        , _gc_consumer(std::move(gc_consumer)) {
    }

    void consume_new_partition(const dht::decorated_key& dk) {
        _state->consume_new_partition(dk);
    }

    void consume(tombstone t) {
        _state->consume(std::move(t), _consumer, _gc_consumer);
    }

    stop_iteration consume(static_row&& sr) {
        return _state->consume(std::move(sr), _consumer, _gc_consumer);
    }

    stop_iteration consume(clustering_row&& cr) {
        return _state->consume(std::move(cr), _consumer, _gc_consumer);
    }

    stop_iteration consume(range_tombstone_change&& rtc) {
        return _state->consume(std::move(rtc), _consumer, _gc_consumer);
    }

    stop_iteration consume_end_of_partition() {
        return _state->consume_end_of_partition(_consumer, _gc_consumer);
    }

    auto consume_end_of_stream() {
        return _state->consume_end_of_stream(_consumer, _gc_consumer);
    }
};

template<emit_only_live_rows only_live, typename Consumer>
requires CompactedFragmentsConsumer<Consumer>
struct compact_for_query : compact_mutation<only_live, compact_for_sstables::no, Consumer, noop_compacted_fragments_consumer> {
    using compact_mutation<only_live, compact_for_sstables::no, Consumer, noop_compacted_fragments_consumer>::compact_mutation;
};

template<emit_only_live_rows OnlyLive>
using compact_for_query_state = compact_mutation_state<OnlyLive, compact_for_sstables::no>;

template<typename Consumer, typename GCConsumer = noop_compacted_fragments_consumer>
requires CompactedFragmentsConsumer<Consumer> && CompactedFragmentsConsumer<GCConsumer>
struct compact_for_compaction : compact_mutation<emit_only_live_rows::no, compact_for_sstables::yes, Consumer, GCConsumer> {
    using compact_mutation<emit_only_live_rows::no, compact_for_sstables::yes, Consumer, GCConsumer>::compact_mutation;
};

template<emit_only_live_rows only_live, typename Consumer>
requires CompactedFragmentsConsumerV2<Consumer>
struct compact_for_query_v2 : compact_mutation_v2<only_live, compact_for_sstables::no, Consumer, noop_compacted_fragments_consumer> {
    using compact_mutation_v2<only_live, compact_for_sstables::no, Consumer, noop_compacted_fragments_consumer>::compact_mutation_v2;
};

template<emit_only_live_rows OnlyLive>
using compact_for_query_state_v2 = compact_mutation_state<OnlyLive, compact_for_sstables::no, compactor_output_format::v2>;

template<typename Consumer, typename GCConsumer = noop_compacted_fragments_consumer>
requires CompactedFragmentsConsumerV2<Consumer> && CompactedFragmentsConsumerV2<GCConsumer>
struct compact_for_compaction_v2 : compact_mutation_v2<emit_only_live_rows::no, compact_for_sstables::yes, Consumer, GCConsumer> {
    using compact_mutation_v2<emit_only_live_rows::no, compact_for_sstables::yes, Consumer, GCConsumer>::compact_mutation_v2;
};
