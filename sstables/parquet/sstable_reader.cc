/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/sstable_reader.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "query/query-request.hh"
#include "schema/schema.hh"
#include "sstables/abstract_index_reader.hh"
#include "mutation/mutation_fragment_stream_validator.hh"
#include "sstables/mutation_fragment_filter.hh"
#include "sstables/parquet/file_verifier.hh"
#include "sstables/parquet/reader.hh"
#include "sstables/parquet/stats.hh"
#include "sstables/sstable_mutation_reader.hh"
#include "sstables/sstables.hh"
#include "utils/error_injection.hh"
#include "utils/hashers.hh"
#include "utils/value_or_reference.hh"

namespace sstables::parquet {

lw_shared_ptr<const read_plan> sstable_read_state::plan_for(const ::schema& s, const column_translation& ct, const file_summary& summary) {
    if (_plans[0] && _plans[0]->version() == s.version()) [[likely]] {
        return _plans[0];
    }
    if (!_plans[1] || _plans[1]->version() != s.version()) {
        _plans[1] = make_lw_shared<const read_plan>(_mapping, summary, s, ct);
    }
    std::swap(_plans[0], _plans[1]);
    return _plans[0];
}

namespace {

// Deletion times and the trie index
// ----------------------------------
//
// The trie index stores a deletion time in 32 bits, as for `mt`: the writer
// caps a deletion time above max_deletion_time at max_deletion_time
// (writer_impl::to_deletion_time()). The Data file stores it uncapped. So
// for a tombstone past 2038, the index and the Data file differ, and the
// Data file is the authority.
//
// The reader therefore never takes a tombstone from the index whose
// deletion time is at the cap: it reads the Data file instead. This needs
// no change to the index, and costs only reads of such tombstones. A
// tombstone with a deletion time of exactly max_deletion_time is read from
// the Data file too, which is correct, only slower.

// Whether a tombstone from the index may have a capped deletion time.
bool may_be_capped(tombstone t) noexcept {
    return t && t.deletion_time.time_since_epoch().count() == max_deletion_time;
}

// The tombstone that the index stores for a tombstone of the Data file.
tombstone as_in_index(tombstone t) noexcept {
    if (!t) {
        return t;
    }
    bool capped;
    const int32_t ldt = adjusted_local_deletion_time(t.deletion_time, capped);
    return tombstone(t.timestamp, gc_clock::time_point(gc_clock::duration(ldt)));
}

bool is_start_bound(fragment_kind k) noexcept {
    return k == fragment_kind::incl_start || k == fragment_kind::excl_start;
}

bool is_end_bound(fragment_kind k) noexcept {
    return k == fragment_kind::incl_end || k == fragment_kind::excl_end;
}

// A row of the Data file, as a full scan consumes it, for validate().
struct consumed_row {
    uint64_t row;
    // The key and the tombstone of a row that starts a partition.
    std::optional<dht::decorated_key> key;
    tombstone partition_tombstone;
    // The position of a clustering row or a range tombstone bound row.
    std::optional<position_in_partition> position;
};

// Reads partitions of an sstable with a Parquet Data file, in either
// direction.
//
// The reader follows the state machine of the mx reader
// (mx_sstable_mutation_reader and mp_row_consumer_m), with row ordinals in
// place of Data file offsets. The main difference is that the Data file can
// be read at any row. So a row that the reader stops at, for example a
// partition after the queried range, is not consumed: the reader decodes
// it again when it gets there. The mx reader saves such a row instead.
//
// A partition has no row of its own. Its key and tombstone are on its
// first row, the head, which also holds the partition's first fragment. A
// partition ends where the next one starts, or at the end of the rows to read.
//
// With no index reader, the reader reads the whole file and supports
// neither fast-forwarding nor next_partition(), like the mx full scan reader.
//
// Reverse reads
// -------------
//
// Navigation between partitions is the same in both directions: the reader
// moves forward from head to head. Within a partition, a reverse read has
// two phases.
//
// 1. The head phase reads forward, as a forward read does, until the first
//    clustering row or range tombstone bound row. It consumes the head and
//    the static row, or the row of kind "none". A partition without
//    clustering rows or bounds ends there, as in a forward read.
// 2. The backward walk reads the rows from the end of the partition down
//    to that first row. The end of the partition comes from the index. The
//    walk then consumes rows from the top, and every fragment is turned
//    into the reverse order: a range tombstone start bound becomes an end
//    bound and the other way round. A boundary, stored as an end bound row
//    followed by a start bound row, is met as the start bound row first,
//    and the reader merges the pair into one change as it does forward.
//    The head row is the last row of the walk if it holds a clustering row
//    or a bound. Its partition key and partition tombstone are ignored then.
//
// Every row is consumed by the same code in both directions. The direction
// decides only which row comes next, and how a bound kind reads.
//
// The index is used as in a forward read: at the ends of the read and after
// a skip between clustering ranges. A skip to a position in a backward walk
// moves the top of the walk down to the start of the first index block after
// that position (abstract_index_reader::advance_reverse()). The walk never
// starts inside a block. The rows between the position and the block start
// are read and filtered, as a forward read filters the rows between a block
// start and the position. The range tombstone open at the new top of the
// walk is the one that the index records for that block: the tombstone open
// before the block's first row (reverse_end_open_marker()). That is the
// tombstone that a backward walk has open when it reaches the row below. At
// the end of the partition no tombstone is open. Below that point the walk
// tracks the open tombstone from the bound rows, as a forward read does.
// This is what the mx reader does, and it needs nothing more in the index or
// in the Data file.
class sstable_mutation_reader final : public mutation_reader::impl {
    shared_sstable _sst;
    value_or_reference<query::partition_slice> _slice_holder;
    const query::partition_slice& _slice;
    tracing::trace_state_ptr _trace_state;
    const streamed_mutation::forwarding _fwd;
    read_monitor& _monitor;
    std::unique_ptr<abstract_index_reader> _index_reader;
    const bool _single_partition_read;
    std::reference_wrapper<const dht::partition_range> _pr;
    std::optional<utils::hashed_key> _single_partition_read_murmur_hash;

    // Engaged by maybe_initialize(). The decoder refers to the codec
    // reader, so it is declared after it.
    lw_shared_ptr<const read_plan> _plan;
    std::optional<reader> _codec;
    std::optional<row_decoder> _rows;
    bool _initialized = false;
    bool _read_enabled = true;
    // The rows to read are [_next_row, _end_row). The rows before
    // _next_row are consumed. During a backward walk, _next_row stays at
    // the bottom of the walk, and _walk tracks the rows of the partition.
    uint64_t _next_row = 0;
    uint64_t _end_row = 0;
    // The sstable's data_size(), which read progress is measured in.
    uint64_t _data_size = 0;
    reader_position_tracker _progress;

    // Whether the lower bound of the index is in the current partition.
    bool _index_in_current_partition = false;
    // Whether the reader finished the fragments of a partition and has not
    // entered the next one. Implies that partition_end was emitted.
    bool _partition_finished = true;
    // Whether the reader is positioned at a partition boundary or at the end.
    bool _before_partition = true;
    // Whether the head of the current partition is consumed and its later
    // rows are not, the "not at mutation end" of the mx consumer.
    bool _in_partition = false;
    // The head of the current partition, if _in_partition.
    uint64_t _head_row = 0;
    std::optional<dht::decorated_key> _current_partition_key;
    std::optional<mutation_fragment_filter> _mf_filter;

    // The backward walk of a reverse read (see above). The rows that the
    // walk has not consumed are [begin, end), and it consumes end - 1 next.
    // `begin` is the partition's first clustering row or bound row, and
    // `partition_end` is one past the partition's last row.
    struct backward_walk {
        uint64_t begin;
        uint64_t end;
        uint64_t partition_end;
    };
    // Engaged while a reverse read is in the walk of a partition.
    std::optional<backward_walk> _walk;

    // Set for validate(): every row, in order, as the reader first positions
    // its decoder there. A full scan positions it at every row.
    std::vector<consumed_row>* _consumed = nullptr;
    uint64_t _next_to_record = 0;

    bool full_scan() const noexcept {
        return !_index_reader;
    }

    bool reversed() const noexcept {
        return _slice.is_reversed();
    }

    // The row that the reader consumes next.
    uint64_t current_row() const noexcept {
        return _walk ? _walk->end - 1 : _next_row;
    }

    // Whether the reader consumed all rows of the walk, or all rows to read.
    bool at_end() const noexcept {
        return _walk ? _walk->end == _walk->begin : _next_row >= _end_row;
    }

    // The row after the current one in the read direction, if it is one of
    // the rows to read.
    std::optional<uint64_t> following_row() const noexcept {
        if (_walk) {
            return _walk->end - 1 > _walk->begin ? std::optional<uint64_t>(_walk->end - 2) : std::nullopt;
        }
        return _next_row + 1 < _end_row ? std::optional<uint64_t>(_next_row + 1) : std::nullopt;
    }

    // Consumes `n` rows from the current one, in the read direction.
    void consume(uint64_t n) {
        if (_walk) {
            set_walk_end(_walk->end - n);
        } else {
            set_next_row(_next_row + n);
        }
    }

    // A reverse read sees the start bound of a range tombstone as its end,
    // and the end bound as its start. Other kinds stay.
    fragment_kind in_read_order(fragment_kind k) const noexcept {
        if (!reversed()) {
            return k;
        }
        switch (k) {
        case fragment_kind::incl_start: return fragment_kind::incl_end;
        case fragment_kind::excl_start: return fragment_kind::excl_end;
        case fragment_kind::incl_end: return fragment_kind::incl_start;
        case fragment_kind::excl_end: return fragment_kind::excl_start;
        default: return k;
        }
    }

    // The bound row at the decoder's row, with its kind in the read order.
    decoded_bound decode_bound() {
        auto b = _rows->decode_bound();
        if (reversed()) {
            b.kind = reverse_kind(b.kind);
        }
        return b;
    }

    bool proceed() const {
        return !is_buffer_full() && !need_preempt();
    }

    uint64_t progress_of(uint64_t row) const noexcept {
        // Read progress is the total raw size times the rows read over the
        // total rows (PLAN.md, "Size measures").
        const uint64_t rows = _codec->summary().rows();
        if (!rows) {
            return 0;
        }
        // The reader computes this for every row, and the product seldom
        // needs more than 64 bits.
        uint64_t product;
        if (!__builtin_mul_overflow(_data_size, row, &product)) [[likely]] {
            return product / rows;
        }
        return uint64_t((unsigned __int128)_data_size * row / rows);
    }

    void set_next_row(uint64_t row) {
        _next_row = row;
        _progress.position = progress_of(row);
    }

    // Moves the top of the walk down to `end`. Read progress counts the
    // rows that the walk consumed or skipped, so it grows in both directions.
    void set_walk_end(uint64_t end) {
        _walk->end = end;
        _progress.position = progress_of(_walk->begin + (_walk->partition_end - end));
    }

    // Moves the rows to read to [begin, end), for example after the index
    // was advanced. Read-ahead restarts there.
    void set_range(uint64_t begin, uint64_t end) {
        _end_row = end;
        set_next_row(begin);
        _codec->fast_forward_to(begin, end);
    }

    [[noreturn]] void malformed(std::string_view what) const {
        throw_malformed_sstable_exception(seastar::format("{} (row {})", what, current_row()), _sst->get_filename());
    }

    void reset_partition() {
        _in_partition = false;
        _mf_filter.reset();
        _walk.reset();
    }

    // Skips to the head of a partition.
    void skip_to_partition(uint64_t begin) {
        if (begin < _next_row) {
            return;
        }
        reset_partition();
        set_range(begin, _end_row);
    }

    future<bool> maybe_initialize() {
        if (_initialized) {
            co_return true;
        }
        const uint64_t rows = _sst->get_parquet_file_summary().rows();
        uint64_t begin = 0;
        uint64_t end = rows;
        if (!full_scan()) {
            if (_single_partition_read) {
                _sst->get_stats().on_single_partition_read();
                const auto& key = dht::ring_position_view(_pr.get().start()->value());
                const bool present = _single_partition_read_murmur_hash
                        ? co_await _index_reader->advance_lower_and_check_if_present(key, *_single_partition_read_murmur_hash)
                        : co_await _index_reader->advance_lower_and_check_if_present(key);
                if (!present) {
                    _sst->get_filter_tracker().add_false_positive();
                    co_return false;
                }
                if (reversed()) {
                    // A backward walk starts at the end of the partition.
                    co_await _index_reader->advance_reverse_to_next_partition();
                } else {
                    const position_in_partition_view pos = get_slice_upper_bound(*_schema, _slice, key);
                    co_await _index_reader->advance_upper_past(pos);
                }
                _sst->get_filter_tracker().add_true_positive();
            } else {
                _sst->get_stats().on_range_partition_read();
                co_await _index_reader->advance_to(_pr);
            }
            const auto positions = _index_reader->data_file_positions();
            parse_assert(bool(positions.end), _sst->get_filename());
            begin = positions.start;
            end = *positions.end;
        }
        _read_enabled = _single_partition_read ? begin != end : begin < end;

        auto& state = _sst->get_parquet_read_state();
        const auto& summary = _sst->get_parquet_file_summary();
        _plan = state.plan_for(*_schema, _sst->get_column_translation(*_schema), summary);
        const auto projection = _plan->projection();
        // The fixed memory of the codec reader and of the row decoder grows
        // with the columns. It is asked for before they are allocated, so
        // that the read waits for it. The codec reader holds it.
        auto fixed_memory = co_await _permit.request_memory(reader::fixed_memory(projection.size()) + row_decoder::fixed_memory(*_plan));
        // The codec reader charges its read buffers to the permit itself.
        _codec.emplace(_sst->make_untracked_data_file_for_read(_trace_state), summary, state.cache(), projection, reader_options{
            .begin = begin,
            .end = end,
            .direction = reversed() ? read_direction::backward : read_direction::forward,
            .hint = _single_partition_read ? read_hint::single_partition : read_hint::scan,
            .permit = _permit,
        }, std::move(fixed_memory));
        _rows.emplace(*_schema, *_plan, *_codec, reversed() ? read_direction::backward : read_direction::forward);
        _data_size = _sst->data_size();
        _end_row = end;
        set_next_row(begin);
        _progress.total_read_size = progress_of(end) - progress_of(begin);
        _monitor.on_read_started(_progress);
        _index_in_current_partition = true;
        _initialized = true;
        co_return true;
    }

    // Partitions

    void on_out_of_clustering_range() {
        // A full scan reads the full slice. The filter reports the end of
        // the slice at a range tombstone bound after all clustering rows,
        // and the scan goes on to the end of the partition, as in the mx
        // full scan reader.
        if (full_scan()) {
            return;
        }
        if (_fwd) {
            _end_of_stream = true;
        } else {
            push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, partition_end()));
            _partition_finished = true;
        }
    }

    // The head of a partition, at _next_row.
    bool consume_partition_start() {
        auto key = dht::decorate_key(*_schema, _rows->decode_partition_key());
        const tombstone tomb = _rows->decode_partition_tombstone();
        _in_partition = true;
        _head_row = _next_row;
        _mf_filter.emplace(*_schema, query::clustering_key_filter_ranges(_slice.row_ranges(*_schema, key.key())), _fwd);

        const dht::ring_position_comparator cmp(*_schema);
        if (_pr.get().before(key, cmp)) {
            // The index returned a range that starts before the queried
            // one. A trie lookup can return a false positive, and for a
            // single-partition read that means the key is not here. A range
            // read skips the partition with the index.
            _end_of_stream = _single_partition_read;
            _before_partition = false;
            _partition_finished = true;
            _current_partition_key = std::move(key);
            return false;
        }
        if (_pr.get().after(key, cmp)) {
            // The index returned a range that ends after the queried one.
            // The read is over. The head is not consumed, so that the reader
            // reads it again if it is forwarded to this partition.
            reset_partition();
            _end_of_stream = true;
            _current_partition_key = std::move(key);
            return false;
        }
        _partition_finished = false;
        _before_partition = false;
        _end_of_stream = false;
        _current_partition_key = std::move(key);
        push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, partition_start(*_current_partition_key, tomb)));
        _sst->get_stats().on_partition_read();
        return proceed();
    }

    bool consume_partition_end() {
        if (_mf_filter && _mf_filter->current_tombstone() && !_mf_filter->out_of_range()) {
            malformed(seastar::format("unclosed range tombstone {} at the end of a partition", _mf_filter->current_tombstone()));
        }
        reset_partition();
        if (_fwd) {
            _end_of_stream = true;
            return false;
        }
        _index_in_current_partition = false;
        _partition_finished = true;
        _before_partition = true;
        push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, partition_end()));
        return proceed();
    }

    // The end of the rows to read.
    void on_end_of_range() {
        if (_mf_filter && _mf_filter->current_tombstone()) {
            if (_mf_filter->out_of_range()) {
                malformed("unclosed range tombstone");
            }
            auto result = _mf_filter->apply(position_in_partition_view::after_all_clustered_rows(), {});
            for (auto&& rt : result.rts) {
                push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(rt)));
            }
        }
        if (!_partition_finished) {
            consume_partition_end();
        }
        _end_of_stream = true;
        release_pages();
    }

    // Drops the pages that the codec reader holds, once the reader has no
    // rows left to read. A combined reader keeps the sstable readers of a
    // single-partition read until the read ends, and a decoded page can be
    // as large as the value that it holds.
    void release_pages() noexcept {
        if (_rows) {
            _rows->release_pages();
        }
    }

    // Fragments

    // Consumes the static row at the current row.
    bool consume_static_row() {
        if (current_row() != _head_row) {
            malformed("a static row is not the first row of its partition");
        }
        static_row sr = _rows->decode_static_row();
        consume(1);
        if (!sr.empty()) {
            switch (_mf_filter->apply(sr)) {
            case mutation_fragment_filter::result::emit:
                push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(sr)));
                break;
            case mutation_fragment_filter::result::ignore:
                break;
            case mutation_fragment_filter::result::store_and_finish:
                on_internal_error(sstlog, "Parquet sstable reader: a static row cannot be stored");
            }
        }
        return proceed();
    }

    // Consumes the clustering row at the current row, unless the filter stops at it.
    bool consume_clustering_row() {
        clustering_key ck = _rows->decode_clustering_key();
        _sst->get_stats().on_row_read();
        auto res = _mf_filter->apply(position_in_partition_view::for_key(ck));
        for (auto&& rt : res.rts) {
            push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(rt)));
        }
        switch (res.action) {
        case mutation_fragment_filter::result::emit: {
            clustering_row cr(std::move(ck));
            _rows->decode_clustering_row(cr);
            if (cr.tomb()) {
                _sst->get_stats().on_row_tombstone_read();
            }
            consume(1);
            push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(cr)));
            return proceed();
        }
        case mutation_fragment_filter::result::ignore:
            if (_mf_filter->out_of_range()) {
                // The reader stops reading, or skips to the next partition
                // with the index. The row is not consumed.
                on_out_of_clustering_range();
                return false;
            }
            if (_mf_filter->is_current_range_changed()) {
                // The reader may skip with the index, and reads the row
                // again otherwise.
                return false;
            }
            consume(1);
            // A long run of rows that the filter ignores must not stall.
            return proceed();
        case mutation_fragment_filter::result::store_and_finish:
            on_out_of_clustering_range();
            return false;
        }
        std::abort();
    }

    // Applies a range tombstone change that `rows` bound rows from the
    // current row make, and consumes them unless the filter stores the change.
    bool on_range_tombstone_change(position_in_partition pos, tombstone t, uint64_t rows) {
        auto result = _mf_filter->apply(pos, t);
        for (auto&& rt : result.rts) {
            push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(rt)));
        }
        switch (result.action) {
        case mutation_fragment_filter::result::emit:
            break;
        case mutation_fragment_filter::result::ignore:
            consume(rows);
            if (_mf_filter->out_of_range()) {
                on_out_of_clustering_range();
                return false;
            }
            if (_mf_filter->is_current_range_changed()) {
                return false;
            }
            return proceed();
        case mutation_fragment_filter::result::store_and_finish:
            // The filter emitted nothing for the change, so the rows are
            // read again when the reader is forwarded past them.
            on_out_of_clustering_range();
            return false;
        }
        consume(rows);
        return proceed();
    }

    // The bounds below are in the read order: a start bound opens a range
    // tombstone in the order of the read, and an end bound closes it.

    bool consume_start_bound(decoded_bound b) {
        if (const auto open = _mf_filter->current_tombstone()) {
            malformed(seastar::format("range tombstones have to be disjoint: current opened range tombstone {}, new tombstone {}", open, b.tomb));
        }
        return on_range_tombstone_change(position_in_partition(position_in_partition::range_tag_t(), b.kind, std::move(b.prefix)), b.tomb, 1);
    }

    // An end bound, and the start bound that follows it in the read
    // direction at the same position, if any. The two rows of a boundary
    // make one change.
    bool consume_end_bound(decoded_bound end, std::optional<decoded_bound> start) {
        const auto open = _mf_filter->current_tombstone();
        if (!open) {
            malformed(seastar::format("closing range tombstone that wasn't opened: clustering {}, kind {}, tombstone {}", end.prefix, end.kind, end.tomb));
        }
        if (open != end.tomb) {
            malformed(seastar::format("range tombstone with ck {} and two different tombstones at ends: {}, {}", end.prefix, open, end.tomb));
        }
        const tombstone next = start ? start->tomb : tombstone();
        return on_range_tombstone_change(position_in_partition(position_in_partition::range_tag_t(), end.kind, std::move(end.prefix)), next, start ? 2 : 1);
    }

    // Starts the backward walk of the current partition, whose head phase
    // is over. `first` is its first clustering row or range tombstone bound
    // row. Returns whether to go on reading rows.
    future<bool> begin_walk(uint64_t first) {
        // The end of the partition is the start of the next one in the index.
        if (!_index_in_current_partition) {
            co_await _index_reader->advance_to_definitely_present_partition(*_current_partition_key);
            _index_in_current_partition = true;
        }
        co_await _index_reader->advance_reverse_to_next_partition();
        const auto positions = _index_reader->data_file_positions();
        if (positions.start != _head_row || !positions.end || *positions.end <= first || *positions.end > _end_row) {
            malformed(seastar::format("the index puts the partition at rows [{}, {}), the Data file at row {} with a clustering row at {}",
                    positions.start, positions.end, _head_row, first));
        }
        const uint64_t end = *positions.end;
        _walk = backward_walk{.begin = first, .end = end, .partition_end = end};
        set_walk_end(end);
        _codec->fast_forward_to(first, end);

        // The filter has not seen a clustering position yet. A forward read
        // moves it to its first clustering range at the first clustering
        // row. A backward walk must know that range before it reads a row,
        // so that it can skip there with the index.
        auto result = _mf_filter->apply(position_in_partition_view::before_all_clustered_rows());
        if (result.action == mutation_fragment_filter::result::store_and_finish || _mf_filter->out_of_range()) {
            on_out_of_clustering_range();
            co_return false;
        }
        // The lower bound may have been consumed by fast_forward_to() during
        // the head phase, so it is taken from the filter.
        _mf_filter->maybe_skip();
        position_in_partition start(_mf_filter->lower_bound());
        if (!start.is_before_all_clustered_rows(*_schema)) {
            co_await advance_context(std::move(start));
        }
        co_return true;
    }

    // The backward walk reached the first clustering row of the partition.
    bool end_walk() {
        const uint64_t end = _walk->partition_end;
        _walk.reset();
        // The next partition starts there. Read-ahead restarts at its head.
        set_range(end, _end_row);
        return consume_partition_end();
    }

    // Records the row at the decoder for validate(), unless it is recorded.
    void maybe_record_row() {
        const uint64_t row = _rows->current_row();
        if (!_consumed || row != _next_to_record) [[likely]] {
            return;
        }
        consumed_row r{.row = row};
        _rows->check_partition_key_columns();
        if (_rows->starts_partition()) {
            r.key = dht::decorate_key(*_schema, _rows->decode_partition_key());
            r.partition_tombstone = _rows->decode_partition_tombstone();
        }
        const fragment_kind kind = _rows->kind();
        if (kind == fragment_kind::clustering_row || kind == fragment_kind::clustering_row_with_marker) {
            r.position = position_in_partition::for_key(_rows->decode_clustering_key());
        } else if (is_start_bound(kind) || is_end_bound(kind)) {
            auto b = _rows->decode_bound();
            r.position = position_in_partition(position_in_partition::range_tag_t(), b.kind, std::move(b.prefix));
        }
        _consumed->push_back(std::move(r));
        ++_next_to_record;
    }

    // Consumes rows until a step asks to stop, the buffer is full, or the
    // task should yield.
    future<> consume_rows() {
        while (true) {
            if (at_end()) {
                if (_walk) {
                    if (!end_walk()) {
                        co_return;
                    }
                    continue;
                }
                on_end_of_range();
                co_return;
            }
            const uint64_t row = current_row();
            if (!_rows->try_position(row)) {
                co_await _rows->finish_position();
            }
            maybe_record_row();
            bool go;
            if (_rows->starts_partition() && !(_in_partition && row == _head_row)) {
                if (_walk) {
                    malformed("a partition starts inside another one");
                }
                go = _in_partition ? consume_partition_end() : consume_partition_start();
            } else {
                if (!_in_partition) {
                    malformed("a row is not in a partition");
                }
                const fragment_kind kind = in_read_order(_rows->kind());
                if (kind == fragment_kind::none) {
                    if (row != _head_row) {
                        malformed("a row of kind none is not the first row of its partition");
                    }
                    _rows->check_none_row();
                    consume(1);
                    go = true;
                } else if (kind == fragment_kind::static_row) {
                    go = consume_static_row();
                } else if (reversed() && !_walk) {
                    go = co_await begin_walk(row);
                } else if (is_start_bound(kind)) {
                    go = consume_start_bound(decode_bound());
                } else if (is_end_bound(kind)) {
                    auto end = decode_bound();
                    std::optional<decoded_bound> start;
                    if (const auto next = following_row()) {
                        if (!_rows->try_position(*next)) {
                            co_await _rows->finish_position();
                        }
                        maybe_record_row();
                        // In a walk, every row is in the partition. The
                        // last one is the head, which starts it.
                        const bool same_partition = _walk || !_rows->starts_partition();
                        if (same_partition && is_start_bound(in_read_order(_rows->kind()))) {
                            auto b = decode_bound();
                            const position_in_partition::equal_compare eq(*_schema);
                            if (eq(position_in_partition_view(position_in_partition_view::range_tag_t(), bound_view(end.prefix, end.kind)),
                                    position_in_partition_view(position_in_partition_view::range_tag_t(), bound_view(b.prefix, b.kind)))) {
                                start = std::move(b);
                            }
                        }
                    }
                    go = consume_end_bound(std::move(end), std::move(start));
                } else {
                    go = consume_clustering_row();
                }
            }
            if (!go) {
                maybe_end_early();
                co_return;
            }
        }
    }

    // Ends the read now if it has nothing left to read, rather than in the
    // next fill_buffer(). A step usually stops because the buffer is full,
    // and the consumer may not call fill_buffer() again soon: a combined
    // reader keeps the sstable readers of a single-partition read until the
    // read ends. So the reader releases its pages as soon as it can.
    void maybe_end_early() {
        if (_end_of_stream || _walk) {
            return;
        }
        if (!_partition_finished && at_end()) {
            // What the next consume_rows() would do first.
            on_end_of_range();
        } else if (_partition_finished && _single_partition_read) {
            // read_next_partition() ends the stream.
            release_pages();
        }
    }

    // Navigation, as in the mx reader.

    future<> advance_to_next_partition() {
        _before_partition = true;
        if (!_in_partition) {
            _index_in_current_partition = false;
            co_return;
        }
        if (full_scan()) {
            on_internal_error(sstlog, "Parquet full scan reader: cannot skip a partition");
        }
        if (_index_in_current_partition) {
            co_await _index_reader->advance_to_next_partition();
        } else {
            co_await _index_reader->advance_past_definitely_present_partition(*_current_partition_key);
        }
        _index_in_current_partition = true;
        // The end of the rows to read, and not the upper bound of the
        // index, which a reverse read moves within the partition.
        const uint64_t start = _index_reader->data_file_positions().start;
        if (start > _end_row) {
            _read_enabled = false;
            co_return;
        }
        parse_assert(_index_reader->element_kind() == indexable_element::partition, _sst->get_filename());
        skip_to_partition(start);
        _sst->get_stats().on_partition_seek();
    }

    future<> read_partition() {
        _end_of_stream = true; // consume_partition_start() resets it
        if (!_read_enabled) {
            co_return;
        }
        if (_in_partition) {
            malformed("the reader is not at a partition boundary");
        }
        co_await consume_rows();
    }

    future<> read_next_partition() {
        _end_of_stream = true;
        if (!_read_enabled || _single_partition_read) {
            release_pages();
            co_return;
        }
        co_await advance_to_next_partition();
        co_await read_partition();
    }

    // Skips with the index to `pos` in the current partition, in the read
    // direction. `pos` is in the order of the read.
    future<> advance_context(std::optional<position_in_partition> pos) {
        if (!pos || position_in_partition_view(*pos).is_before_all_fragments(*_schema) || full_scan()) {
            co_return;
        }
        if (reversed() && !_walk) {
            // begin_walk() skips to the filter's lower bound.
            co_return;
        }
        parse_assert(bool(_current_partition_key), _sst->get_filename());
        if (!_index_in_current_partition) {
            _index_in_current_partition = true;
            co_await _index_reader->advance_to_definitely_present_partition(*_current_partition_key);
        }
        if (reversed()) {
            co_await skip_backward(*pos);
            co_return;
        }
        co_await _index_reader->advance_to(*pos);
        const uint64_t start = _index_reader->data_file_positions().start;
        if (start <= _next_row) {
            co_return;
        }
        // The index block starts at a row of the current partition, after
        // its head, and records the range tombstone open before that row.
        const auto open_end_marker = _index_reader->end_open_marker();
        const tombstone open = open_end_marker ? open_end_marker->tomb : tombstone();
        if (may_be_capped(open)) {
            // The reader reads on from where it is, and takes the open
            // tombstone from the bound row in the Data file.
            co_return;
        }
        set_range(start, _end_row);
        _sst->get_stats().on_partition_seek();
        _mf_filter->set_tombstone(open);
    }

    // Moves the top of the walk down to the first index block after `pos`,
    // if that is below the rows that the walk consumed. See the top of the
    // class.
    future<> skip_backward(position_in_partition_view pos) {
        // The index takes positions in the order of the table.
        co_await _index_reader->advance_reverse(pos.reversed());
        const auto end = _index_reader->data_file_positions().end;
        parse_assert(bool(end), _sst->get_filename());
        if (*end >= _walk->end) {
            co_return;
        }
        const auto open_marker = _index_reader->reverse_end_open_marker();
        const tombstone open = open_marker ? open_marker->tomb : tombstone();
        if (may_be_capped(open)) {
            // The walk reads on from where it is, and takes the open
            // tombstone from the bound row in the Data file.
            co_return;
        }
        // The first block of a partition starts at its first clustering row
        // or bound row, which is the bottom of the walk.
        set_walk_end(std::max(*end, _walk->begin));
        _codec->fast_forward_to(_walk->begin, _walk->end);
        _sst->get_stats().on_partition_seek();
        _mf_filter->set_tombstone(open);
    }

    std::optional<position_in_partition> maybe_skip() {
        if (!_mf_filter) {
            return std::nullopt;
        }
        if (auto pos = _mf_filter->maybe_skip()) {
            return position_in_partition(*pos);
        }
        return std::nullopt;
    }

    future<> advance_index_until_unseen_partition() {
        while (_index_reader->data_file_positions().start < _next_row) {
            co_await _index_reader->advance_to_next_partition();
        }
    }

    future<> do_fast_forward_to(const dht::partition_range& pr) {
        if (!co_await maybe_initialize()) {
            _end_of_stream = true;
            co_return;
        }
        _pr = pr;
        clear_buffer();
        _partition_finished = true;
        _before_partition = true;
        _end_of_stream = false;
        reset_partition();
        co_await _index_reader->advance_to(pr);
        auto positions = _index_reader->data_file_positions();
        parse_assert(bool(positions.end), _sst->get_filename());
        bool index_in_partition = true;
        if (positions.start < _next_row) {
            // The index is inexact, and returned a start before the rows
            // that the reader consumed. Fast-forwarding only moves forward,
            // so the read starts at the first partition at or after them.
            index_in_partition = false;
            co_await advance_index_until_unseen_partition();
            positions = _index_reader->data_file_positions();
        }
        if (positions.start < *positions.end) {
            _read_enabled = true;
            _index_in_current_partition = index_in_partition;
            set_range(positions.start, *positions.end);
        } else {
            _index_in_current_partition = false;
            _read_enabled = false;
        }
    }

    // Fast-forwards the clustering filter, as mp_row_consumer_m::fast_forward_to().
    std::optional<position_in_partition> forward_filter(position_range r) {
        if (!_mf_filter) {
            on_out_of_clustering_range();
            return std::nullopt;
        }
        // The filter trims range tombstones, which can be trimmed only to
        // positions that are not clustering rows.
        if (r.start().is_clustering_row()) {
            r.set_start(position_in_partition::before_key(r.start().key()));
        }
        if (r.end().is_clustering_row()) {
            r.set_end(position_in_partition::before_key(r.end().key()));
        }
        auto skip = _mf_filter->fast_forward_to(std::move(r));
        if (_mf_filter->out_of_range()) {
            on_out_of_clustering_range();
        }
        if (skip) {
            return position_in_partition(*skip);
        }
        return std::nullopt;
    }

    std::exception_ptr attribute(std::exception_ptr ex) const {
        return maybe_attribute_malformed_sstable_exception(std::move(ex), _sst->generation());
    }

public:
    sstable_mutation_reader(shared_sstable sst,
            schema_ptr schema,
            reader_permit permit,
            const dht::partition_range& pr,
            value_or_reference<query::partition_slice> slice,
            tracing::trace_state_ptr trace_state,
            streamed_mutation::forwarding fwd,
            // Read-ahead stops at the end of the current partition range
            // whether or not the reader may be fast-forwarded past it.
            [[maybe_unused]] mutation_reader::forwarding fwd_mr,
            read_monitor& monitor,
            std::unique_ptr<abstract_index_reader> ir,
            const utils::hashed_key* single_partition_read_murmur_hash,
            std::vector<consumed_row>* consumed = nullptr)
        : impl(std::move(schema), std::move(permit))
        , _sst(std::move(sst))
        , _slice_holder(std::move(slice))
        , _slice(_slice_holder.get())
        , _trace_state(std::move(trace_state))
        , _fwd(fwd)
        , _monitor(monitor)
        , _index_reader(std::move(ir))
        , _single_partition_read(_index_reader && pr.is_singular())
        , _pr(pr)
        , _single_partition_read_murmur_hash(single_partition_read_murmur_hash
                ? std::optional<utils::hashed_key>(*single_partition_read_murmur_hash) : std::nullopt)
        , _consumed(consumed)
    {
        _permit.on_start_sstable_read();
        if (reversed() && !_index_reader) {
            on_internal_error(sstlog, "Parquet full scan reader: reversed reads are not supported");
        }
        if (_consumed && _index_reader) {
            on_internal_error(sstlog, "Parquet sstable reader: only a full scan records its rows");
        }
    }
    sstable_mutation_reader(const sstable_mutation_reader&) = delete;
    ~sstable_mutation_reader() {
        _permit.on_finish_sstable_read();
    }

    virtual future<> fill_buffer() override {
        if (_end_of_stream) {
            co_return;
        }
        std::exception_ptr ex;
        try {
            if (!co_await maybe_initialize()) {
                _end_of_stream = true;
                co_return;
            }
            while (!is_end_of_stream() && !is_buffer_full()) {
                co_await coroutine::maybe_yield();
                if (auto table_name = utils::get_local_injector().inject_parameter("sstables_parquet_reader_fill_buffer_timeout", "table"); table_name) {
                    // Repeat the sleep until the permit is aborted due to timeout.
                    if (*table_name == seastar::format("{}.{}", _schema->ks_name(), _schema->cf_name()) && !get_abort_exception()) {
                        co_await seastar::sleep(std::chrono::milliseconds(10));
                        continue;
                    }
                }
                // A read that times out must fail without throwing, because
                // an overloaded node times out many reads, and exceptions
                // are costly.
                if (auto abort = get_abort_exception()) {
                    ex = std::move(abort);
                    break;
                }
                if (_partition_finished) {
                    if (_before_partition) {
                        co_await read_partition();
                    } else {
                        co_await read_next_partition();
                    }
                } else {
                    // In the head phase of a reverse read, the filter's lower
                    // bound is left for begin_walk().
                    if (!reversed() || _walk) {
                        if (auto pos = maybe_skip()) {
                            co_await advance_context(std::move(pos));
                        }
                    }
                    if (!is_buffer_full() && !_partition_finished && !_end_of_stream) {
                        co_await consume_rows();
                    }
                }
            }
        } catch (...) {
            ex = std::current_exception();
        }
        if (ex) {
            co_await coroutine::return_exception_ptr(maybe_attribute_malformed_sstable_exception(std::move(ex), _sst->generation(),
                    seastar::format("Failed to read partition from SSTable {}", _sst->get_filename())));
        }
    }

    virtual future<> next_partition() override {
        if (full_scan()) {
            on_internal_error(sstlog, "Parquet full scan reader: doesn't support next_partition()");
        }
        if (_initialized) {
            if (_fwd) {
                clear_buffer();
                _partition_finished = true;
                _end_of_stream = false;
            } else {
                clear_buffer_to_next_partition();
                if (!_partition_finished && is_buffer_empty()) {
                    _partition_finished = true;
                }
            }
        }
        return make_ready_future<>();
    }

    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        if (full_scan()) {
            on_internal_error(sstlog, "Parquet full scan reader: doesn't support fast_forward_to(const dht::partition_range&)");
        }
        std::exception_ptr ex;
        try {
            co_await do_fast_forward_to(pr);
        } catch (...) {
            ex = std::current_exception();
        }
        if (ex) {
            co_await coroutine::return_exception_ptr(attribute(std::move(ex)));
        }
    }

    virtual future<> fast_forward_to(position_range cr) override {
        if (full_scan()) {
            on_internal_error(sstlog, "Parquet full scan reader: doesn't support fast_forward_to(position_range)");
        }
        clear_buffer();
        if (_partition_finished) {
            _end_of_stream = true;
            co_return;
        }
        _end_of_stream = false;
        std::exception_ptr ex;
        try {
            co_await advance_context(forward_filter(std::move(cr)));
        } catch (...) {
            ex = std::current_exception();
        }
        if (ex) {
            co_await coroutine::return_exception_ptr(attribute(std::move(ex)));
        }
    }

    virtual future<> close() noexcept override {
        if (_codec) {
            _monitor.on_read_completed();
            co_await _codec->close();
            // After close(), the counters include the reads that were in flight.
            const auto& fs = _codec->stats();
            auto& stats = local_shard_stats;
            stats.read_requests += fs.reads;
            stats.bytes_read += fs.bytes_read;
            stats.data_pages_read += fs.pages_taken;
            stats.dictionary_pages_read += fs.dictionary_pages_taken;
            stats.offset_index_reads += fs.index_reads;
        }
        if (_index_reader) {
            try {
                co_await _index_reader->close();
            } catch (...) {
                sstlog.warn("Failed closing of the Parquet sstable reader: {}. Ignored since the reader is already done.", std::current_exception());
            }
        }
    }
};

mutation_reader make_sstable_mutation_reader(
        shared_sstable sstable,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& range,
        value_or_reference<query::partition_slice> slice,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        read_monitor& monitor,
        std::unique_ptr<abstract_index_reader> ir,
        const utils::hashed_key* single_partition_read_murmur_hash) {
    return make_mutation_reader<sstable_mutation_reader>(std::move(sstable), std::move(schema), std::move(permit), range,
            std::move(slice), std::move(trace_state), fwd, fwd_mr, monitor, std::move(ir), single_partition_read_murmur_hash);
}

// Cross-checks the rows of a full scan against the trie index, for
// validate(). The index stores row ordinals (see sstable_writer.hh):
//
// - Every partition has an entry, in order, whose position is the ordinal
//   of the partition's first row, and whose key is the partition's key if
//   the index stores it. The index has no other entries.
// - In a partition with a row index, each row index block starts at a row
//   of the partition. For each clustering row and range tombstone bound row,
//   the index finds the block that starts at or before the row, and that
//   block changes exactly at the rows where blocks start. The last block is
//   the one that the index records as last.
//
// The index finds a block by a lookup per row, so validation pays a trie
// walk per clustering row. It does not compare the index with the rows when
// the rows are out of order, because the order validator reports that.
class index_checker {
    shared_sstable _sst;
    const ::schema& _schema;
    std::function<void(sstring)>& _report;
    std::unique_ptr<abstract_index_reader> _index;
    // The index has an entry for the current partition.
    bool _in_partition = false;
    bool _at_end = false;
    uint64_t _partition_start = 0;
    bool _has_row_index = false;
    std::optional<uint64_t> _last_block_offset;
    // The start of the block of the last row.
    std::optional<uint64_t> _block;
    std::optional<position_in_partition> _last_position;

    void report(sstring what) {
        _report(seastar::format("mismatching index/data: {}", what));
    }

    future<> end_partition() {
        if (_has_row_index && _last_block_offset && (!_block || *_block - _partition_start != *_last_block_offset)) {
            report(seastar::format("the last row index block of the partition at row {} starts at offset {}, the rows have it at {}",
                    _partition_start, *_last_block_offset, _block ? int64_t(*_block - _partition_start) : -1));
        }
        _has_row_index = false;
        if (_in_partition) {
            co_await _index->advance_to_next_partition();
            _in_partition = false;
        }
    }

    future<> start_partition(const consumed_row& r) {
        _partition_start = r.row;
        _block.reset();
        _last_position.reset();
        _last_block_offset.reset();
        if (_at_end) {
            co_return;
        }
        if (_index->eof()) {
            report(seastar::format("the index is at its end, but the partition {} starts at row {}", r.key->key().with_schema(_schema), r.row));
            _at_end = true;
            co_return;
        }
        co_await _index->read_partition_data();
        _in_partition = true;
        const uint64_t index_row = _index->data_file_positions().start;
        if (index_row != r.row) {
            report(seastar::format("the index puts a partition at row {}, the Data file has the partition {} at row {}",
                    index_row, r.key->key().with_schema(_schema), r.row));
        }
        // The index stores the key and the tombstone of a partition only
        // with its row index.
        if (auto pk = _index->get_partition_key()) {
            if (!pk->equal(_schema, r.key->key())) {
                report(seastar::format("the index has the partition {} at row {}, the Data file has the partition {}",
                        pk->with_schema(_schema), index_row, r.key->key().with_schema(_schema)));
            }
            _has_row_index = true;
            _last_block_offset = co_await _index->last_block_offset();
        }
        // The index holds the deletion time capped (see may_be_capped()).
        if (auto t = _index->partition_tombstone(); t && tombstone(*t) != as_in_index(r.partition_tombstone)) {
            report(seastar::format("the index has the partition tombstone {} for the partition {}, the Data file has {}",
                    tombstone(*t), r.key->key().with_schema(_schema), r.partition_tombstone));
        }
    }

    future<> on_clustering_row(uint64_t row, const position_in_partition& pos) {
        if (_last_position && !position_in_partition::less_compare(_schema)(*_last_position, pos)) {
            if (!position_in_partition::equal_compare(_schema)(*_last_position, pos)) {
                // Out of order. The block lookups would be meaningless.
                _has_row_index = false;
                co_return;
            }
        }
        _last_position = pos;
        co_await _index->advance_to(pos);
        const uint64_t block = _index->data_file_positions().start;
        if (block > row || block < _partition_start) {
            report(seastar::format("the row index finds the block of row {} ({}) at row {}, outside the rows [{}, {}]",
                    row, pos, block, _partition_start, row));
        } else if (block != _block) {
            if (block != row) {
                report(seastar::format("the row index finds the block of row {} ({}) at row {}, which is neither the block of the previous row nor a block that starts at this row",
                        row, pos, block));
            }
            _block = block;
        }
    }

public:
    index_checker(shared_sstable sst, const ::schema& s, reader_permit permit, std::function<void(sstring)>& report)
        : _sst(std::move(sst))
        , _schema(s)
        , _report(report)
        , _index(_sst->make_index_reader(std::move(permit), tracing::trace_state_ptr(), use_caching::no, false))
    {}

    future<> on_row(const consumed_row& r) {
        if (r.key) {
            co_await end_partition();
            co_await start_partition(r);
        }
        if (r.position && _has_row_index) {
            co_await on_clustering_row(r.row, *r.position);
        }
    }

    future<> finish() {
        co_await end_partition();
        if (!_at_end && !_index->eof()) {
            report(seastar::format("the index has a partition at row {} after the last partition of the Data file", _index->data_file_positions().start));
        }
    }

    future<> close() noexcept {
        return _index->close();
    }
};

} // anonymous namespace

mutation_reader make_reader(
        shared_sstable sstable,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        read_monitor& monitor,
        integrity_check,
        std::unique_ptr<abstract_index_reader> ir,
        const utils::hashed_key* single_partition_read_murmur_hash) {
    return make_sstable_mutation_reader(std::move(sstable), std::move(schema), std::move(permit), range, value_or_reference(slice),
            std::move(trace_state), fwd, fwd_mr, monitor, std::move(ir), single_partition_read_murmur_hash);
}

mutation_reader make_full_scan_reader(
        shared_sstable sstable,
        schema_ptr schema,
        reader_permit permit,
        tracing::trace_state_ptr trace_state,
        read_monitor& monitor,
        integrity_check) {
    const auto& slice = schema->full_slice();
    return make_sstable_mutation_reader(std::move(sstable), std::move(schema), std::move(permit), query::full_partition_range,
            value_or_reference(slice), std::move(trace_state), streamed_mutation::forwarding::no, mutation_reader::forwarding::no,
            monitor, nullptr, nullptr);
}

future<uint64_t> validate(shared_sstable sst, reader_permit permit, abort_source& abort, std::function<void(sstring)> error_handler,
        read_monitor& monitor) {
    uint64_t errors = 0;
    std::function<void(sstring)> report = [&] (sstring what) {
        ++errors;
        error_handler(std::move(what));
    };
    auto describe = [] (std::exception_ptr ex) -> sstring {
        try {
            std::rethrow_exception(std::move(ex));
        } catch (const malformed_sstable_exception& e) {
            return seastar::format("unrecoverable error: {}", e.what());
        } catch (...) {
            return seastar::format("unexpected exception: {}", std::current_exception());
        }
    };

    // The whole file, in order: the checksums, the OffsetIndexes, the
    // footer, and the digest.
    {
        auto in = co_await sst->data_stream(0, sst->ondisk_data_size(), permit, nullptr, nullptr);
        std::exception_ptr ex;
        try {
            const auto* entry = sst->get_scylla_metadata()->get_parquet_data_file();
            co_await verify_file(in, sst->make_data_file_for_read(permit, nullptr), sst->get_parquet_file_summary(), verify_options{
                .file_size = sst->ondisk_data_size(),
                .footer_crc32 = entry->footer_crc32,
                .digest = sst->get_digest(),
                .abort = &abort,
                .permit = permit,
            }, report);
        } catch (...) {
            ex = std::current_exception();
        }
        if (ex) {
            report(describe(std::move(ex)));
        }
        co_await in.close();
    }
    if (abort.abort_requested()) {
        co_return errors;
    }

    // A full scan with order validation, and the cross-check of its rows
    // with the index.
    auto schema = sst->get_schema();
    std::vector<consumed_row> consumed;
    const auto& slice = schema->full_slice();
    auto reader = make_mutation_reader<sstable_mutation_reader>(sst, schema, permit, query::full_partition_range,
            value_or_reference(slice), tracing::trace_state_ptr(), streamed_mutation::forwarding::no, mutation_reader::forwarding::no,
            monitor, nullptr, nullptr, &consumed);
    index_checker index(sst, *schema, permit, report);
    mutation_fragment_stream_validator validator(*schema);
    // The memory of the rows that the reader recorded since the previous
    // fragment. The reader records them while it fills its buffer, so they
    // are charged afterwards, while the index checks them.
    auto consumed_memory = [&] {
        size_t r = consumed.capacity() * sizeof(consumed_row);
        for (const auto& row : consumed) {
            r += (row.key ? row.key->external_memory_usage() : 0) + (row.position ? row.position->external_memory_usage() : 0);
        }
        return r;
    };
    std::exception_ptr ex;
    try {
        while (!abort.abort_requested()) {
            auto mf = co_await reader();
            {
                auto memory = co_await permit.request_memory(consumed_memory());
                for (const auto& r : consumed) {
                    co_await index.on_row(r);
                }
                consumed.clear();
            }
            if (!mf) {
                if (auto res = validator.on_end_of_stream(); !res) {
                    report(res.what());
                }
                co_await index.finish();
                break;
            }
            if (auto res = validator(*mf); !res) {
                report(res.what());
                validator.reset(*mf);
            }
            if (mf->is_partition_start()) {
                const auto& key = mf->as_partition_start().key();
                if (auto res = validator(key); !res) {
                    report(res.what());
                    validator.reset(key);
                }
            }
        }
    } catch (...) {
        ex = std::current_exception();
    }
    if (ex) {
        report(describe(std::move(ex)));
    }
    co_await reader.close();
    co_await index.close();
    co_return errors;
}

} // namespace sstables::parquet
