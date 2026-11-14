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
#include "readers/forwardable.hh"
#include "readers/reversing.hh"
#include "schema/schema.hh"
#include "sstables/abstract_index_reader.hh"
#include "sstables/mutation_fragment_filter.hh"
#include "sstables/parquet/reader.hh"
#include "sstables/sstable_mutation_reader.hh"
#include "sstables/sstables.hh"
#include "utils/error_injection.hh"
#include "utils/hashers.hh"
#include "utils/value_or_reference.hh"

namespace sstables::parquet {

lw_shared_ptr<const read_plan> sstable_read_state::plan_for(const ::schema& s, const column_translation& ct, const file_summary& summary) {
    if (!_plan || _plan->version() != s.version()) [[unlikely]] {
        _plan = make_lw_shared<const read_plan>(_mapping, summary, s, ct);
    }
    return _plan;
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

bool is_start_bound(fragment_kind k) noexcept {
    return k == fragment_kind::incl_start || k == fragment_kind::excl_start;
}

bool is_end_bound(fragment_kind k) noexcept {
    return k == fragment_kind::incl_end || k == fragment_kind::excl_end;
}

// Reads partitions of an sstable with a Parquet Data file in the forward
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
    // _next_row are consumed.
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

    bool full_scan() const noexcept {
        return !_index_reader;
    }

    bool proceed() const {
        return !is_buffer_full() && !need_preempt();
    }

    uint64_t progress_of(uint64_t row) const noexcept {
        // Read progress is the total raw size times the rows read over the
        // total rows.
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

    // Moves the rows to read to [begin, end), for example after the index
    // was advanced. Read-ahead restarts there.
    void set_range(uint64_t begin, uint64_t end) {
        _end_row = end;
        set_next_row(begin);
        _codec->fast_forward_to(begin, end);
    }

    [[noreturn]] void malformed(std::string_view what) const {
        throw_malformed_sstable_exception(seastar::format("{} (row {})", what, _next_row), _sst->get_filename());
    }

    void reset_partition() {
        _in_partition = false;
        _mf_filter.reset();
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
                const position_in_partition_view pos = get_slice_upper_bound(*_schema, _slice, key);
                co_await _index_reader->advance_upper_past(pos);
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
            .direction = read_direction::forward,
            .hint = _single_partition_read ? read_hint::single_partition : read_hint::scan,
            .permit = _permit,
        }, std::move(fixed_memory));
        _rows.emplace(*_schema, *_plan, *_codec, read_direction::forward);
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

    // Consumes the static row at _next_row.
    bool consume_static_row() {
        if (_next_row != _head_row) {
            malformed("a static row is not the first row of its partition");
        }
        static_row sr = _rows->decode_static_row();
        set_next_row(_next_row + 1);
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

    // Consumes the clustering row at _next_row, unless the filter stops at it.
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
            set_next_row(_next_row + 1);
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
            set_next_row(_next_row + 1);
            // A long run of rows that the filter ignores must not stall.
            return proceed();
        case mutation_fragment_filter::result::store_and_finish:
            on_out_of_clustering_range();
            return false;
        }
        std::abort();
    }

    // Applies a range tombstone change that `rows` bound rows at _next_row
    // make, and consumes them unless the filter stores the change.
    bool on_range_tombstone_change(position_in_partition pos, tombstone t, uint64_t rows) {
        auto result = _mf_filter->apply(pos, t);
        for (auto&& rt : result.rts) {
            push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(rt)));
        }
        switch (result.action) {
        case mutation_fragment_filter::result::emit:
            break;
        case mutation_fragment_filter::result::ignore:
            set_next_row(_next_row + rows);
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
        set_next_row(_next_row + rows);
        return proceed();
    }

    bool consume_start_bound(decoded_bound b) {
        if (const auto open = _mf_filter->current_tombstone()) {
            malformed(seastar::format("range tombstones have to be disjoint: current opened range tombstone {}, new tombstone {}", open, b.tomb));
        }
        return on_range_tombstone_change(position_in_partition(position_in_partition::range_tag_t(), b.kind, std::move(b.prefix)), b.tomb, 1);
    }

    // An end bound, and the start bound that follows it at the same
    // position, if any. The two rows of a boundary make one change.
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

    // Consumes rows from _next_row until a step asks to stop, the buffer
    // is full, or the task should yield.
    future<> consume_rows() {
        while (true) {
            if (_next_row >= _end_row) {
                on_end_of_range();
                co_return;
            }
            if (!_rows->try_position(_next_row)) {
                co_await _rows->finish_position();
            }
            bool go;
            if (_rows->starts_partition() && !(_in_partition && _next_row == _head_row)) {
                go = _in_partition ? consume_partition_end() : consume_partition_start();
            } else {
                if (!_in_partition) {
                    malformed("a row is not in a partition");
                }
                const fragment_kind kind = _rows->kind();
                if (kind == fragment_kind::none) {
                    if (_next_row != _head_row) {
                        malformed("a row of kind none is not the first row of its partition");
                    }
                    _rows->check_none_row();
                    set_next_row(_next_row + 1);
                    go = true;
                } else if (kind == fragment_kind::static_row) {
                    go = consume_static_row();
                } else if (is_start_bound(kind)) {
                    go = consume_start_bound(_rows->decode_bound());
                } else if (is_end_bound(kind)) {
                    auto end = _rows->decode_bound();
                    std::optional<decoded_bound> start;
                    const uint64_t next = _next_row + 1;
                    if (next < _end_row) {
                        if (!_rows->try_position(next)) {
                            co_await _rows->finish_position();
                        }
                        if (!_rows->starts_partition() && is_start_bound(_rows->kind())) {
                            auto b = _rows->decode_bound();
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
        if (_end_of_stream) {
            return;
        }
        if (!_partition_finished && _next_row >= _end_row) {
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
        const auto [start, end] = _index_reader->data_file_positions();
        if (end && start > *end) {
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

    // Skips forward with the index to `pos` in the current partition.
    future<> advance_context(std::optional<position_in_partition> pos) {
        if (!pos || position_in_partition_view(*pos).is_before_all_fragments(*_schema) || full_scan()) {
            co_return;
        }
        parse_assert(bool(_current_partition_key), _sst->get_filename());
        if (!_index_in_current_partition) {
            _index_in_current_partition = true;
            co_await _index_reader->advance_to_definitely_present_partition(*_current_partition_key);
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
            const utils::hashed_key* single_partition_read_murmur_hash)
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
    {
        _permit.on_start_sstable_read();
        if (_slice.is_reversed()) {
            on_internal_error(sstlog, "Parquet sstable reader: reversed reads are not supported natively yet");
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
                    if (auto pos = maybe_skip()) {
                        co_await advance_context(std::move(pos));
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

mutation_reader make_forward_reader(
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

// FIXME: a temporary scaffold, so that the tests of reverse reads run
// before the reader reads backward. It reads forward and reverses the
// result in memory, which the next step replaces with native reverse reads.
// Then this function goes away.
mutation_reader make_reversed_reader_scaffold(
        shared_sstable sstable,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        read_monitor& monitor,
        std::unique_ptr<abstract_index_reader> ir,
        const utils::hashed_key* single_partition_read_murmur_hash) {
    const auto max_result_size = permit.max_result_size();
    auto rd = make_reversing_reader(make_forward_reader(std::move(sstable), schema->make_reversed(), std::move(permit), range,
            value_or_reference(query::reverse_slice(*schema, slice)), std::move(trace_state), streamed_mutation::forwarding::no, fwd_mr,
            monitor, std::move(ir), single_partition_read_murmur_hash), max_result_size);
    if (fwd) {
        rd = make_forwardable(std::move(rd));
    }
    return rd;
}

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
    if (slice.is_reversed()) {
        return make_reversed_reader_scaffold(std::move(sstable), std::move(schema), std::move(permit), range, slice,
                std::move(trace_state), fwd, fwd_mr, monitor, std::move(ir), single_partition_read_murmur_hash);
    }
    return make_forward_reader(std::move(sstable), std::move(schema), std::move(permit), range, value_or_reference(slice),
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
    return make_forward_reader(std::move(sstable), std::move(schema), std::move(permit), query::full_partition_range,
            value_or_reference(slice), std::move(trace_state), streamed_mutation::forwarding::no, mutation_reader::forwarding::no,
            monitor, nullptr, nullptr);
}

} // namespace sstables::parquet
