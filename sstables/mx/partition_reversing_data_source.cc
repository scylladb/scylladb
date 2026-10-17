/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/core/coroutine.hh>
#include <seastar/core/iostream.hh>
#include <seastar/util/memory-data-source.hh>
#include "partition_reversing_data_source.hh"
#include "reader_permit.hh"
#include "sstables/compress.hh"
#include "sstables/decompressing_cursor.hh"
#include "sstables/consumer.hh"
#include "sstables/processing_result_generator.hh"
#include "sstables/reversing_source_cursor.hh"
#include "sstables/sstable_position.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/sstables.hh"
#include "sstables/types.hh"
#include "utils/to_string.hh"

namespace sstables {

extern logging::logger sstlog;

namespace mx {

// Parser for the partition header and the static row, if present.
//
// After consuming the input stream, allows reading the position after the consumed
// segment using header_end_pos(), as an absolute file position.
// Parsing copied from the sstable reader, with verification removed.
//
class partition_header_context : public data_consumer::continuous_data_consumer<partition_header_context> {
    // Set while parsing, before parsing can finish.
    std::optional<sstable_position> _header_end_pos;
    // Size of the header in uncompressed bytes, i.e. the distance from the
    // partition start to _header_end_pos. bytes_consumed() can't be used for
    // this. The parser reads the flags of the first row to find the end of the
    // header, so after parsing, bytes_consumed() can be past the end of the
    // header.
    uint64_t _header_size = 0;
    bool _finished = false;
    processing_result_generator _gen;
    temporary_buffer<char>* _processing_data;
public:
    bool non_consuming() const {
        return false;
    }
    void verify_end_state() const {
        if (!_finished) {
            throw std::runtime_error("partition_header_context - no more data but parsing is incomplete");
        }
    }
    sstable_position header_end_pos() {
        SCYLLA_ASSERT(_header_end_pos);
        return *_header_end_pos;
    }
    uint64_t header_size() const {
        return _header_size;
    }
    data_consumer::processing_result process_state(temporary_buffer<char>& data) {
        _processing_data = &data;
        auto ret = _gen.generate();
        if (ret == data_consumer::proceed::no) {
            _finished = true;
        }
        return ret;
    }
private:
    // Absolute file position of the current parse point (the first byte not yet
    // consumed from _processing_data).
    sstable_position current_position() {
        return compute_relative_position(-static_cast<int64_t>(_processing_data->size()));
    }
    // Record the current parse point as the end of the header, both as an
    // absolute position and as a byte count from the partition start.
    void set_header_end() {
        _header_end_pos = current_position();
        _header_size = bytes_consumed() - _processing_data->size();
    }
    processing_result_generator do_process_state() {
        // length of the partition key
        co_yield read_16(*_processing_data);
        co_yield skip(*_processing_data,
                // skip partition key
                uint32_t{_u16}
                // skip deletion_time::local_deletion_time
                + sizeof(uint32_t)
                // skip deletion_time::marked_for_delete_at
                + sizeof(uint64_t));

        // Peek the first row or tombstone. If it's a static row, determine where it ends,
        // i.e. where the sequence of clustering rows starts.

        set_header_end();
        co_yield read_8(*_processing_data);
        auto flags = unfiltered_flags_m(_u8);
        if (flags.is_end_of_partition() || flags.is_range_tombstone() || !flags.has_extended_flags()) {
            co_yield data_consumer::proceed::no;
        } else {
            co_yield read_8(*_processing_data);
            auto extended_flags = unfiltered_extended_flags_m(_u8);
            if (!extended_flags.is_static()) {
                co_yield data_consumer::proceed::no;
            }
        }

        // A static row is present.
        // There are no clustering blocks. Read the row body size:
        co_yield read_unsigned_vint(*_processing_data);
        // skip the row body
        co_yield skip(*_processing_data, _u64);
        set_header_end();
        // _header_end_pos is where the clustering rows start
        co_yield data_consumer::proceed::no;
    }
public:

    // `start` is the absolute file position the stream begins at (the partition
    // start). Positions reported by this context (e.g. header_end_pos()) are
    // absolute file positions. The segment to parse is unbounded; parsing stops
    // when the header (and static row, if any) has been consumed.
    partition_header_context(std::unique_ptr<data_consumer::continuous_data_consumer_input_stream> input, sstable_position start, reader_permit permit)
                : continuous_data_consumer(std::move(permit), std::move(input), start, std::optional<sstable_position>())
                , _gen(do_process_state())
    {}
};

// Parser of rows/tombstones that skips their bodies.
//
// Reads rows in their file order, pausing consumption after each row.
// To read rows in reverse order, use the prev_len() value to find
// the start position of the previous row, and create a new context
// to read that row.
// After reading the end_of_partition flag, end_of_partition() returns
// true.
// After reading a tombstone, current_tombstone_reversing_info() returns
// information about the tombstone kind, as well as the offsets of its
// members, which is useful for reversing the tombstone.
//
// `row_body_skipping_context` does not handle the static row (if there is one in the partition),
// only `unfiltered`s (clustering rows and tombstones).
class row_body_skipping_context : public data_consumer::continuous_data_consumer<row_body_skipping_context> {
    bool _end_of_partition = false;
    bool _finished = false;
    processing_result_generator _gen;
    temporary_buffer<char>* _processing_data;

public:
    struct tombstone_reversing_info {
        uint64_t kind_offset;
        bound_kind_m range_tombstone_kind;

        // Range tombstone markers in the sstable data file come in two kinds: bound markers and boundary markers.
        // Bound markers happen when a range tombstone opens or ends.
        // Boundary markers happen when one range tombstone ends but another opens at the same position.
        //
        // Bound markers have one `delta_deletion_time` structs (tombstone timestamp + local deletion time) at the end.
        // Boundary markers have two.
        //
        // `first_deletion_time_offset` gives the position of the first `delta_deletion_time` (which is present for both kinds),
        // after_first_deletion_time gives its end position (i.e. position of last byte plus one), which in case of boundary
        // markers is the start position of the second `delta_deletion_time` (in case of bound markers its the end of the whole marker).
        uint64_t first_deletion_time_offset;
        uint64_t after_first_deletion_time_offset;
    };
private:
    unfiltered_flags_m _flags{0};
    std::optional<tombstone_reversing_info> _current_tombstone_reversing_info;
    uint64_t _prev_unfiltered_size;
    uint64_t _row_size = 0;

    // for calculating the clustering blocks
    std::ranges::subrange<std::vector<std::optional<uint32_t>>::const_iterator> _ck_column_value_fix_lengths;
    uint64_t _ck_blocks_header;
    uint32_t _ck_blocks_header_offset;
    bool _reading_range_tombstone_ck = false;
    bool _reading_row_ck = false; // includes single row tombstones
    uint16_t _ck_size;
    column_translation _column_translation;

    void setup_ck(const std::vector<std::optional<uint32_t>>& column_value_fix_lengths) {
        if (column_value_fix_lengths.empty()) {
            _ck_column_value_fix_lengths = std::ranges::subrange(column_value_fix_lengths);
        } else {
            _ck_column_value_fix_lengths = std::ranges::subrange(std::begin(column_value_fix_lengths),
                                                                 std::begin(column_value_fix_lengths) + _ck_size);
        }
        _ck_blocks_header_offset = 0u;
    }
    bool no_more_ck_blocks() const { return _ck_column_value_fix_lengths.empty(); }
    void move_to_next_ck_block() {
        _ck_column_value_fix_lengths.advance(1);
        ++_ck_blocks_header_offset;
        if (_ck_blocks_header_offset == 32u) {
            _ck_blocks_header_offset = 0u;
        }
    }
    std::optional<uint32_t> get_ck_block_value_length() const {
        return _ck_column_value_fix_lengths.front();
    }
    bool is_block_empty() const {
        return (_ck_blocks_header & (uint64_t(1) << (2 * _ck_blocks_header_offset))) != 0;
    }
    bool is_block_null() const {
        return (_ck_blocks_header & (uint64_t(1) << (2 * _ck_blocks_header_offset + 1))) != 0;
    }
    bool should_read_block_header() const {
        return _ck_blocks_header_offset == 0u;
    }

public:
    bool non_consuming() const {
        return false;
    }
    void verify_end_state() const {
        if (!_finished) {
            throw std::runtime_error("row_body_skipping_context - no more data but parsing is incomplete");
        }
    }
    bool end_of_partition() const {
        return _end_of_partition;
    }
    uint64_t prev_len() {
        return _prev_unfiltered_size;
    }
    // The size in uncompressed bytes of the last consumed unfiltered, from its
    // flags to the start of the next unfiltered. Not updated by the
    // end_of_partition flag.
    uint64_t row_size() {
        return _row_size;
    }
    std::optional<tombstone_reversing_info> current_tombstone_reversing_info() {
        // std::nullopt if the last consumed unfiltered was not a tombstone
        return _current_tombstone_reversing_info;
    }
    data_consumer::processing_result process_state(temporary_buffer<char>& data) {
        _processing_data = &data;
        auto ret = _gen.generate();
        return ret;
    }
private:
    // The distance, in uncompressed bytes, from the start of the stream to the
    // current parse point. It is not a position in Data.db.
    uint64_t offset_in_stream() {
        return bytes_consumed() - _processing_data->size();
    }
    processing_result_generator do_process_state() {
        while (true) {
            _finished = false;
            uint64_t row_start = offset_in_stream();
            co_yield read_8(*_processing_data);
            auto flags = unfiltered_flags_m(_u8);
            _current_tombstone_reversing_info.reset();
            if (flags.is_end_of_partition()) {
                _end_of_partition = true;
                _finished = true;
                co_yield data_consumer::proceed::no;
                break;
            } else if (flags.is_range_tombstone()) {
                _current_tombstone_reversing_info.emplace();
                _current_tombstone_reversing_info->kind_offset = offset_in_stream();
                co_yield read_8(*_processing_data);
                _current_tombstone_reversing_info->range_tombstone_kind = bound_kind_m(_u8);
                co_yield read_16(*_processing_data);
                _ck_size = _u16;
                if (_ck_size != 0) {
                    _reading_range_tombstone_ck = true;
                }
            } else {
                if (flags.has_extended_flags()) {
                    // we only read the flags to perform a sanity check
                    co_yield read_8(*_processing_data);
                    auto extended_flags = unfiltered_extended_flags_m(_u8);
                    if (extended_flags.is_static()) {
                        on_internal_error(sstlog, "partition_reversing_data_source: row_body_skipping_context constructed on a static row");
                    }
                }
                _ck_size = _column_translation.clustering_column_value_fix_legths().size();
                _reading_row_ck = true;
            }
            if (_reading_row_ck || _reading_range_tombstone_ck) {
                setup_ck(_column_translation.clustering_column_value_fix_legths());
                while (!no_more_ck_blocks()) {
                    if (should_read_block_header()) {
                        co_yield read_unsigned_vint(*_processing_data);
                        _ck_blocks_header = _u64;
                    }
                    if (is_block_null() || is_block_empty()) {
                        move_to_next_ck_block();
                        continue;
                    }
                    // possibly read the length of, and then skip the clustering cell
                    if (auto len = get_ck_block_value_length()) {
                        co_yield skip(*_processing_data, *len);
                    } else {
                        co_yield read_unsigned_vint(*_processing_data);
                        co_yield skip(*_processing_data, _u64);
                    }
                    move_to_next_ck_block();
                }
                _reading_row_ck = false;
                _reading_range_tombstone_ck = false;
            }
            co_yield read_unsigned_vint(*_processing_data);
            // marker_body_size or row_body_size
            uint64_t next_row_offset = offset_in_stream() + _u64;
            _row_size = next_row_offset - row_start;
            co_yield read_unsigned_vint(*_processing_data);
            _prev_unfiltered_size = _u64;
            if (_current_tombstone_reversing_info) {
                _current_tombstone_reversing_info->first_deletion_time_offset = offset_in_stream();
                // skip delta_marked_for_delete_at and delta_local_deletion_time
                co_yield read_unsigned_vint(*_processing_data);
                co_yield read_unsigned_vint(*_processing_data);
                _current_tombstone_reversing_info->after_first_deletion_time_offset = offset_in_stream();
            }
            _finished = true;
            // skip until the next row, allowing to read consecutive rows in disk order
            co_yield skip(*_processing_data, next_row_offset - offset_in_stream());
            co_yield data_consumer::proceed::no;
        }
    }
public:
    // `start` is the position the stream begins at and `end` bounds the segment
    // to parse. position() reported by this context is relative to the same
    // origin as `start`: an absolute file position when parsing the data file,
    // or 0 when parsing a row in memory. The offsets in tombstone_reversing_info
    // are relative to the start of `input`, i.e. to the row the stream starts
    // at, so they index directly into the row buffer.
    row_body_skipping_context(std::unique_ptr<data_consumer::continuous_data_consumer_input_stream> input, sstable_position start, sstable_position end, reader_permit permit, column_translation ct)
                : continuous_data_consumer(std::move(permit), std::move(input), start, std::optional<sstable_position>(end))
                , _gen(do_process_state())
                , _column_translation(std::move(ct))
    {}
};

// Precondition: `k` is not static_clustering or clustering
bound_kind_m reverse_tombstone_kind(bound_kind_m k) {
    switch (k) {
        case bound_kind_m::excl_end:
            return bound_kind_m::excl_start;
        case bound_kind_m::incl_start:
            return bound_kind_m::incl_end;
        case bound_kind_m::excl_end_incl_start:
            return bound_kind_m::incl_end_excl_start;
        case bound_kind_m::incl_end_excl_start:
            return bound_kind_m::excl_end_incl_start;
        case bound_kind_m::incl_end:
            return bound_kind_m::incl_start;
        case bound_kind_m::excl_start:
            return bound_kind_m::excl_end;
        default:
            on_internal_error(sstlog, format(
                "reverse_tombstone_kind: expected tombstone kind, got {}", k));
    }
}

// A 'row' consisting of a single byte, representing the end of partition in sstable data file.
static temporary_buffer<char> end_of_partition() {
    temporary_buffer<char> tmp(1);
    *tmp.get_write() = 1;
    return tmp;
}

// An owned input-stream facade for a borrowed compressed cursor. A parser owns
// and closes its input stream. However, forward parsing and backward row reads
// must share one physical cursor. This facade sends all operations to that
// cursor. close() does nothing. The reversing data source closes the cursor.
//
// The parser needs no data past `end`. The parser passes its end to
// consume_one() itself, but skip() has no such argument, so the facade passes
// `end` to the cursor's skip().
class borrowed_cursor_stream final : public data_consumer::continuous_data_consumer_input_stream {
    decompressing_cursor& _cursor;
    sstable_position _end;
public:
    borrowed_cursor_stream(decompressing_cursor& cursor, sstable_position end) noexcept
        : continuous_data_consumer_input_stream(cursor.stream_position())
        , _cursor(cursor)
        , _end(end)
    {}
    future<> skip_to(sstable_position target) override {
        return _cursor.skip_to(target);
    }
    future<> skip(uint64_t n) override {
        return _cursor.skip(n, _end);
    }
    future<> close() override {
        // The cursor is owned by the reversing data source, not by this stream.
        return make_ready_future<>();
    }
    sstable_position compute_relative_position(int64_t offset) override {
        return _cursor.compute_relative_position(offset);
    }
    void init_stream_position(sstable_position start) override {
        _cursor.init_stream_position(start);
    }
    future<consumption_result<char>> consume_one(std::optional<sstable_position> end_position, consumer_one_fn consumer) override {
        return _cursor.consume_one(end_position, std::move(consumer));
    }
};

// Reversing cursor for logical positions. Each forward parser owns an
// independent sstable data stream. Backward reads share a buffer that ends at
// `_back_cache_end`. Refill reads grow from 4 KiB to 128 KiB. The cursor removes
// each returned row from the end of the buffer. Thus, consecutive reverse reads
// can use one disk read. Logical positions are byte offsets. Therefore, the
// start of a backward read is found by subtraction.
class logical_reversing_cursor final : public reversing_source_cursor {
    shared_sstable _sst;
    reader_permit _permit;
    tracing::trace_state_ptr _trace_state;
    // Backward reads never go below this offset (the partition start).
    int64_t _lower_bound;

    // Single-buffer backward cache. Holds the bytes ending at _back_cache_end;
    // read_backwards() refills it (growing 4KB..128KB) when it does not already
    // cover the requested segment, and trims served bytes off its end so it stays
    // anchored at the top of the backward walk.
    temporary_buffer<char> _back_cache;
    int64_t _back_cache_end = 0;
    uint64_t _read_size = 4 * 1024;
    static constexpr uint64_t max_read_size = 128 * 1024;

public:
    logical_reversing_cursor(shared_sstable sst, reader_permit permit, tracing::trace_state_ptr trace_state,
            sstable_position partition_start)
        : _sst(std::move(sst))
        , _permit(std::move(permit))
        , _trace_state(std::move(trace_state))
        , _lower_bound(partition_start.to_logical())
    {}

    future<std::unique_ptr<data_consumer::continuous_data_consumer_input_stream>>
    make_forward_input_stream(sstable_position start, sstable_position end) override {
        co_return co_await _sst->data_stream(disk_read_range(start, end), _permit, _trace_state, {});
    }

    future<temporary_buffer<char>>
    read_forwards(sstable_position start, sstable_position end) override {
        co_return co_await _sst->data_read(start.to_logical(), end.to_logical() - start.to_logical(), _permit);
    }

    future<backward_read>
    read_backwards(sstable_position end, size_t size) override {
        int64_t e = end.to_logical();
        int64_t start = e - static_cast<int64_t>(size);
        if (start < _lower_bound) {
            throw_malformed_sstable_exception(format(
                    "partition_reversing_data_source: backward read of {} bytes ending at {} starts before the partition start {}",
                    size, end, _lower_bound));
        }
        // If the cache is not anchored at `end` (e.g. after a range shrink or a
        // non-contiguous jump), start a fresh one there.
        if (_back_cache_end != e) {
            _back_cache = {};
            _back_cache_end = e;
        }
        if (_back_cache.size() < size) {
            int64_t begin;
            if (_lower_bound + static_cast<int64_t>(_read_size) < e) {
                begin = std::min<int64_t>(e - static_cast<int64_t>(_read_size), start);
            } else {
                begin = _lower_bound;
            }
            _back_cache = co_await _sst->data_read(begin, e - begin, _permit);
            _back_cache_end = e;
            _read_size = std::min<uint64_t>(max_read_size, _read_size * 2);
        }
        // Hand out the last `size` bytes and trim them off the cache so it stays
        // anchored at `start` for the next backward read. The returned view no
        // longer overlaps the cache, so the caller may mutate it.
        auto ret = _back_cache.share(_back_cache.size() - size, size);
        _back_cache.trim(_back_cache.size() - size);
        _back_cache_end = start;
        co_return backward_read{sstable_position::from_logical(start), std::move(ret)};
    }

    void trim_read_ahead(sstable_position new_end) override {
        // If the cache reaches down to `new_end`, keep the part below it. The
        // next backward read ends at `new_end`, so it can use that part.
        int64_t e = new_end.to_logical();
        int64_t cache_start = _back_cache_end - static_cast<int64_t>(_back_cache.size());
        if (cache_start <= e && e <= _back_cache_end) {
            _back_cache.trim(e - cache_start);
            _back_cache_end = e;
        } else {
            _back_cache = {};
            _back_cache_end = 0;
        }
    }

    future<> close() override {
        // Parsers own and close their own streams; data_read has no stream to close.
        return make_ready_future<>();
    }
};

// Reversing cursor for physical positions. Forward parsers borrow the
// bidirectional decompressing cursor that supplies backward row reads. Thus,
// reads in both directions share its physical position and its buffers.
class physical_reversing_cursor final : public reversing_source_cursor {
    std::unique_ptr<decompressing_cursor> _cursor;

public:
    explicit physical_reversing_cursor(std::unique_ptr<decompressing_cursor> cursor)
        : _cursor(std::move(cursor))
    {}

    future<std::unique_ptr<data_consumer::continuous_data_consumer_input_stream>>
    make_forward_input_stream(sstable_position, sstable_position end) override {
        // The parser positions the borrowed cursor itself (init_stream_position),
        // so the start is not needed here. The end limits read-ahead, as it
        // bounds the stream of the logical path.
        co_return std::make_unique<borrowed_cursor_stream>(*_cursor, end);
    }

    future<temporary_buffer<char>>
    read_forwards(sstable_position start, sstable_position end) override {
        // Reading rows backwards is the cursor's usual job; read the header forwards
        // through the same cursor's consume interface. The size of [start, end)
        // is unknown until it is read, so collect the fragments first.
        _cursor->init_stream_position(start);
        std::vector<temporary_buffer<char>> fragments;
        size_t size = 0;
        bool at_end = false;
        while (!at_end) {
            co_await _cursor->consume_one(end, [&] (temporary_buffer<char> data) -> consumption_result<char> {
                // An empty buffer means that the stream reached `end` or eof.
                at_end = data.empty();
                size += data.size();
                fragments.push_back(std::move(data));
                return continue_consuming{};
            });
        }
        auto out = temporary_buffer<char>(size);
        size_t filled = 0;
        for (const auto& fragment : fragments) {
            std::memcpy(out.get_write() + filled, fragment.get(), fragment.size());
            filled += fragment.size();
        }
        co_return out;
    }

    future<backward_read>
    read_backwards(sstable_position end, size_t size) override {
        _cursor->seek(end);
        auto data = co_await _cursor->read_backwards_exactly(size);
        co_return backward_read{_cursor->stream_position().position, std::move(data)};
    }

    void trim_read_ahead(sstable_position) override {
        // Nothing to discard: the next backward read seeks to the new end.
    }

    future<> close() override {
        return _cursor->close();
    }
};

// The intermediary data source that reads from an sstable, and produces
// data buffers, as if the sstable had all rows written in a reversed order.
//
// The intermediary always starts by reading the partition header and the
// static row using partition_header_context. The offset after the parsed
// segment is the new actual "partition end" in reversed order - when
// reached, an unfiltered with a single flag "partition_end" is produced.
//
// After reading the partition header, the data source advances to the end
// of the clustering range. Afterwards, we may encounter 2 situations:
// there is another unfiltered after the clustering range, or there is
// partition end. In the former case, we read the following unfiltered, and
// deduce the position of the first row of our actual range using
// row_body_skipping_context::prev_len(). If it's the latter, we find the
// last row by iterating over the entire last promoted index block.
//
// After finding the last row, we produce rows in reversed order one by one,
// parsing current row using row_body_skipping_context, and finding file
// offsets of the previous one using the start of the current row as the end,
// and the end decreased by row_body_skipping_context::prev_len() as the start
//
// We skip between clustering ranges using the index_reader's data range.
// When we detect that the range end has been decreased, we return to the same
// state as after reading the partition header, and continue as if the new
// range was the original.
//
// All IO into the data file goes through a single reversing_source_cursor,
// chosen at construction: a physical_reversing_cursor for the physical-position
// compressed format (currently `mu`), or a logical_reversing_cursor otherwise.
// The parsers that scan forward (the partition header, the search for the
// range end) get their own forward input stream from the cursor
// (make_forward_input_stream). The rows handed back to the sstable reader are
// read backwards from it (read_backwards), and then parsed in memory. The size
// of each row is known before it is read: it is the prev_len() of the row
// after it, or the row_size() found by the forward scan. A backward read
// returns the row's start together with its bytes. Physical positions can't be
// subtracted, so the start is known only after the row is read. Thus, the
// position shared with the sstable reader never needs the next row's start:
// while rows remain, it is the end of the next row, and that row is read only
// when get() returns it. The partition header, handed back
// in file order, is read forwards from the cursor (read_forwards).
//
// Because the range tombstones are read in reversed order, we need to swap
// the start tombstones with the ends. We achieve that by finding the file
// offsets of the row tombstone member variables using row_body_skipping_context,
// and modifying them in the returned row buffer accordingly.
//
class partition_reversing_data_source_impl final : public data_source_impl {
    const schema& _schema;
    shared_sstable _sst;
    abstract_index_reader& _ir;
    reader_permit _permit;
    tracing::trace_state_ptr _trace_state;

    // The single cursor through which all IO into the sstable data file is done:
    // it hands out the forward streams the parsers own, reads the partition header
    // forwards, and reads the rows handed back to the sstable reader backwards.
    std::unique_ptr<reversing_source_cursor> _cursor;

    std::optional<partition_header_context> _partition_header_context;
    std::optional<row_body_skipping_context> _row_skipping_context;
    // Set by the first get(), which reads the partition header.
    std::optional<sstable_position> _clustering_range_start;
    sstable_position _partition_start;
    sstable_position _partition_end;

    // _row_start denotes our current position in the input stream:
    // either _partition_end or the start of some row (_row_start never lands in the middle of a row).
    // We share this position with the user (they can only read it, not modify it)
    // so they can e.g. compare it with index positions.
    // Everything from _row_start to the partition end has been returned or skipped.
    sstable_position _row_start;
    sstable_position _row_end;
    // Invariant: _row_start <= _row_end
    // The size of the next row to return, which ends at _row_end, in
    // uncompressed bytes. Physical positions can't be subtracted, so it is
    // tracked separately. Between get() calls in state ROWS, _row_start ==
    // _row_end, and the next row's start is unknown until it is read.
    uint64_t _row_size = 0;

    column_translation _cached_column_translation;

    enum class state {
        // Looking for the first row entry (last in original order) in the clustering range being read
        RANGE_END,

        // Returning a buffer containing a row entry
        ROWS,

        // Returning a partition end flag
        PARTITION_END,

        // Nothing more to return
        FINISHED
    } _state = state::RANGE_END;
private:
    // Reverse the range tombstone bound/boundary stored in `row`, which holds
    // the bytes of the row spanning [_row_start, _row_end). `info`'s offsets are
    // relative to the row's start (the row_body_skipping_context that produced
    // them was started at _row_start), so they index into `row` directly.
    void modify_tombstone(temporary_buffer<char>& row, const row_body_skipping_context::tombstone_reversing_info& info) {
        char& out = row.get_write()[info.kind_offset];
        // reverse the kind of the range tombstone bound/boundary
        out = (char)reverse_tombstone_kind(info.range_tombstone_kind);
        if (is_boundary_between_adjacent_intervals(info.range_tombstone_kind)) {
            // if the tombstone is a boundary, we need to swap the order of end/start deletion times
            // Need to clone part of the buffer containing first_del_time because we overwrite it with second_del_time before using first_del_time
            auto first_del_time = row.share(info.first_deletion_time_offset, info.after_first_deletion_time_offset - info.first_deletion_time_offset).clone();
            // We also need to clone the part containing second_del_time as we may overwrite a prefix of that part while writing second_del_time
            // (if second_del_time is longer than first_del_time - it may be as we're dealing with varints here)
            auto second_del_time = row.share(info.after_first_deletion_time_offset, row.size() - info.after_first_deletion_time_offset).clone();
            std::copy(second_del_time.begin(), second_del_time.end(), row.get_write() + info.first_deletion_time_offset);
            std::copy(first_del_time.begin(), first_del_time.end(), row.get_write() + info.first_deletion_time_offset + second_del_time.size());
        }
    }

    future<> emplace_row_skipping_context(sstable_position row_start, sstable_position row_end) {
        if (_row_skipping_context) {
            co_await _row_skipping_context->close();
        }
        _row_skipping_context.emplace(
                co_await _cursor->make_forward_input_stream(row_start, row_end),
                row_start, row_end,
                _permit, _cached_column_translation);
    }

    // Parses the row in `row`, which holds the bytes of [_row_start, _row_end).
    // An in-memory stream can only advance its position by logical offsets, and
    // _row_start can be a physical position. So the parser sees positions
    // relative to the row start.
    future<> parse_row_in_memory(temporary_buffer<char>& row) {
        if (_row_skipping_context) {
            co_await _row_skipping_context->close();
        }
        _row_skipping_context.emplace(
                std::make_unique<data_consumer::continuous_data_consumer_seastar_input_stream>(
                        seastar::util::as_input_stream(row.share())),
                sstable_position::from_logical(0), sstable_position::from_logical(row.size()),
                _permit, _cached_column_translation);
        co_await _row_skipping_context->consume_input();
        if (_row_skipping_context->end_of_partition() || _row_skipping_context->row_size() != row.size()) {
            throw_malformed_sstable_exception(format(
                    "partition_reversing_data_source: the {} bytes at [{}, {}) are not a single row",
                    row.size(), _row_start, _row_end));
        }
    }

    // Builds the cursor backing all IO into the data file: a physical_reversing_cursor
    // for the physical-position compressed format, or a logical_reversing_cursor
    // (data_stream/data_read) otherwise.
    static std::unique_ptr<reversing_source_cursor> make_cursor(const shared_sstable& sst, reader_permit permit,
            tracing::trace_state_ptr trace_state, sstable_position partition_start, sstable_position partition_end) {
        if (sst->uses_physical_positions()) {
            return std::make_unique<physical_reversing_cursor>(make_bidirectional_decompressing_cursor(
                    sst, disk_read_range(partition_start, partition_end), permit, std::move(trace_state)));
        }
        return std::make_unique<logical_reversing_cursor>(sst, std::move(permit), std::move(trace_state),
                partition_start);
    }

public:
    partition_reversing_data_source_impl(const schema& s,
            shared_sstable sst,
            abstract_index_reader& ir,
            sstable_position partition_start,
            sstable_position partition_end,
            reader_permit permit,
            tracing::trace_state_ptr trace_state)
        : _schema(s)
        , _sst(std::move(sst))
        , _ir(ir)
        , _permit(std::move(permit))
        , _trace_state(std::move(trace_state))
        , _cursor(make_cursor(_sst, _permit, _trace_state, partition_start, partition_end))
        , _partition_start(partition_start)
        , _partition_end(partition_end)
        , _row_start(_partition_end)
        , _row_end(_partition_end)
        , _cached_column_translation(_sst->get_column_translation(_schema, _sst->get_serialization_header(), _sst->features()))
    { }

    virtual future<temporary_buffer<char>> get() override {
        if (!_partition_header_context) {
            _partition_header_context.emplace(
                    co_await _cursor->make_forward_input_stream(_partition_start, _partition_end),
                    _partition_start, _permit);
            co_await _partition_header_context->consume_input();
            _clustering_range_start = _partition_header_context->header_end_pos();
            auto header_buf = co_await _cursor->read_forwards(_partition_start, *_clustering_range_start);
            if (header_buf.size() != _partition_header_context->header_size()) {
                throw_malformed_sstable_exception(format(
                        "partition_reversing_data_source: forward read of [{}, {}) yielded {} bytes, expected {}",
                        _partition_start, *_clustering_range_start, header_buf.size(), _partition_header_context->header_size()));
            }
            co_return header_buf;
        }
        auto ir_end = _ir.sstable_positions().end;
        if (ir_end && *ir_end < _row_start) {
            // we can skip at least one row
            _row_start = *ir_end;
            _cursor->trim_read_ahead(_row_start);
            _state = state::RANGE_END;
        }
        switch (_state) {
        case state::RANGE_END: {
            bool look_in_last_block = false;
            if (_row_start >= _row_end) {
                if (_row_start != _row_end) {
                    on_internal_error(sstlog, format(
                        "partition_reversing_data_source: invariant broken: _row_start({}) > _row_end({})",
                        _row_start, _row_end));
                }
                if (_row_start != _partition_end) {
                    on_internal_error(sstlog, format(
                        "partition_reversing_data_source: invariant broken: _row_start({}) == _row_end({}), but"
                        " != _partition_end({})", _row_start, _row_end, _partition_end));
                }
                look_in_last_block = true;
            } else {
                co_await emplace_row_skipping_context(_row_start, _row_end);
                co_await _row_skipping_context->consume_input();
                if (_row_skipping_context->end_of_partition()) {
                    look_in_last_block = true;
                } else {
                    // The row before the one just parsed is the last one in the
                    // range. Its start is unknown until state ROWS reads it.
                    _row_end = _row_start;
                    _row_size = _row_skipping_context->prev_len();
                }
            }
            if (look_in_last_block) {
                if (auto offset = co_await _ir.last_block_offset()) {
                    // there was a promoted index block in the partition, read from its beginning to find the last row
                    _row_start = _partition_start + offset.value();
                } else {
                    // no promoted index blocks in the partition, read from the beginning
                    _row_start = *_clustering_range_start;
                }
                co_await emplace_row_skipping_context(_row_start, _partition_end);
                auto current_row_start = _row_skipping_context->position();
                auto last_row_start = current_row_start;
                uint64_t last_row_size = 0;
                co_await _row_skipping_context->consume_input();
                while (!_row_skipping_context->end_of_partition()) {
                    last_row_start = current_row_start;
                    last_row_size = _row_skipping_context->row_size();
                    current_row_start = _row_skipping_context->position();
                    co_await _row_skipping_context->consume_input();
                }
                _row_end = current_row_start;
                _row_start = last_row_start;
                _row_size = last_row_size;
                if (_row_start == _row_end) {
                    // empty partition
                    _state = state::FINISHED;
                    co_return end_of_partition();
                }
            }

            if (_row_end <= *_clustering_range_start) {
                // The first index block starts after the range being read,
                // i.e. the range being read is empty.
                //
                // Check that the row before the range starts at the partition
                // start. The partition header lies between the two, so the
                // row's size must be the header's size. Checking the size
                // needs no read, unlike finding the row's start.
                if (_row_skipping_context->prev_len() != _partition_header_context->header_size()) {
                    on_internal_error(sstlog, format(
                        "partition_reversing_data_source: invariant broken: _row_end({}) <= _clustering_range_start({})"
                        ", but _row_skipping_context->prev_len()({}) != header_size({})",
                        _row_end, *_clustering_range_start, _row_skipping_context->prev_len(),
                        _partition_header_context->header_size()));
                }
                _row_start = *_clustering_range_start;
                _state = state::FINISHED;
                co_return end_of_partition();
            }

            _state = state::ROWS;
            [[fallthrough]];
        }
        case state::ROWS: {
            auto row = co_await _cursor->read_backwards(_row_end, _row_size);
            _row_start = row.start;
            auto ret = std::move(row.data);
            co_await parse_row_in_memory(ret);
            if (_row_skipping_context->current_tombstone_reversing_info()) {
                modify_tombstone(ret, *_row_skipping_context->current_tombstone_reversing_info());
            }
            _row_end = _row_start;
            _row_size = _row_skipping_context->prev_len();
            if (_row_end == *_clustering_range_start) {
                // The row before is the partition header, which the first get()
                // returned, so it is not read again.
                _row_start = _partition_start;
                _state = state::PARTITION_END;
            }
            co_return ret;
        }
        case state::PARTITION_END: {
            _state = state::FINISHED;
            co_return end_of_partition();
        }
        case state::FINISHED:
            co_return temporary_buffer<char>();
        }
    }

    virtual future<temporary_buffer<char>> skip(uint64_t n) override {
        // Skipping is implemented by checking the index.
        on_internal_error(sstlog, "partition_reversing_data_source does not support skipping");
    }

    // Must not be run concurrently with `get()`.
    virtual future<> close() noexcept override {
        auto close_partition_header_context = _partition_header_context ? _partition_header_context->close() : make_ready_future<>();
        auto close_row_skipping_context = _row_skipping_context ? _row_skipping_context->close() : make_ready_future();
        auto close_cursor = _cursor ? _cursor->close() : make_ready_future<>();
        co_await when_all(std::move(close_partition_header_context), std::move(close_row_skipping_context), std::move(close_cursor));
    }

    // Points to the current position of the source over the sstable file, which
    // is either the end of partition or the beginning of some row. Everything
    // from it to the end of the partition has been returned or skipped.
    // Can only decrease.
    const sstable_position& current_position_in_sstable() const {
        return _row_start;
    }
};

partition_reversing_data_source make_partition_reversing_data_source(const schema& s, shared_sstable sst, abstract_index_reader& ir, sstable_position start, sstable_position end,
                                                          reader_permit permit, tracing::trace_state_ptr trace_state) {
    auto source_impl = std::make_unique<partition_reversing_data_source_impl>(
            s, std::move(sst), ir, start, end, std::move(permit), trace_state);
    auto& curr_pos = source_impl->current_position_in_sstable();
    return partition_reversing_data_source {
        .the_source = seastar::data_source{std::move(source_impl)},
        .current_position_in_sstable = curr_pos
    };
}

}

}
