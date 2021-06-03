/*
 * Copyright (C) 2021-present ScyllaDB
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

#include "reader.hh"
#include "concrete_types.hh"
#include "sstables/liveness_info.hh"
#include "sstables/mutation_fragment_filter.hh"
#include "sstables/sstable_mutation_reader.hh"

namespace sstables {
namespace mx {

class consumer_m {
    reader_permit _permit;
    tracing::trace_state_ptr _trace_state;
    const io_priority_class& _pc;
public:
    using proceed = data_consumer::proceed;

    enum class row_processing_result {
        // Causes the parser to return the control to the caller without advancing.
        // Next time when the parser is called, the same consumer method will be called.
        retry_later,

        // Causes the parser to proceed to the next element.
        do_proceed,

        // Causes the parser to skip the whole row. consume_row_end() will not be called for the current row.
        skip_row
    };

    consumer_m(reader_permit permit, tracing::trace_state_ptr trace_state, const io_priority_class& pc)
    : _permit(std::move(permit))
    , _trace_state(std::move(trace_state))
    , _pc(pc) {
    }

    virtual ~consumer_m() = default;

    // Consume the row's key and deletion_time. The latter determines if the
    // row is a tombstone, and if so, when it has been deleted.
    // Note that the key is in serialized form, and should be deserialized
    // (according to the schema) before use.
    // As explained above, the key object is only valid during this call, and
    // if the implementation wishes to save it, it must copy the *contents*.
    virtual proceed consume_partition_start(sstables::key_view key, sstables::deletion_time deltime) = 0;

    // Called at the end of the row, after all cells.
    // Returns a flag saying whether the sstable consumer should stop now, or
    // proceed consuming more data.
    virtual proceed consume_partition_end() = 0;

    virtual row_processing_result consume_row_start(const std::vector<fragmented_temporary_buffer>& ecp) = 0;

    virtual proceed consume_row_marker_and_tombstone(
            const sstables::liveness_info& info, tombstone tomb, tombstone shadowable_tomb) = 0;

    virtual row_processing_result consume_static_row_start() = 0;

    virtual proceed consume_column(const sstables::column_translation::column_info& column_info,
                                   bytes_view cell_path,
                                   fragmented_temporary_buffer::view value,
                                   api::timestamp_type timestamp,
                                   gc_clock::duration ttl,
                                   gc_clock::time_point local_deletion_time,
                                   bool is_deleted) = 0;

    virtual proceed consume_complex_column_start(const sstables::column_translation::column_info& column_info,
                                                 tombstone tomb) = 0;

    virtual proceed consume_complex_column_end(const sstables::column_translation::column_info& column_info) = 0;

    virtual proceed consume_counter_column(const sstables::column_translation::column_info& column_info,
                                           fragmented_temporary_buffer::view value, api::timestamp_type timestamp) = 0;

    virtual proceed consume_range_tombstone(const std::vector<fragmented_temporary_buffer>& ecp,
                                            bound_kind kind,
                                            tombstone tomb) = 0;

    virtual proceed consume_range_tombstone(const std::vector<fragmented_temporary_buffer>& ecp,
                                            sstables::bound_kind_m,
                                            tombstone end_tombstone,
                                            tombstone start_tombstone) = 0;

    virtual proceed consume_row_end() = 0;

    virtual void on_end_of_stream() = 0;

    // Called when the reader is fast forwarded to given element.
    virtual void reset(sstables::indexable_element) = 0;

    virtual position_in_partition_view position() = 0;

    // Under which priority class to place I/O coming from this consumer
    const io_priority_class& io_priority() const {
        return _pc;
    }

    // The permit for this read
    reader_permit& permit() {
        return _permit;
    }

    tracing::trace_state_ptr trace_state() const {
        return _trace_state;
    }
};

// data_consume_rows_context_m remembers the context that an ongoing
// data_consume_rows() future is in for SSTable in 3_x format.
class data_consume_rows_context_m : public data_consumer::continuous_data_consumer<data_consume_rows_context_m> {
private:
    enum class state {
        PARTITION_START,
        DELETION_TIME,
        DELETION_TIME_2,
        DELETION_TIME_3,
        FLAGS,
        FLAGS_2,
        EXTENDED_FLAGS,
        CLUSTERING_ROW,
        CK_BLOCK,
        CK_BLOCK_HEADER,
        CK_BLOCK2,
        CK_BLOCK_END,
        ROW_BODY,
        ROW_BODY_SIZE,
        ROW_BODY_PREV_SIZE,
        ROW_BODY_TIMESTAMP,
        ROW_BODY_TIMESTAMP_TTL,
        ROW_BODY_TIMESTAMP_DELTIME,
        ROW_BODY_DELETION,
        ROW_BODY_DELETION_2,
        ROW_BODY_DELETION_3,
        ROW_BODY_SHADOWABLE_DELETION,
        ROW_BODY_SHADOWABLE_DELETION_2,
        ROW_BODY_SHADOWABLE_DELETION_3,
        ROW_BODY_MARKER,
        ROW_BODY_MISSING_COLUMNS,
        ROW_BODY_MISSING_COLUMNS_2,
        ROW_BODY_MISSING_COLUMNS_READ_COLUMNS,
        ROW_BODY_MISSING_COLUMNS_READ_COLUMNS_2,
        COLUMN,
        SIMPLE_COLUMN,
        COMPLEX_COLUMN,
        COMPLEX_COLUMN_MARKED_FOR_DELETE,
        COMPLEX_COLUMN_LOCAL_DELETION_TIME,
        COMPLEX_COLUMN_2,
        COMPLEX_COLUMN_SIZE,
        COMPLEX_COLUMN_SIZE_2,
        NEXT_COLUMN,
        COLUMN_FLAGS,
        COLUMN_TIMESTAMP,
        COLUMN_DELETION_TIME,
        COLUMN_DELETION_TIME_2,
        COLUMN_TTL,
        COLUMN_TTL_2,
        COLUMN_CELL_PATH,
        COLUMN_VALUE,
        COLUMN_END,
        RANGE_TOMBSTONE_MARKER,
        RANGE_TOMBSTONE_KIND,
        RANGE_TOMBSTONE_SIZE,
        RANGE_TOMBSTONE_CONSUME_CK,
        RANGE_TOMBSTONE_BODY,
        RANGE_TOMBSTONE_BODY_SIZE,
        RANGE_TOMBSTONE_BODY_PREV_SIZE,
        RANGE_TOMBSTONE_BODY_TIMESTAMP,
        RANGE_TOMBSTONE_BODY_TIMESTAMP2,
        RANGE_TOMBSTONE_BODY_LOCAL_DELTIME,
        RANGE_TOMBSTONE_BODY_LOCAL_DELTIME2,
    } _state = state::PARTITION_START;

    consumer_m& _consumer;
    shared_sstable _sst;
    const serialization_header& _header;
    column_translation _column_translation;
    const bool _has_shadowable_tombstones;

    temporary_buffer<char> _pk;

    unfiltered_flags_m _flags{0};
    unfiltered_extended_flags_m _extended_flags{0};
    uint64_t _next_row_offset;
    liveness_info _liveness;
    bool _is_first_unfiltered = true;

    std::vector<fragmented_temporary_buffer> _row_key;

    struct row_schema {
        using column_range = boost::iterator_range<std::vector<column_translation::column_info>::const_iterator>;

        // All columns for this kind of row inside column_translation of the current sstable
        column_range _all_columns;

        // Subrange of _all_columns which is yet to be processed for current row
        column_range _columns;

        // Represents the subset of _all_columns present in current row
        boost::dynamic_bitset<uint64_t> _columns_selector; // size() == _columns.size()
    };

    row_schema _regular_row;
    row_schema _static_row;
    row_schema* _row;

    uint64_t _missing_columns_to_read;

    boost::iterator_range<std::vector<std::optional<uint32_t>>::const_iterator> _ck_column_value_fix_lengths;

    tombstone _row_tombstone;
    tombstone _row_shadowable_tombstone;

    column_flags_m _column_flags{0};
    api::timestamp_type _column_timestamp;
    gc_clock::time_point _column_local_deletion_time;
    gc_clock::duration _column_ttl;
    fragmented_temporary_buffer _column_value;
    temporary_buffer<char> _cell_path;
    uint64_t _ck_blocks_header;
    uint32_t _ck_blocks_header_offset;
    bool _null_component_occured;
    uint64_t _subcolumns_to_read = 0;
    api::timestamp_type _complex_column_marked_for_delete;
    tombstone _complex_column_tombstone;
    bool _reading_range_tombstone_ck = false;
    bound_kind_m _range_tombstone_kind;
    uint16_t _ck_size;
    /*
     * We need two range tombstones because range tombstone marker can be either a single bound
     * or a double bound that represents end of one range tombstone and start of another at the same time.
     * If range tombstone marker is a single bound then only _left_range_tombstone is used.
     * Otherwise, _left_range_tombstone represents tombstone for a range tombstone that's being closed
     * and _right_range_tombstone represents a tombstone for a range tombstone that's being opened.
     */
    tombstone _left_range_tombstone;
    tombstone _right_range_tombstone;
    void start_row(row_schema& rs) {
        _row = &rs;
        _row->_columns = _row->_all_columns;
    }
    void setup_columns(row_schema& rs, const std::vector<column_translation::column_info>& columns) {
        rs._all_columns = boost::make_iterator_range(columns);
        rs._columns_selector = boost::dynamic_bitset<uint64_t>(columns.size());
    }
    void skip_absent_columns() {
        size_t pos = _row->_columns_selector.find_first();
        if (pos == boost::dynamic_bitset<uint64_t>::npos) {
            pos = _row->_columns.size();
        }
        _row->_columns.advance_begin(pos);
    }
    bool no_more_columns() const { return _row->_columns.empty(); }
    void move_to_next_column() {
        size_t current_pos = _row->_columns_selector.size() - _row->_columns.size();
        size_t next_pos = _row->_columns_selector.find_next(current_pos);
        size_t jump_to_next = (next_pos == boost::dynamic_bitset<uint64_t>::npos) ? _row->_columns.size()
                                                                                  : next_pos - current_pos;
        _row->_columns.advance_begin(jump_to_next);
    }
    bool is_column_simple() const { return !_row->_columns.front().is_collection; }
    bool is_column_counter() const { return _row->_columns.front().is_counter; }
    const column_translation::column_info& get_column_info() const {
        return _row->_columns.front();
    }
    std::optional<uint32_t> get_column_value_length() const {
        return _row->_columns.front().value_length;
    }
    void setup_ck(const std::vector<std::optional<uint32_t>>& column_value_fix_lengths) {
        _row_key.clear();
        _row_key.reserve(column_value_fix_lengths.size());
        if (column_value_fix_lengths.empty()) {
            _ck_column_value_fix_lengths = boost::make_iterator_range(column_value_fix_lengths);
        } else {
            _ck_column_value_fix_lengths = boost::make_iterator_range(std::begin(column_value_fix_lengths),
                                                                      std::begin(column_value_fix_lengths) + _ck_size);
        }
        _ck_blocks_header_offset = 0u;
    }
    bool no_more_ck_blocks() const { return _ck_column_value_fix_lengths.empty(); }
    void move_to_next_ck_block() {
        _ck_column_value_fix_lengths.advance_begin(1);
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
    using consumer = consumer_m;
    bool non_consuming() const {
        return (_state == state::DELETION_TIME_3
                || _state == state::FLAGS_2
                || _state == state::EXTENDED_FLAGS
                || _state == state::CLUSTERING_ROW
                || _state == state::CK_BLOCK_HEADER
                || _state == state::CK_BLOCK_END
                || _state == state::ROW_BODY_TIMESTAMP_DELTIME
                || _state == state::ROW_BODY_DELETION_3
                || _state == state::ROW_BODY_MISSING_COLUMNS_2
                || _state == state::ROW_BODY_MISSING_COLUMNS_READ_COLUMNS_2
                || _state == state::COLUMN
                || _state == state::NEXT_COLUMN
                || _state == state::COLUMN_TIMESTAMP
                || _state == state::COLUMN_DELETION_TIME_2
                || _state == state::COLUMN_TTL_2
                || _state == state::COLUMN_END);
    }

    data_consumer::processing_result process_state(temporary_buffer<char>& data) {
        try {
            return do_process_state(data);
        } catch (malformed_sstable_exception& exp) {
            throw malformed_sstable_exception(exp.what(), _sst->get_filename());
        }
    }
private:
    data_consumer::processing_result do_process_state(temporary_buffer<char>& data) {
        switch (_state) {
        case state::PARTITION_START:
        partition_start_label:
            _is_first_unfiltered = true;
            if (read_short_length_bytes(data, _pk) != read_status::ready) {
                _state = state::DELETION_TIME;
                break;
            }
        case state::DELETION_TIME:
            if (read_32(data) != read_status::ready) {
                _state = state::DELETION_TIME_2;
                break;
            }
        case state::DELETION_TIME_2:
            if (read_64(data) != read_status::ready) {
                _state = state::DELETION_TIME_3;
                break;
            }
        case state::DELETION_TIME_3: {
            deletion_time del;
            del.local_deletion_time = _u32;
            del.marked_for_delete_at = _u64;
            auto ret = _consumer.consume_partition_start(key_view(to_bytes_view(_pk)), del);
            // after calling the consume function, we can release the
            // buffers we held for it.
            _pk.release();
            _state = state::FLAGS;
            if (ret == consumer_m::proceed::no) {
                return consumer_m::proceed::no;
            }
        }
        case state::FLAGS:
        flags_label:
            _liveness = {};
            _row_tombstone = {};
            _row_shadowable_tombstone = {};
            if (read_8(data) != read_status::ready) {
                _state = state::FLAGS_2;
                break;
            }
        case state::FLAGS_2:
            _flags = unfiltered_flags_m(_u8);
            if (_flags.is_end_of_partition()) {
                _state = state::PARTITION_START;
                if (_consumer.consume_partition_end() == consumer_m::proceed::no) {
                    return consumer_m::proceed::no;
                }
                goto partition_start_label;
            } else if (_flags.is_range_tombstone()) {
                _state = state::RANGE_TOMBSTONE_MARKER;
                goto range_tombstone_marker_label;
            } else if (!_flags.has_extended_flags()) {
                _extended_flags = unfiltered_extended_flags_m(uint8_t{0u});
                _state = state::CLUSTERING_ROW;
                start_row(_regular_row);
                _ck_size = _column_translation.clustering_column_value_fix_legths().size();
                goto clustering_row_label;
            }
            if (read_8(data) != read_status::ready) {
                _state = state::EXTENDED_FLAGS;
                break;
            }
        case state::EXTENDED_FLAGS:
            _extended_flags = unfiltered_extended_flags_m(_u8);
            if (_extended_flags.has_cassandra_shadowable_deletion()) {
                throw std::runtime_error("SSTables with Cassandra-style shadowable deletion cannot be read by Scylla");
            }
            if (_extended_flags.is_static()) {
                if (_is_first_unfiltered) {
                    start_row(_static_row);
                    _is_first_unfiltered = false;
                    goto row_body_label;
                } else {
                    throw malformed_sstable_exception("static row should be a first unfiltered in a partition");
                }
            }
            start_row(_regular_row);
            _ck_size = _column_translation.clustering_column_value_fix_legths().size();
        case state::CLUSTERING_ROW:
        clustering_row_label:
            _is_first_unfiltered = false;
            _null_component_occured = false;
            setup_ck(_column_translation.clustering_column_value_fix_legths());
        case state::CK_BLOCK:
        ck_block_label:
            if (no_more_ck_blocks()) {
                if (_reading_range_tombstone_ck) {
                    goto range_tombstone_consume_ck_label;
                } else {
                    goto row_body_label;
                }
            }
            if (!should_read_block_header()) {
                _state = state::CK_BLOCK2;
                goto ck_block2_label;
            }
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::CK_BLOCK_HEADER;
                break;
            }
        case state::CK_BLOCK_HEADER:
            _ck_blocks_header = _u64;
        case state::CK_BLOCK2:
        ck_block2_label: {
            if (is_block_null()) {
                _null_component_occured = true;
                move_to_next_ck_block();
                goto ck_block_label;
            }
            if (_null_component_occured) {
                throw malformed_sstable_exception("non-null component after null component");
            }
            if (is_block_empty()) {
                _row_key.push_back({});
                move_to_next_ck_block();
                goto ck_block_label;
            }
            read_status status = read_status::waiting;
            if (auto len = get_ck_block_value_length()) {
                status = read_bytes(data, *len, _column_value);
            } else {
                status = read_unsigned_vint_length_bytes(data, _column_value);
            }
            if (status != read_status::ready) {
                _state = state::CK_BLOCK_END;
                break;
            }
        }
        case state::CK_BLOCK_END:
            _row_key.push_back(std::move(_column_value));
            move_to_next_ck_block();
            _state = state::CK_BLOCK;
            goto ck_block_label;
        case state::ROW_BODY:
        row_body_label:
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::ROW_BODY_SIZE;
                break;
            }
        case state::ROW_BODY_SIZE:
            _next_row_offset = position() - data.size() + _u64;
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::ROW_BODY_PREV_SIZE;
                break;
            }
        case state::ROW_BODY_PREV_SIZE:
          {
            // Ignore the result
            consumer_m::row_processing_result ret = _extended_flags.is_static()
                ? _consumer.consume_static_row_start()
                : _consumer.consume_row_start(_row_key);

            if (ret == consumer_m::row_processing_result::retry_later) {
                _state = state::ROW_BODY_PREV_SIZE;
                return consumer_m::proceed::no;
            } else if (ret == consumer_m::row_processing_result::skip_row) {
                _state = state::FLAGS;
                auto current_pos = position() - data.size();
                return skip(data, _next_row_offset - current_pos);
            }

            if (_extended_flags.is_static()) {
                if (_flags.has_timestamp() || _flags.has_ttl() || _flags.has_deletion()) {
                    throw malformed_sstable_exception(format("Static row has unexpected flags: timestamp={}, ttl={}, deletion={}",
                        _flags.has_timestamp(), _flags.has_ttl(), _flags.has_deletion()));
                }
                goto row_body_missing_columns_label;
            }
            if (!_flags.has_timestamp()) {
                _state = state::ROW_BODY_DELETION;
                goto row_body_deletion_label;
            }
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::ROW_BODY_TIMESTAMP;
                break;
            }
          }
        case state::ROW_BODY_TIMESTAMP:
            _liveness.set_timestamp(parse_timestamp(_header, _u64));
            if (!_flags.has_ttl()) {
                _state = state::ROW_BODY_DELETION;
                goto row_body_deletion_label;
            }
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::ROW_BODY_TIMESTAMP_TTL;
                break;
            }
        case state::ROW_BODY_TIMESTAMP_TTL:
            _liveness.set_ttl(parse_ttl(_header, _u64));
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::ROW_BODY_TIMESTAMP_DELTIME;
                break;
            }
        case state::ROW_BODY_TIMESTAMP_DELTIME:
            _liveness.set_local_deletion_time(parse_expiry(_header, _u64));
        case state::ROW_BODY_DELETION:
        row_body_deletion_label:
            if (!_flags.has_deletion()) {
                _state = state::ROW_BODY_SHADOWABLE_DELETION;
                goto row_body_shadowable_deletion_label;
            }
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::ROW_BODY_DELETION_2;
                break;
            }
        case state::ROW_BODY_DELETION_2:
            _row_tombstone.timestamp = parse_timestamp(_header, _u64);
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::ROW_BODY_DELETION_3;
                break;
            }
        case state::ROW_BODY_DELETION_3:
            _row_tombstone.deletion_time = parse_expiry(_header, _u64);
        case state::ROW_BODY_SHADOWABLE_DELETION:
        row_body_shadowable_deletion_label:
            if (_extended_flags.has_scylla_shadowable_deletion()) {
                if (!_has_shadowable_tombstones) {
                    throw malformed_sstable_exception("Scylla shadowable tombstone flag is set but not supported on this SSTables");
                }
            } else {
                _state = state::ROW_BODY_MARKER;
                goto row_body_marker_label;
            }
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::ROW_BODY_SHADOWABLE_DELETION_2;
                break;
            }
        case state::ROW_BODY_SHADOWABLE_DELETION_2:
            _row_shadowable_tombstone.timestamp = parse_timestamp(_header, _u64);
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::ROW_BODY_SHADOWABLE_DELETION_3;
                break;
            }
        case state::ROW_BODY_SHADOWABLE_DELETION_3:
            _row_shadowable_tombstone.deletion_time = parse_expiry(_header, _u64);
        case state::ROW_BODY_MARKER:
        row_body_marker_label:
            if (_consumer.consume_row_marker_and_tombstone(
                    _liveness, std::move(_row_tombstone), std::move(_row_shadowable_tombstone)) == consumer_m::proceed::no) {
                _state = state::ROW_BODY_MISSING_COLUMNS;
                break;
            }
        case state::ROW_BODY_MISSING_COLUMNS:
        row_body_missing_columns_label:
            if (!_flags.has_all_columns()) {
                if (read_unsigned_vint(data) != read_status::ready) {
                    _state = state::ROW_BODY_MISSING_COLUMNS_2;
                    break;
                }
                goto row_body_missing_columns_2_label;
            } else {
                _row->_columns_selector.set();
            }
        case state::COLUMN:
        column_label:
            if (_subcolumns_to_read == 0) {
                if (no_more_columns()) {
                    _state = state::FLAGS;
                    if (_consumer.consume_row_end() == consumer_m::proceed::no) {
                        return consumer_m::proceed::no;
                    }
                    goto flags_label;
                }
                if (!is_column_simple()) {
                    _state = state::COMPLEX_COLUMN;
                    goto complex_column_label;
                }
                _subcolumns_to_read = 0;
            }
        case state::SIMPLE_COLUMN:
            if (read_8(data) != read_status::ready) {
                _state = state::COLUMN_FLAGS;
                break;
            }
        case state::COLUMN_FLAGS:
            _column_flags = column_flags_m(_u8);

            if (_column_flags.use_row_timestamp()) {
                _column_timestamp = _liveness.timestamp();
                _state = state::COLUMN_DELETION_TIME;
                goto column_deletion_time_label;
            }
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::COLUMN_TIMESTAMP;
                break;
            }
        case state::COLUMN_TIMESTAMP:
            _column_timestamp = parse_timestamp(_header, _u64);
        case state::COLUMN_DELETION_TIME:
        column_deletion_time_label:
            if (_column_flags.use_row_ttl()) {
                _column_local_deletion_time = _liveness.local_deletion_time();
                _state = state::COLUMN_TTL;
                goto column_ttl_label;
            } else if (!_column_flags.is_deleted() && ! _column_flags.is_expiring()) {
                _column_local_deletion_time = gc_clock::time_point::max();
                _state = state::COLUMN_TTL;
                goto column_ttl_label;
            }
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::COLUMN_DELETION_TIME_2;
                break;
            }
        case state::COLUMN_DELETION_TIME_2:
            _column_local_deletion_time = parse_expiry(_header, _u64);
        case state::COLUMN_TTL:
        column_ttl_label:
            if (_column_flags.use_row_ttl()) {
                _column_ttl = _liveness.ttl();
                _state = state::COLUMN_VALUE;
                goto column_cell_path_label;
            } else if (!_column_flags.is_expiring()) {
                _column_ttl = gc_clock::duration::zero();
                _state = state::COLUMN_VALUE;
                goto column_cell_path_label;
            }
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::COLUMN_TTL_2;
                break;
            }
        case state::COLUMN_TTL_2:
            _column_ttl = parse_ttl(_header, _u64);
        case state::COLUMN_CELL_PATH:
        column_cell_path_label:
            if (!is_column_simple()) {
                if (read_unsigned_vint_length_bytes_contiguous(data, _cell_path) != read_status::ready) {
                    _state = state::COLUMN_VALUE;
                    break;
                }
            } else {
                _cell_path = temporary_buffer<char>(0);
            }
        case state::COLUMN_VALUE:
        {
            if (!_column_flags.has_value()) {
                _column_value = fragmented_temporary_buffer();
                _state = state::COLUMN_END;
                goto column_end_label;
            }
            read_status status = read_status::waiting;
            if (auto len = get_column_value_length()) {
                status = read_bytes(data, *len, _column_value);
            } else {
                status = read_unsigned_vint_length_bytes(data, _column_value);
            }
            if (status != read_status::ready) {
                _state = state::COLUMN_END;
                break;
            }
        }
        case state::COLUMN_END:
        column_end_label:
            _state = state::NEXT_COLUMN;
            if (is_column_counter() && !_column_flags.is_deleted()) {
                if (_consumer.consume_counter_column(get_column_info(),
                                                     fragmented_temporary_buffer::view(_column_value),
                                                     _column_timestamp) == consumer_m::proceed::no) {
                    return consumer_m::proceed::no;
                }
            } else {
                if (_consumer.consume_column(get_column_info(),
                                             to_bytes_view(_cell_path),
                                             fragmented_temporary_buffer::view(_column_value),
                                             _column_timestamp,
                                             _column_ttl,
                                             _column_local_deletion_time,
                                             _column_flags.is_deleted()) == consumer_m::proceed::no) {
                    return consumer_m::proceed::no;
                }
            }
        case state::NEXT_COLUMN:
            if (!is_column_simple()) {
                --_subcolumns_to_read;
                if (_subcolumns_to_read == 0) {
                    const sstables::column_translation::column_info& column_info = get_column_info();
                    move_to_next_column();
                    if (_consumer.consume_complex_column_end(column_info) != consumer_m::proceed::yes) {
                        _state = state::COLUMN;
                        return consumer_m::proceed::no;
                    }
                }
            } else {
                move_to_next_column();
            }
            goto column_label;
        case state::ROW_BODY_MISSING_COLUMNS_2:
        row_body_missing_columns_2_label: {
            uint64_t missing_column_bitmap_or_count = _u64;
            if (_row->_columns.size() < 64) {
                _row->_columns_selector.clear();
                _row->_columns_selector.append(missing_column_bitmap_or_count);
                _row->_columns_selector.flip();
                _row->_columns_selector.resize(_row->_columns.size());
                skip_absent_columns();
                goto column_label;
            }
            _row->_columns_selector.resize(_row->_columns.size());
            if (_row->_columns.size() - missing_column_bitmap_or_count < _row->_columns.size() / 2) {
                _missing_columns_to_read = _row->_columns.size() - missing_column_bitmap_or_count;
                _row->_columns_selector.reset();
            } else {
                _missing_columns_to_read = missing_column_bitmap_or_count;
                _row->_columns_selector.set();
            }
            goto row_body_missing_columns_read_columns_label;
        }
        case state::ROW_BODY_MISSING_COLUMNS_READ_COLUMNS:
        row_body_missing_columns_read_columns_label:
            if (_missing_columns_to_read == 0) {
                skip_absent_columns();
                goto column_label;
            }
            --_missing_columns_to_read;
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::ROW_BODY_MISSING_COLUMNS_READ_COLUMNS_2;
                break;
            }
        case state::ROW_BODY_MISSING_COLUMNS_READ_COLUMNS_2:
            _row->_columns_selector.flip(_u64);
            goto row_body_missing_columns_read_columns_label;
        case state::COMPLEX_COLUMN:
        complex_column_label:
            if (!_flags.has_complex_deletion()) {
                _complex_column_tombstone = {};
                goto complex_column_2_label;
            }
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::COMPLEX_COLUMN_MARKED_FOR_DELETE;
                break;
            }
        case state::COMPLEX_COLUMN_MARKED_FOR_DELETE:
            _complex_column_marked_for_delete = parse_timestamp(_header, _u64);
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::COMPLEX_COLUMN_LOCAL_DELETION_TIME;
                break;
            }
        case state::COMPLEX_COLUMN_LOCAL_DELETION_TIME:
            _complex_column_tombstone = {_complex_column_marked_for_delete, parse_expiry(_header, _u64)};
        case state::COMPLEX_COLUMN_2:
        complex_column_2_label:
            if (_consumer.consume_complex_column_start(get_column_info(), _complex_column_tombstone) == consumer_m::proceed::no) {
                _state = state::COMPLEX_COLUMN_SIZE;
                return consumer_m::proceed::no;
            }
        case state::COMPLEX_COLUMN_SIZE:
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::COMPLEX_COLUMN_SIZE_2;
                break;
            }
        case state::COMPLEX_COLUMN_SIZE_2:
            _subcolumns_to_read = _u64;
            if (_subcolumns_to_read == 0) {
                const sstables::column_translation::column_info& column_info = get_column_info();
                move_to_next_column();
                if (_consumer.consume_complex_column_end(column_info) != consumer_m::proceed::yes) {
                    _state = state::COLUMN;
                    return consumer_m::proceed::no;
                }
            }
            goto column_label;
        case state::RANGE_TOMBSTONE_MARKER:
        range_tombstone_marker_label:
            _is_first_unfiltered = false;
            if (read_8(data) != read_status::ready) {
                _state = state::RANGE_TOMBSTONE_KIND;
                break;
            }
        case state::RANGE_TOMBSTONE_KIND:
            _range_tombstone_kind = bound_kind_m(_u8);
            if (read_16(data) != read_status::ready) {
                _state = state::RANGE_TOMBSTONE_SIZE;
                break;
            }
        case state::RANGE_TOMBSTONE_SIZE:
            _ck_size = _u16;
            if (_ck_size == 0) {
                _row_key.clear();
                _range_tombstone_kind = is_start(_range_tombstone_kind)
                        ? bound_kind_m::incl_start : bound_kind_m::incl_end;
                goto range_tombstone_body_label;
            } else {
                _reading_range_tombstone_ck = true;
                goto clustering_row_label;
            }
            assert(0);
        case state::RANGE_TOMBSTONE_CONSUME_CK:
        range_tombstone_consume_ck_label:
            _reading_range_tombstone_ck = false;
        case state::RANGE_TOMBSTONE_BODY:
        range_tombstone_body_label:
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::RANGE_TOMBSTONE_BODY_SIZE;
                break;
            }
        case state::RANGE_TOMBSTONE_BODY_SIZE:
            // Ignore result
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::RANGE_TOMBSTONE_BODY_PREV_SIZE;
                break;
            }
        case state::RANGE_TOMBSTONE_BODY_PREV_SIZE:
            // Ignore result
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::RANGE_TOMBSTONE_BODY_TIMESTAMP;
                break;
            }
        case state::RANGE_TOMBSTONE_BODY_TIMESTAMP:
            _left_range_tombstone.timestamp = parse_timestamp(_header, _u64);
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::RANGE_TOMBSTONE_BODY_LOCAL_DELTIME;
                break;
            }
        case state::RANGE_TOMBSTONE_BODY_LOCAL_DELTIME:
            _left_range_tombstone.deletion_time = parse_expiry(_header, _u64);
            if (!is_boundary_between_adjacent_intervals(_range_tombstone_kind)) {
                if (!is_bound_kind(_range_tombstone_kind)) {
                    throw sstables::malformed_sstable_exception(
                        format("Corrupted range tombstone: invalid boundary type {}", _range_tombstone_kind));
                }
                if (_consumer.consume_range_tombstone(_row_key,
                                                      to_bound_kind(_range_tombstone_kind),
                                                      _left_range_tombstone) == consumer_m::proceed::no) {
                    _row_key.clear();
                    _state = state::FLAGS;
                    return consumer_m::proceed::no;
                }
                _row_key.clear();
                goto flags_label;
            }
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::RANGE_TOMBSTONE_BODY_TIMESTAMP2;
                break;
            }
        case state::RANGE_TOMBSTONE_BODY_TIMESTAMP2:
            _right_range_tombstone.timestamp = parse_timestamp(_header, _u64);
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::RANGE_TOMBSTONE_BODY_LOCAL_DELTIME2;
                break;
            }
        case state::RANGE_TOMBSTONE_BODY_LOCAL_DELTIME2:
            _right_range_tombstone.deletion_time = parse_expiry(_header, _u64);
            if (_consumer.consume_range_tombstone(_row_key,
                                                  _range_tombstone_kind,
                                                  _left_range_tombstone,
                                                  _right_range_tombstone) == consumer_m::proceed::no) {
                _row_key.clear();
                _state = state::FLAGS;
                return consumer_m::proceed::no;
            }
            _row_key.clear();
            goto flags_label;
        }

        return data_consumer::proceed::yes;
    }
public:

    data_consume_rows_context_m(const schema& s,
                                const shared_sstable& sst,
                                consumer_m& consumer,
                                input_stream<char> && input,
                                uint64_t start,
                                uint64_t maxlen)
        : continuous_data_consumer(consumer.permit(), std::move(input), start, maxlen)
        , _consumer(consumer)
        , _sst(sst)
        , _header(sst->get_serialization_header())
        , _column_translation(sst->get_column_translation(s, _header, sst->features()))
        , _has_shadowable_tombstones(sst->has_shadowable_tombstones())
    {
        setup_columns(_regular_row, _column_translation.regular_columns());
        setup_columns(_static_row, _column_translation.static_columns());
    }

    void verify_end_state() {
        // If reading a partial row (i.e., when we have a clustering row
        // filter and using a promoted index), we may be in FLAGS or FLAGS_2
        // state instead of PARTITION_START.
        if (_state == state::FLAGS || _state == state::FLAGS_2) {
            _consumer.on_end_of_stream();
            return;
        }

        // We may end up in state::DELETION_TIME after consuming last partition's end marker
        // and proceeding to attempt to parse the next partition, since state::DELETION_TIME
        // is the first state corresponding to the contents of a new partition.
        if (_state != state::DELETION_TIME
                && (_state != state::PARTITION_START || primitive_consumer::active())) {
            throw malformed_sstable_exception("end of input, but not end of partition");
        }
    }

    void reset(indexable_element el) {
        auto reset_to_state = [this, el] (state s) {
            _state = s;
            _consumer.reset(el);
        };
        switch (el) {
            case indexable_element::partition:
                return reset_to_state(state::PARTITION_START);
            case indexable_element::cell:
                return reset_to_state(state::FLAGS);
        }
        // We should not get here unless some enum member is not handled by the switch
        throw std::logic_error(format("Unable to reset - unknown indexable element: {}", el));
    }

    reader_permit& permit() {
        return _consumer.permit();
    }
};

class mp_row_consumer_m : public consumer_m {
    mp_row_consumer_reader* _reader;
    schema_ptr _schema;
    const query::partition_slice& _slice;
    std::optional<mutation_fragment_filter> _mf_filter;

    bool _is_mutation_end = true;
    streamed_mutation::forwarding _fwd;
    // For static-compact tables C* stores the only row in the static row but in our representation they're regular rows.
    const bool _treat_static_row_as_regular;

    std::optional<clustering_row> _in_progress_row;
    std::optional<range_tombstone> _stored_tombstone;
    static_row _in_progress_static_row;
    bool _inside_static_row = false;

    struct cell {
        column_id id;
        atomic_cell_or_collection val;
    };
    std::vector<cell> _cells;
    collection_mutation_description _cm;

    struct range_tombstone_start {
        clustering_key_prefix ck;
        bound_kind kind;
        tombstone tomb;

        position_in_partition_view position() const {
            return position_in_partition_view(position_in_partition_view::range_tag_t{}, bound_view(ck, kind));
        }
    };

    inline friend std::ostream& operator<<(std::ostream& o, const mp_row_consumer_m::range_tombstone_start& rt_start) {
        o << "{ clustering: " << rt_start.ck
          << ", kind: " << rt_start.kind
          << ", tombstone: " << rt_start.tomb << " }";
        return o;
    }

    std::optional<range_tombstone_start> _opened_range_tombstone;

    void consume_range_tombstone_start(clustering_key_prefix ck, bound_kind k, tombstone t) {
        sstlog.trace("mp_row_consumer_m {}: consume_range_tombstone_start(ck={}, k={}, t={})", fmt::ptr(this), ck, k, t);
        if (_opened_range_tombstone) {
            throw sstables::malformed_sstable_exception(
                    format("Range tombstones have to be disjoint: current opened range tombstone {}, new tombstone {}",
                           *_opened_range_tombstone, t));
        }
        _opened_range_tombstone = {std::move(ck), k, std::move(t)};
    }

    proceed consume_range_tombstone_end(clustering_key_prefix ck, bound_kind k, tombstone t) {
        sstlog.trace("mp_row_consumer_m {}: consume_range_tombstone_end(ck={}, k={}, t={})", fmt::ptr(this), ck, k, t);
        if (!_opened_range_tombstone) {
            throw sstables::malformed_sstable_exception(
                    format("Closing range tombstone that wasn't opened: clustering {}, kind {}, tombstone {}",
                           ck, k, t));
        }
        if (_opened_range_tombstone->tomb != t) {
            throw sstables::malformed_sstable_exception(
                    format("Range tombstone with ck {} and two different tombstones at ends: {}, {}",
                           ck, _opened_range_tombstone->tomb, t));
        }


        auto rt = range_tombstone {std::move(_opened_range_tombstone->ck),
                            _opened_range_tombstone->kind,
                            std::move(ck),
                            k,
                            std::move(t)};
        _opened_range_tombstone.reset();
        return maybe_push_range_tombstone(std::move(rt));
    }

    const column_definition& get_column_definition(std::optional<column_id> column_id) const {
        auto column_type = _inside_static_row ? column_kind::static_column : column_kind::regular_column;
        return _schema->column_at(column_type, *column_id);
    }

    inline proceed maybe_push_range_tombstone(range_tombstone&& rt) {
        const auto action = _mf_filter->apply(rt);
        switch (action) {
        case mutation_fragment_filter::result::emit:
            _reader->push_mutation_fragment(mutation_fragment(*_schema, permit(), std::move(rt)));
            break;
        case mutation_fragment_filter::result::ignore:
            if (_mf_filter->out_of_range()) {
                _reader->on_out_of_clustering_range();
                return proceed::no;
            }
            if (_mf_filter->is_current_range_changed()) {
                return proceed::no;
            }
            break;
        case mutation_fragment_filter::result::store_and_finish:
            _stored_tombstone = std::move(rt);
            _reader->on_out_of_clustering_range();
            return proceed::no;
        }

        return proceed(!_reader->is_buffer_full() && !need_preempt());
    }

    inline void reset_for_new_partition() {
        _is_mutation_end = true;
        _in_progress_row.reset();
        _stored_tombstone.reset();
        _mf_filter.reset();
        _opened_range_tombstone.reset();
    }

    void check_schema_mismatch(const column_translation::column_info& column_info, const column_definition& column_def) const {
        if (column_info.schema_mismatch) {
            throw malformed_sstable_exception(
                    format("{} definition in serialization header does not match schema. Expected {} but got {}",
                        column_def.name_as_text(),
                        column_def.type->name(),
                        column_info.type->name()));
        }
    }

    void check_column_missing_in_current_schema(const column_translation::column_info& column_info,
                                                api::timestamp_type timestamp) const {
        if (!column_info.id) {
            sstring name = sstring(to_sstring_view(*column_info.name));
            auto it = _schema->dropped_columns().find(name);
            if (it == _schema->dropped_columns().end() || timestamp > it->second.timestamp) {
                throw malformed_sstable_exception(format("Column {} missing in current schema", name));
            }
        }
    }

public:

    /*
     * In m format, RTs are represented as separate start and end bounds,
     * so setting/resetting RT start is needed so that we could skip using index.
     * For this, the following methods need to be defined:
     *
     * void set_range_tombstone_start(clustering_key_prefix, bound_kind, tombstone);
     * void reset_range_tombstone_start();
     */
    constexpr static bool is_setting_range_tombstone_start_supported = true;

    mp_row_consumer_m(mp_row_consumer_reader* reader,
                        const schema_ptr schema,
                        reader_permit permit,
                        const query::partition_slice& slice,
                        const io_priority_class& pc,
                        tracing::trace_state_ptr trace_state,
                        streamed_mutation::forwarding fwd,
                        const shared_sstable& sst)
        : consumer_m(std::move(permit), std::move(trace_state), pc)
        , _reader(reader)
        , _schema(schema)
        , _slice(slice)
        , _fwd(fwd)
        , _treat_static_row_as_regular(_schema->is_static_compact_table()
            && (!sst->has_scylla_component() || sst->features().is_enabled(sstable_feature::CorrectStaticCompact))) // See #4139
    {
        _cells.reserve(std::max(_schema->static_columns_count(), _schema->regular_columns_count()));
    }

    mp_row_consumer_m(mp_row_consumer_reader* reader,
                        const schema_ptr schema,
                        reader_permit permit,
                        const io_priority_class& pc,
                        tracing::trace_state_ptr trace_state,
                        streamed_mutation::forwarding fwd,
                        const shared_sstable& sst)
    : mp_row_consumer_m(reader, schema, std::move(permit), schema->full_slice(), pc, std::move(trace_state), fwd, sst)
    { }

    virtual ~mp_row_consumer_m() {}

    // See the RowConsumer concept
    void push_ready_fragments() {
        auto maybe_push = [this] (auto&& mfopt) {
            if (mfopt) {
                assert(_mf_filter);
                switch (_mf_filter->apply(*mfopt)) {
                case mutation_fragment_filter::result::emit:
                    _reader->push_mutation_fragment(mutation_fragment(*_schema, permit(), *std::exchange(mfopt, {})));
                    break;
                case mutation_fragment_filter::result::ignore:
                    mfopt.reset();
                    break;
                case mutation_fragment_filter::result::store_and_finish:
                    _reader->on_out_of_clustering_range();
                    break;
                }
            }
        };

        maybe_push(_stored_tombstone);
    }

    std::optional<position_in_partition_view> maybe_skip() {
        if (!_mf_filter) {
            return {};
        }
        return _mf_filter->maybe_skip();
    }

    bool is_mutation_end() const {
        return _is_mutation_end;
    }

    void setup_for_partition(const partition_key& pk) {
        sstlog.trace("mp_row_consumer_m {}: setup_for_partition({})", fmt::ptr(this), pk);
        _is_mutation_end = false;
        _mf_filter.emplace(*_schema, _slice, pk, _fwd);
    }

    std::optional<position_in_partition_view> fast_forward_to(position_range r, db::timeout_clock::time_point) {
        if (!_mf_filter) {
            _reader->on_out_of_clustering_range();
            return {};
        }
        auto skip = _mf_filter->fast_forward_to(std::move(r));
        if (skip) {
            position_in_partition::less_compare less(*_schema);
            // No need to skip using index if stored fragments are after the start of the range
            if (_in_progress_row && !less(_in_progress_row->position(), *skip)) {
                return {};
            }
            if (_stored_tombstone && !less(_stored_tombstone->position(), *skip)) {
                return {};
            }
        }
        if (_mf_filter->out_of_range()) {
            _reader->on_out_of_clustering_range();
        }
        return skip;
    }

    /*
     * Sets the range tombstone start. Overwrites the currently set RT start if any.
     * Used for skipping through wide partitions using index when the data block
     * skipped to starts in the middle of an opened range tombstone.
     */
    void set_range_tombstone_start(clustering_key_prefix ck, bound_kind k, tombstone t) {
        _opened_range_tombstone = {std::move(ck), k, std::move(t)};
    }

    /*
     * Resets the previously set range tombstone start if any.
     */
    void reset_range_tombstone_start() {
        _opened_range_tombstone.reset();
    }

    virtual proceed consume_partition_start(sstables::key_view key, sstables::deletion_time deltime) override {
        sstlog.trace("mp_row_consumer_m {}: consume_partition_start(deltime=({}, {})), _is_mutation_end={}", fmt::ptr(this),
            deltime.local_deletion_time, deltime.marked_for_delete_at, _is_mutation_end);
        if (!_is_mutation_end) {
            return proceed::yes;
        }
        auto pk = partition_key::from_exploded(key.explode(*_schema));
        setup_for_partition(pk);
        auto dk = dht::decorate_key(*_schema, pk);
        _reader->on_next_partition(std::move(dk), tombstone(deltime));
        return proceed(!_reader->is_buffer_full() && !need_preempt());
    }

    virtual consumer_m::row_processing_result consume_row_start(const std::vector<fragmented_temporary_buffer>& ecp) override {
        auto key = clustering_key_prefix::from_range(ecp | boost::adaptors::transformed(
            [] (const fragmented_temporary_buffer& b) { return fragmented_temporary_buffer::view(b); }));

        sstlog.trace("mp_row_consumer_m {}: consume_row_start({})", fmt::ptr(this), key);

        // enagaged _in_progress_row means we have already split around this key.
        if (_opened_range_tombstone && !_in_progress_row) {
            // We have an opened range tombstone which means that the current row is spanned by that RT.
            auto ck = key;
            bool was_non_full_key = clustering_key::make_full(*_schema, ck);
            auto end_kind = was_non_full_key ? bound_kind::excl_end : bound_kind::incl_end;
            assert(!_stored_tombstone);
            auto rt = range_tombstone(std::move(_opened_range_tombstone->ck),
                _opened_range_tombstone->kind,
                ck,
                end_kind,
                _opened_range_tombstone->tomb);
            sstlog.trace("mp_row_consumer_m {}: push({})", fmt::ptr(this), rt);
            _opened_range_tombstone->ck = std::move(ck);
            _opened_range_tombstone->kind = was_non_full_key ? bound_kind::incl_start : bound_kind::excl_start;

            if (maybe_push_range_tombstone(std::move(rt)) == proceed::no) {
                _in_progress_row.emplace(std::move(key));
                return consumer_m::row_processing_result::retry_later;
            }
        }

        _in_progress_row.emplace(std::move(key));

        switch (_mf_filter->apply(_in_progress_row->position())) {
        case mutation_fragment_filter::result::emit:
            sstlog.trace("mp_row_consumer_m {}: emit", fmt::ptr(this));
            return consumer_m::row_processing_result::do_proceed;
        case mutation_fragment_filter::result::ignore:
            sstlog.trace("mp_row_consumer_m {}: ignore", fmt::ptr(this));
            if (_mf_filter->out_of_range()) {
                _reader->on_out_of_clustering_range();
                // We actually want skip_later, which doesn't exist, but retry_later
                // is ok because signalling out-of-range on the reader will cause it
                // to either stop reading or skip to the next partition using index,
                // not by ignoring fragments.
                return consumer_m::row_processing_result::retry_later;
            }
            if (_mf_filter->is_current_range_changed()) {
                return consumer_m::row_processing_result::retry_later;
            } else {
                _in_progress_row.reset();
                return consumer_m::row_processing_result::skip_row;
            }
        case mutation_fragment_filter::result::store_and_finish:
            sstlog.trace("mp_row_consumer_m {}: store_and_finish", fmt::ptr(this));
            _reader->on_out_of_clustering_range();
            return consumer_m::row_processing_result::retry_later;
        }
        abort();
    }

    virtual proceed consume_row_marker_and_tombstone(
            const liveness_info& info, tombstone tomb, tombstone shadowable_tomb) override {
        sstlog.trace("mp_row_consumer_m {}: consume_row_marker_and_tombstone({}, {}, {}), key={}",
            fmt::ptr(this), info.to_row_marker(), tomb, shadowable_tomb, _in_progress_row->position());
        _in_progress_row->apply(info.to_row_marker());
        _in_progress_row->apply(tomb);
        if (shadowable_tomb) {
            _in_progress_row->apply(shadowable_tombstone{shadowable_tomb});
        }
        return proceed::yes;
    }

    virtual consumer_m::row_processing_result consume_static_row_start() override {
        sstlog.trace("mp_row_consumer_m {}: consume_static_row_start()", fmt::ptr(this));
        if (_treat_static_row_as_regular) {
            return consume_row_start({});
        }
        _inside_static_row = true;
        _in_progress_static_row = static_row();
        return consumer_m::row_processing_result::do_proceed;
    }

    virtual proceed consume_column(const column_translation::column_info& column_info,
                                   bytes_view cell_path,
                                   fragmented_temporary_buffer::view value,
                                   api::timestamp_type timestamp,
                                   gc_clock::duration ttl,
                                   gc_clock::time_point local_deletion_time,
                                   bool is_deleted) override {
        const std::optional<column_id>& column_id = column_info.id;
        sstlog.trace("mp_row_consumer_m {}: consume_column(id={}, path={}, value={}, ts={}, ttl={}, del_time={}, deleted={})", fmt::ptr(this),
            column_id, fmt_hex(cell_path), value, timestamp, ttl.count(), local_deletion_time.time_since_epoch().count(), is_deleted);
        check_column_missing_in_current_schema(column_info, timestamp);
        if (!column_id) {
            return proceed::yes;
        }
        const column_definition& column_def = get_column_definition(column_id);
        if (timestamp <= column_def.dropped_at()) {
            return proceed::yes;
        }
        check_schema_mismatch(column_info, column_def);
        if (column_def.is_multi_cell()) {
            auto& value_type = visit(*column_def.type, make_visitor(
                [] (const collection_type_impl& ctype) -> const abstract_type& { return *ctype.value_comparator(); },
                [&] (const user_type_impl& utype) -> const abstract_type& {
                    if (cell_path.size() != sizeof(int16_t)) {
                        throw malformed_sstable_exception(format("wrong size of field index while reading UDT column: expected {}, got {}",
                                    sizeof(int16_t), cell_path.size()));
                    }

                    auto field_idx = deserialize_field_index(cell_path);
                    if (field_idx >= utype.size()) {
                        throw malformed_sstable_exception(format("field index too big while reading UDT column: type has {} fields, got {}",
                                    utype.size(), field_idx));
                    }

                    return *utype.type(field_idx);
                },
                [] (const abstract_type& o) -> const abstract_type& {
                    throw malformed_sstable_exception(format("attempted to read multi-cell column, but expected type was {}", o.name()));
                }
            ));
            auto ac = is_deleted ? atomic_cell::make_dead(timestamp, local_deletion_time)
                                 : make_atomic_cell(value_type,
                                                    timestamp,
                                                    value,
                                                    ttl,
                                                    local_deletion_time,
                                                    atomic_cell::collection_member::yes);
            _cm.cells.emplace_back(to_bytes(cell_path), std::move(ac));
        } else {
            auto ac = is_deleted ? atomic_cell::make_dead(timestamp, local_deletion_time)
                                 : make_atomic_cell(*column_def.type, timestamp, value, ttl, local_deletion_time,
                                       atomic_cell::collection_member::no);
            _cells.push_back({*column_id, atomic_cell_or_collection(std::move(ac))});
        }
        return proceed::yes;
    }

    virtual proceed consume_complex_column_start(const sstables::column_translation::column_info& column_info,
                                                 tombstone tomb) override {
        sstlog.trace("mp_row_consumer_m {}: consume_complex_column_start({}, {})", fmt::ptr(this), column_info.id, tomb);
        _cm.tomb = tomb;
        _cm.cells.clear();
        return proceed::yes;
    }

    virtual proceed consume_complex_column_end(const sstables::column_translation::column_info& column_info) override {
        const std::optional<column_id>& column_id = column_info.id;
        sstlog.trace("mp_row_consumer_m {}: consume_complex_column_end({})", fmt::ptr(this), column_id);
        if (_cm.tomb) {
            check_column_missing_in_current_schema(column_info, _cm.tomb.timestamp);
        }
        if (column_id) {
            const column_definition& column_def = get_column_definition(column_id);
            if (!_cm.cells.empty() || (_cm.tomb && _cm.tomb.timestamp > column_def.dropped_at())) {
                check_schema_mismatch(column_info, column_def);
                _cells.push_back({column_def.id, _cm.serialize(*column_def.type)});
            }
        }
        _cm.tomb = {};
        _cm.cells.clear();
        return proceed::yes;
    }

    virtual proceed consume_counter_column(const column_translation::column_info& column_info,
                                           fragmented_temporary_buffer::view value,
                                           api::timestamp_type timestamp) override {
        const std::optional<column_id>& column_id = column_info.id;
        sstlog.trace("mp_row_consumer_m {}: consume_counter_column({}, {}, {})", fmt::ptr(this), column_id, value, timestamp);
        check_column_missing_in_current_schema(column_info, timestamp);
        if (!column_id) {
            return proceed::yes;
        }
        const column_definition& column_def = get_column_definition(column_id);
        if (timestamp <= column_def.dropped_at()) {
            return proceed::yes;
        }
        check_schema_mismatch(column_info, column_def);
        auto ac = make_counter_cell(timestamp, value);
        _cells.push_back({*column_id, atomic_cell_or_collection(std::move(ac))});
        return proceed::yes;
    }

    virtual proceed consume_range_tombstone(const std::vector<fragmented_temporary_buffer>& ecp,
                                            bound_kind kind,
                                            tombstone tomb) override {
        auto ck = clustering_key_prefix::from_range(ecp | boost::adaptors::transformed(
            [] (const fragmented_temporary_buffer& b) { return fragmented_temporary_buffer::view(b); }));
        if (kind == bound_kind::incl_start || kind == bound_kind::excl_start) {
            consume_range_tombstone_start(std::move(ck), kind, std::move(tomb));
            return proceed(!_reader->is_buffer_full() && !need_preempt());
        } else { // *_end kind
            return consume_range_tombstone_end(std::move(ck), kind, std::move(tomb));
        }
    }

    virtual proceed consume_range_tombstone(const std::vector<fragmented_temporary_buffer>& ecp,
                                            sstables::bound_kind_m kind,
                                            tombstone end_tombstone,
                                            tombstone start_tombstone) override {
        auto result = proceed::yes;
        auto ck = clustering_key_prefix::from_range(ecp | boost::adaptors::transformed(
            [] (const fragmented_temporary_buffer& b) { return fragmented_temporary_buffer::view(b); }));
        switch (kind) {
        case bound_kind_m::incl_end_excl_start:
            result = consume_range_tombstone_end(ck, bound_kind::incl_end, std::move(end_tombstone));
            consume_range_tombstone_start(std::move(ck), bound_kind::excl_start, std::move(start_tombstone));
            break;
        case bound_kind_m::excl_end_incl_start:
            result = consume_range_tombstone_end(ck, bound_kind::excl_end, std::move(end_tombstone));
            consume_range_tombstone_start(std::move(ck), bound_kind::incl_start, std::move(start_tombstone));
            break;
        default:
            assert(false && "Invalid boundary type");
        }

        return result;
    }

    virtual proceed consume_row_end() override {
        auto fill_cells = [this] (column_kind kind, row& cells) {
            for (auto &&c : _cells) {
                cells.apply(_schema->column_at(kind, c.id), std::move(c.val));
            }
            _cells.clear();
        };

        if (_inside_static_row) {
            fill_cells(column_kind::static_column, _in_progress_static_row.cells());
            sstlog.trace("mp_row_consumer_m {}: consume_row_end(_in_progress_static_row={})", fmt::ptr(this), static_row::printer(*_schema, _in_progress_static_row));
            _inside_static_row = false;
            if (!_in_progress_static_row.empty()) {
                auto action = _mf_filter->apply(_in_progress_static_row);
                switch (action) {
                case mutation_fragment_filter::result::emit:
                    _reader->push_mutation_fragment(mutation_fragment(*_schema, permit(), std::move(_in_progress_static_row)));
                    break;
                case mutation_fragment_filter::result::ignore:
                    break;
                case mutation_fragment_filter::result::store_and_finish:
                    // static row is always either emited or ignored.
                    throw runtime_exception("We should never need to store static row");
                }
            }
        } else {
            if (!_cells.empty()) {
                fill_cells(column_kind::regular_column, _in_progress_row->cells());
            }
            _reader->push_mutation_fragment(mutation_fragment(*_schema, permit(), *std::exchange(_in_progress_row, {})));
        }

        return proceed(!_reader->is_buffer_full() && !need_preempt());
    }

    virtual void on_end_of_stream() override {
        sstlog.trace("mp_row_consumer_m {}: on_end_of_stream()", fmt::ptr(this));
        if (_opened_range_tombstone) {
            if (!_mf_filter || _mf_filter->out_of_range()) {
                throw sstables::malformed_sstable_exception("Unclosed range tombstone.");
            }
            auto range_end = _mf_filter->uppermost_bound();
            position_in_partition::less_compare less(*_schema);
            auto start_pos = position_in_partition_view(position_in_partition_view::range_tag_t{},
                                                        bound_view(_opened_range_tombstone->ck, _opened_range_tombstone->kind));
            if (less(start_pos, range_end)) {
                auto end_bound = range_end.is_clustering_row()
                    ? position_in_partition_view::after_key(range_end.key()).as_end_bound_view()
                    : range_end.as_end_bound_view();
                auto rt = range_tombstone {std::move(_opened_range_tombstone->ck),
                                           _opened_range_tombstone->kind,
                                           end_bound.prefix(),
                                           end_bound.kind(),
                                           _opened_range_tombstone->tomb};
                sstlog.trace("mp_row_consumer_m {}: on_end_of_stream(), emitting last tombstone: {}", fmt::ptr(this), rt);
                _opened_range_tombstone.reset();
                _reader->push_mutation_fragment(mutation_fragment(*_schema, permit(), std::move(rt)));
            }
        }
        if (!_reader->_partition_finished) {
            consume_partition_end();
        }
        _reader->_end_of_stream = true;
    }

    virtual proceed consume_partition_end() override {
        sstlog.trace("mp_row_consumer_m {}: consume_partition_end()", fmt::ptr(this));
        reset_for_new_partition();

        if (_fwd == streamed_mutation::forwarding::yes) {
            _reader->_end_of_stream = true;
            return proceed::no;
        }

        _reader->_index_in_current_partition = false;
        _reader->_partition_finished = true;
        _reader->_before_partition = true;
        _reader->push_mutation_fragment(mutation_fragment(*_schema, permit(), partition_end()));
        return proceed(!_reader->is_buffer_full() && !need_preempt());
    }

    virtual void reset(sstables::indexable_element el) override {
        sstlog.trace("mp_row_consumer_m {}: reset({})", fmt::ptr(this), static_cast<int>(el));
        if (el == indexable_element::partition) {
            reset_for_new_partition();
        } else {
            _in_progress_row.reset();
            _is_mutation_end = false;
        }
    }

    virtual position_in_partition_view position() override {
        if (_inside_static_row) {
            return position_in_partition_view(position_in_partition_view::static_row_tag_t{});
        }
        if (_stored_tombstone) {
            return _stored_tombstone->position();
        }
        if (_in_progress_row) {
            return _in_progress_row->position();
        }
        if (_is_mutation_end) {
            return position_in_partition_view(position_in_partition_view::end_of_partition_tag_t{});
        }
        return position_in_partition_view(position_in_partition_view::partition_start_tag_t{});
    }
};

flat_mutation_reader make_reader(
        shared_sstable sstable,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        read_monitor& monitor) {
    return make_flat_mutation_reader<sstable_mutation_reader<data_consume_rows_context_m, mp_row_consumer_m>>(
        std::move(sstable), std::move(schema), std::move(permit), range, slice, pc, std::move(trace_state), fwd, fwd_mr, monitor);
}

} // namespace mx
} // namespace sstables
