/*
 * Copyright (C) 2015 ScyllaDB
 *
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

#include "bytes.hh"
#include "key.hh"
#include "core/temporary_buffer.hh"
#include "consumer.hh"
#include "sstables/types.hh"
#include "reader_concurrency_semaphore.hh"

// sstables::data_consume_row feeds the contents of a single row into a
// row_consumer object:
//
// * First, consume_row_start() is called, with some information about the
//   whole row: The row's key, timestamp, etc.
// * Next, consume_cell() is called once for every column.
// * Finally, consume_row_end() is called. A consumer written for a single
//   column will likely not want to do anything here.
//
// Important note: the row key, column name and column value, passed to the
// consume_* functions, are passed as a "bytes_view" object, which points to
// internal data held by the feeder. This internal data is only valid for the
// duration of the single consume function it was passed to. If the object
// wants to hold these strings longer, it must make a copy of the bytes_view's
// contents. [Note, in reality, because our implementation reads the whole
// row into one buffer, the byte_views remain valid until consume_row_end()
// is called.]
class row_consumer {
    reader_resource_tracker _resource_tracker;
    const io_priority_class& _pc;

public:
    using proceed = data_consumer::proceed;

    row_consumer(reader_resource_tracker resource_tracker, const io_priority_class& pc)
        : _resource_tracker(resource_tracker)
        , _pc(pc) {
    }

    virtual ~row_consumer() = default;

    // Consume the row's key and deletion_time. The latter determines if the
    // row is a tombstone, and if so, when it has been deleted.
    // Note that the key is in serialized form, and should be deserialized
    // (according to the schema) before use.
    // As explained above, the key object is only valid during this call, and
    // if the implementation wishes to save it, it must copy the *contents*.
    virtual proceed consume_row_start(sstables::key_view key, sstables::deletion_time deltime) = 0;

    // Consume one cell (column name and value). Both are serialized, and need
    // to be deserialized according to the schema.
    // When a cell is set with an expiration time, "ttl" is the time to live
    // (in seconds) originally set for this cell, and "expiration" is the
    // absolute time (in seconds since the UNIX epoch) when this cell will
    // expire. Typical cells, not set to expire, will get expiration = 0.
    virtual proceed consume_cell(bytes_view col_name, bytes_view value,
            int64_t timestamp,
            int32_t ttl, int32_t expiration) = 0;

    // Consume one counter cell. Column name and value are serialized, and need
    // to be deserialized according to the schema.
    virtual proceed consume_counter_cell(bytes_view col_name, bytes_view value,
            int64_t timestamp) = 0;

    // Consume a deleted cell (i.e., a cell tombstone).
    virtual proceed consume_deleted_cell(bytes_view col_name, sstables::deletion_time deltime) = 0;

    // Consume one row tombstone.
    virtual proceed consume_shadowable_row_tombstone(bytes_view col_name, sstables::deletion_time deltime) = 0;

    // Consume one range tombstone.
    virtual proceed consume_range_tombstone(
            bytes_view start_col, bytes_view end_col,
            sstables::deletion_time deltime) = 0;

    // Called at the end of the row, after all cells.
    // Returns a flag saying whether the sstable consumer should stop now, or
    // proceed consuming more data.
    virtual proceed consume_row_end() = 0;

    // Called when the reader is fast forwarded to given element.
    virtual void reset(sstables::indexable_element) = 0;

    // Under which priority class to place I/O coming from this consumer
    const io_priority_class& io_priority() const {
        return _pc;
    }

    // The restriction that applies to this consumer
    reader_resource_tracker resource_tracker() const {
        return _resource_tracker;
    }
};

class consumer_m {
    reader_resource_tracker _resource_tracker;
    const io_priority_class& _pc;
public:
    using proceed = data_consumer::proceed;

    consumer_m(reader_resource_tracker resource_tracker, const io_priority_class& pc)
    : _resource_tracker(resource_tracker)
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

    // Called when the reader is fast forwarded to given element.
    virtual void reset(sstables::indexable_element) = 0;

    // Under which priority class to place I/O coming from this consumer
    const io_priority_class& io_priority() const {
        return _pc;
    }

    // The restriction that applies to this consumer
    reader_resource_tracker resource_tracker() const {
        return _resource_tracker;
    }
};

namespace sstables {

// data_consume_rows_context remembers the context that an ongoing
// data_consume_rows() future is in.
class data_consume_rows_context : public data_consumer::continuous_data_consumer<data_consume_rows_context> {
private:
    enum class state {
        ROW_START,
        DELETION_TIME,
        DELETION_TIME_2,
        DELETION_TIME_3,
        ATOM_START,
        ATOM_START_2,
        ATOM_MASK,
        ATOM_MASK_2,
        COUNTER_CELL,
        COUNTER_CELL_2,
        EXPIRING_CELL,
        EXPIRING_CELL_2,
        EXPIRING_CELL_3,
        CELL,
        CELL_2,
        CELL_VALUE_BYTES,
        CELL_VALUE_BYTES_2,
        RANGE_TOMBSTONE,
        RANGE_TOMBSTONE_2,
        RANGE_TOMBSTONE_3,
        RANGE_TOMBSTONE_4,
        STOP_THEN_ATOM_START,
    } _state = state::ROW_START;

    row_consumer& _consumer;

    temporary_buffer<char> _key;
    temporary_buffer<char> _val;

    // state for reading a cell
    bool _deleted;
    bool _counter;
    uint32_t _ttl, _expiration;

    bool _shadowable;
public:
    using consumer = row_consumer;
    bool non_consuming() const {
        return (((_state == state::DELETION_TIME_3)
                || (_state == state::CELL_VALUE_BYTES_2)
                || (_state == state::ATOM_START_2)
                || (_state == state::ATOM_MASK_2)
                || (_state == state::STOP_THEN_ATOM_START)
                || (_state == state::COUNTER_CELL_2)
                || (_state == state::RANGE_TOMBSTONE_4)
                || (_state == state::EXPIRING_CELL_3)) && (_prestate == prestate::NONE));
    }

    // process() feeds the given data into the state machine.
    // The consumer may request at any point (e.g., after reading a whole
    // row) to stop the processing, in which case we trim the buffer to
    // leave only the unprocessed part. The caller must handle calling
    // process() again, and/or refilling the buffer, as needed.
    data_consumer::processing_result process_state(temporary_buffer<char>& data) {
#if 0
        // Testing hack: call process() for tiny chunks separately, to verify
        // that primitive types crossing input buffer are handled correctly.
        constexpr size_t tiny_chunk = 1; // try various tiny sizes
        if (data.size() > tiny_chunk) {
            for (unsigned i = 0; i < data.size(); i += tiny_chunk) {
                auto chunk_size = std::min(tiny_chunk, data.size() - i);
                auto chunk = data.share(i, chunk_size);
                if (process(chunk) == row_consumer::proceed::no) {
                    data.trim_front(i + chunk_size - chunk.size());
                    return row_consumer::proceed::no;
                }
            }
            data.trim(0);
            return row_consumer::proceed::yes;
        }
#endif
        sstlog.trace("data_consume_row_context {}: state={}, size={}", this, static_cast<int>(_state), data.size());
        switch (_state) {
        case state::ROW_START:
            if (read_short_length_bytes(data, _key) != read_status::ready) {
                _state = state::DELETION_TIME;
                break;
            }
        case state::DELETION_TIME:
            if (read_32(data) != read_status::ready) {
                _state = state::DELETION_TIME_2;
                break;
            }
            // fallthrough
        case state::DELETION_TIME_2:
            if (read_64(data) != read_status::ready) {
                _state = state::DELETION_TIME_3;
                break;
            }
            // fallthrough
        case state::DELETION_TIME_3: {
            deletion_time del;
            del.local_deletion_time = _u32;
            del.marked_for_delete_at = _u64;
            auto ret = _consumer.consume_row_start(key_view(to_bytes_view(_key)), del);
            // after calling the consume function, we can release the
            // buffers we held for it.
            _key.release();
            _state = state::ATOM_START;
            if (ret == row_consumer::proceed::no) {
                return row_consumer::proceed::no;
            }
        }
        case state::ATOM_START:
            if (read_short_length_bytes(data, _key) != read_status::ready) {
                _state = state::ATOM_START_2;
                break;
            }
        case state::ATOM_START_2:
            if (_u16 == 0) {
                // end of row marker
                _state = state::ROW_START;
                if (_consumer.consume_row_end() ==
                        row_consumer::proceed::no) {
                    return row_consumer::proceed::no;
                }
            } else {
                _state = state::ATOM_MASK;
            }
            break;
        case state::ATOM_MASK:
            if (read_8(data) != read_status::ready) {
                _state = state::ATOM_MASK_2;
                break;
            }
            // fallthrough
        case state::ATOM_MASK_2: {
            auto const mask = column_mask(_u8);

            if ((mask & (column_mask::range_tombstone | column_mask::shadowable)) != column_mask::none) {
                _state = state::RANGE_TOMBSTONE;
                _shadowable = (mask & column_mask::shadowable) != column_mask::none;
            } else if ((mask & column_mask::counter) != column_mask::none) {
                _deleted = false;
                _counter = true;
                _state = state::COUNTER_CELL;
            } else if ((mask & column_mask::expiration) != column_mask::none) {
                _deleted = false;
                _counter = false;
                _state = state::EXPIRING_CELL;
            } else {
                // FIXME: see ColumnSerializer.java:deserializeColumnBody
                if ((mask & column_mask::counter_update) != column_mask::none) {
                    throw malformed_sstable_exception("FIXME COUNTER_UPDATE_MASK");
                }
                _ttl = _expiration = 0;
                _deleted = (mask & column_mask::deletion) != column_mask::none;
                _counter = false;
                _state = state::CELL;
            }
            break;
        }
        case state::COUNTER_CELL:
            if (read_64(data) != read_status::ready) {
                _state = state::COUNTER_CELL_2;
                break;
            }
            // fallthrough
        case state::COUNTER_CELL_2:
            // _timestamp_of_last_deletion = _u64;
            _state = state::CELL;
            goto state_CELL;
        case state::EXPIRING_CELL:
            if (read_32(data) != read_status::ready) {
                _state = state::EXPIRING_CELL_2;
                break;
            }
            // fallthrough
        case state::EXPIRING_CELL_2:
            _ttl = _u32;
            if (read_32(data) != read_status::ready) {
                _state = state::EXPIRING_CELL_3;
                break;
            }
            // fallthrough
        case state::EXPIRING_CELL_3:
            _expiration = _u32;
            _state = state::CELL;
        state_CELL:
        case state::CELL: {
            if (read_64(data) != read_status::ready) {
                _state = state::CELL_2;
                break;
            }
        }
        case state::CELL_2:
            if (read_32(data) != read_status::ready) {
                _state = state::CELL_VALUE_BYTES;
                break;
            }
        case state::CELL_VALUE_BYTES:
            if (read_bytes(data, _u32, _val) == read_status::ready) {
                // If the whole string is in our buffer, great, we don't
                // need to copy, and can skip the CELL_VALUE_BYTES_2 state.
                //
                // finally pass it to the consumer:
                row_consumer::proceed ret;
                if (_deleted) {
                    if (_val.size() != 4) {
                        throw malformed_sstable_exception("deleted cell expects local_deletion_time value");
                    }
                    deletion_time del;
                    del.local_deletion_time = consume_be<uint32_t>(_val);
                    del.marked_for_delete_at = _u64;
                    ret = _consumer.consume_deleted_cell(to_bytes_view(_key), del);
                } else if (_counter) {
                    ret = _consumer.consume_counter_cell(to_bytes_view(_key),
                            to_bytes_view(_val), _u64);
                } else {
                    ret = _consumer.consume_cell(to_bytes_view(_key),
                            to_bytes_view(_val), _u64, _ttl, _expiration);
                }
                // after calling the consume function, we can release the
                // buffers we held for it.
                _key.release();
                _val.release();
                _state = state::ATOM_START;
                if (ret == row_consumer::proceed::no) {
                    return row_consumer::proceed::no;
                }
            } else {
                _state = state::CELL_VALUE_BYTES_2;
            }
            break;
        case state::CELL_VALUE_BYTES_2:
        {
            row_consumer::proceed ret;
            if (_deleted) {
                if (_val.size() != 4) {
                    throw malformed_sstable_exception("deleted cell expects local_deletion_time value");
                }
                deletion_time del;
                del.local_deletion_time = consume_be<uint32_t>(_val);
                del.marked_for_delete_at = _u64;
                ret = _consumer.consume_deleted_cell(to_bytes_view(_key), del);
            } else if (_counter) {
                ret = _consumer.consume_counter_cell(to_bytes_view(_key),
                        to_bytes_view(_val), _u64);
            } else {
                ret = _consumer.consume_cell(to_bytes_view(_key),
                        to_bytes_view(_val), _u64, _ttl, _expiration);
            }
            // after calling the consume function, we can release the
            // buffers we held for it.
            _key.release();
            _val.release();
            _state = state::ATOM_START;
            if (ret == row_consumer::proceed::no) {
                return row_consumer::proceed::no;
            }
            break;
        }
        case state::RANGE_TOMBSTONE:
            if (read_short_length_bytes(data, _val) != read_status::ready) {
                _state = state::RANGE_TOMBSTONE_2;
                break;
            }
        case state::RANGE_TOMBSTONE_2:
            if (read_32(data) != read_status::ready) {
                _state = state::RANGE_TOMBSTONE_3;
                break;
            }
        case state::RANGE_TOMBSTONE_3:
            if (read_64(data) != read_status::ready) {
                _state = state::RANGE_TOMBSTONE_4;
                break;
            }
        case state::RANGE_TOMBSTONE_4:
        {
            deletion_time del;
            del.local_deletion_time = _u32;
            del.marked_for_delete_at = _u64;
            auto ret = _shadowable
                     ? _consumer.consume_shadowable_row_tombstone(to_bytes_view(_key), del)
                     : _consumer.consume_range_tombstone(to_bytes_view(_key), to_bytes_view(_val), del);
            _key.release();
            _val.release();
            _state = state::ATOM_START;
            if (ret == row_consumer::proceed::no) {
                return row_consumer::proceed::no;
            }
            break;
        }
        case state::STOP_THEN_ATOM_START:
            _state = state::ATOM_START;
            return row_consumer::proceed::no;
        default:
            throw malformed_sstable_exception("unknown state");
        }

        return row_consumer::proceed::yes;
    }

    data_consume_rows_context(row_consumer& consumer,
            input_stream<char> && input, uint64_t start, uint64_t maxlen)
                : continuous_data_consumer(std::move(input), start, maxlen)
                , _consumer(consumer) {
    }

    void verify_end_state() {
        // If reading a partial row (i.e., when we have a clustering row
        // filter and using a promoted index), we may be in ATOM_START or ATOM_START_2
        // state instead of ROW_START. In that case we did not read the
        // end-of-row marker and consume_row_end() was never called.
        if (_state == state::ATOM_START || _state == state::ATOM_START_2) {
            _consumer.consume_row_end();
            return;
        }
        if (_state != state::ROW_START || _prestate != prestate::NONE) {
            throw malformed_sstable_exception("end of input, but not end of row");
        }
    }

    void reset(indexable_element el) {
        switch (el) {
        case indexable_element::partition:
            _state = state::ROW_START;
            break;
        case indexable_element::cell:
            _state = state::ATOM_START;
            break;
        default:
            assert(0);
        }
        _consumer.reset(el);
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
        STATIC_ROW,
        NON_STATIC_ROW,
        NON_STATIC_ROW_SIZE,
        NON_STATIC_ROW_PREV_SIZE,
        NON_STATIC_ROW_TIMESTAMP,
        NON_STATIC_ROW_TIMESTAMP_TTL,
        NON_STATIC_ROW_TIMESTAMP_DELTIME,
        NON_STATIC_ROW_DELETION,
        NON_STATIC_ROW_DELETION_2,
        NON_STATIC_ROW_DELETION_3,
        RANGE_TOMBSTONE_MARKER,
    } _state = state::PARTITION_START;

    consumer_m& _consumer;

    temporary_buffer<char> _pk;

    unfiltered_flags_m _flags{0};
    unfiltered_extended_flags_m _extended_flags{0};
    bool _is_first_unfiltered = true;
public:
    using consumer = consumer_m;
    bool non_consuming() const {
        return (_state == state::DELETION_TIME_3
                || _state == state::FLAGS_2
                || _state == state::EXTENDED_FLAGS
                || _state == state::NON_STATIC_ROW_TIMESTAMP_DELTIME
                || _state == state::NON_STATIC_ROW_DELETION_3) && (_prestate == prestate::NONE);
    }

    data_consumer::processing_result process_state(temporary_buffer<char>& data) {
        switch (_state) {
        case state::PARTITION_START:
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
                break;
            } else if (_flags.is_range_tombstone()) {
                _state = state::RANGE_TOMBSTONE_MARKER;
                break;
            } else if (!_flags.has_extended_flags()) {
                _extended_flags = unfiltered_extended_flags_m(uint8_t{0u});
                _state = state::NON_STATIC_ROW;
                break;
            }
            if (read_8(data) != read_status::ready) {
                _state = state::EXTENDED_FLAGS;
                break;
            }
        case state::EXTENDED_FLAGS:
            _extended_flags = unfiltered_extended_flags_m(_u8);
            if (_extended_flags.is_static()) {
                if (_is_first_unfiltered) {
                    _state = state::STATIC_ROW;
                    break;
                } else {
                    throw malformed_sstable_exception("static row should be a first unfiltered in a partition");
                }
            }
        case state::NON_STATIC_ROW:
            _is_first_unfiltered = false;
            // Clustering blocks should be read here but serialization header is needed for that.
            // Table with just partition key does not have any so it's ok for the first version.

            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::NON_STATIC_ROW_SIZE;
                break;
            }
        case state::NON_STATIC_ROW_SIZE:
            // Ignore the result
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::NON_STATIC_ROW_PREV_SIZE;
                break;
            }
        case state::NON_STATIC_ROW_PREV_SIZE:
            // Ignore the result
            if (!_flags.has_timestamp()) {
                _state = state::NON_STATIC_ROW_DELETION;
                break;
            }
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::NON_STATIC_ROW_TIMESTAMP;
                break;
            }
        case state::NON_STATIC_ROW_TIMESTAMP:
            // TODO: consume timestamp
            if (!_flags.has_ttl()) {
                _state = state::NON_STATIC_ROW_DELETION;
                break;
            }
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::NON_STATIC_ROW_TIMESTAMP_TTL;
                break;
            }
        case state::NON_STATIC_ROW_TIMESTAMP_TTL:
            // TODO consume ttl
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::NON_STATIC_ROW_TIMESTAMP_DELTIME;
                break;
            }
        case state::NON_STATIC_ROW_TIMESTAMP_DELTIME:
            // TODO consume deltime
        case state::NON_STATIC_ROW_DELETION:
            if (!_flags.has_deletion()) {
                _state = state::FLAGS;
                break;
            }
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::NON_STATIC_ROW_DELETION_2;
                break;
            }
        case state::NON_STATIC_ROW_DELETION_2:
            // TODO consume mark_for_deleted_at
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = state::NON_STATIC_ROW_DELETION_3;
                break;
            }
        case state::NON_STATIC_ROW_DELETION_3:
            // TODO consume local_deletion_time
            _state = state::FLAGS;
            break;
        case state::STATIC_ROW:
            throw malformed_sstable_exception("unimplemented state");
        case state::RANGE_TOMBSTONE_MARKER:
            throw malformed_sstable_exception("unimplemented state");
        default:
            throw malformed_sstable_exception("unknown state");
        }

        return row_consumer::proceed::yes;
    }

    data_consume_rows_context_m(consumer_m& consumer, input_stream<char> && input, uint64_t start, uint64_t maxlen)
        : continuous_data_consumer(std::move(input), start, maxlen), _consumer(consumer)
    { }

    void verify_end_state() {
        if (_state != state::PARTITION_START || _prestate != prestate::NONE) {
            throw malformed_sstable_exception("end of input, but not end of partition");
        }
    }

    void reset(indexable_element el) {
        switch (el) {
            case indexable_element::partition:
                _state = state::PARTITION_START;
                break;
            default:
                assert(0);
        }
        _consumer.reset(el);
    }
};

}
