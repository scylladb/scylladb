/*
 * Copyright (C) 2015 ScyllaDB
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

#include "sstables.hh"
#include "consumer.hh"

namespace sstables {

// data_consume_rows_context remembers the context that an ongoing
// data_consume_rows() future is in.
class data_consume_rows_context : public data_consumer::continuous_data_consumer<data_consume_rows_context> {
private:
    enum class state {
        ROW_START,
        ROW_KEY_BYTES,
        DELETION_TIME,
        DELETION_TIME_2,
        DELETION_TIME_3,
        ATOM_START,
        ATOM_START_2,
        ATOM_NAME_BYTES,
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
        RANGE_TOMBSTONE_5,
        STOP_THEN_ATOM_START,
    } _state = state::ROW_START;

    row_consumer& _consumer;

    temporary_buffer<char> _key;
    temporary_buffer<char> _val;

    // state for reading a cell
    bool _deleted;
    bool _counter;
    uint32_t _ttl, _expiration;

    bool _read_partial_row = false;

public:
    bool non_consuming() const {
        return (((_state == state::DELETION_TIME_3)
                || (_state == state::CELL_VALUE_BYTES_2)
                || (_state == state::ATOM_START_2)
                || (_state == state::ATOM_MASK_2)
                || (_state == state::STOP_THEN_ATOM_START)
                || (_state == state::COUNTER_CELL_2)
                || (_state == state::EXPIRING_CELL_3)) && (_prestate == prestate::NONE));
    }

    // process() feeds the given data into the state machine.
    // The consumer may request at any point (e.g., after reading a whole
    // row) to stop the processing, in which case we trim the buffer to
    // leave only the unprocessed part. The caller must handle calling
    // process() again, and/or refilling the buffer, as needed.
    row_consumer::proceed process_state(temporary_buffer<char>& data) {
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
        switch (_state) {
        case state::ROW_START:
            // read 2-byte key length into _u16
            if (read_16(data) != read_status::ready) {
                _state = state::ROW_KEY_BYTES;
                break;
            }
        case state::ROW_KEY_BYTES:
            // After previously reading 16-bit length, read key's bytes.
            if (read_bytes(data, _u16, _key) != read_status::ready) {
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
            auto ret = _consumer.consume_row_start(to_bytes_view(_key), del);
            // after calling the consume function, we can release the
            // buffers we held for it.
            _key.release();
            _state = state::ATOM_START;
            if (ret == row_consumer::proceed::no) {
                return row_consumer::proceed::no;
            }
        }
        case state::ATOM_START:
            if (read_16(data) == read_status::ready) {
                if (_u16 == 0) {
                    // end of row marker
                    _state = state::ROW_START;
                    if (_consumer.consume_row_end() ==
                            row_consumer::proceed::no) {
                        return row_consumer::proceed::no;
                    }
                } else {
                    _state = state::ATOM_NAME_BYTES;
                }
            } else {
                _state = state::ATOM_START_2;
            }
            break;
        case state::ATOM_START_2:
            if (_u16 == 0) {
                // end of row marker
                _state = state::ROW_START;
                if (_consumer.consume_row_end() ==
                        row_consumer::proceed::no) {
                    return row_consumer::proceed::no;
                }
            } else {
                _state = state::ATOM_NAME_BYTES;
            }
            break;
        case state::ATOM_NAME_BYTES:
            if (read_bytes(data, _u16, _key) != read_status::ready) {
                _state = state::ATOM_MASK;
                break;
            }
        case state::ATOM_MASK:
            if (read_8(data) != read_status::ready) {
                _state = state::ATOM_MASK_2;
                break;
            }
            // fallthrough
        case state::ATOM_MASK_2: {
            auto mask = _u8;
            enum mask_type {
                DELETION_MASK = 0x01,
                EXPIRATION_MASK = 0x02,
                COUNTER_MASK = 0x04,
                COUNTER_UPDATE_MASK = 0x08,
                RANGE_TOMBSTONE_MASK = 0x10,
            };
            if (mask & RANGE_TOMBSTONE_MASK) {
                _state = state::RANGE_TOMBSTONE;
            } else if (mask & COUNTER_MASK) {
                _deleted = false;
                _counter = true;
                _state = state::COUNTER_CELL;
            } else if (mask & EXPIRATION_MASK) {
                _deleted = false;
                _counter = false;
                _state = state::EXPIRING_CELL;
            } else {
                // FIXME: see ColumnSerializer.java:deserializeColumnBody
                if (mask & COUNTER_UPDATE_MASK) {
                    throw malformed_sstable_exception("FIXME COUNTER_UPDATE_MASK");
                }
                _ttl = _expiration = 0;
                _deleted = mask & DELETION_MASK;
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
            if (read_16(data) != read_status::ready) {
                _state = state::RANGE_TOMBSTONE_2;
                break;
            }
        case state::RANGE_TOMBSTONE_2:
            // read the end column into _val.
            if (read_bytes(data, _u16, _val) != read_status::ready) {
                _state = state::RANGE_TOMBSTONE_3;
                break;
            }
        case state::RANGE_TOMBSTONE_3:
            if (read_32(data) != read_status::ready) {
                _state = state::RANGE_TOMBSTONE_4;
                break;
            }
        case state::RANGE_TOMBSTONE_4:
            if (read_64(data) != read_status::ready) {
                _state = state::RANGE_TOMBSTONE_5;
                break;
            }
        case state::RANGE_TOMBSTONE_5:
        {
            deletion_time del;
            del.local_deletion_time = _u32;
            del.marked_for_delete_at = _u64;
            auto ret = _consumer.consume_range_tombstone(to_bytes_view(_key),
                    to_bytes_view(_val), del);
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
            input_stream<char> && input, uint64_t start, uint64_t maxlen,
            std::experimental::optional<sstable::disk_read_range::row_info> ri = {})
                : continuous_data_consumer(std::move(input), start, maxlen)
                , _consumer(consumer) {
        // If the "ri" option is given, we are reading a partition from the
        // middle (in the beginning of an atom), as would happen when we use
        // the "promoted index" to skip closer to where a particular column
        // starts. When we start in the middle of the partition, we will not
        // read the key nor the tombstone from the disk, so the caller needs
        // to provide them (the tombstone is provided in the promoted index
        // exactly for that reason).
        if (ri) {
            _read_partial_row = true;
            auto ret = _consumer.consume_row_start(ri->k, ri->deltime);
            if (ret == row_consumer::proceed::yes) {
                _state = state::ATOM_START;
            } else {
                // If we were asked to stop parsing after consuming the row
                // start, we can't go to ATOM_START, need to use a new state
                // which stops parsing, and continues at ATOM_START later.
                _state = state::STOP_THEN_ATOM_START;
            }
        }
    }

    void verify_end_state() {
        if (_read_partial_row) {
            // If reading a partial row (i.e., when we have a clustering row
            // filter and using a promoted index), we may be in ATOM_START
            // state instead of ROW_START. In that case we did not read the
            // end-of-row marker and consume_row_end() was never called.
            if (_state == state::ATOM_START) {
                _consumer.consume_row_end();
                return;
            }
        }
        if (_state != state::ROW_START || _prestate != prestate::NONE) {
            throw malformed_sstable_exception("end of input, but not end of row");
        }
    }

    void reset(uint64_t offset) {
        _state = state::ROW_START;
        _consumer.reset();
    }
};

// data_consume_rows() and data_consume_rows_at_once() both can read just a
// single row or many rows. The difference is that data_consume_rows_at_once()
// is optimized to reading one or few rows (reading it all into memory), while
// data_consume_rows() uses a read buffer, so not all the rows need to fit
// memory in the same time (they are delivered to the consumer one by one).
class data_consume_context::impl {
private:
    shared_sstable _sst;
    std::unique_ptr<data_consume_rows_context> _ctx;
public:
    impl(shared_sstable sst, row_consumer& consumer, input_stream<char>&& input, uint64_t start,
             uint64_t maxlen, std::experimental::optional<sstable::disk_read_range::row_info> ri)
        : _sst(std::move(sst))
        , _ctx(new data_consume_rows_context(consumer, std::move(input), start, maxlen, ri))
    { }
    ~impl() {
        if (_ctx) {
            auto f = _ctx->close();
            f.handle_exception([ctx = std::move(_ctx), sst = std::move(_sst)] (auto) { });
        }
    }
    future<> read() {
        return _ctx->consume_input(*_ctx);
    }
    future<> fast_forward_to(uint64_t begin, uint64_t end) {
        return _ctx->fast_forward_to(begin, end);
    }
};

data_consume_context::~data_consume_context() = default;
data_consume_context::data_consume_context(data_consume_context&& o) noexcept
    : _pimpl(std::move(o._pimpl))
{ }
data_consume_context& data_consume_context::operator=(data_consume_context&& o) noexcept {
    _pimpl = std::move(o._pimpl);
    return *this;
}
data_consume_context::data_consume_context(std::unique_ptr<impl> p) : _pimpl(std::move(p)) { }
future<> data_consume_context::read() {
    return _pimpl->read();
}
future<> data_consume_context::fast_forward_to(uint64_t begin, uint64_t end) {
    return _pimpl->fast_forward_to(begin, end);
}

data_consume_context sstable::data_consume_rows(
        row_consumer& consumer, sstable::disk_read_range toread, uint64_t last_end) {
    // Although we were only asked to read until toread.end, we'll not limit
    // the underlying file input stream to this end, but rather to last_end.
    // This potentially enables read-ahead beyond end, until last_end, which
    // can be beneficial if the user wants to fast_forward_to() on the
    // returned context, and may make small skips.
    return std::make_unique<data_consume_context::impl>(shared_from_this(),
            consumer, data_stream(toread.start, last_end - toread.start,
                consumer.io_priority(), _partition_range_history), toread.start, toread.end - toread.start, toread.ri);
}

data_consume_context sstable::data_consume_single_partition(
        row_consumer& consumer, sstable::disk_read_range toread) {
    return std::make_unique<data_consume_context::impl>(shared_from_this(),
            consumer, data_stream(toread.start, toread.end - toread.start,
                 consumer.io_priority(), _single_partition_history), toread.start, toread.end - toread.start, toread.ri);
}


data_consume_context sstable::data_consume_rows(row_consumer& consumer) {
    return data_consume_rows(consumer, {0, data_size()}, data_size());
}

future<> sstable::data_consume_rows_at_once(row_consumer& consumer,
        uint64_t start, uint64_t end) {
    return data_read(start, end - start, consumer.io_priority()).then([&consumer]
                                               (temporary_buffer<char> buf) {
        data_consume_rows_context ctx(consumer, input_stream<char>(), 0, -1);
        ctx.process(buf);
        ctx.verify_end_state();
    });
}

}
