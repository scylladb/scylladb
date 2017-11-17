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

    bool _shadowable;
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
        sstlog.trace("data_consume_row_context {}: state={}, size={}", this, static_cast<int>(_state), data.size());
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

data_consume_context::~data_consume_context() {
    if (_ctx) {
        auto f = _ctx->close();
        f.handle_exception([ctx = std::move(_ctx), sst = std::move(_sst)](auto) {});
    }
};
data_consume_context::data_consume_context(data_consume_context&& o) noexcept
    : _ctx(std::move(o._ctx))
{ }
data_consume_context& data_consume_context::operator=(data_consume_context&& o) noexcept {
    _ctx = std::move(o._ctx);
    return *this;
}
data_consume_context::data_consume_context(shared_sstable sst, row_consumer& consumer, input_stream<char>&& input, uint64_t start, uint64_t maxlen)
    : _sst(std::move(sst)), _ctx(new data_consume_rows_context(consumer, std::move(input), start, maxlen))
{ }
data_consume_context::data_consume_context() = default;
data_consume_context::operator bool() const noexcept {
    return bool(_ctx);
}
future<> data_consume_context::read() {
    return _ctx->consume_input(*_ctx);
}
future<> data_consume_context::fast_forward_to(uint64_t begin, uint64_t end) {
    _ctx->reset(indexable_element::partition);
    return _ctx->fast_forward_to(begin, end);
}
future<> data_consume_context::skip_to(indexable_element el, uint64_t begin) {
    sstlog.trace("data_consume_rows_context {}: skip_to({} -> {}, el={})", _ctx.get(), _ctx->position(), begin, static_cast<int>(el));
    if (begin <= _ctx->position()) {
        return make_ready_future<>();
    }
    _ctx->reset(el);
    return _ctx->skip_to(begin);
}
bool data_consume_context::eof() const {
    return _ctx->eof();
}

data_consume_context sstable::data_consume_rows(
        row_consumer& consumer, sstable::disk_read_range toread, uint64_t last_end) {
    // Although we were only asked to read until toread.end, we'll not limit
    // the underlying file input stream to this end, but rather to last_end.
    // This potentially enables read-ahead beyond end, until last_end, which
    // can be beneficial if the user wants to fast_forward_to() on the
    // returned context, and may make small skips.
    return { shared_from_this(), consumer, data_stream(toread.start, last_end - toread.start,
             consumer.io_priority(), consumer.resource_tracker(), _partition_range_history), toread.start, toread.end - toread.start };
}

data_consume_context sstable::data_consume_single_partition(
        row_consumer& consumer, sstable::disk_read_range toread) {
    return { shared_from_this(), consumer, data_stream(toread.start, toread.end - toread.start,
             consumer.io_priority(), consumer.resource_tracker(), _single_partition_history), toread.start, toread.end - toread.start };
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
