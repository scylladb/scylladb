/*
 * Copyright 2015 Cloudius Systems
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
    } _state = state::ROW_START;

    row_consumer& _consumer;

    temporary_buffer<char> _key;
    temporary_buffer<char> _val;

    // state for reading a cell
    bool _deleted;
    uint32_t _ttl, _expiration;

    static inline bytes_view to_bytes_view(temporary_buffer<char>& b) {
        // The sstable code works with char, our "bytes_view" works with
        // byte_t. Rather than change all the code, let's do a cast...
        using byte = bytes_view::value_type;
        return bytes_view(reinterpret_cast<const byte*>(b.get()), b.size());
    }

public:
    bool non_consuming() const {
        return (((_state == state::DELETION_TIME_3)
                || (_state == state::CELL_VALUE_BYTES_2)
                || (_state == state::ATOM_START_2)
                || (_state == state::ATOM_MASK_2)
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
            _consumer.consume_row_start(to_bytes_view(_key), del);
            // after calling the consume function, we can release the
            // buffers we held for it.
            _key.release();
            _state = state::ATOM_START;
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
                // FIXME: see ColumnSerializer.java:deserializeColumnBody
                throw malformed_sstable_exception("FIXME COUNTER_MASK");
            } else if (mask & EXPIRATION_MASK) {
                _deleted = false;
                _state = state::EXPIRING_CELL;
            } else {
                // FIXME: see ColumnSerializer.java:deserializeColumnBody
                if (mask & COUNTER_UPDATE_MASK) {
                    throw malformed_sstable_exception("FIXME COUNTER_UPDATE_MASK");
                }
                _ttl = _expiration = 0;
                _deleted = mask & DELETION_MASK;
                _state = state::CELL;
            }
            break;
        }
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
                if (_deleted) {
                    if (_val.size() != 4) {
                        throw malformed_sstable_exception("deleted cell expects local_deletion_time value");
                    }
                    deletion_time del;
                    del.local_deletion_time = consume_be<uint32_t>(_val);
                    del.marked_for_delete_at = _u64;
                    _consumer.consume_deleted_cell(to_bytes_view(_key), del);
                } else {
                    _consumer.consume_cell(to_bytes_view(_key),
                            to_bytes_view(_val), _u64, _ttl, _expiration);
                }
                // after calling the consume function, we can release the
                // buffers we held for it.
                _key.release();
                _val.release();
                _state = state::ATOM_START;
            } else {
                _state = state::CELL_VALUE_BYTES_2;
            }
            break;
        case state::CELL_VALUE_BYTES_2:
            if (_deleted) {
                if (_val.size() != 4) {
                    throw malformed_sstable_exception("deleted cell expects local_deletion_time value");
                }
                deletion_time del;
                del.local_deletion_time = consume_be<uint32_t>(_val);
                del.marked_for_delete_at = _u64;
                _consumer.consume_deleted_cell(to_bytes_view(_key), del);
            } else {
                _consumer.consume_cell(to_bytes_view(_key),
                        to_bytes_view(_val), _u64, _ttl, _expiration);
            }
            // after calling the consume function, we can release the
            // buffers we held for it.
            _key.release();
            _val.release();
            _state = state::ATOM_START;
            break;
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
            _consumer.consume_range_tombstone(to_bytes_view(_key),
                    to_bytes_view(_val), del);
            _key.release();
            _val.release();
            _state = state::ATOM_START;
            break;
        }
        default:
            throw malformed_sstable_exception("unknown state");
        }

        return row_consumer::proceed::yes;
    }

    data_consume_rows_context(row_consumer& consumer,
            input_stream<char> && input, uint64_t maxlen) :
            continuous_data_consumer(std::move(input), maxlen)
            , _consumer(consumer) {
    }

    void verify_end_state() {
        if (_state != state::ROW_START || _prestate != prestate::NONE) {
            throw malformed_sstable_exception("end of input, but not end of row");
        }
    }
};

// data_consume_rows() and data_consume_rows_at_once() both can read just a
// single row or many rows. The difference is that data_consume_rows_at_once()
// is optimized to reading one or few rows (reading it all into memory), while
// data_consume_rows() uses a read buffer, so not all the rows need to fit
// memory in the same time (they are delivered to the consumer one by one).
class data_consume_context::impl {
private:
    std::unique_ptr<data_consume_rows_context> _ctx;
public:
    impl(row_consumer& consumer,
            input_stream<char>&& input, uint64_t maxlen) :
                _ctx(new data_consume_rows_context(consumer, std::move(input), maxlen)) { }
    future<> read() {
        return _ctx->consume_input(*_ctx);
    }
};

data_consume_context::~data_consume_context() = default;
data_consume_context::data_consume_context(data_consume_context&&) = default;
data_consume_context& data_consume_context::operator=(data_consume_context&&) = default;
data_consume_context::data_consume_context(std::unique_ptr<impl> p) : _pimpl(std::move(p)) { }
future<> data_consume_context::read() {
    return _pimpl->read();
}

data_consume_context sstable::data_consume_rows(
        row_consumer& consumer, uint64_t start, uint64_t end) {
    auto estimated_size = std::min(uint64_t(sstable_buffer_size), align_up(end - start, uint64_t(8 << 10)));
    return std::make_unique<data_consume_context::impl>(
            consumer, data_stream_at(start, std::max<size_t>(estimated_size, 8192)), end - start);
}

data_consume_context sstable::data_consume_rows(row_consumer& consumer) {
    return data_consume_rows(consumer, 0, data_size());
}

future<> sstable::data_consume_rows_at_once(row_consumer& consumer,
        uint64_t start, uint64_t end) {
    return data_read(start, end - start).then([&consumer]
                                               (temporary_buffer<char> buf) {
        data_consume_rows_context ctx(consumer, input_stream<char>(), -1);
        ctx.process(buf);
        ctx.verify_end_state();
    });
}

}
