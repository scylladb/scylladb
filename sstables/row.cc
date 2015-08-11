/*
 * Copyright 2015 Cloudius Systems
 */

#include "sstables.hh"

template<typename T>
static inline T consume_be(temporary_buffer<char>& p) {
    T i = net::ntoh(*unaligned_cast<const T*>(p.get()));
    p.trim_front(sizeof(T));
    return i;
}

namespace sstables {

// data_consume_rows_context remembers the context that an ongoing
// data_consume_rows() future is in.
class data_consume_rows_context {
private:
    row_consumer& _consumer;
    input_stream<char> _input;
    // remaining length of input to read (if <0, continue until end of file).
    int64_t _remain;

    // state machine progress:
    enum class prestate {
        NONE,
        READING_U16,
        READING_U32,
        READING_U64,
        READING_BYTES,
    } _prestate = prestate::NONE;
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
    // some states do not consume input (its only exists to perform some
    // action when finishing to read a primitive type via a prestate, in
    // the rare case that a primitive type crossed a buffer). Such
    // non-consuming states need to run even if the data buffer is empty.
    static bool non_consuming(state s, prestate ps) {
        return (((s == state::DELETION_TIME_3)
                || (s == state::CELL_VALUE_BYTES_2)
                || (s == state::ATOM_START_2)
                || (s == state::EXPIRING_CELL_3)) && (ps == prestate::NONE));
    }
    // state for non-NONE prestates
    uint32_t _pos;
    // state for READING_U16, READING_U32, READING_U64 prestate
    uint16_t _u16;
    uint32_t _u32;
    uint64_t _u64;
    union {
        char bytes[sizeof(uint64_t)];
        uint64_t uint64;
        uint32_t uint32;
        uint16_t uint16;
    } _read_int;
    // state for READING_BYTES prestate
    temporary_buffer<char> _read_bytes;
    temporary_buffer<char>* _read_bytes_where; // which temporary_buffer to set, _key or _val?

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
    data_consume_rows_context(row_consumer& consumer,
            input_stream<char> && input, uint64_t maxlen) :
            _consumer(consumer), _input(std::move(input)), _remain(maxlen) {
    }
    template<typename Consumer>
    future<> consume_input(Consumer& c) {
        return _input.consume(c);
    }

    void verify_end_state() {
        if (_state != state::ROW_START || _prestate != prestate::NONE) {
            throw malformed_sstable_exception("end of input, but not end of row");
        }
    }

    using unconsumed_remainder = input_stream<char>::unconsumed_remainder;
    // called by input_stream::consume():
    future<unconsumed_remainder>
    operator()(temporary_buffer<char> data) {
        if (_remain >= 0 && data.size() >= (uint64_t)_remain) {
            // We received more data than we actually care about, so process
            // the beginning of the buffer, and return the rest to the stream
            auto segment = data.share(0, _remain);
            process(segment);
            data.trim_front(_remain - segment.size());
            _remain -= (_remain - segment.size());
            if (_remain == 0) {
                verify_end_state();
            }
            return make_ready_future<unconsumed_remainder>(std::move(data));
        } else if (data.empty()) {
            // End of file
            verify_end_state();
            return make_ready_future<unconsumed_remainder>(std::move(data));
        } else {
            // We can process the entire buffer (if the consumer wants to).
            auto orig_data_size = data.size();
            if (process(data) == row_consumer::proceed::yes) {
                assert(data.size() == 0);
                if (_remain >= 0) {
                    _remain -= orig_data_size;
                }
                return make_ready_future<unconsumed_remainder>();
            } else {
                if (_remain >= 0) {
                    _remain -= orig_data_size - data.size();
                }
                return make_ready_future<unconsumed_remainder>(std::move(data));
            }
        }
    }

private:
    enum class read_status { ready, waiting };
    // Read a 16-bit integer into _u16. If the whole thing is in the buffer
    // (this is the common case), do this immediately. Otherwise, remember
    // what we have in the buffer, and remember to continue later by using
    // a "prestate":
    inline read_status read_16(temporary_buffer<char>& data) {
        if (data.size() >= sizeof(uint16_t)) {
            _u16 = consume_be<uint16_t>(data);
            return read_status::ready;
        } else {
            std::copy(data.begin(), data.end(), _read_int.bytes);
            _pos = data.size();
            data.trim(0);
            _prestate = prestate::READING_U16;
            return read_status::waiting;
        }
    }
    inline read_status read_32(temporary_buffer<char>& data) {
        if (data.size() >= sizeof(uint32_t)) {
            _u32 = consume_be<uint32_t>(data);
            return read_status::ready;
        } else {
            std::copy(data.begin(), data.end(), _read_int.bytes);
            _pos = data.size();
            data.trim(0);
            _prestate = prestate::READING_U32;
            return read_status::waiting;
        }
    }
    inline read_status read_64(temporary_buffer<char>& data) {
        if (data.size() >= sizeof(uint64_t)) {
            _u64 = consume_be<uint64_t>(data);
            return read_status::ready;
        } else {
            std::copy(data.begin(), data.end(), _read_int.bytes);
            _pos = data.size();
            data.trim(0);
            _prestate = prestate::READING_U64;
            return read_status::waiting;
        }
    }
    inline read_status read_bytes(temporary_buffer<char>& data, uint32_t len, temporary_buffer<char>& where) {
        if (data.size() >=  len) {
            where = data.share(0, len);
            data.trim_front(len);
            return read_status::ready;
        } else {
            // copy what we have so far, read the rest later
            _read_bytes = temporary_buffer<char>(len);
            std::copy(data.begin(), data.end(),_read_bytes.get_write());
            _read_bytes_where = &where;
            _pos = data.size();
            data.trim(0);
            _prestate = prestate::READING_BYTES;
            return read_status::waiting;
        }
    }

public:
    // process() feeds the given data into the state machine.
    // The consumer may request at any point (e.g., after reading a whole
    // row) to stop the processing, in which case we trim the buffer to
    // leave only the unprocessed part. The caller must handle calling
    // process() again, and/or refilling the buffer, as needed.
    row_consumer::proceed process(temporary_buffer<char>& data) {
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
        while (data || non_consuming(_state, _prestate)) {
            if (_prestate != prestate::NONE) {
                // We're in the middle of reading a basic type, which crossed
                // an input buffer. Resume that read before continuing to
                // handle the current state:
                if (_prestate == prestate::READING_BYTES) {
                    auto n = std::min(_read_bytes.size() - _pos, data.size());
                    std::copy(data.begin(), data.begin() + n,
                            _read_bytes.get_write() + _pos);
                    data.trim_front(n);
                    _pos += n;
                    if (_pos == _read_bytes.size()) {
                        *_read_bytes_where = std::move(_read_bytes);
                        _prestate = prestate::NONE;
                    }
                } else {
                    // in the middle of reading an integer
                    unsigned len;
                    switch (_prestate) {
                    case prestate::READING_U16:
                        len = sizeof(uint16_t);
                        break;
                    case prestate::READING_U32:
                        len = sizeof(uint32_t);
                        break;
                    case prestate::READING_U64:
                        len = sizeof(uint64_t);
                        break;
                    default:
                        throw malformed_sstable_exception("unknown prestate");
                    }
                    assert(_pos < len);
                    auto n = std::min((size_t)(len - _pos), data.size());
                    std::copy(data.begin(), data.begin() + n, _read_int.bytes + _pos);
                    data.trim_front(n);
                    _pos += n;
                    if (_pos == len) {
                        // done reading the integer, store it in _u16, _u32 or _u64:
                        switch (_prestate) {
                        case prestate::READING_U16:
                            _u16 = net::ntoh(_read_int.uint16);
                            break;
                        case prestate::READING_U32:
                            _u32 = net::ntoh(_read_int.uint32);
                            break;
                        case prestate::READING_U64:
                            _u64 = net::ntoh(_read_int.uint64);
                            break;
                        default:
                            throw malformed_sstable_exception(
                                    "unknown prestate");
                        }
                        _prestate = prestate::NONE;
                    }
                }
                continue;
            }

            switch (_state) {
            case state::ROW_START:
                // read 2-byte key length into _u16
                read_16(data);
                _state = state::ROW_KEY_BYTES;
                break;
            case state::ROW_KEY_BYTES:
                // After previously reading 16-bit length, read key's bytes.
                read_bytes(data, _u16, _key);
                _state = state::DELETION_TIME;
                break;
            case state::DELETION_TIME:
                if (data.size() >= sizeof(uint32_t) + sizeof(uint64_t)) {
                    // If we can read the entire deletion time at once, we can
                    // skip the DELETION_TIME_2 and DELETION_TIME_3 states.
                    deletion_time del;
                    del.local_deletion_time = consume_be<uint32_t>(data);
                    del.marked_for_delete_at = consume_be<uint64_t>(data);
                    _consumer.consume_row_start(to_bytes_view(_key), del);
                    // after calling the consume function, we can release the
                    // buffers we held for it.
                    _key.release();
                    _state = state::ATOM_START;
                } else {
                    read_32(data);
                    _state = state::DELETION_TIME_2;
                }
                break;
            case state::DELETION_TIME_2:
                read_64(data);
                _state = state::DELETION_TIME_3;
                break;
            case state::DELETION_TIME_3: {
                deletion_time del;
                del.local_deletion_time = _u32;
                del.marked_for_delete_at = _u64;
                _consumer.consume_row_start(to_bytes_view(_key), del);
                // after calling the consume function, we can release the
                // buffers we held for it.
                _key.release();
                _state = state::ATOM_START;
                break;
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
                read_bytes(data, _u16, _key);
                _state = state::ATOM_MASK;
                break;
            case state::ATOM_MASK: {
                auto mask = consume_be<uint8_t>(data);
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
                if (data.size() >= sizeof(uint32_t) + sizeof(uint32_t)) {
                    _ttl = consume_be<uint32_t>(data);
                    _expiration = consume_be<uint32_t>(data);
                    _state = state::CELL;
                } else {
                    read_32(data);
                    _state = state::EXPIRING_CELL_2;
                }
                break;
            case state::EXPIRING_CELL_2:
                _ttl = _u32;
                read_32(data);
                _state = state::EXPIRING_CELL_3;
                break;
            case state::EXPIRING_CELL_3:
                _expiration = _u32;
                _state = state::CELL;
                break;
            case state::CELL:
                if (data.size() >= sizeof(uint64_t) + sizeof(uint32_t)) {
                    _u64 = consume_be<uint64_t>(data);
                    _u32 = consume_be<uint32_t>(data);
                    _state = state::CELL_VALUE_BYTES;
                } else {
                    read_64(data);
                    _state = state::CELL_2;
                }
                break;
            case state::CELL_2:
                read_32(data);
                _state = state::CELL_VALUE_BYTES;
                break;
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
                read_16(data);
                _state = state::RANGE_TOMBSTONE_2;
                break;
            case state::RANGE_TOMBSTONE_2:
                // read the end column into _val.
                read_bytes(data, _u16, _val);
                _state = state::RANGE_TOMBSTONE_3;
                break;
            case state::RANGE_TOMBSTONE_3:
                read_32(data);
                _state = state::RANGE_TOMBSTONE_4;
                break;
            case state::RANGE_TOMBSTONE_4:
                read_64(data);
                _state = state::RANGE_TOMBSTONE_5;
                break;
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
        }
        return row_consumer::proceed::yes;
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
            consumer, data_stream_at(start, estimated_size), end - start);
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
