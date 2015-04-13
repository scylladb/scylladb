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
    // remaining length of input to read (if 0, continue until end of file).
    uint64_t _remain;

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
                || (s == state::ATOM_START_2)) && (ps == prestate::NONE));
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

    // called by input_stream::consume():
    template<typename Done>
    void operator()(temporary_buffer<char> data, Done&& done) {
        if (_remain && data.size() >= _remain) {
            // We've been asked to stop before the end of file, and we've
            // read past the amount we need. So process the beginning of the
            // buffer, and return the rest to the stream with done():
            process(data.share(0, _remain));
            data.trim_front(_remain);
            done(std::move(data));
            verify_end_state();
        } else if (data.empty()) {
            // End of file
            done(std::move(data));
            verify_end_state();
        } else {
            if (_remain) {
                _remain -= data.size();
            }
            process(std::move(data));
        }
    }

private:
    // Read a 16-bit integer into _u16. If the whole thing is in the buffer
    // (this is the common case), do this immediately. Otherwise, remember
    // what we have in the buffer, and remember to continue later by using
    // a "prestate":
    inline void read_16(temporary_buffer<char>& data, state next_state) {
        if (data.size() >= sizeof(uint16_t)) {
            _u16 = consume_be<uint16_t>(data);
        } else {
            std::copy(data.begin(), data.end(), _read_int.bytes);
            _pos = data.size();
            data.trim(0);
            _prestate = prestate::READING_U16;
        }
        _state = next_state;
    }
    inline void read_32(temporary_buffer<char>& data, state next_state) {
        if (data.size() >= sizeof(uint32_t)) {
            _u32 = consume_be<uint32_t>(data);
        } else {
            std::copy(data.begin(), data.end(), _read_int.bytes);
            _pos = data.size();
            data.trim(0);
            _prestate = prestate::READING_U32;
        }
        _state = next_state;
    }
    inline void read_64(temporary_buffer<char>& data, state next_state) {
        if (data.size() >= sizeof(uint64_t)) {
            _u32 = consume_be<uint64_t>(data);
        } else {
            std::copy(data.begin(), data.end(), _read_int.bytes);
            _pos = data.size();
            data.trim(0);
            _prestate = prestate::READING_U64;
        }
        _state = next_state;
    }
    inline void read_bytes(temporary_buffer<char>& data, uint32_t len, temporary_buffer<char>& where, state next_state) {
        if (data.size() >=  len) {
            where = data.share(0, len);
            data.trim_front(len);
        } else {
            // copy what we have so far, read the rest later
            _read_bytes = temporary_buffer<char>(_u16);
            std::copy(data.begin(), data.end(),_read_bytes.get_write());
            _read_bytes_where = &where;
            _pos = data.size();
            data.trim(0);
            _prestate = prestate::READING_BYTES;
        }
        _state = next_state;
    }

public:
    // process() feeds the given data into the state machine
    void process(temporary_buffer<char> data) {
#if 0
        // Testing hack: call process() for tiny chunks separately, to verify
        // that primitive types crossing input buffer are handled correctly.
        constexpr size_t tiny_chunk = 1; // try various tiny sizes
        if (data.size() > tiny_chunk) {
            for (unsigned i = 0; i < data.size(); i += tiny_chunk) {
                process(data.share(i, std::min(tiny_chunk, data.size() - i)));
            }
            return;
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
                read_16(data, state::ROW_KEY_BYTES);
                break;
            case state::ROW_KEY_BYTES:
                // After previously reading 16-bit length, read key's bytes.
                read_bytes(data, _u16, _key, state::DELETION_TIME);
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
                    read_32(data, state::DELETION_TIME_2);
                }
                break;
            case state::DELETION_TIME_2:
                read_64(data, state::DELETION_TIME_3);
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
                // TODO: use read_16() here too. have read_16 return true if read now.
                if (data.size() >= sizeof(uint16_t)) {
                    _u16 = consume_be<uint16_t>(data);
                    if (_u16 == 0) {
                        // end of row marker
                        _consumer.consume_row_end();
                        _state = state::ROW_START;
                    } else {
                        _state = state::ATOM_NAME_BYTES;
                    }
                } else {
                    std::copy(data.begin(), data.end(), _read_int.bytes);
                    _pos = data.size();
                    data.trim(0);
                    _prestate = prestate::READING_U16;
                    _state = state::ATOM_START_2;
                }
                break;
            case state::ATOM_START_2:
                if (_u16 == 0) {
                    // end of row marker
                    _consumer.consume_row_end();
                    _state = state::ROW_START;
                } else {
                    _state = state::ATOM_NAME_BYTES;
                }
                break;
            case state::ATOM_NAME_BYTES:
                read_bytes(data, _u16, _key, state::ATOM_MASK);
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
                if ((mask
                        & (RANGE_TOMBSTONE_MASK | COUNTER_MASK | EXPIRATION_MASK))
                        == 0) {
                    // read regular cell:
                    _state = state::CELL;
                } else if(mask & RANGE_TOMBSTONE_MASK) {
                    _state = state::RANGE_TOMBSTONE;
                } else {
                    // continue here. see sstable data file format document
                    throw malformed_sstable_exception("FIXME misc cell types");
                }
                break;
            }
            case state::CELL:
                if (data.size() >= sizeof(uint64_t) + sizeof(uint32_t)) {
                    _u64 = consume_be<uint64_t>(data);
                    _u32 = consume_be<uint32_t>(data);
                    _state = state::CELL_VALUE_BYTES;
                } else {
                    read_64(data, state::CELL_2);
                }
                break;
            case state::CELL_2:
                read_32(data, state::CELL_VALUE_BYTES);
                break;
            case state::CELL_VALUE_BYTES:
                // TODO: use read_bytes(data, _u32, _key, state::ATOM_START), but need to know if it was successful to decide on next state and on running consumer
                if (data.size() >= _u32) {
                    // If the whole string is in our buffer, great, we don't
                    // need to copy, and can skip the CELL_VALUE_BYTES_2 state
                    _val = data.share(0, _u32);
                    data.trim_front(_u32);
                    // finally pass it to the consumer:
                    _consumer.consume_cell(to_bytes_view(_key),
                            to_bytes_view(_val), _u64);
                    // after calling the consume function, we can release the
                    // buffers we held for it.
                    _key.release();
                    _val.release();
                    _state = state::ATOM_START;
                } else {
                    // copy what we have so far, read the rest later
                    _read_bytes = temporary_buffer<char>(_u32);
                    std::copy(data.begin(), data.end(),
                            _read_bytes.get_write());
                    _read_bytes_where = &_val;
                    _pos = data.size();
                    data.trim(0);
                    _prestate = prestate::READING_BYTES;
                    _state = state::CELL_VALUE_BYTES_2;
                }
                break;
            case state::CELL_VALUE_BYTES_2:
                _consumer.consume_cell(to_bytes_view(_key),
                        to_bytes_view(_val), _u64);
                // after calling the consume function, we can release the
                // buffers we held for it.
                _key.release();
                _val.release();
                _state = state::ATOM_START;
                break;
            case state::RANGE_TOMBSTONE:
                read_16(data, state::RANGE_TOMBSTONE_2);
                break;
            case state::RANGE_TOMBSTONE_2:
                // read the end column into _val.
                read_bytes(data, _u16, _val, state::RANGE_TOMBSTONE_3);
                break;
            case state::RANGE_TOMBSTONE_3:
                read_32(data, state::RANGE_TOMBSTONE_4);
                break;
            case state::RANGE_TOMBSTONE_4:
                read_64(data, state::RANGE_TOMBSTONE_5);
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
    }
};

// data_consume_row() and data_consume_rows() both can read just a single row
// or many rows. The difference is that data_consume_row() is optimized to
// reading one or few rows (reading it all into memory), while
// data_consume_rows() uses a read buffer, so not all the rows need to fit
// memory in the same time (they are delivered to the consumer one by one).

future<> sstable::data_consume_rows(row_consumer& consumer, uint64_t start,
        uint64_t end) {
    auto ctx = make_lw_shared<data_consume_rows_context>(consumer,
            data_stream_at(start), end ? (end - start) : 0);
    return ctx->consume_input(*ctx).then([ctx] {});
}

future<> sstable::data_consume_rows_at_once(row_consumer& consumer,
        uint64_t start, uint64_t end) {
    return data_read(start, end - start).then([&consumer]
                                               (temporary_buffer<char> buf) {
        data_consume_rows_context ctx(consumer, input_stream<char>(), 0);
        ctx.process(std::move(buf));
        ctx.verify_end_state();
    });
}

}
