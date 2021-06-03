/*
 * Copyright (C) 2015-present ScyllaDB
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

#include "vint-serialization.hh"
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include "sstables/exceptions.hh"
#include "sstables/progress_monitor.hh"
#include <seastar/core/byteorder.hh>
#include <seastar/util/variant_utils.hh>
#include <seastar/net/byteorder.hh>
#include "bytes.hh"
#include "reader_permit.hh"
#include "utils/fragmented_temporary_buffer.hh"
#include "utils/overloaded_functor.hh"
#include "utils/small_vector.hh"

#include <variant>

template<typename T>
static inline T consume_be(temporary_buffer<char>& p) {
    T i = read_be<T>(p.get());
    p.trim_front(sizeof(T));
    return i;
}

namespace data_consumer {
enum class proceed { no, yes };
using processing_result = std::variant<proceed, skip_bytes>;

inline bool operator==(const processing_result& result, proceed value) {
    const proceed* p = std::get_if<proceed>(&result);
    return (p != nullptr && *p == value);
}

inline bool operator!=(const processing_result& result, proceed value) {
    return !(result == value);
}

enum class read_status { ready, waiting };

// Incremental parser for primitive data types.
//
// The parser is first programmed to read particular data type using
// one of the read_*() methods and fed with buffers until it reaches
// its final state.
// When the final state is reached, the value can be collected from
// the member field designated by the read method as the holder for
// the result.
//
// Example usage:
//
//   Assuming that next_buf() provides the next temporary_buffer.
//
//   primitive_consumer pc;
//   if (pc.read_32(next_buf()) == read_status::waiting) {
//      while (pc.consume(next_buf()) == read_status::waiting) {}
//   }
//   return pc._u32;
//
class primitive_consumer {
private:
    // state machine progress:
    enum class prestate {
        NONE,
        READING_U8,
        READING_U16,
        READING_U32,
        READING_U64,
        READING_BYTES_CONTIGUOUS,
        READING_BYTES,
        READING_U16_BYTES,
        READING_UNSIGNED_VINT,
        READING_UNSIGNED_VINT_LENGTH_BYTES_CONTIGUOUS,
        READING_UNSIGNED_VINT_LENGTH_BYTES,
        READING_UNSIGNED_VINT_WITH_LEN,
        READING_UNSIGNED_VINT_LENGTH_BYTES_WITH_LEN_CONTIGUOUS,
        READING_UNSIGNED_VINT_LENGTH_BYTES_WITH_LEN,
        READING_SIGNED_VINT,
        READING_SIGNED_VINT_WITH_LEN,
    } _prestate = prestate::NONE;

public:
    // state for non-NONE prestates
    uint32_t _pos;
    // state for READING_U8, READING_U16, READING_U32, READING_U64 prestate
    uint8_t  _u8;
    uint16_t _u16;
    uint32_t _u32;
    uint64_t _u64;
    int64_t _i64; // for reading signed vints
    reader_permit _permit;
private:
    union {
        char bytes[sizeof(uint64_t)];
        uint64_t uint64;
        uint32_t uint32;
        uint16_t uint16;
        uint8_t  uint8;
    } _read_int;

    // state for READING_BYTES prestate
    size_t _read_bytes_len = 0;
    utils::small_vector<temporary_buffer<char>, 1> _read_bytes;
    temporary_buffer<char>* _read_bytes_where_contiguous; // which buffer to set, _key, _val, _cell_path or _pk?
    fragmented_temporary_buffer* _read_bytes_where;
    inline read_status read_partial_int(temporary_buffer<char>& data, prestate next_state) {
        std::copy(data.begin(), data.end(), _read_int.bytes);
        _pos = data.size();
        data.trim(0);
        _prestate = next_state;
        return read_status::waiting;
    }
    template <typename VintType, prestate ReadingVint, prestate ReadingVintWithLen, typename T>
    inline read_status read_vint(temporary_buffer<char>& data, T& dest) {
        static_assert(std::is_same_v<T, typename VintType::value_type>, "Destination type mismatch");
        if (data.empty()) {
            _prestate = ReadingVint;
            return read_status::waiting;
        } else {
            const vint_size_type len = VintType::serialized_size_from_first_byte(*data.begin());
            if (data.size() >= len) {
                dest = VintType::deserialize(
                        bytes_view(reinterpret_cast<bytes::value_type*>(data.get_write()), data.size()));
                data.trim_front(len);
                return read_status::ready;
            } else {
                _read_bytes.clear();
                _read_bytes.push_back(make_tracked_temporary_buffer(temporary_buffer<char>(len), _permit));
                std::copy(data.begin(), data.end(), _read_bytes.front().get_write());
                _read_bytes_len = len;
                _pos = data.size();
                data.trim(0);
                _prestate = ReadingVintWithLen;
                return read_status::waiting;
            }
        }
    }
    template <typename VintType, typename T>
    inline read_status read_vint_with_len(temporary_buffer<char>& data, T& dest) {
        static_assert(std::is_same_v<T, typename VintType::value_type>, "Destination type mismatch");
        const auto n = std::min(_read_bytes_len - _pos, data.size());
        std::copy_n(data.begin(), n, _read_bytes.front().get_write() + _pos);
        data.trim_front(n);
        _pos += n;
        if (_pos == _read_bytes_len) {
            dest = VintType::deserialize(
                    bytes_view(reinterpret_cast<bytes::value_type*>(_read_bytes.front().get_write()), _read_bytes_len));
            _prestate = prestate::NONE;
            return read_status::ready;
        }
        return read_status::waiting;
    };
public:
    primitive_consumer(reader_permit permit) : _permit(std::move(permit)) {}

    inline read_status read_8(temporary_buffer<char>& data) {
        if (data.size() >= sizeof(uint8_t)) {
            _u8 = consume_be<uint8_t>(data);
            return read_status::ready;
        } else {
            _pos = 0;
            _prestate = prestate::READING_U8;
            return read_status::waiting;
        }
    }
    // Read a 16-bit integer into _u16. If the whole thing is in the buffer
    // (this is the common case), do this immediately. Otherwise, remember
    // what we have in the buffer, and remember to continue later by using
    // a "prestate":
    inline read_status read_16(temporary_buffer<char>& data) {
        if (data.size() >= sizeof(uint16_t)) {
            _u16 = consume_be<uint16_t>(data);
            return read_status::ready;
        } else {
            return read_partial_int(data, prestate::READING_U16);
        }
    }
    inline read_status read_32(temporary_buffer<char>& data) {
        if (data.size() >= sizeof(uint32_t)) {
            _u32 = consume_be<uint32_t>(data);
            return read_status::ready;
        } else {
            return read_partial_int(data, prestate::READING_U32);
        }
    }
    inline read_status read_64(temporary_buffer<char>& data) {
        if (data.size() >= sizeof(uint64_t)) {
            _u64 = consume_be<uint64_t>(data);
            return read_status::ready;
        } else {
            return read_partial_int(data, prestate::READING_U64);
        }
    }
    inline read_status read_bytes_contiguous(temporary_buffer<char>& data, uint32_t len, temporary_buffer<char>& where) {
        if (data.size() >= len) {
            where = data.share(0, len);
            data.trim_front(len);
            return read_status::ready;
        } else {
            // copy what we have so far, read the rest later
            _read_bytes.clear();
            _read_bytes.push_back(make_tracked_temporary_buffer(temporary_buffer<char>(len), _permit));
            std::copy(data.begin(), data.end(),_read_bytes.front().get_write());
            _read_bytes_len = len;
            _read_bytes_where_contiguous = &where;
            _pos = data.size();
            data.trim(0);
            _prestate = prestate::READING_BYTES_CONTIGUOUS;
            return read_status::waiting;
        }
    }
    inline read_status read_bytes(temporary_buffer<char>& data, uint32_t len, fragmented_temporary_buffer& where) {
        if (data.size() >= len) {
            std::vector<temporary_buffer<char>> fragments;
            fragments.push_back(data.share(0,len));
            where = fragmented_temporary_buffer(std::move(fragments), len);
            data.trim_front(len);
            return read_status::ready;
        } else {
            // copy what we have so far, read the rest later
            _read_bytes.clear();
            _read_bytes.push_back(data.share());
            _read_bytes_len = len;
            _read_bytes_where = &where;
            _pos = data.size();
            data.trim(0);
            _prestate = prestate::READING_BYTES;
            return read_status::waiting;
        }
    }
    inline read_status read_short_length_bytes(temporary_buffer<char>& data, temporary_buffer<char>& where) {
        if (data.size() >= sizeof(uint16_t)) {
            _u16 = consume_be<uint16_t>(data);
        } else {
            _read_bytes_where_contiguous = &where;
            return read_partial_int(data, prestate::READING_U16_BYTES);
        }
        return read_bytes_contiguous(data, uint32_t{_u16}, where);
    }
    inline read_status read_unsigned_vint(temporary_buffer<char>& data) {
        return read_vint<
                unsigned_vint,
                prestate::READING_UNSIGNED_VINT,
                prestate::READING_UNSIGNED_VINT_WITH_LEN>(data, _u64);
    }
    inline read_status read_signed_vint(temporary_buffer<char>& data) {
        return read_vint<
                signed_vint,
                prestate::READING_SIGNED_VINT,
                prestate::READING_SIGNED_VINT_WITH_LEN>(data, _i64);
    }
    inline read_status read_unsigned_vint_length_bytes_contiguous(temporary_buffer<char>& data, temporary_buffer<char>& where) {
        if (data.empty()) {
            _prestate = prestate::READING_UNSIGNED_VINT_LENGTH_BYTES_CONTIGUOUS;
            _read_bytes_where_contiguous = &where;
            return read_status::waiting;
        } else {
            const vint_size_type len = unsigned_vint::serialized_size_from_first_byte(*data.begin());
            if (data.size() >= len) {
                _u64 = unsigned_vint::deserialize(
                    bytes_view(reinterpret_cast<bytes::value_type*>(data.get_write()), data.size()));
                data.trim_front(len);
                return read_bytes_contiguous(data, static_cast<uint32_t>(_u64), where);
            } else {
                _read_bytes.clear();
                _read_bytes.push_back(make_tracked_temporary_buffer(temporary_buffer<char>(len), _permit));
                std::copy(data.begin(), data.end(),_read_bytes.front().get_write());
                _read_bytes_len = len;
                _pos = data.size();
                data.trim(0);
                _read_bytes_where_contiguous = &where;
                _prestate = prestate::READING_UNSIGNED_VINT_LENGTH_BYTES_WITH_LEN_CONTIGUOUS;
                return read_status::waiting;
            }
        }
    }
    inline read_status read_unsigned_vint_length_bytes(temporary_buffer<char>& data, fragmented_temporary_buffer& where) {
        if (data.empty()) {
            _prestate = prestate::READING_UNSIGNED_VINT_LENGTH_BYTES;
            _read_bytes_where = &where;
            return read_status::waiting;
        } else {
            const vint_size_type len = unsigned_vint::serialized_size_from_first_byte(*data.begin());
            if (data.size() >= len) {
                _u64 = unsigned_vint::deserialize(
                    bytes_view(reinterpret_cast<bytes::value_type*>(data.get_write()), data.size()));
                data.trim_front(len);
                return read_bytes(data, static_cast<uint32_t>(_u64), where);
            } else {
                _read_bytes.clear();
                _read_bytes.push_back(make_tracked_temporary_buffer(temporary_buffer<char>(len), _permit));
                std::copy(data.begin(), data.end(),_read_bytes.front().get_write());
                _read_bytes_len = len;
                _pos = data.size();
                data.trim(0);
                _read_bytes_where = &where;
                _prestate = prestate::READING_UNSIGNED_VINT_LENGTH_BYTES_WITH_LEN;
                return read_status::waiting;
            }
        }
    }
private:
    // Reads bytes belonging to an integer of size len. Returns true
    // if a full integer is now available.
    bool process_int(temporary_buffer<char>& data, unsigned len) {
        assert(_pos < len);
        auto n = std::min((size_t)(len - _pos), data.size());
        std::copy(data.begin(), data.begin() + n, _read_int.bytes + _pos);
        data.trim_front(n);
        _pos += n;
        return _pos == len;
    }
public:
    // Feeds data into the state machine.
    // After the call, when data is not empty then active() can be assumed to be false.
    read_status consume(temporary_buffer<char>& data) {
        if (__builtin_expect(_prestate == prestate::NONE, true)) {
            return read_status::ready;
        }
        // We're in the middle of reading a basic type, which crossed
        // an input buffer. Resume that read before continuing to
        // handle the current state:
        switch (_prestate) {
        case prestate::NONE:
            // This is handled above
            __builtin_unreachable();
            break;
        case prestate::READING_UNSIGNED_VINT:
            if (read_unsigned_vint(data) == read_status::ready) {
                _prestate = prestate::NONE;
                return read_status::ready;
            }
            break;
        case prestate::READING_SIGNED_VINT:
            if (read_signed_vint(data) == read_status::ready) {
                _prestate = prestate::NONE;
                return read_status::ready;
            }
            break;
        case prestate::READING_UNSIGNED_VINT_LENGTH_BYTES_CONTIGUOUS:
            if (read_unsigned_vint_length_bytes_contiguous(data, *_read_bytes_where_contiguous) == read_status::ready) {
                _prestate = prestate::NONE;
                return read_status::ready;
            }
            break;
        case prestate::READING_UNSIGNED_VINT_LENGTH_BYTES:
            if (read_unsigned_vint_length_bytes(data, *_read_bytes_where) == read_status::ready) {
                _prestate = prestate::NONE;
                return read_status::ready;
            }
            break;
        case prestate::READING_UNSIGNED_VINT_WITH_LEN:
            return read_vint_with_len<unsigned_vint>(data, _u64);
        case prestate::READING_SIGNED_VINT_WITH_LEN:
            return read_vint_with_len<signed_vint>(data, _i64);
        case prestate::READING_UNSIGNED_VINT_LENGTH_BYTES_WITH_LEN_CONTIGUOUS: {
            const auto n = std::min(_read_bytes_len - _pos, data.size());
            std::copy_n(data.begin(), n, _read_bytes.front().get_write() + _pos);
            data.trim_front(n);
            _pos += n;
            if (_pos == _read_bytes_len) {
                _u64 = unsigned_vint::deserialize(
                        bytes_view(reinterpret_cast<bytes::value_type*>(_read_bytes.front().get_write()), _read_bytes_len));
                if (read_bytes_contiguous(data, _u64, *_read_bytes_where_contiguous) == read_status::ready) {
                    _prestate = prestate::NONE;
                    return read_status::ready;
                }
            }
            break;
        }
        case prestate::READING_UNSIGNED_VINT_LENGTH_BYTES_WITH_LEN: {
            const auto n = std::min(_read_bytes_len - _pos, data.size());
            std::copy_n(data.begin(), n, _read_bytes.front().get_write() + _pos);
            data.trim_front(n);
            _pos += n;
            if (_pos == _read_bytes_len) {
                _u64 = unsigned_vint::deserialize(
                        bytes_view(reinterpret_cast<bytes::value_type*>(_read_bytes.front().get_write()), _read_bytes_len));
                if (read_bytes(data, _u64, *_read_bytes_where) == read_status::ready) {
                    _prestate = prestate::NONE;
                    return read_status::ready;
                }
            }
            break;
        }
        case prestate::READING_BYTES_CONTIGUOUS: {
            auto n = std::min(_read_bytes_len - _pos, data.size());
            std::copy(data.begin(), data.begin() + n, _read_bytes.front().get_write() + _pos);
            data.trim_front(n);
            _pos += n;
            if (_pos == _read_bytes_len) {
                *_read_bytes_where_contiguous = std::move(_read_bytes.front());
                _prestate = prestate::NONE;
                return read_status::ready;
            }
            break;
        }
        case prestate::READING_BYTES: {
            auto n = std::min(_read_bytes_len - _pos, data.size());
            _read_bytes.push_back(data.share(0, n));
            data.trim_front(n);
            _pos += n;
            if (_pos == _read_bytes_len) {
                std::vector<temporary_buffer<char>> fragments(std::make_move_iterator(_read_bytes.begin()), std::make_move_iterator(_read_bytes.end()));
                *_read_bytes_where = fragmented_temporary_buffer(std::move(fragments), _read_bytes_len);
                _prestate = prestate::NONE;
                return read_status::ready;
            }
            break;
        }
        case prestate::READING_U8:
            if (process_int(data, sizeof(uint8_t))) {
                _u8 = _read_int.uint8;
                _prestate = prestate::NONE;
                return read_status::ready;
            }
            break;
        case prestate::READING_U16:
            if (process_int(data, sizeof(uint16_t))) {
                _u16 = net::ntoh(_read_int.uint16);
                _prestate = prestate::NONE;
                return read_status::ready;
            }
            break;
        case prestate::READING_U16_BYTES:
            if (process_int(data, sizeof(uint16_t))) {
                _u16 = net::ntoh(_read_int.uint16);
                _prestate = prestate::NONE;
                return read_bytes_contiguous(data, _u16, *_read_bytes_where_contiguous);
            }
            break;
        case prestate::READING_U32:
            if (process_int(data, sizeof(uint32_t))) {
                _u32 = net::ntoh(_read_int.uint32);
                _prestate = prestate::NONE;
                return read_status::ready;
            }
            break;
        case prestate::READING_U64:
            if (process_int(data, sizeof(uint64_t))) {
                _u64 = net::ntoh(_read_int.uint64);
                _prestate = prestate::NONE;
                return read_status::ready;
            }
            break;
        }
        return read_status::waiting;
    }

    void reset() {
        _prestate = prestate::NONE;
    }

    bool active() const {
        return _prestate != prestate::NONE;
    }
};

template <typename StateProcessor>
class continuous_data_consumer : protected primitive_consumer {
    using proceed = data_consumer::proceed;
    StateProcessor& state_processor() {
        return static_cast<StateProcessor&>(*this);
    };
protected:
    input_stream<char> _input;
    sstables::reader_position_tracker _stream_position;
    // remaining length of input to read (if <0, continue until end of file).
    uint64_t _remain;
public:
    using read_status = data_consumer::read_status;

    continuous_data_consumer(reader_permit permit, input_stream<char>&& input, uint64_t start, uint64_t maxlen)
            : primitive_consumer(std::move(permit))
            , _input(std::move(input))
            , _stream_position(sstables::reader_position_tracker{start, maxlen})
            , _remain(maxlen) {}

    future<> consume_input() {
        return _input.consume(state_processor());
    }

    void verify_end_state() {
        state_processor().verify_end_state();
    }

    data_consumer::processing_result skip(temporary_buffer<char>& data, uint32_t len) {
        if (data.size() >= len) {
            data.trim_front(len);
            return proceed::yes;
        } else {
            auto left = len - data.size();
            data.trim(0);
            return skip_bytes{left};
        }
    }

    // some states do not consume input (its only exists to perform some
    // action when finishing to read a primitive type via a prestate, in
    // the rare case that a primitive type crossed a buffer). Such
    // non-consuming states need to run even if the data buffer is empty.
    bool non_consuming() {
        return state_processor().non_consuming();
    }

    using unconsumed_remainder = input_stream<char>::unconsumed_remainder;
    using consumption_result_type = consumption_result<char>;

    inline processing_result process(temporary_buffer<char>& data) {
        while (data || (!primitive_consumer::active() && non_consuming())) {
            // The primitive_consumer must finish before the enclosing state machine can continue.
            if (__builtin_expect(primitive_consumer::consume(data) == read_status::waiting, false)) {
                assert(data.size() == 0);
                return proceed::yes;
            }
            auto ret = state_processor().process_state(data);
            if (__builtin_expect(ret != proceed::yes, 0)) {
                return ret;
            }
        }
        return proceed::yes;
    }

    // called by input_stream::consume():
    future<consumption_result_type>
    operator()(temporary_buffer<char> data) {
        if (data.size() >= _remain) {
            // We received more data than we actually care about, so process
            // the beginning of the buffer, and return the rest to the stream
            auto segment = data.share(0, _remain);
            _stream_position.position += _remain;
            auto ret = process(segment);
            _stream_position.position -= segment.size();
            data.trim_front(_remain - segment.size());
            auto len = _remain - segment.size();
            _remain -= len;
            if (_remain == 0 && ret == proceed::yes) {
                verify_end_state();
            }
            return make_ready_future<consumption_result_type>(stop_consuming<char>{std::move(data)});
        } else if (data.empty()) {
            // End of file
            verify_end_state();
            return make_ready_future<consumption_result_type>(stop_consuming<char>{std::move(data)});
        } else {
            // We can process the entire buffer (if the consumer wants to).
            auto orig_data_size = data.size();
            _stream_position.position += data.size();
            auto result = process(data);
            return seastar::visit(result, [this, &data, orig_data_size] (proceed value) {
                _remain -= orig_data_size - data.size();
                _stream_position.position -= data.size();
                if (value == proceed::yes) {
                    return make_ready_future<consumption_result_type>(continue_consuming{});
                } else {
                    return make_ready_future<consumption_result_type>(stop_consuming<char>{std::move(data)});
                }
            }, [this, &data, orig_data_size](skip_bytes skip) {
                // we only expect skip_bytes to be used if reader needs to skip beyond the provided buffer
                // otherwise it should just trim_front and proceed as usual
                assert(data.size() == 0);
                _remain -= orig_data_size;
                if (skip.get_value() >= _remain) {
                    _stream_position.position += _remain;
                    _remain = 0;
                    verify_end_state();
                    return make_ready_future<consumption_result_type>(stop_consuming<char>{std::move(data)});
                }
                _stream_position.position += skip.get_value();
                _remain -= skip.get_value();
                return make_ready_future<consumption_result_type>(std::move(skip));
            });
        }
    }

    future<> fast_forward_to(size_t begin, size_t end) {
        assert(begin >= _stream_position.position);
        auto n = begin - _stream_position.position;
        _stream_position.position = begin;

        assert(end >= _stream_position.position);
        _remain = end - _stream_position.position;

        primitive_consumer::reset();
        return _input.skip(n);
    }

    future<> skip_to(size_t begin) {
        return fast_forward_to(begin, _stream_position.position + _remain);
    }

    // Returns the offset of the first byte which has not been consumed yet.
    // When called from state_processor::process_state() invoked by this consumer,
    // returns the offset of the first byte after the buffer passed to process_state().
    uint64_t position() const {
        return _stream_position.position;
    }

    const sstables::reader_position_tracker& reader_position() {
        return _stream_position;
    }

    bool eof() const {
        return _remain == 0;
    }

    future<> close() noexcept {
        return _input.close();
    }
};
}
