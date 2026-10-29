/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "vint-serialization.hh"
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/util/noncopyable_function.hh>
#include "sstables/progress_monitor.hh"
#include <seastar/core/byteorder.hh>
#include <seastar/util/variant_utils.hh>
#include <seastar/net/byteorder.hh>
#include "bytes.hh"
#include "reader_permit.hh"
#include "utils/fragmented_temporary_buffer.hh"
#include "utils/small_vector.hh"
#include "exceptions.hh"

#include <variant>

template<typename T, ContiguousSharedBuffer Buffer>
inline T consume_be(Buffer& p) {
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
template<ContiguousSharedBuffer Buffer>
class primitive_consumer_impl {
    using FragmentedBuffer = basic_fragmented_buffer<Buffer>;
private:
    // state machine progress:
    enum class prestate {
        NONE,
        READING_U8,
        READING_U16,
        READING_U32,
        READING_U56,
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
    temporary_buffer<char> _read_bytes_buf; // for contiguous reading.
    utils::small_vector<Buffer, 1> _read_bytes;
    temporary_buffer<char>* _read_bytes_where_contiguous; // which buffer to set, _key, _val, _cell_path or _pk?
    FragmentedBuffer* _read_bytes_where;

    // Alloc-free
    inline read_status read_partial_int(Buffer& data, prestate next_state) noexcept {
        std::copy(data.begin(), data.end(), _read_int.bytes);
        _pos = data.size();
        data.trim(0);
        _prestate = next_state;
        return read_status::waiting;
    }
    inline read_status read_partial_int(prestate next_state) noexcept {
        _pos = 0;
        _prestate = next_state;
        return read_status::waiting;
    }
    template <typename VintType, prestate ReadingVint, prestate ReadingVintWithLen>
    inline read_status read_vint(Buffer& data, typename VintType::value_type& dest) {
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
                _read_bytes_buf = make_new_tracked_temporary_buffer(len, _permit);
                std::copy(data.begin(), data.end(), _read_bytes_buf.get_write());
                _read_bytes_len = len;
                _pos = data.size();
                data.trim(0);
                _prestate = ReadingVintWithLen;
                return read_status::waiting;
            }
        }
    }
    template <typename VintType>
    inline read_status read_vint_with_len(Buffer& data, typename VintType::value_type& dest) {
        const auto n = std::min(_read_bytes_len - _pos, data.size());
        std::copy_n(data.begin(), n, _read_bytes_buf.get_write() + _pos);
        data.trim_front(n);
        _pos += n;
        if (_pos == _read_bytes_len) {
            dest = VintType::deserialize(
                    bytes_view(reinterpret_cast<bytes::value_type*>(_read_bytes_buf.get_write()), _read_bytes_len));
            _prestate = prestate::NONE;
            return read_status::ready;
        }
        return read_status::waiting;
    };
public:
    primitive_consumer_impl(reader_permit permit) : _permit(std::move(permit)) {}

    inline read_status read_8(Buffer& data) {
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
    inline read_status read_16(Buffer& data) {
        if (data.size() >= sizeof(uint16_t)) {
            _u16 = consume_be<uint16_t>(data);
            return read_status::ready;
        } else {
            return read_partial_int(data, prestate::READING_U16);
        }
    }
    // Alloc-free
    inline read_status read_32(Buffer& data) noexcept {
        if (data.size() >= sizeof(uint32_t)) {
            _u32 = consume_be<uint32_t>(data);
            return read_status::ready;
        } else {
            return read_partial_int(data, prestate::READING_U32);
        }
    }
    inline read_status read_32() noexcept {
        return read_partial_int(prestate::READING_U32);
    }
    inline read_status read_56(Buffer& data) {
        if (data.size() >= 7) {
            char buf[8] = {0};
            std::memcpy(buf + 1, data.get(), 7);
            _u64 = read_be<uint64_t>(buf);
            data.trim_front(7);
            return read_status::ready;
        } else {
            return read_partial_int(data, prestate::READING_U56);
        }
    }
    inline read_status read_64(Buffer& data) {
        if (data.size() >= sizeof(uint64_t)) {
            _u64 = consume_be<uint64_t>(data);
            return read_status::ready;
        } else {
            return read_partial_int(data, prestate::READING_U64);
        }
    }
    temporary_buffer<char> share(Buffer& data, uint32_t offset, uint32_t len) {
        if constexpr(std::is_same_v<Buffer, temporary_buffer<char>>) {
            return data.share(offset, len);
        } else {
            auto ret = make_new_tracked_temporary_buffer(len, _permit);
            std::copy(data.begin() + offset, data.begin() + offset + len, ret.get_write());
            return ret;
        }
    }
    inline read_status read_bytes_contiguous(Buffer& data, uint32_t len, temporary_buffer<char>& where) {
        if (data.size() >= len) {
            where = share(data, 0, len);
            data.trim_front(len);
            return read_status::ready;
        } else {
            // copy what we have so far, read the rest later
            _read_bytes_buf = make_new_tracked_temporary_buffer(len, _permit);
            std::copy(data.begin(), data.end(), _read_bytes_buf.get_write());
            _read_bytes_len = len;
            _read_bytes_where_contiguous = &where;
            _pos = data.size();
            data.trim(0);
            _prestate = prestate::READING_BYTES_CONTIGUOUS;
            return read_status::waiting;
        }
    }
    inline read_status read_bytes(Buffer& data, uint32_t len, FragmentedBuffer& where) {
        if (data.size() >= len) {
            auto fragments = std::move(where).release();
            fragments.clear();
            fragments.push_back(data.share(0, len));
            where = FragmentedBuffer(std::move(fragments), len);
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
    inline read_status read_short_length_bytes(Buffer& data, temporary_buffer<char>& where) {
        if (data.size() >= sizeof(uint16_t)) {
            _u16 = consume_be<uint16_t>(data);
        } else {
            _read_bytes_where_contiguous = &where;
            return read_partial_int(data, prestate::READING_U16_BYTES);
        }
        return read_bytes_contiguous(data, uint32_t{_u16}, where);
    }
    inline read_status read_unsigned_vint(Buffer& data) {
        return read_vint<
                unsigned_vint,
                prestate::READING_UNSIGNED_VINT,
                prestate::READING_UNSIGNED_VINT_WITH_LEN>(data, _u64);
    }
    inline read_status read_signed_vint(Buffer& data) {
        return read_vint<
                signed_vint,
                prestate::READING_SIGNED_VINT,
                prestate::READING_SIGNED_VINT_WITH_LEN>(data, _i64);
    }
    inline read_status read_unsigned_vint_length_bytes_contiguous(Buffer& data, temporary_buffer<char>& where) {
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
                _read_bytes_buf = make_new_tracked_temporary_buffer(len, _permit);
                std::copy(data.begin(), data.end(), _read_bytes_buf.get_write());
                _read_bytes_len = len;
                _pos = data.size();
                data.trim(0);
                _read_bytes_where_contiguous = &where;
                _prestate = prestate::READING_UNSIGNED_VINT_LENGTH_BYTES_WITH_LEN_CONTIGUOUS;
                return read_status::waiting;
            }
        }
    }
    inline read_status read_unsigned_vint_length_bytes(Buffer& data, FragmentedBuffer& where) {
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
                _read_bytes_buf = make_new_tracked_temporary_buffer(len, _permit);
                std::copy(data.begin(), data.end(), _read_bytes_buf.get_write());
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
    bool process_int(Buffer& data, unsigned len) {
        sstables::parse_assert(_pos < len);
        auto n = std::min((size_t)(len - _pos), data.size());
        std::copy(data.begin(), data.begin() + n, _read_int.bytes + _pos);
        data.trim_front(n);
        _pos += n;
        return _pos == len;
    }
public:
    read_status consume_u32(Buffer& data) {
        if (process_int(data, sizeof(uint32_t))) {
            _u32 = net::ntoh(_read_int.uint32);
            _prestate = prestate::NONE;
            return read_status::ready;
        }
        return read_status::waiting;
    }

    // Feeds data into the state machine.
    // After the call, when data is not empty then active() can be assumed to be false.
    read_status consume(Buffer& data) {
        if (_prestate == prestate::NONE) [[likely]] {
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
            std::copy_n(data.begin(), n, _read_bytes_buf.get_write() + _pos);
            data.trim_front(n);
            _pos += n;
            if (_pos == _read_bytes_len) {
                _u64 = unsigned_vint::deserialize(
                        bytes_view(reinterpret_cast<bytes::value_type*>(_read_bytes_buf.get_write()), _read_bytes_len));
                if (read_bytes_contiguous(data, _u64, *_read_bytes_where_contiguous) == read_status::ready) {
                    _prestate = prestate::NONE;
                    return read_status::ready;
                }
            }
            break;
        }
        case prestate::READING_UNSIGNED_VINT_LENGTH_BYTES_WITH_LEN: {
            const auto n = std::min(_read_bytes_len - _pos, data.size());
            std::copy_n(data.begin(), n, _read_bytes_buf.get_write() + _pos);
            data.trim_front(n);
            _pos += n;
            if (_pos == _read_bytes_len) {
                _u64 = unsigned_vint::deserialize(
                        bytes_view(reinterpret_cast<bytes::value_type*>(_read_bytes_buf.get_write()), _read_bytes_len));
                if (read_bytes(data, _u64, *_read_bytes_where) == read_status::ready) {
                    _prestate = prestate::NONE;
                    return read_status::ready;
                }
            }
            break;
        }
        case prestate::READING_BYTES_CONTIGUOUS: {
            auto n = std::min(_read_bytes_len - _pos, data.size());
            std::copy(data.begin(), data.begin() + n, _read_bytes_buf.get_write() + _pos);
            data.trim_front(n);
            _pos += n;
            if (_pos == _read_bytes_len) {
                *_read_bytes_where_contiguous = std::move(_read_bytes_buf);
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
                std::vector<Buffer> fragments(std::make_move_iterator(_read_bytes.begin()), std::make_move_iterator(_read_bytes.end()));
                *_read_bytes_where = FragmentedBuffer(std::move(fragments), _read_bytes_len);
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
            return consume_u32(data);
        case prestate::READING_U56:
            if (process_int(data, 7)) {
                _u64 = net::ntoh(_read_int.uint64) >> 8;
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

using primitive_consumer = primitive_consumer_impl<temporary_buffer<char>>;

// The I/O interface for continuous_data_consumer. seastar::input_stream cannot
// report or seek to an sstable_position. This interface can. Thus, the consumer
// can use logical offsets or physical compression-chunk coordinates.
class continuous_data_consumer_input_stream {
    // The position tracker of the implementation. Parsers read the position
    // often, up to once per field, so reading it must not need a virtual call.
    const sstables::reader_position_tracker& _position_tracker;
protected:
    explicit continuous_data_consumer_input_stream(const sstables::reader_position_tracker& tracker) noexcept
        : _position_tracker(tracker) {}
public:
    continuous_data_consumer_input_stream(const continuous_data_consumer_input_stream&) = delete;
    continuous_data_consumer_input_stream& operator=(const continuous_data_consumer_input_stream&) = delete;

    using consumer_one_fn = noncopyable_function<consumption_result<char>(temporary_buffer<char>)>;
    using consumer_fn = noncopyable_function<future<consumption_result<char>>(temporary_buffer<char>)>;

    virtual ~continuous_data_consumer_input_stream() = default;
    // Moves forward to `target`.
    virtual future<> skip_to(sstables::sstable_position target) = 0;
    // Moves forward by `n` uncompressed bytes.
    virtual future<> skip(uint64_t n) = 0;
    virtual future<> close() = 0;
    // Returns the position `offset` uncompressed bytes away from the current
    // position. Physical implementations support this only inside the consumer
    // callback of consume_one(), for offsets <= 0 that stay within the buffer
    // passed to the callback.
    virtual sstables::sstable_position compute_relative_position(int64_t offset) = 0;
    // Sets the position where the stream starts. The consumer calls it before
    // the first read.
    virtual void init_stream_position(sstables::sstable_position start) = 0;
    // The position of the first byte that has not been consumed yet.
    const sstables::reader_position_tracker& stream_position() const noexcept {
        return _position_tracker;
    }

    // Feeds a single buffer to `consumer` (fetching from the stream as needed)
    // and returns its result, so the caller can drive the read loop itself --
    // deciding whether to continue, stop, or skip -- instead of handing control
    // over to the underlying stream's own consume loop. If `end_position` is
    // set, the buffer passed to `consumer` is clipped to that position.
    virtual future<consumption_result<char>> consume_one(std::optional<sstables::sstable_position> end_position, consumer_one_fn consumer) = 0;
};

// The implementation of continuous_data_consumer_input_stream used with
// logically-indexed sstables. Wraps a seastar::input_stream<char>, keeps the
// reader_position_tracker up to date and implements consume_one() on top of the
// underlying stream's consume().
class continuous_data_consumer_seastar_input_stream final : public continuous_data_consumer_input_stream {
    input_stream<char> _input;
    // This stream is always logical. init_stream_position() sets the start.
    sstables::reader_position_tracker _stream_position{.position = sstables::sstable_position::from_logical(0)};
public:
    explicit continuous_data_consumer_seastar_input_stream(input_stream<char> input)
        : continuous_data_consumer_input_stream(_stream_position)
        , _input(std::move(input))
    {}

    future<> skip_to(sstables::sstable_position target) override {
        auto current = _stream_position.position;
        _stream_position.position = target;
        return _input.skip(subtract_positions(target, current));
    }
    future<> skip(uint64_t n) override {
        co_await _input.skip(n);
        apply_position_delta(static_cast<int64_t>(n));
    }

    future<> close() override {
        return _input.close();
    }

    void init_stream_position(sstables::sstable_position start) override {
        _stream_position = sstables::reader_position_tracker{.position = start, .bytes_consumed = 0};
    }

    sstables::sstable_position compute_relative_position(int64_t offset) override {
        return _stream_position.position + sstables::sstable_position_offset::from_logical(offset);
    }

    // Feeds a single buffer to `consumer` (fetching from the stream as needed)
    // and returns its result, so the caller can drive the read loop itself --
    // deciding whether to continue, stop, or skip -- instead of handing control
    // over to the underlying stream's own consume loop. If `end_position` is
    // set, the buffer passed to `consumer` is clipped to that position.
    //
    // Internally this runs the underlying stream's consume(), but forces it to
    // return after a single buffer by always reporting stop_consuming to it.
    future<consumption_result<char>> consume_one(std::optional<sstables::sstable_position> end_position, consumer_one_fn consumer) override {
        consumption_result<char> result = continue_consuming{};
        co_await _input.consume([this, end_position, &result, consumer = std::move(consumer)] (temporary_buffer<char> data) mutable {
            auto original_data = data.share();
            const auto original_size = data.size();
            auto consumer_size = original_size;
            if (end_position) {
                auto buffer_end_position = compute_relative_position(static_cast<int64_t>(original_size));
                if (*end_position <= buffer_end_position) {
                    auto bytes_to_end = subtract_positions(*end_position, _stream_position.position);
                    sstables::parse_assert(bytes_to_end >= 0);
                    consumer_size = static_cast<size_t>(bytes_to_end);
                    data.trim(consumer_size);
                }
            }
            apply_position_delta(static_cast<int64_t>(consumer_size));
            consumption_result<char> r = consumer(std::move(data));
            // Preserve whatever the consumer left unconsumed by handing the
            // equivalent suffix of the original buffer back to the underlying
            // stream, then force that stream to stop so control returns to us
            // after this single buffer.
            temporary_buffer<char> remainder;
            auto consumed_size = consumer_size;
            if (auto* stop = std::get_if<stop_consuming<char>>(&r.get())) {
                const auto consumer_remainder_size = stop->get_buffer().size();
                sstables::parse_assert(consumer_remainder_size <= consumer_size);
                consumed_size -= consumer_remainder_size;
                apply_position_delta(-static_cast<int64_t>(consumer_remainder_size));
            }
            if (consumed_size < original_size) {
                remainder = original_data.share(consumed_size, original_size - consumed_size);
            }
            result = std::move(r);
            return make_ready_future<consumption_result<char>>(stop_consuming<char>{std::move(remainder)});
        });
        co_return std::move(result);
    }

private:
    static int64_t subtract_positions(sstables::sstable_position b, sstables::sstable_position a) {
        return b.to_logical() - a.to_logical();
    }
    void apply_position_delta(int64_t n) {
        _stream_position.bytes_consumed += n;
        _stream_position.position = compute_relative_position(n);
    }
};

template <typename StateProcessor>
class continuous_data_consumer : protected primitive_consumer {
    using proceed = data_consumer::proceed;
    StateProcessor& state_processor() {
        return static_cast<StateProcessor&>(*this);
    };
protected:
    std::unique_ptr<continuous_data_consumer_input_stream> _input;
    // Absolute position of the first byte past the region we care about; a
    // disengaged value means "continue until end of file".
    std::optional<sstables::sstable_position> _end_position;
    std::optional<reader_permit::awaits_guard> _awaits_guard;
    bool _first_invoke = true;
public:
    using read_status = data_consumer::read_status;

    continuous_data_consumer(reader_permit permit, std::unique_ptr<continuous_data_consumer_input_stream> input, sstables::sstable_position start, std::optional<sstables::sstable_position> end)
            : primitive_consumer(std::move(permit))
            , _input(std::move(input))
            , _end_position(end) {
        _input->init_stream_position(start);
    }

    continuous_data_consumer(reader_permit permit, input_stream<char> input, sstables::sstable_position start, std::optional<sstables::sstable_position> end)
            : continuous_data_consumer(std::move(permit), std::make_unique<continuous_data_consumer_seastar_input_stream>(std::move(input)), start, end) {}

    future<> consume_input() {
        // On first invoke we are guaranteed to go to the disk, so mark as
        // blocked unconditionally. On succeeding invokes we mark blocked only
        // right before a fetch (see the bottom of the loop); if the previous
        // call left buffered data behind, consume_one() serves it without going
        // to disk and we correctly stay unblocked.
        if (_first_invoke) {
            _first_invoke = false;
            mark_blocked();
        }
        // Drive the read loop ourselves. Only the buffer-touching core runs
        // inside consume_one(); the surrounding control flow -- interpreting the
        // outcome, dispatching skips, blocking for I/O -- lives out here.
        while (true) {
            bool verify = false;
            auto result = co_await _input->consume_one(_end_position, [this, &verify] (temporary_buffer<char> data) -> consumption_result_type {
                // We got a buffer, so we are no longer waiting on I/O.
                mark_unblocked();
                if (data.empty()) {
                    // End of file.
                    verify = true;
                    return stop_consuming<char>{std::move(data)};
                }
                // We can process the entire buffer (if the state machine wants to).
                auto ret = process(data);
                if (_end_position && position() >= *_end_position) {
                    if (ret == proceed::yes) {
                        verify = true;
                    }
                    return stop_consuming<char>{std::move(data)};
                }
                if (auto* skip = std::get_if<skip_bytes>(&ret)) {
                    // skip_bytes is only used to skip beyond the provided buffer;
                    // otherwise process() just trims and proceeds as usual.
                    sstables::parse_assert(data.size() == 0);
                    return skip_bytes{skip->get_value()};
                }
                if (ret == proceed::yes) {
                    return continue_consuming{};
                }
                return stop_consuming<char>{std::move(data)};
            });
            auto& outcome = result.get();
            if (std::holds_alternative<stop_consuming<char>>(outcome)) {
                if (verify) {
                    verify_end_state();
                }
                break;
            }
            // Both continue_consuming and skip_bytes go on to fetch another
            // buffer, so mark blocked for the upcoming I/O.
            mark_blocked();
            if (auto* skip = std::get_if<skip_bytes>(&outcome)) {
                // The state machine asked to skip past the current buffer; the
                // input stream advances its tracked position with the skip.
                check_skip_within_end(skip->get_value());
                co_await _input->skip(skip->get_value());
            }
        }
    }

    void verify_end_state() {
        state_processor().verify_end_state();
    }

    // In a valid sstable, no skip crosses the end position. The input streams
    // of logical positions treat such a skip as an internal error, so report
    // it here as a malformed sstable. A physical position doesn't give the
    // number of bytes before the end without reading the chunks in between,
    // so physical skips are not checked here.
    void check_skip_within_end(uint64_t n) const {
        if (!_end_position || _end_position->is_physical()) {
            return;
        }
        const auto pos = position();
        const int64_t remaining = _end_position->to_logical() - pos.to_logical();
        if (remaining < 0 || n > uint64_t(remaining)) {
            sstables::throw_malformed_sstable_exception(fmt::format("Skip of {} bytes at position {} crosses the end position {}",
                    n, pos, *_end_position));
        }
    }

    void mark_blocked() {
        _awaits_guard.emplace(_permit);
    }

    void mark_unblocked() {
        _awaits_guard.reset();
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
            if (primitive_consumer::consume(data) == read_status::waiting) [[unlikely]] {
                sstables::parse_assert(data.size() == 0);
                return proceed::yes;
            }
            auto ret = state_processor().process_state(data);
            if (ret != proceed::yes) [[unlikely]] {
                return ret;
            }
        }
        return proceed::yes;
    }

    sstables::sstable_position compute_relative_position(int64_t n) {
        return _input->compute_relative_position(n);
    }

    future<> fast_forward_to_impl(sstables::sstable_position begin, std::optional<sstables::sstable_position> end) {
        sstables::parse_assert(begin >= position());

        sstables::parse_assert(!end || *end >= begin);
        _end_position = end;

        primitive_consumer::reset();
        reader_permit::awaits_guard _{_permit};
        co_await _input->skip_to(begin);
    }

    future<> fast_forward_to(sstables::sstable_position begin, sstables::sstable_position end) {
        return fast_forward_to_impl(begin, end);
    }

    future<> skip_to(sstables::sstable_position begin) {
        return fast_forward_to_impl(begin, _end_position);
    }

    // Returns the position of the first byte which has not been consumed yet.
    // When called from state_processor::process_state() invoked by this consumer,
    // returns the position of the first byte after the buffer passed to process_state().
    sstables::sstable_position position() const {
        return _input->stream_position().position;
    }

    // The number of uncompressed bytes consumed since the start of the stream.
    int64_t bytes_consumed() const {
        return _input->stream_position().bytes_consumed;
    }

    const sstables::reader_position_tracker& reader_position() const {
        return _input->stream_position();
    }

    bool eof() const {
        return _end_position.has_value() && position() >= _end_position.value();
    }

    future<> close() noexcept {
        return _input->close();
    }
};
}
