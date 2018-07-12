/*
 * Copyright (C) 2017 ScyllaDB
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
#include "consumer.hh"
#include "types.hh"
#include <boost/variant.hpp>
#include <seastar/util/variant_utils.hh>

namespace sstables {

class promoted_index_block {
public:
    promoted_index_block(temporary_buffer<char>&& start, temporary_buffer<char>&& end,
            uint64_t offset, uint64_t width)
        : _start(std::move(start)), _end(std::move(end))
        , _offset(offset), _width(width)
    {}
    promoted_index_block(const promoted_index_block& rhs)
        : _start(rhs._start.get(), rhs._start.size()), _end(rhs._end.get(), rhs._end.size())
        , _offset(rhs._offset), _width(rhs._width)
    {}
    promoted_index_block(promoted_index_block&&) noexcept = default;

    promoted_index_block& operator=(const promoted_index_block& rhs) {
        if (this != &rhs) {
            _start = temporary_buffer<char>(rhs._start.get(), rhs._start.size());
            _end = temporary_buffer<char>(rhs._end.get(), rhs._end.size());
            _offset = rhs._offset;
            _width = rhs._width;
        }
        return *this;
    }
    promoted_index_block& operator=(promoted_index_block&&) noexcept = default;

    composite_view start(const schema& s) const { return composite_view(to_bytes_view(_start), s.is_compound());}
    composite_view end(const schema& s) const { return composite_view(to_bytes_view(_end), s.is_compound());}
    uint64_t offset() const { return _offset; }
    uint64_t width() const { return _width; }

private:
    temporary_buffer<char> _start;
    temporary_buffer<char> _end;
    uint64_t _offset;
    uint64_t _width;
};

using promoted_index_blocks = seastar::circular_buffer<promoted_index_block>;

inline void erase_all_but_last(promoted_index_blocks& pi_blocks) {
    while (pi_blocks.size() > 1) {
        pi_blocks.pop_front();
    }
}

// promoted_index_blocks_reader parses the promoted index blocks from the provided stream.
// It has two operational modes:
//   1. consume_until - in this mode, a position is provided and the reader will read & parse
//      buffer by buffer until it either finds the upper bound for the given position or exhausts the stream
//   2. consume_next - in this mode, the reader unconditionally reads & parses the next buffer and stops
//
class promoted_index_blocks_reader : public data_consumer::continuous_data_consumer<promoted_index_blocks_reader> {
    using proceed = data_consumer::proceed;
    using processing_result = data_consumer::processing_result;
    using continuous_data_consumer = data_consumer::continuous_data_consumer<promoted_index_blocks_reader>;

private:
    enum class consuming_mode {
        consume_until, // reads/parses buffers until finds an upper bound block for given position
        consume_next,  // reads/parses the next buffer from stream and stops unconditionally
    };

    uint32_t _total_num_blocks; // the total number of blocks in the stream
    uint32_t _num_blocks_left; // the number of unread blocks left in the stream
    const schema& _s;
    consuming_mode _mode = consuming_mode::consume_next;
    size_t _current_pi_idx = 0; // for consume_until mode
    std::optional<position_in_partition_view> _pos; // for consume_until mode

    enum class state {
        START_NAME_LENGTH,
        START_NAME_BYTES,
        END_NAME_LENGTH,
        END_NAME_BYTES,
        OFFSET,
        WIDTH,
        ADD_BLOCK,
    } _state = state::START_NAME_LENGTH;

    temporary_buffer<char> _start;
    temporary_buffer<char> _end;
    uint64_t _offset;
    uint64_t _width;

    promoted_index_blocks _pi_blocks;

public:
    void verify_end_state() {
        if (_num_blocks_left != 0) {
            throw std::runtime_error("promoted_index_blocks_reader - no more data but parsing is incomplete");
        }
    }

    bool non_consuming() const {
        return (_state == state::ADD_BLOCK);
    }

    processing_result process_state(temporary_buffer<char>& data) {
        while (true) {
            switch (_state) {
            case state::START_NAME_LENGTH:
                if (this->read_16(data) != continuous_data_consumer::read_status::ready) {
                    _state = state::START_NAME_BYTES;
                    break;
                }
            case state::START_NAME_BYTES:
                if (this->read_bytes(data, this->_u16, _start) != continuous_data_consumer::read_status::ready) {
                    _state = state::END_NAME_LENGTH;
                    break;
                }
            case state::END_NAME_LENGTH:
                if (this->read_16(data) != continuous_data_consumer::read_status::ready) {
                    _state = state::END_NAME_BYTES;
                    break;
                }
            case state::END_NAME_BYTES:
                if (this->read_bytes(data, this->_u16, _end) != continuous_data_consumer::read_status::ready) {
                    _state = state::OFFSET;
                    break;
                }
            case state::OFFSET:
                if (this->read_64(data) != continuous_data_consumer::read_status::ready) {
                    _state = state::WIDTH;
                    break;
                }
            case state::WIDTH:
                _offset = this->_u64;
                if (this->read_64(data) != continuous_data_consumer::read_status::ready) {
                    _state = state::ADD_BLOCK;
                    break;
                }
            case state::ADD_BLOCK:
                _width = this->_u64;
                _state = state::START_NAME_LENGTH;
                --_num_blocks_left;
                _pi_blocks.emplace_back(std::move(_start), std::move(_end), _offset, _width);
                if (_num_blocks_left == 0) {
                    break;
                } else {
                    // keep running in the loop until we either are out of data
                    // or have consumed all the blocks
                    continue;
                }
            default:
                throw malformed_sstable_exception("unknown state");
            }
            break;
        };

        if (_mode == consuming_mode::consume_until) {
            assert(_pos);
            auto cmp_with_start = [this, pos_cmp = position_in_partition::composite_less_compare(_s)]
                    (position_in_partition_view pos, const promoted_index_block& block) -> bool {
                return pos_cmp(pos, block.start(_s));
            };
            auto i = std::upper_bound(std::begin(_pi_blocks), std::end(_pi_blocks), *_pos, cmp_with_start);
            _current_pi_idx = std::distance(std::begin(_pi_blocks), i);
            if ((i != std::end(_pi_blocks)) || (_num_blocks_left == 0)) {
                return proceed::no;
            } else {
                // we need to preserve the last block as if the next one we read
                // appears to be the upper bound, we will take the data file position
                // from the previous block
                erase_all_but_last(_pi_blocks);
            }
        }

        return (_mode == consuming_mode::consume_next) ? proceed::no : proceed::yes;
    }

    uint32_t get_total_num_blocks() const { return _total_num_blocks; }
    uint32_t get_read_num_blocks() const { return _total_num_blocks - _num_blocks_left; }
    size_t get_current_pi_index() const { return _current_pi_idx; }
    void switch_to_consume_next_mode() { _mode = consuming_mode::consume_next; }
    void switch_to_consume_until_mode(position_in_partition_view pos) { _pos = pos; _mode = consuming_mode::consume_until; }
    promoted_index_blocks& get_pi_blocks() { return _pi_blocks; };

    promoted_index_blocks_reader(input_stream<char>&& promoted_index_stream, uint32_t num_blocks, const schema& s, uint64_t start, uint64_t maxlen)
        : continuous_data_consumer(std::move(promoted_index_stream), start, maxlen)
        , _total_num_blocks(num_blocks)
        , _num_blocks_left(num_blocks)
        , _s(s)
    {}
};

class promoted_index {
    deletion_time _del_time;
    uint32_t _promoted_index_size;
    promoted_index_blocks_reader _reader;
    bool _reader_closed = false;

public:
    promoted_index(const schema& s, deletion_time del_time, input_stream<char>&& promoted_index_stream,
                   uint32_t promoted_index_size, uint32_t blocks_count)
            : _del_time{del_time}
            , _promoted_index_size(promoted_index_size)
            , _reader{std::move(promoted_index_stream), blocks_count, s, 0, promoted_index_size}
    {}

    [[nodiscard]] deletion_time get_deletion_time() const { return _del_time; }
    [[nodiscard]] uint32_t get_promoted_index_size() const { return _promoted_index_size; }
    [[nodiscard]] promoted_index_blocks_reader& get_reader() { return _reader; };
    [[nodiscard]] const promoted_index_blocks_reader& get_reader() const { return _reader; };
    future<> close_reader() {
        if (!_reader_closed) {
            _reader_closed = true;
            return _reader.close();
        }

        return make_ready_future<>();
    }
};

class index_entry {
private:
    temporary_buffer<char> _key;
    mutable std::optional<dht::token> _token;
    uint64_t _position;
    std::unique_ptr<promoted_index> _index;

public:

    bytes_view get_key_bytes() const {
        return to_bytes_view(_key);
    }

    key_view get_key() const {
        return key_view{get_key_bytes()};
    }

    decorated_key_view get_decorated_key() const {
        if (!_token) {
            _token.emplace(dht::global_partitioner().get_token(get_key()));
        }
        return decorated_key_view(*_token, get_key());
    }

    uint64_t position() const { return _position; };

    std::optional<deletion_time> get_deletion_time() const {
        if (_index) {
            return _index->get_deletion_time();
        }

        return {};
    }

    uint32_t get_promoted_index_size() const { return _index ? _index->get_promoted_index_size() : 0; }

    index_entry(temporary_buffer<char>&& key, uint64_t position, std::unique_ptr<promoted_index>&& index)
        : _key(std::move(key))
        , _position(position)
        , _index(std::move(index))
    {}

    index_entry(index_entry&&) = default;
    index_entry& operator=(index_entry&&) = default;

    // Reads promoted index blocks from the stream until it finds the upper bound
    // for a given position.
    // Returns the index of the element right before the upper bound one.
    future<size_t> get_pi_blocks_until(position_in_partition_view pos) {
        if (!_index) {
            return make_ready_future<size_t>(0);
        }

        auto& reader = _index->get_reader();
        reader.switch_to_consume_until_mode(pos);
        promoted_index_blocks& blocks = reader.get_pi_blocks();
        if (!blocks.empty()) {
            erase_all_but_last(blocks);
        }
        return reader.consume_input().then([this, &reader] {
            return reader.get_current_pi_index();
        });
    }

    // Unconditionally reads the promoted index blocks from the next data buffer
    future<> get_next_pi_blocks() {
        if (!_index) {
            return make_ready_future<>();
        }

        auto& reader = _index->get_reader();
        promoted_index_blocks& blocks = reader.get_pi_blocks();
        blocks = promoted_index_blocks{};
        reader.switch_to_consume_next_mode();
        return reader.consume_input();
    }

    [[nodiscard]] uint32_t get_total_pi_blocks_count() const {
        return _index ? _index->get_reader().get_total_num_blocks() : 0;
    }
    [[nodiscard]] uint32_t get_read_pi_blocks_count() const {
        return _index ? _index->get_reader().get_read_num_blocks() : 0;
    }
    [[nodiscard]] promoted_index_blocks* get_pi_blocks() {
        return _index ? &_index->get_reader().get_pi_blocks() : nullptr;
    }
    future<> close_pi_stream() {
        if (_index) {
            return _index->close_reader();
        }

        return make_ready_future<>();
    }
};

}

