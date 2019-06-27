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
#include <functional>
#include <variant>
#include "position_in_partition.hh"
#include "consumer.hh"
#include "types.hh"
#include "column_translation.hh"
#include "m_format_read_helpers.hh"
#include "utils/overloaded_functor.hh"
#include "sstables/mc/parsers.hh"

namespace sstables {

using promoted_index_block_position = std::variant<composite_view, position_in_partition_view>;

class promoted_index_block_compare {
    const position_in_partition::composite_less_compare _cmp;
public:
    explicit promoted_index_block_compare(const schema& s) : _cmp{s} {}

    bool operator()(const promoted_index_block_position& lhs, position_in_partition_view rhs) const {
        return std::visit([this, rhs] (const auto& pos) { return _cmp(pos, rhs); }, lhs);
    }

    bool operator()(position_in_partition_view lhs, const promoted_index_block_position& rhs) const {
        return std::visit([this, lhs] (const auto& pos) { return _cmp(lhs, pos); }, rhs);
    }

    bool operator()(const promoted_index_block_position& lhs, composite_view rhs) const {
        return std::visit([this, rhs] (const auto& pos) { return _cmp(pos, rhs); }, lhs);
    }

    bool operator()(composite_view lhs, const promoted_index_block_position& rhs) const {
        return std::visit([this, lhs] (const auto& pos) { return _cmp(lhs, pos); }, rhs);
    }

    bool operator()(const promoted_index_block_position& lhs, const promoted_index_block_position& rhs) const {
        return std::visit([this, &lhs] (const auto& pos) { return (*this)(lhs, pos); }, rhs);
    }
};

class promoted_index_block {
    /*
     * Block bounds are read and stored differently for ka/la and mc formats.
     * For ka/la formats, we just read and store the whole sequence of bytes representing a 'composite' key,
     * but for 'mc' we need to parse the clustering key prefix entirely along with its bound_kind.
     * So we store them as a discriminated union, aka std::variant.
     * As those representations are used differently for comparing positions in partition,
     * we expose it through a discriminated union of views.
     */
    using bound_storage = std::variant<temporary_buffer<char>, position_in_partition>;
    // The block includes positions in the [_start, _end] range (both bounds inclusive)
    bound_storage _start;
    bound_storage _end;
    uint64_t _offset;
    uint64_t _width;
    std::optional<deletion_time> _end_open_marker;

    inline static
    promoted_index_block_position get_position(const schema& s, const bound_storage& storage) {
        return std::visit(overloaded_functor{
            [&s] (const temporary_buffer<char>& buf) -> promoted_index_block_position {
                return composite_view{to_bytes_view(buf), s.is_compound()}; },
            [] (const position_in_partition& pos) -> promoted_index_block_position {
                return pos;
            }}, storage);
    }

public:
    // Constructor for ka/la format blocks
    promoted_index_block(temporary_buffer<char>&& start, temporary_buffer<char>&& end,
            uint64_t offset, uint64_t width)
        : _start(std::move(start)), _end(std::move(end))
        , _offset(offset), _width(width)
    {}
    // Constructor for mc format blocks
    promoted_index_block(position_in_partition&& start, position_in_partition&& end,
            uint64_t offset, uint64_t width, std::optional<deletion_time>&& end_open_marker)
        : _start{std::move(start)}, _end{std::move(end)}
        , _offset{offset}, _width{width}, _end_open_marker{end_open_marker}
    {}

    promoted_index_block(const promoted_index_block&) = delete;
    promoted_index_block(promoted_index_block&&) noexcept = default;

    promoted_index_block& operator=(const promoted_index_block&) = delete;
    promoted_index_block& operator=(promoted_index_block&&) noexcept = default;

    promoted_index_block_position start(const schema& s) const { return get_position(s, _start);}
    promoted_index_block_position end(const schema& s) const { return get_position(s, _end);}
    uint64_t offset() const { return _offset; }
    uint64_t width() const { return _width; }
    std::optional<deletion_time> end_open_marker() const { return _end_open_marker; }

};

using promoted_index_blocks = seastar::circular_buffer<promoted_index_block>;

inline void erase_all_but_last_two(promoted_index_blocks& pi_blocks) {
    while (pi_blocks.size() > 2) {
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

    promoted_index_blocks _pi_blocks;

    struct k_l_parser_context {
        k_l_parser_context() {};

        temporary_buffer<char> start;
        temporary_buffer<char> end;
        uint64_t offset = 0;
        uint64_t width = 0;

        enum class state {
            START_NAME_LENGTH,
            START_NAME_BYTES,
            END_NAME_LENGTH,
            END_NAME_BYTES,
            OFFSET,
            WIDTH,
            ADD_BLOCK,
        } state = state::START_NAME_LENGTH;
    };

    struct m_parser_context {
        mc::promoted_index_block_parser block_parser;

        m_parser_context(const schema& s, reader_permit permit, column_values_fixed_lengths cvfl)
            : block_parser(s, std::move(permit), std::move(cvfl))
        { }
    };

    std::variant<k_l_parser_context, m_parser_context> _ctx;

    void process_state(temporary_buffer<char>& data, k_l_parser_context& ctx) {
        using state_k_l = typename k_l_parser_context::state;
        // keep running in the loop until we either are out of data or have consumed all the blocks
        while (true) {
            switch (ctx.state) {
            case state_k_l::START_NAME_LENGTH:
                if (this->read_16(data) != continuous_data_consumer::read_status::ready) {
                    ctx.state = state_k_l::START_NAME_BYTES;
                    return;
                }
            case state_k_l::START_NAME_BYTES:
                if (this->read_bytes(data, this->_u16, ctx.start) != continuous_data_consumer::read_status::ready) {
                    ctx.state = state_k_l::END_NAME_LENGTH;
                    return;
                }
            case state_k_l::END_NAME_LENGTH:
                if (this->read_16(data) != continuous_data_consumer::read_status::ready) {
                    ctx.state = state_k_l::END_NAME_BYTES;
                    return;
                }
            case state_k_l::END_NAME_BYTES:
                if (this->read_bytes(data, this->_u16, ctx.end) != continuous_data_consumer::read_status::ready) {
                    ctx.state = state_k_l::OFFSET;
                    return;
                }
            case state_k_l::OFFSET:
                if (this->read_64(data) != continuous_data_consumer::read_status::ready) {
                    ctx.state = state_k_l::WIDTH;
                    return;
                }
            case state_k_l::WIDTH:
                ctx.offset = this->_u64;
                if (this->read_64(data) != continuous_data_consumer::read_status::ready) {
                    ctx.state = state_k_l::ADD_BLOCK;
                    return;
                }
            case state_k_l::ADD_BLOCK:
                ctx.width = this->_u64;
                ctx.state = state_k_l::START_NAME_LENGTH;
                --_num_blocks_left;
                _pi_blocks.emplace_back(std::move(ctx.start), std::move(ctx.end), ctx.offset, ctx.width);
                if (_num_blocks_left == 0) {
                    return;
                }
            }
        }
    }

    void process_state(temporary_buffer<char>& data, m_parser_context& ctx) {
        // keep running in the loop until we either are out of data or have consumed all the blocks
        while (_num_blocks_left) {
            if (ctx.block_parser.consume(data) == read_status::waiting) {
                return;
            }
            _pi_blocks.emplace_back(std::move(ctx.block_parser.start()),
                                    std::move(ctx.block_parser.end()),
                                    ctx.block_parser.offset(),
                                    ctx.block_parser.width(),
                                    std::move(ctx.block_parser.end_open_marker()));
            --_num_blocks_left;
            ctx.block_parser.reset();
        }
    }

public:
    void verify_end_state() const {
        if (_num_blocks_left != 0) {
            throw std::runtime_error("promoted_index_blocks_reader - no more data but parsing is incomplete");
        }
    }

    bool non_consuming(const k_l_parser_context& ctx) const {
        return ctx.state == k_l_parser_context::state::ADD_BLOCK;
    }

    bool non_consuming(const m_parser_context& ctx) const {
        return false;
    }

    bool non_consuming() const {
        return std::visit([this] (const auto& ctx) { return non_consuming(ctx); }, _ctx);
    }

    processing_result process_state(temporary_buffer<char>& data) {
        std::visit([this, &data] (auto& ctx) mutable { return process_state(data, ctx); }, _ctx);

        if (_mode == consuming_mode::consume_until) {
            assert(_pos);
            auto cmp_with_start = [this, pos_cmp = promoted_index_block_compare(_s)]
                    (position_in_partition_view pos, const promoted_index_block& block) -> bool {
                return pos_cmp(pos, block.start(_s));
            };
            auto i = std::upper_bound(std::begin(_pi_blocks), std::end(_pi_blocks), *_pos, cmp_with_start);
            _current_pi_idx = std::distance(std::begin(_pi_blocks), i);
            if ((i != std::end(_pi_blocks)) || (_num_blocks_left == 0)) {
                return proceed::no;
            } else {
                // we need to preserve last two blocks as if the next one we read
                // appears to be the upper bound, we will take the data file position
                // from the previous block and the end open marker, if set, from the one before it
                erase_all_but_last_two(_pi_blocks);
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

    // For the mc format clustering_values_fixed_lengths must be engaged. When not engaged ka/la is assumed.
    promoted_index_blocks_reader(reader_permit permit, input_stream<char>&& promoted_index_stream, uint32_t num_blocks,
                                 const schema& s, uint64_t start, uint64_t maxlen,
                                 std::optional<column_values_fixed_lengths> clustering_values_fixed_lengths)
        : continuous_data_consumer(permit, std::move(promoted_index_stream), start, maxlen)
        , _total_num_blocks{num_blocks}
        , _num_blocks_left{num_blocks}
        , _s{s}
    {
        if (clustering_values_fixed_lengths) {
            _ctx.emplace<m_parser_context>(m_parser_context{s, std::move(permit), std::move(*clustering_values_fixed_lengths)});
        }
    }
};

// Cursor over the index for clustered elements of a single partition.
//
// The user is expected to call advance_to() for monotonically increasing positions
// in order to check if the index has information about more precise location
// of the fragments relevant for the range starting at given position.
//
// The user must serialize all async methods. The next call may start only when the future
// returned by the previous one has resolved.
//
// The user must call close() and wait for it to resolve before destroying.
//
class clustered_index_cursor {
public:
    // Position of indexed elements in the data file realative to the start of the partition.
    using offset_in_partition = uint64_t;

    struct skip_info {
        offset_in_partition offset;
        tombstone active_tombstone;
        position_in_partition active_tombstone_pos;
    };

    struct entry_info {
        promoted_index_block_position start;
        promoted_index_block_position end;
        offset_in_partition offset;
    };

    virtual ~clustered_index_cursor() {};
    virtual future<> close() = 0;

    // Advances the cursor to given position. When the cursor has more accurate information about
    // location of the fragments from the range [pos, +inf) in the data file (since it was last advanced)
    // it resolves with an engaged optional containing skip_info.
    //
    // The index may not be precise, so fragments from the range [pos, +inf) may be located after the
    // position indicated by skip_info. It is guaranteed that no such fragments are located before the returned position.
    //
    // Offsets returned in skip_info are monotonically increasing.
    //
    // Must be called for non-decreasing positions.
    // The caller must ensure that pos remains valid until the future resolves.
    virtual future<std::optional<skip_info>> advance_to(position_in_partition_view pos) = 0;

    // Determines the data file offset relative to the start of the partition such that fragments
    // from the range (-inf, pos] are located before that offset.
    //
    // If such offset cannot be determined in a cheap way, returns a disengaged optional.
    //
    // Does not advance the cursor.
    //
    // The caller must ensure that pos remains valid until the future resolves.
    virtual future<std::optional<offset_in_partition>> probe_upper_bound(position_in_partition_view pos) = 0;

    // Returns skip information about the next position after the cursor
    // or nullopt if there is no information about further positions.
    //
    // When entry_info is returned, the cursor was advanced to entry_info::start.
    virtual future<std::optional<entry_info>> next_entry() = 0;
};

class promoted_index {
    deletion_time _del_time;
    uint32_t _promoted_index_size;
    std::unique_ptr<clustered_index_cursor> _cursor;
    bool _reader_closed = false;
public:
    promoted_index(const schema& s, deletion_time del_time, uint32_t promoted_index_size, std::unique_ptr<clustered_index_cursor> index)
            : _del_time{del_time}
            , _promoted_index_size(promoted_index_size)
            , _cursor(std::move(index))
    { }

    [[nodiscard]] deletion_time get_deletion_time() const { return _del_time; }
    [[nodiscard]] uint32_t get_promoted_index_size() const { return _promoted_index_size; }
    [[nodiscard]] clustered_index_cursor& cursor() { return *_cursor; };
    [[nodiscard]] const clustered_index_cursor& cursor() const { return *_cursor; };
    future<> close_reader() { return _cursor->close(); }
};

class index_entry {
private:
    std::reference_wrapper<const schema> _s;
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
            _token.emplace(_s.get().get_partitioner().get_token(get_key()));
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

    index_entry(const schema& s, temporary_buffer<char>&& key, uint64_t position, std::unique_ptr<promoted_index>&& index)
        : _s(std::cref(s))
        , _key(std::move(key))
        , _position(position)
        , _index(std::move(index))
    {}

    index_entry(index_entry&&) = default;
    index_entry& operator=(index_entry&&) = default;

    // Can be nullptr
    const std::unique_ptr<promoted_index>& get_promoted_index() const { return _index; }
    std::unique_ptr<promoted_index>& get_promoted_index() { return _index; }
    uint32_t get_promoted_index_size() const { return _index ? _index->get_promoted_index_size() : 0; }

    future<> close_pi_stream() {
        if (_index) {
            return _index->close_reader();
        }

        return make_ready_future<>();
    }
};

}

inline std::ostream& operator<<(std::ostream& out, const sstables::promoted_index_block_position& pos) {
    std::visit([&out] (const auto& pos) mutable { out << pos; }, pos);
    return out;
}

