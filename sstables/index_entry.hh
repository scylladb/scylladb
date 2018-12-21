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
#include <variant>
#include "position_in_partition.hh"
#include "consumer.hh"
#include "types.hh"
#include "column_translation.hh"
#include "m_format_read_helpers.hh"
#include "utils/overloaded_functor.hh"

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
        column_values_fixed_lengths clustering_values_fixed_lengths;
        bool parsing_start_key = true;
        boost::iterator_range<column_values_fixed_lengths::const_iterator> ck_range;

        std::vector<temporary_buffer<char>> clustering_key_values;
        bound_kind_m kind;

        temporary_buffer<char> column_value;
        uint64_t ck_blocks_header = 0;
        uint32_t ck_blocks_header_offset = 0;
        std::optional<position_in_partition> start_pos;
        std::optional<position_in_partition> end_pos;
        std::optional<deletion_time> end_open_marker;

        uint64_t offset;
        uint64_t width;

        enum class state {
            CLUSTERING_START,
            CK_KIND,
            CK_SIZE,
            CK_BLOCK,
            CK_BLOCK_HEADER,
            CK_BLOCK2,
            CK_BLOCK_END,
            ADD_CLUSTERING_KEY,
            OFFSET,
            WIDTH,
            END_OPEN_MARKER_FLAG,
            END_OPEN_MARKER_LOCAL_DELETION_TIME,
            END_OPEN_MARKER_MARKED_FOR_DELETE_AT_1,
            END_OPEN_MARKER_MARKED_FOR_DELETE_AT_2,
            ADD_BLOCK,
        } state = state::CLUSTERING_START;

        bool is_block_empty() {
            return (ck_blocks_header & (uint64_t(1) << (2 * ck_blocks_header_offset))) != 0;
        }

        bool is_block_null() {
            return (ck_blocks_header & (uint64_t(1) << (2 * ck_blocks_header_offset + 1))) != 0;
        }

        bool no_more_ck_blocks() { return ck_range.empty(); }

        void move_to_next_ck_block() {
            ck_range.advance_begin(1);
            ++ck_blocks_header_offset;
            if (ck_blocks_header_offset == 32u) {
                ck_blocks_header_offset = 0u;
            }
        }

        bool should_read_block_header() {
            return ck_blocks_header_offset == 0u;
        }
        std::optional<uint32_t> get_ck_block_value_length() {
            return ck_range.front();
        }

        position_in_partition make_position() {
            auto key = clustering_key_prefix::from_range(clustering_key_values | boost::adaptors::transformed(
                [] (const temporary_buffer<char>& b) { return to_bytes_view(b); }));

            if (kind == bound_kind_m::clustering) {
                return position_in_partition::for_key(std::move(key));
            }

            bound_kind rt_marker_kind = is_in_bound_kind(kind)
                    ? to_bound_kind(kind)
                    :(parsing_start_key ? boundary_to_start_bound : boundary_to_end_bound)(kind);
            return position_in_partition(position_in_partition::range_tag_t{}, rt_marker_kind, std::move(key));
        }
    };

    std::variant<k_l_parser_context, m_parser_context> _ctx;

    void process_state(temporary_buffer<char>& data, k_l_parser_context& ctx) {
        using state_k_l = typename k_l_parser_context::state;
        while (true) {
            switch (ctx.state) {
            case state_k_l::START_NAME_LENGTH:
                if (this->read_16(data) != continuous_data_consumer::read_status::ready) {
                    ctx.state = state_k_l::START_NAME_BYTES;
                    break;
                }
            case state_k_l::START_NAME_BYTES:
                if (this->read_bytes(data, this->_u16, ctx.start) != continuous_data_consumer::read_status::ready) {
                    ctx.state = state_k_l::END_NAME_LENGTH;
                    break;
                }
            case state_k_l::END_NAME_LENGTH:
                if (this->read_16(data) != continuous_data_consumer::read_status::ready) {
                    ctx.state = state_k_l::END_NAME_BYTES;
                    break;
                }
            case state_k_l::END_NAME_BYTES:
                if (this->read_bytes(data, this->_u16, ctx.end) != continuous_data_consumer::read_status::ready) {
                    ctx.state = state_k_l::OFFSET;
                    break;
                }
            case state_k_l::OFFSET:
                if (this->read_64(data) != continuous_data_consumer::read_status::ready) {
                    ctx.state = state_k_l::WIDTH;
                    break;
                }
            case state_k_l::WIDTH:
                ctx.offset = this->_u64;
                if (this->read_64(data) != continuous_data_consumer::read_status::ready) {
                    ctx.state = state_k_l::ADD_BLOCK;
                    break;
                }
            case state_k_l::ADD_BLOCK:
                ctx.width = this->_u64;
                ctx.state = state_k_l::START_NAME_LENGTH;
                --_num_blocks_left;
                _pi_blocks.emplace_back(std::move(ctx.start), std::move(ctx.end), ctx.offset, ctx.width);
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
    }

    void process_state(temporary_buffer<char>& data, m_parser_context& ctx) {
        static constexpr size_t width_base = 65536;
        using state_m = typename m_parser_context::state;
        while (true) {
            switch (ctx.state) {
            case state_m::CLUSTERING_START:
            clustering_start_label:
                ctx.clustering_key_values.clear();
                ctx.clustering_key_values.reserve(ctx.clustering_values_fixed_lengths.size());
                ctx.ck_range = boost::make_iterator_range(ctx.clustering_values_fixed_lengths);
                ctx.ck_blocks_header_offset = 0u;
                if (read_8(data) != read_status::ready) {
                    ctx.state = state_m::CK_KIND;
                    break;
                }
            case state_m::CK_KIND:
                ctx.kind = bound_kind_m{_u8};
                if (ctx.kind == bound_kind_m::clustering) {
                    ctx.state = state_m::CK_BLOCK;
                    goto ck_block_label;
                }
                if (read_16(data) != read_status::ready) {
                    ctx.state = state_m::CK_SIZE;
                    return;
                }
            case state_m::CK_SIZE:
                if (_u16 < _s.clustering_key_size()) {
                    ctx.ck_range.drop_back(_s.clustering_key_size() - _u16);
                }

            case state_m::CK_BLOCK:
            ck_block_label:
                if (ctx.no_more_ck_blocks()) {
                    goto add_clustering_key_label;
                }
                if (!ctx.should_read_block_header()) {
                    ctx.state = state_m::CK_BLOCK2;
                    goto ck_block2_label;
                }
                if (read_unsigned_vint(data) != read_status::ready) {
                    ctx.state = state_m::CK_BLOCK_HEADER;
                    break;
                }
            case state_m::CK_BLOCK_HEADER:
                ctx.ck_blocks_header = _u64;
            case state_m::CK_BLOCK2:
            ck_block2_label:
            {
                if (ctx.is_block_empty()) {
                    ctx.clustering_key_values.push_back({});
                    ctx.move_to_next_ck_block();
                    goto ck_block_label;
                }
                if (ctx.is_block_null()) {
                    ctx.move_to_next_ck_block();
                    goto ck_block_label;
                }
                read_status status = read_status::waiting;
                if (auto len = ctx.get_ck_block_value_length()) {
                    status = read_bytes(data, *len, ctx.column_value);
                } else {
                    status = read_unsigned_vint_length_bytes(data, ctx.column_value);
                }
                if (status != read_status::ready) {
                    ctx.state = state_m::CK_BLOCK_END;
                    break;
                }
            }
            case state_m::CK_BLOCK_END:
                ctx.clustering_key_values.push_back(std::move(ctx.column_value));
                ctx.move_to_next_ck_block();
                ctx.state = state_m::CK_BLOCK;
                goto ck_block_label;
            case state_m::ADD_CLUSTERING_KEY:
            add_clustering_key_label:
                (ctx.parsing_start_key ? ctx.start_pos : ctx.end_pos) = ctx.make_position();
                ctx.parsing_start_key = !ctx.parsing_start_key;
                if (!ctx.end_pos) {
                    ctx.state = state_m::CLUSTERING_START;
                    goto clustering_start_label;
                }
            case state_m::OFFSET:
                if (read_unsigned_vint(data) != continuous_data_consumer::read_status::ready) {
                    ctx.state = state_m::WIDTH;
                    break;
                }
            case state_m::WIDTH:
                ctx.offset = _u64;
                if (read_signed_vint(data) != continuous_data_consumer::read_status::ready) {
                    ctx.state = state_m::END_OPEN_MARKER_FLAG;
                    break;
                }
            case state_m::END_OPEN_MARKER_FLAG:
                assert(_i64 + width_base > 0);
                ctx.width = (_i64 + width_base);
                if (read_8(data) != continuous_data_consumer::read_status::ready) {
                    ctx.state = state_m::END_OPEN_MARKER_LOCAL_DELETION_TIME;
                    break;
                }
            case state_m::END_OPEN_MARKER_LOCAL_DELETION_TIME:
                if (_u8 == 0) {
                    ctx.state = state_m::ADD_BLOCK;
                    goto add_block_label;
                }
                ctx.end_open_marker.emplace();
                if (read_32(data) != continuous_data_consumer::read_status::ready) {
                    ctx.state = state_m::END_OPEN_MARKER_MARKED_FOR_DELETE_AT_1;
                    break;
                }
            case state_m::END_OPEN_MARKER_MARKED_FOR_DELETE_AT_1:
                ctx.end_open_marker->local_deletion_time = _u32;
                if (read_64(data) != continuous_data_consumer::read_status::ready) {
                    ctx.state = state_m::END_OPEN_MARKER_MARKED_FOR_DELETE_AT_2;
                    break;
                }
            case state_m::END_OPEN_MARKER_MARKED_FOR_DELETE_AT_2:
                ctx.end_open_marker->marked_for_delete_at = _u64;
            case m_parser_context::state::ADD_BLOCK:
            add_block_label:

                _pi_blocks.emplace_back(*std::exchange(ctx.start_pos, {}),
                                        *std::exchange(ctx.end_pos, {}),
                                        ctx.offset,
                                        ctx.width,
                                        std::exchange(ctx.end_open_marker, {}));
                ctx.state = state_m::CLUSTERING_START;
                --_num_blocks_left;
                if (_num_blocks_left == 0) {
                    break;
                } else {
                    // keep running in the loop until we either are out of data
                    // or have consumed all the blocks
                    continue;
                }
            }
            break;
        }
    }

public:
    void verify_end_state() {
        if (_num_blocks_left != 0) {
            throw std::runtime_error("promoted_index_blocks_reader - no more data but parsing is incomplete");
        }
    }

    bool non_consuming(const k_l_parser_context& ctx) const {
        return ctx.state == k_l_parser_context::state::ADD_BLOCK;
    }

    bool non_consuming(const m_parser_context& ctx) const {
        using state_m = typename m_parser_context::state;
        return ctx.state == state_m::CK_SIZE
                || ctx.state == state_m::CK_BLOCK_HEADER
                || ctx.state == state_m::CK_BLOCK_END
                || ctx.state == state_m::ADD_CLUSTERING_KEY
                || ctx.state == state_m::END_OPEN_MARKER_MARKED_FOR_DELETE_AT_2
                || ctx.state == state_m::ADD_BLOCK;
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

    // This constructor is used for ka/la format which does not have information about columns fixed lengths
    promoted_index_blocks_reader(input_stream<char>&& promoted_index_stream, uint32_t num_blocks,
                                 const schema& s, uint64_t start, uint64_t maxlen)
        : continuous_data_consumer(std::move(promoted_index_stream), start, maxlen)
        , _total_num_blocks(num_blocks)
        , _num_blocks_left(num_blocks)
        , _s(s)
    {}

    // This constructor is used for mc format which requires information about columns fixed lengths for parsing
    promoted_index_blocks_reader(input_stream<char>&& promoted_index_stream, uint32_t num_blocks,
                                 const schema& s, uint64_t start, uint64_t maxlen,
                                 column_values_fixed_lengths&& clustering_values_fixed_lengths)
        : continuous_data_consumer(std::move(promoted_index_stream), start, maxlen)
        , _total_num_blocks{num_blocks}
        , _num_blocks_left{num_blocks}
        , _s{s}
        , _ctx{m_parser_context{std::move(clustering_values_fixed_lengths)}}
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

    promoted_index(const schema& s, deletion_time del_time, input_stream<char>&& promoted_index_stream,
                   uint32_t promoted_index_size, uint32_t blocks_count, column_values_fixed_lengths clustering_values_fixed_lengths)
            : _del_time{del_time}
            , _promoted_index_size(promoted_index_size)
            , _reader{std::move(promoted_index_stream), blocks_count, s, 0, promoted_index_size, std::move(clustering_values_fixed_lengths)}
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
        erase_all_but_last_two(blocks);
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

inline std::ostream& operator<<(std::ostream& out, const sstables::promoted_index_block_position& pos) {
    std::visit([&out] (const auto& pos) mutable { out << pos; }, pos);
    return out;
}

