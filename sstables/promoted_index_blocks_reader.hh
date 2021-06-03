/*
 * Copyright (C) 2021-present ScyllaDB
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
#include "column_translation.hh"
#include "m_format_read_helpers.hh"
#include "sstables/mx/parsers.hh"
#include "sstables/index_entry.hh"

namespace sstables {

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

        enum class state_type {
            START_NAME_LENGTH,
            START_NAME_BYTES,
            END_NAME_LENGTH,
            END_NAME_BYTES,
            OFFSET,
            WIDTH,
            ADD_BLOCK,
        } state = state_type::START_NAME_LENGTH;
    };

    struct m_parser_context {
        mc::promoted_index_block_parser block_parser;

        m_parser_context(const schema& s, reader_permit permit, column_values_fixed_lengths cvfl)
            : block_parser(s, std::move(permit), std::move(cvfl))
        { }
    };

    std::variant<k_l_parser_context, m_parser_context> _ctx;

    void process_state(temporary_buffer<char>& data, k_l_parser_context& ctx) {
        using state_k_l = typename k_l_parser_context::state_type;
        // keep running in the loop until we either are out of data or have consumed all the blocks
        while (true) {
            switch (ctx.state) {
            case state_k_l::START_NAME_LENGTH:
                if (this->read_16(data) != continuous_data_consumer::read_status::ready) {
                    ctx.state = state_k_l::START_NAME_BYTES;
                    return;
                }
            case state_k_l::START_NAME_BYTES:
                if (this->read_bytes_contiguous(data, this->_u16, ctx.start) != continuous_data_consumer::read_status::ready) {
                    ctx.state = state_k_l::END_NAME_LENGTH;
                    return;
                }
            case state_k_l::END_NAME_LENGTH:
                if (this->read_16(data) != continuous_data_consumer::read_status::ready) {
                    ctx.state = state_k_l::END_NAME_BYTES;
                    return;
                }
            case state_k_l::END_NAME_BYTES:
                if (this->read_bytes_contiguous(data, this->_u16, ctx.end) != continuous_data_consumer::read_status::ready) {
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
        return ctx.state == k_l_parser_context::state_type::ADD_BLOCK;
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

} // namespace sstables
