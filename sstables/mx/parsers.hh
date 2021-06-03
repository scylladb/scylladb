/*
 * Copyright (C) 2017-present ScyllaDB
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

#include "sstables/consumer.hh"
#include "sstables/types.hh"
#include "sstables/column_translation.hh"
#include "sstables/m_format_read_helpers.hh"
#include "position_in_partition.hh"

namespace sstables {
namespace mc {

// Incremental parser for the MC-format clustering.
//
// Usage:
//
//   clustering_parser& cp;
//   while (cp.consume(next_buf()) == read_status::waiting) {}
//   position_in_partition pos = cp.get();
//
class clustering_parser {
    const schema& _s;
    column_values_fixed_lengths _clustering_values_fixed_lengths;
    bool _parsing_start_key;
    boost::iterator_range<column_values_fixed_lengths::const_iterator> ck_range;

    std::vector<fragmented_temporary_buffer> clustering_key_values;
    bound_kind_m kind{};

    fragmented_temporary_buffer column_value;
    uint64_t ck_blocks_header = 0;
    uint32_t ck_blocks_header_offset = 0;
    std::optional<position_in_partition> _pos;
    data_consumer::primitive_consumer _primitive;

    enum class state {
        CLUSTERING_START,
        CK_KIND,
        CK_SIZE,
        CK_BLOCK,
        CK_BLOCK_HEADER,
        CK_BLOCK2,
        CK_BLOCK_END,
        DONE
    } _state = state::CLUSTERING_START;

    bool is_block_empty() const {
        return (ck_blocks_header & (uint64_t(1) << (2 * ck_blocks_header_offset))) != 0;
    }

    bool is_block_null() const {
        return (ck_blocks_header & (uint64_t(1) << (2 * ck_blocks_header_offset + 1))) != 0;
    }

    bool no_more_ck_blocks() const { return ck_range.empty(); }

    void move_to_next_ck_block() {
        ck_range.advance_begin(1);
        ++ck_blocks_header_offset;
        if (ck_blocks_header_offset == 32u) {
            ck_blocks_header_offset = 0u;
        }
    }

    bool should_read_block_header() const {
        return ck_blocks_header_offset == 0u;
    }
    std::optional<uint32_t> get_ck_block_value_length() const {
        return ck_range.front();
    }

    position_in_partition make_position() {
        auto key = clustering_key_prefix::from_range(clustering_key_values | boost::adaptors::transformed(
            [] (const fragmented_temporary_buffer& b) { return fragmented_temporary_buffer::view(b); }));

        if (kind == bound_kind_m::clustering) {
            return position_in_partition::for_key(std::move(key));
        }

        bound_kind rt_marker_kind = is_bound_kind(kind)
                                    ? to_bound_kind(kind)
                                    :(_parsing_start_key ? boundary_to_start_bound : boundary_to_end_bound)(kind);
        return position_in_partition(position_in_partition::range_tag_t{}, rt_marker_kind, std::move(key));
    }
public:
    using read_status = data_consumer::read_status;

    clustering_parser(const schema& s, reader_permit permit, column_values_fixed_lengths cvfl, bool parsing_start_key)
        : _s(s)
        , _clustering_values_fixed_lengths(std::move(cvfl))
        , _parsing_start_key(parsing_start_key)
        , _primitive(std::move(permit))
    { }

    // Valid when !active()
    position_in_partition get_and_reset() {
        _state = state::CLUSTERING_START;
        return std::move(*_pos);
    }

    // Feeds the data into the state machine.
    // Returns read_status::ready when !active() after the call.
    read_status consume(temporary_buffer<char>& data) {
        if (_primitive.consume(data) == read_status::waiting) {
            return read_status::waiting;
        }
        switch (_state) {
        case state::DONE:
            return read_status::ready;
        case state::CLUSTERING_START:
            clustering_key_values.clear();
            clustering_key_values.reserve(_clustering_values_fixed_lengths.size());
            ck_range = boost::make_iterator_range(_clustering_values_fixed_lengths);
            ck_blocks_header_offset = 0u;
            if (_primitive.read_8(data) != read_status::ready) {
                _state = state::CK_KIND;
                return read_status::waiting;
            }
        case state::CK_KIND:
            kind = bound_kind_m{_primitive._u8};
            if (kind == bound_kind_m::clustering) {
                _state = state::CK_BLOCK;
                goto ck_block_label;
            }
            if (_primitive.read_16(data) != read_status::ready) {
                _state = state::CK_SIZE;
                return read_status::waiting;
            }
        case state::CK_SIZE:
            if (_primitive._u16 < _s.clustering_key_size()) {
                ck_range.drop_back(_s.clustering_key_size() - _primitive._u16);
            }
        case state::CK_BLOCK:
        ck_block_label:
            if (no_more_ck_blocks()) {
                _pos = make_position();
                _state = state::DONE;
                return read_status::ready;
            }
            if (!should_read_block_header()) {
                _state = state::CK_BLOCK2;
                goto ck_block2_label;
            }
            if (_primitive.read_unsigned_vint(data) != read_status::ready) {
                _state = state::CK_BLOCK_HEADER;
                return read_status::waiting;
            }
        case state::CK_BLOCK_HEADER:
            ck_blocks_header = _primitive._u64;
        case state::CK_BLOCK2:
        ck_block2_label:
        {
            if (is_block_empty()) {
                clustering_key_values.push_back({});
                move_to_next_ck_block();
                goto ck_block_label;
            }
            if (is_block_null()) {
                move_to_next_ck_block();
                goto ck_block_label;
            }
            read_status status = read_status::waiting;
            if (auto len = get_ck_block_value_length()) {
                status = _primitive.read_bytes(data, *len, column_value);
            } else {
                status = _primitive.read_unsigned_vint_length_bytes(data, column_value);
            }
            if (status != read_status::ready) {
                _state = state::CK_BLOCK_END;
                return read_status::waiting;
            }
        }
        case state::CK_BLOCK_END:
            clustering_key_values.push_back(std::move(column_value));
            move_to_next_ck_block();
            _state = state::CK_BLOCK;
            goto ck_block_label;
        }
        abort(); // unreachable
    }

    bool active() const {
        return _state != state::DONE;
    }

    void set_parsing_start_key(bool parsing_start_key) {
        _parsing_start_key = parsing_start_key;
    }

    void reset() {
        _state = state::CLUSTERING_START;
    }
};

class promoted_index_block_parser {
    clustering_parser _clustering;

    std::optional<position_in_partition> _start_pos;
    std::optional<position_in_partition> _end_pos;
    std::optional<deletion_time> _end_open_marker;

    uint64_t _offset{};
    uint64_t _width{};

    enum class state {
        START,
        END,
        OFFSET,
        WIDTH,
        END_OPEN_MARKER_FLAG,
        END_OPEN_MARKER_LOCAL_DELETION_TIME,
        END_OPEN_MARKER_MARKED_FOR_DELETE_AT_1,
        END_OPEN_MARKER_MARKED_FOR_DELETE_AT_2,
        DONE,
    } _state = state::START;

    data_consumer::primitive_consumer _primitive;
public:
    using read_status = data_consumer::read_status;

    promoted_index_block_parser(const schema& s, reader_permit permit, column_values_fixed_lengths cvfl)
        : _clustering(s, permit, std::move(cvfl), true)
        , _primitive(permit)
    { }

    position_in_partition& start() { return *_start_pos; }
    position_in_partition& end() { return *_end_pos; }
    std::optional<deletion_time>& end_open_marker() { return _end_open_marker; }
    uint64_t offset() const { return _offset; }
    uint64_t width() const { return _width; }

    // Feeds the data into the state machine.
    // Returns read_status::ready when whole block was parsed.
    // If returns read_status::waiting then data.empty() after the call.
    read_status consume(temporary_buffer<char>& data) {
        static constexpr size_t width_base = 65536;
        if (_primitive.consume(data) == read_status::waiting) {
            return read_status::waiting;
        }
        switch (_state) {
        case state::DONE:
            return read_status::ready;
        case state::START:
            if (_clustering.consume(data) == read_status::waiting) {
                return read_status::waiting;
            }
            _start_pos = _clustering.get_and_reset();
            _clustering.set_parsing_start_key(false);
            _state = state::END;
            // fall-through
        case state::END:
            if (_clustering.consume(data) == read_status::waiting) {
                return read_status::waiting;
            }
            _end_pos = _clustering.get_and_reset();
            _state = state::OFFSET;
            // fall-through
        case state::OFFSET:
            if (_primitive.read_unsigned_vint(data) != read_status::ready) {
                _state = state::WIDTH;
                return read_status::waiting;
            }
        case state::WIDTH:
            _offset = _primitive._u64;
            if (_primitive.read_signed_vint(data) != read_status::ready) {
                _state = state::END_OPEN_MARKER_FLAG;
                return read_status::waiting;
            }
        case state::END_OPEN_MARKER_FLAG:
            assert(_primitive._i64 + width_base > 0);
            _width = (_primitive._i64 + width_base);
            if (_primitive.read_8(data) != read_status::ready) {
                _state = state::END_OPEN_MARKER_LOCAL_DELETION_TIME;
                return read_status::waiting;
            }
        case state::END_OPEN_MARKER_LOCAL_DELETION_TIME:
            if (_primitive._u8 == 0) {
                _state = state::DONE;
                return read_status::ready;
            }
            _end_open_marker.emplace();
            if (_primitive.read_32(data) != read_status::ready) {
                _state = state::END_OPEN_MARKER_MARKED_FOR_DELETE_AT_1;
                return read_status::waiting;
            }
        case state::END_OPEN_MARKER_MARKED_FOR_DELETE_AT_1:
            _end_open_marker->local_deletion_time = _primitive._u32;
            if (_primitive.read_64(data) != read_status::ready) {
                _state = state::END_OPEN_MARKER_MARKED_FOR_DELETE_AT_2;
                return read_status::waiting;
            }
        case state::END_OPEN_MARKER_MARKED_FOR_DELETE_AT_2:
            _end_open_marker->marked_for_delete_at = _primitive._u64;
            _state = state::DONE;
            return read_status::ready;
        }
        abort();
    }

    void reset() {
        _end_open_marker.reset();
        _clustering.set_parsing_start_key(true);
        _state = state::START;
    }
};

}
}
