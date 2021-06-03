/*
 * Copyright (C) 2019-present ScyllaDB
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

#include "sstables/index_entry.hh"
#include "sstables/promoted_index_blocks_reader.hh"
#include "schema.hh"
#include "log.hh"

namespace sstables {

extern logging::logger sstlog;

// Cursor implementation which incrementally consumes promoted index entries
// from an input stream.
//
// Memory complexity: O(1)
//
// Average cost of first lookup:
//
//    comparisons: O(N)
//    I/O:         O(N)
//
// N = number of index entries
//
class scanning_clustered_index_cursor : public clustered_index_cursor {
    const schema& _s;
    uint64_t _current_pi_idx = 0; // Points to upper bound of the cursor.
    promoted_index_blocks_reader _reader;
    bool _reader_closed = false;
private:
    // Drops blocks which are behind the cursor and are not
    // need by get_info_from_promoted_block() when invoked for _current_pi_idx.
    // Adjusts _current_pi_idx so that it still points to the same entry.
    void trim_blocks() {
        promoted_index_blocks& blocks = _reader.get_pi_blocks();
        // Leave one block before _current_pi_idx.
        auto target_size = blocks.size() - _current_pi_idx + 1;
        while (blocks.size() > target_size) {
            blocks.pop_front();
            --_current_pi_idx;
        }
    }

    // Reads promoted index blocks from the stream until it finds the upper bound
    // for a given position.
    // Returns the index of the element right before the upper bound one.
    future<size_t> get_pi_blocks_until(position_in_partition_view pos) {
        _reader.switch_to_consume_until_mode(pos);
        promoted_index_blocks& blocks = _reader.get_pi_blocks();
        erase_all_but_last_two(blocks);
        return _reader.consume_input().then([this] {
            return _reader.get_current_pi_index();
        });
    }

    // Unconditionally reads the promoted index blocks from the next data buffer
    future<> get_next_pi_blocks() {
        sstlog.trace("scanning_clustered_index_cursor {}: parsing more blocks", fmt::ptr(this));
        promoted_index_blocks& blocks = _reader.get_pi_blocks();
        blocks = promoted_index_blocks{};
        _reader.switch_to_consume_next_mode();
        return _reader.consume_input();
    }

    [[nodiscard]] uint32_t get_total_pi_blocks_count() const {
        return _reader.get_total_num_blocks();
    }

    [[nodiscard]] uint32_t get_read_pi_blocks_count() const {
        return _reader.get_read_num_blocks();
    }

    [[nodiscard]] bool all_blocks_read() const {
        return _reader.get_read_num_blocks() == _reader.get_total_num_blocks();
    }

    [[nodiscard]] promoted_index_blocks& get_pi_blocks() {
        return _reader.get_pi_blocks();
    }

    [[nodiscard]] skip_info get_info_from_promoted_block(const promoted_index_blocks::const_iterator iter,
        const promoted_index_blocks& pi_blocks) {
        auto offset = iter->offset();
        if (iter == pi_blocks.cbegin() || !std::prev(iter)->end_open_marker()) {
            return skip_info{offset, tombstone(), position_in_partition::before_all_clustered_rows()};
        } else {
            auto prev = std::prev(iter);
            // End open marker can be only engaged in SSTables 3.x ('mc' format) and never in ka/la
            auto end_pos = prev->end(_s);
            position_in_partition_view* open_rt_pos = std::get_if<position_in_partition_view>(&end_pos);
            assert(open_rt_pos);
            return skip_info{offset, tombstone(*prev->end_open_marker()), position_in_partition{*open_rt_pos}};
        }
    }
public:
    scanning_clustered_index_cursor(const schema& s,
        reader_permit permit,
        input_stream<char>&& promoted_index_stream,
        uint32_t promoted_index_size,
        uint32_t blocks_count,
        std::optional<column_values_fixed_lengths> clustering_values_fixed_lengths)
        : _s(s)
        , _reader{std::move(permit), std::move(promoted_index_stream), blocks_count, s, 0, promoted_index_size, std::move(clustering_values_fixed_lengths)}
    { }

    future<std::optional<skip_info>> advance_to(position_in_partition_view pos) override {
        const promoted_index_blocks* pi_blocks = &get_pi_blocks();

        if (all_blocks_read() && _current_pi_idx >= pi_blocks->size() - 1) {
            sstlog.trace("scanning_clustered_index_cursor {}: position in current block (all blocks are read)", fmt::ptr(this));
            return make_ready_future<std::optional<skip_info>>(std::nullopt);
        }

        auto cmp_with_start = [pos_cmp = promoted_index_block_compare(_s), this]
            (position_in_partition_view pos, const promoted_index_block& info) -> bool {
            return pos_cmp(pos, info.start(_s));
        };

        if (!pi_blocks->empty() && cmp_with_start(pos, (*pi_blocks)[_current_pi_idx])) {
            sstlog.trace("scanning_clustered_index_cursor {}: position in current block (exact match)", fmt::ptr(this));
            return make_ready_future<std::optional<skip_info>>(std::nullopt);
        }

        auto i = std::upper_bound(pi_blocks->cbegin() + _current_pi_idx, pi_blocks->cend(), pos, cmp_with_start);
        _current_pi_idx = std::distance(pi_blocks->cbegin(), i);
        if (i != pi_blocks->cend() || all_blocks_read()) {
            if (i != pi_blocks->begin()) {
                --i;
            }

            auto info = get_info_from_promoted_block(i, *pi_blocks);
            sstlog.trace("scanning_clustered_index_cursor {}: lower bound skipped to cell, _current_pi_idx={}, offset={}",
                fmt::ptr(this), _current_pi_idx, info.offset);
            return make_ready_future<std::optional<skip_info>>(std::move(info));
        }

        return get_pi_blocks_until(pos).then([this, pi_blocks] (size_t current_pi_idx) {
            _current_pi_idx = current_pi_idx;
            auto i = std::cbegin(*pi_blocks);
            if (_current_pi_idx > 0) {
                std::advance(i, _current_pi_idx - 1);
            }
            auto info = get_info_from_promoted_block(i, *pi_blocks);
            sstlog.trace("scanning_clustered_index_cursor {}: skipped to cell, _current_pi_idx={}, offset={}",
                fmt::ptr(this), _current_pi_idx, info.offset);
            return std::make_optional(std::move(info));
        });
    }

    future<std::optional<offset_in_partition>> probe_upper_bound(position_in_partition_view pos) override {
        if (get_total_pi_blocks_count() == 0) {
            sstlog.trace("scanning_clustered_index_cursor {}: no promoted index", fmt::ptr(this));
            return make_ready_future<std::optional<offset_in_partition>>(std::nullopt);
        }

        if (get_read_pi_blocks_count() == 0) {
            // It is expected that this will not do any IO, since the front of the input stream
            // will be populated from the buffers we got when reading the partition index.
            return get_next_pi_blocks().then([this, pos] {
                return probe_upper_bound(pos);
            });
        }

        auto cmp_with_start = [pos_cmp = promoted_index_block_compare(_s), this]
            (position_in_partition_view pos, const promoted_index_block& info) -> bool {
            return pos_cmp(pos, info.start(_s));
        };

        promoted_index_blocks* pi_blocks = &get_pi_blocks();
        auto i = std::upper_bound(pi_blocks->begin(), pi_blocks->end(), pos, cmp_with_start);
        if (i == pi_blocks->end()) {
            // Give up. We don't want to expend I/O on fetching more entries. probe_upper_bound()
            // is used to determine read-ahead boundary. Doing that accurately may be more
            // expensive that the potential waste due to read-ahead over-reads.
            return make_ready_future<std::optional<offset_in_partition>>(std::nullopt);
        }

        auto pi_index = std::distance(pi_blocks->begin(), i);
        sstlog.trace("scanning_clustered_index_cursor {} upper bound: skipped to cell, pi_idx={}, offset={}", fmt::ptr(this), pi_index, i->offset());
        return make_ready_future<std::optional<offset_in_partition>>(offset_in_partition(i->offset()));
    }

    future<std::optional<entry_info>> next_entry() override {
        promoted_index_blocks& pi_blocks = get_pi_blocks();

        if (pi_blocks.empty() || _current_pi_idx >= pi_blocks.size()) {
            if (all_blocks_read()) {
                return make_ready_future<std::optional<entry_info>>(std::nullopt);
            }
            return get_next_pi_blocks().then([this] {
                return next_entry();
            });
        }

        sstlog.trace("scanning_clustered_index_cursor {}: next_entry(), pi_idx={}", fmt::ptr(this), _current_pi_idx);
        promoted_index_block& block = pi_blocks[_current_pi_idx];
        auto ei = entry_info{block.start(_s), block.end(_s), block.offset()};
        ++_current_pi_idx;
        trim_blocks();
        return make_ready_future<std::optional<entry_info>>(std::move(ei));
    }

    future<> close() noexcept override {
        if (!_reader_closed) {
            _reader_closed = true;
            return _reader.close();
        }

        return make_ready_future<>();
    }
};

}
