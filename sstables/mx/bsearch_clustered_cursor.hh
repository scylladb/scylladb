/*
 * Copyright (C) 2020-present ScyllaDB
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
#include "sstables/column_translation.hh"
#include "sstables/promoted_index_blocks_reader.hh"
#include "parsers.hh"
#include "schema.hh"
#include "utils/cached_file.hh"

#include <seastar/core/byteorder.hh>
#include <seastar/core/on_internal_error.hh>

#include <optional>

namespace sstables {

extern logging::logger sstlog;

}

namespace sstables::mc {

/// A read-through cache of promoted index entries.
///
/// Designed for a single user. Methods must not be invoked concurrently.
///
/// All methods provide basic exception guarantee.
class cached_promoted_index {
public:
    using pi_index_type = uint32_t;  // promoted index block sequence number, 0 .. _blocks_count
    using pi_offset_type = uint32_t; // Offset into the promoted index region in the index file, relative
                                     // to the start of the promoted index.

    // Can be in one of the three states, with increasing number of fields being valid:
    //
    //  l0) start is not engaged: only index and offset is valid
    //  l1) start is engaged: in addition to the above, start is valid
    //  l2) end is engaged: all fields are valid
    //
    // This is in order to save on CPU by avoiding parsing the whole block during binary search,
    // which only needs the "start" field.
    struct promoted_index_block {
        pi_index_type index;
        pi_offset_type offset;
        std::optional<position_in_partition> start;
        std::optional<position_in_partition> end;
        std::optional<deletion_time> end_open_marker;
        uint64_t data_file_offset;
        uint64_t width;

        promoted_index_block(pi_index_type index, pi_offset_type offset)
            : index(index)
            , offset(offset)
        {}

        bool operator<(const promoted_index_block& other) const { return index < other.index; }
        bool operator==(const promoted_index_block& other) const { return index == other.index; }
        bool operator!=(const promoted_index_block& other) const { return index != other.index; }

        friend std::ostream& operator<<(std::ostream& out, const promoted_index_block& b) {
            return out << "{idx=" << b.index
                << ", offset=" << b.offset
                << ", start=" << b.start
                << ", end=" << b.end
                << ", end_open_marker=" << b.end_open_marker
                << ", datafile_offset=" << b.data_file_offset
                << ", width=" << b.width << "}";
        }

        /// \brief Returns the amount of memory occupied by this object and all its contents.
        size_t memory_usage() const {
            size_t result = sizeof(promoted_index_block);
            if (!start) {
                return result;
            }
            result += start->external_memory_usage();
            if (!end) {
                return result;
            }
            result += end->external_memory_usage();
            return result;
        }
    };

    struct metrics {
        uint64_t hits_l0 = 0; // Number of requests for promoted_index_block in state l0
                              // which didn't have to go to the page cache
        uint64_t hits_l1 = 0; // Number of requests for promoted_index_block in state l1
                              // which didn't have to go to the page cache
        uint64_t hits_l2 = 0; // Number of requests for promoted_index_block in state l2
                              // which didn't have to go to the page cache
        uint64_t misses_l0 = 0; // Number of requests for promoted_index_block in state l0
                                // which didn't have to go to the page cache
        uint64_t misses_l1 = 0; // Number of requests for promoted_index_block in state l1
                                // which didn't have to go to the page cache
        uint64_t misses_l2 = 0; // Number of requests for promoted_index_block in state l2
                                // which didn't have to go to the page cache

        uint64_t evictions = 0; // Number of promoted_index_blocks which got evicted
        uint64_t populations = 0; // Number of promoted_index_blocks which got inserted
        uint64_t block_count = 0; // Number of promoted_index_blocks currently cached
        uint64_t used_bytes = 0; // Number of bytes currently used by promoted_index_blocks
    };

    struct block_comparator {
        using is_transparent = void; // for std::set to see the lower_bound() overload which takes any key.

        const schema& _s;

        bool operator()(const promoted_index_block& lhs, position_in_partition_view rhs) const {
            assert(lhs.start);
            position_in_partition::less_compare less(_s);
            return less(*lhs.start, rhs);
        }

        bool operator()(position_in_partition_view lhs, const promoted_index_block& rhs) const {
            assert(rhs.start);
            position_in_partition::less_compare less(_s);
            return less(lhs, *rhs.start);
        }

        bool operator()(const promoted_index_block& lhs, const promoted_index_block& rhs) const {
            return lhs < rhs;
        }

        bool operator()(const promoted_index_block& lhs, pi_index_type rhs) const {
            return lhs.index < rhs;
        }

        bool operator()(pi_index_type lhs, const promoted_index_block& rhs) const {
            return lhs < rhs.index;
        }
    };
private:
    // Cache of the parsed promoted index blocks.
    //
    // Why have it? We could have cached only at the cached_file level
    // and parse on each get_block_*(). Caching parsed blocks is still
    // useful for implementing upper_bound_cache_only(), which can leverage
    // materialized _blocks to find approximation of the upper bound.
    //
    // Index lookups are I/O bound and amortized by reading from the data file,
    // so extra CPU overhead to maintain the blocks is not noticeable and
    // savings in CPU time from less over-reads more than compensate
    // for it.
    //
    using block_set_type = std::set<promoted_index_block, block_comparator>;
    block_set_type _blocks;
public:
    const schema& _s;
    metrics& _metrics;
    const pi_index_type _blocks_count;
    const io_priority_class _pc;
    cached_file _cached_file;
    data_consumer::primitive_consumer _primitive_parser;
    clustering_parser _clustering_parser;
    promoted_index_block_parser _block_parser;
    cached_file::stream _stream;
private:
    // Feeds the stream into the consumer until the consumer is satisfied.
    // Does not give unconsumed data back to the stream.
    template <typename Consumer>
    static future<> consume_stream(cached_file::stream& s, Consumer& c) {
        return repeat([&] {
            return s.next().then([&] (temporary_buffer<char>&& buf) {
                if (buf.empty()) {
                    on_internal_error(sstlog, "End of stream while parsing");
                }
                return stop_iteration(c.consume(buf) == data_consumer::read_status::ready);
            });
        });
    }

    // Returns offset of the entry in the offset map for the promoted index block of the index idx.
    // The offset is relative to the promoted index start in the index file.
    // idx must be in the range 0..(_blocks_count-1)
    pi_offset_type get_offset_entry_pos(pi_index_type idx) const {
        return _cached_file.size() - (_blocks_count - idx) * sizeof(pi_offset_type);
    }

    future<pi_offset_type> read_block_offset(pi_index_type idx, tracing::trace_state_ptr trace_state) {
        _stream = _cached_file.read(get_offset_entry_pos(idx), _pc, trace_state);
        return _stream.next().then([this, idx] (temporary_buffer<char>&& buf) {
            if (__builtin_expect(_primitive_parser.read_32(buf) == data_consumer::read_status::ready, true)) {
                return make_ready_future<pi_offset_type>(_primitive_parser._u32);
            }
            return consume_stream(_stream, _primitive_parser).then([this] {
                return _primitive_parser._u32;
            });
        });
    }

    // Postconditions:
    //   - block.start is engaged and valid.
    future<> read_block_start(promoted_index_block& block, tracing::trace_state_ptr trace_state) {
        _stream = _cached_file.read(block.offset, _pc, trace_state);
        _clustering_parser.reset();
        return consume_stream(_stream, _clustering_parser).then([this, &block] {
            auto mem_before = block.memory_usage();
            block.start.emplace(_clustering_parser.get_and_reset());
            _metrics.used_bytes += block.memory_usage() - mem_before;
        });
    }

    // Postconditions:
    //   - block.end is engaged, all fields in the block are valid
    future<> read_block(promoted_index_block& block, tracing::trace_state_ptr trace_state) {
        _stream = _cached_file.read(block.offset, _pc, trace_state);
        _block_parser.reset();
        return consume_stream(_stream, _block_parser).then([this, &block] {
            auto mem_before = block.memory_usage();
            block.start.emplace(std::move(_block_parser.start()));
            block.end.emplace(std::move(_block_parser.end()));
            block.end_open_marker = _block_parser.end_open_marker();
            block.data_file_offset = _block_parser.offset();
            block.width = _block_parser.width();
            _metrics.used_bytes += block.memory_usage() - mem_before;
        });
    }

    /// \brief Returns a pointer to promoted_index_block entry which has at least offset and index fields valid.
    future<promoted_index_block*> get_block_only_offset(pi_index_type idx, tracing::trace_state_ptr trace_state) {
        auto i = _blocks.lower_bound(idx);
        if (i != _blocks.end() && i->index == idx) {
            ++_metrics.hits_l0;
            return make_ready_future<promoted_index_block*>(const_cast<promoted_index_block*>(&*i));
        }
        ++_metrics.misses_l0;
        return read_block_offset(idx, trace_state).then([this, idx, hint = i] (pi_offset_type offset) {
            auto i = this->_blocks.emplace_hint(hint, idx, offset);
            _metrics.used_bytes += sizeof(promoted_index_block);
            ++_metrics.block_count;
            ++_metrics.populations;
            return const_cast<promoted_index_block*>(&*i);
        });
    }

    void erase_range(block_set_type::iterator begin, block_set_type::iterator end) {
        while (begin != end) {
            --_metrics.block_count;
            ++_metrics.evictions;
            _metrics.used_bytes -= begin->memory_usage();
            begin = _blocks.erase(begin);
        }
    }
public:
    cached_promoted_index(const schema& s,
            metrics& m,
            reader_permit permit,
            column_values_fixed_lengths cvfl,
            cached_file f,
            io_priority_class pc,
            pi_index_type blocks_count)
        : _blocks(block_comparator{s})
        , _s(s)
        , _metrics(m)
        , _blocks_count(blocks_count)
        , _pc(pc)
        , _cached_file(std::move(f))
        , _primitive_parser(permit)
        , _clustering_parser(s, permit, cvfl, true)
        , _block_parser(s, std::move(permit), std::move(cvfl))
    { }

    ~cached_promoted_index() {
        _metrics.block_count -= _blocks.size();
        _metrics.evictions += _blocks.size();
        for (auto&& b : _blocks) {
            _metrics.used_bytes -= b.memory_usage();
        }
    }

    /// \brief Returns a pointer to promoted_index_block entry which has at least offset, index and start fields valid.
    future<promoted_index_block*> get_block_with_start(pi_index_type idx, tracing::trace_state_ptr trace_state) {
        return get_block_only_offset(idx, trace_state).then([this, trace_state] (promoted_index_block* block) {
            if (block->start) {
                ++_metrics.hits_l1;
                return make_ready_future<promoted_index_block*>(block);
            }
            ++_metrics.misses_l1;
            return read_block_start(*block, trace_state).then([block] { return block; });
        });
    }

    /// \brief Returns a pointer to promoted_index_block entry which has all the fields valid.
    future<promoted_index_block*> get_block(pi_index_type idx, tracing::trace_state_ptr trace_state) {
        return get_block_only_offset(idx, trace_state).then([this, trace_state] (promoted_index_block* block) {
            if (block->end) {
                ++_metrics.hits_l2;
                return make_ready_future<promoted_index_block*>(block);
            }
            ++_metrics.misses_l2;
            return read_block(*block, trace_state).then([block] { return block; });
        });
    }

    /// \brief Returns a data file offset into the partition such that all fragments
    /// that follow have strictly greater positions than pos.
    ///
    /// The returned information can be useful in determining the I/O boundary for read-ahead.
    ///
    /// \note There may be still elements with positions > pos before the returned position,
    /// so this does not return an exact upper bound.
    ///
    /// Resolving with std::nullopt means the position is not known. The caller should
    /// use the end of the partition as the upper bound.
    future<std::optional<uint64_t>> upper_bound_cache_only(position_in_partition_view pos, tracing::trace_state_ptr trace_state) {
        auto i = _blocks.upper_bound(pos);
        if (i == _blocks.end()) {
            return make_ready_future<std::optional<uint64_t>>(std::nullopt);
        }
        auto& block = const_cast<promoted_index_block&>(*i);
        if (!block.end) {
            return read_block(block, trace_state).then([this, &block] {
                return make_ready_future<std::optional<uint64_t>>(block.data_file_offset);
            });
        }
        return make_ready_future<std::optional<uint64_t>>(block.data_file_offset);
    }

    // Invalidates information about blocks with smaller indexes than a given block.
    void invalidate_prior(promoted_index_block* block, tracing::trace_state_ptr trace_state) {
        _cached_file.invalidate_at_most_front(block->offset, trace_state);
        _cached_file.invalidate_at_most(get_offset_entry_pos(0), get_offset_entry_pos(block->index), trace_state);
        erase_range(_blocks.begin(), _blocks.lower_bound(block->index));
    }

    cached_file& file() { return _cached_file; }
};

/// Cursor implementation which does binary search over index entries.
///
/// Memory consumption: O(log(N))
///
/// Worst-case lookup cost:
///
///    comparisons: O(log(N))
///    I/O:         O(log(N))
///
/// N = number of index entries
///
class bsearch_clustered_cursor : public clustered_index_cursor {
    using pi_offset_type = cached_promoted_index::pi_offset_type;
    using pi_index_type = cached_promoted_index::pi_index_type;
    using promoted_index_block = cached_promoted_index::promoted_index_block;

    const schema& _s;
    const pi_index_type _blocks_count;
    cached_promoted_index _promoted_index;

    // Points to the block whose start is greater than the position of the cursor (its upper bound).
    pi_index_type _current_idx = 0;

    // Used internally by advance_to_upper_bound() to avoid allocating state.
    pi_index_type _upper_idx;

    // Points to the upper bound of the cursor.
    std::optional<position_in_partition> _current_pos;

    tracing::trace_state_ptr _trace_state;
private:
    // Advances the cursor to the nearest block whose start position is > pos.
    //
    // upper_idx should be the index of the block which is known to have start position > pos.
    // upper_idx can be set to _blocks_count if no such entry is known.
    //
    // Async calls must be serialized.
    future<> advance_to_upper_bound(position_in_partition_view pos) {
        // Binary search over blocks.
        //
        // Post conditions:
        //
        //   pos < get_block_start(_current_idx)
        //   For each i < _current_idx: pos >= get_block_start(i)
        //
        // Invariants:
        //
        //   pos < get_block_start(_upper_idx) [*]
        //   pos >= get_block_start(_current_idx)
        //
        // [*] Assuming get_block_start(_blocks_count) == position_in_partition::after_all_clustered_rows().
        //
        //     get_block_start(x) = *_promoted_index.get_block_with_start(x).start
        //
        // Eventually _current_idx will reach _upper_idx.

        _upper_idx = _blocks_count;
        return repeat([this, pos] {
            if (_current_idx >= _upper_idx) {
                if (_current_idx == _blocks_count) {
                    _current_pos = position_in_partition::after_all_clustered_rows();
                }
                tracing::trace(_trace_state, "mc_bsearch_clustered_cursor: bisecting done, current=[{}] .start={}", _current_idx, _current_pos);
                sstlog.trace("mc_bsearch_clustered_cursor {}: bisecting done, current=[{}] .start={}", fmt::ptr(this), _current_idx, _current_pos);
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }

            auto mid = _current_idx + (_upper_idx - _current_idx) / 2;
            tracing::trace(_trace_state, "mc_bsearch_clustered_cursor: bisecting range [{}, {}], mid={}", _current_idx, _upper_idx, mid);
            sstlog.trace("mc_bsearch_clustered_cursor {}: bisecting range [{}, {}], mid={}", fmt::ptr(this), _current_idx, _upper_idx, mid);
            return _promoted_index.get_block_with_start(mid, _trace_state).then([this, mid, pos] (promoted_index_block* block) {
                position_in_partition::less_compare less(_s);
                sstlog.trace("mc_bsearch_clustered_cursor {}: compare with [{}] .start={}", fmt::ptr(this), mid, block->start);
                if (less(pos, *block->start)) {
                    // Eventually _current_idx will reach _upper_idx, so _current_pos only needs to be
                    // updated whenever _upper_idx changes.
                    _current_pos = *block->start;
                    _upper_idx = mid;
                } else {
                    _current_idx = mid + 1;
                }
                return stop_iteration::no;
            });
        });
    }
public:
    bsearch_clustered_cursor(const schema& s,
            cached_promoted_index::metrics& metrics,
            reader_permit permit,
            column_values_fixed_lengths cvfl,
            cached_file f,
            io_priority_class pc,
            pi_index_type blocks_count,
            tracing::trace_state_ptr trace_state)
        : _s(s)
        , _blocks_count(blocks_count)
        , _promoted_index(s, metrics, std::move(permit), std::move(cvfl), std::move(f), pc, blocks_count)
        , _trace_state(std::move(trace_state))
    { }

    future<std::optional<skip_info>> advance_to(position_in_partition_view pos) override {
        position_in_partition::less_compare less(_s);

        sstlog.trace("mc_bsearch_clustered_cursor {}: advance_to({}), _current_pos={}, _current_idx={}, cached={}",
            fmt::ptr(this), pos, _current_pos, _current_idx, _promoted_index.file().cached_bytes());

        if (_current_pos) {
            if (less(pos, *_current_pos)) {
                sstlog.trace("mc_bsearch_clustered_cursor {}: same block", fmt::ptr(this));
                return make_ready_future<std::optional<skip_info>>(std::nullopt);
            }
            ++_current_idx;
        }

        return advance_to_upper_bound(pos).then([this] {
            if (_current_idx == 0) {
                sstlog.trace("mc_bsearch_clustered_cursor {}: same block", fmt::ptr(this));
                return make_ready_future<std::optional<skip_info>>(std::nullopt);
            }
            return _promoted_index.get_block(_current_idx - 1, _trace_state).then([this] (promoted_index_block* block) {
                sstlog.trace("mc_bsearch_clustered_cursor {}: [{}] = {}", fmt::ptr(this), _current_idx - 1, *block);
                offset_in_partition datafile_offset = block->data_file_offset;
                sstlog.trace("mc_bsearch_clustered_cursor {}: datafile_offset={}", fmt::ptr(this), datafile_offset);
                if (_current_idx < 2) {
                    return make_ready_future<std::optional<skip_info>>(
                        skip_info{datafile_offset, tombstone(), position_in_partition::before_all_clustered_rows()});
                }
                // _current_idx points to the block whose start is > than the cursor (upper bound).
                // The cursor is in _current_idx - 1. We will tell the data file reader to skip to
                // the beginning of _current_idx - 1. The index block contains tombstone which was
                // active at the end of the block, not at the beginning of the block, so we need
                // to read the active tombstone from the preceding block, _current_idx - 2.
                return _promoted_index.get_block(_current_idx - 2, _trace_state)
                        .then([this, datafile_offset] (promoted_index_block* block) -> std::optional<skip_info> {
                    sstlog.trace("mc_bsearch_clustered_cursor {}: [{}] = {}", fmt::ptr(this), _current_idx - 2, *block);
                    // XXX: Until we have automatic eviction, we need to invalidate cached index blocks
                    // as we walk so that memory footprint is not O(N) but O(log(N)).
                    _promoted_index.invalidate_prior(block, _trace_state);
                    if (!block->end_open_marker) {
                        return skip_info{datafile_offset, tombstone(), position_in_partition::before_all_clustered_rows()};
                    }
                    auto tomb = tombstone(*block->end_open_marker);
                    sstlog.trace("mc_bsearch_clustered_cursor {}: tombstone={}, pos={}", fmt::ptr(this), tomb, *block->end);
                    return skip_info{datafile_offset, tomb, *block->end};
                });
            });
        });
    }

    future<std::optional<offset_in_partition>> probe_upper_bound(position_in_partition_view pos) override {
        return _promoted_index.upper_bound_cache_only(pos, _trace_state);
    }

    future<std::optional<entry_info>> next_entry() override {
        if (_current_idx == _blocks_count) {
            return make_ready_future<std::optional<entry_info>>(std::nullopt);
        }
        return _promoted_index.get_block(_current_idx, _trace_state)
                .then([this] (promoted_index_block* block) -> std::optional<entry_info> {
            sstlog.trace("mc_bsearch_clustered_cursor {}: block {}: start={}, end={}, offset={}", fmt::ptr(this), _current_idx,
                *block->start, *block->end, block->data_file_offset);
            ++_current_idx;
            return entry_info{*block->start, *block->end, block->data_file_offset};
        });
    }

    future<> close() noexcept override {
        return make_ready_future<>();
    }
};

}
